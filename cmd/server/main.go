package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gitlab.com/jbirdkerr/temporal-order-processing/internal/domain"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/observability"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/workflow"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

func main() {
	// Initialize observability
	observability.InitMetrics()

	tracerShutdown, err := observability.InitTracer("temporal-order-processing")
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer func() {
		if err := tracerShutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer: %v", err)
		}
	}()

	// Connect to Temporal
	temporalHost := os.Getenv("TEMPORAL_HOST")
	if temporalHost == "" {
		temporalHost = "localhost:7233"
	}
	log.Printf("Connecting to Temporal at %s", temporalHost)

	c, err := client.Dial(client.Options{
		HostPort: temporalHost,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	defer c.Close()

	log.Printf("Connected to Temporal at %s", temporalHost)

	// Setup REST API
	r := chi.NewRouter()

	// POST /orders - Start order workflow
	r.Post("/orders", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("POST /orders: received request")
		var order domain.Order
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			log.Printf("POST /orders: failed to decode request body: %v", err)
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		// The order ID is also the Temporal Workflow ID.
		// It must be supplied by the caller so that duplicate
		// requests for the same order map to the same workflow.
		if order.ID == "" {
			log.Printf("POST /orders: missing order ID")
			http.Error(w, "order ID is required", http.StatusBadRequest)
			return
		}
		order.CreatedAt = time.Now()

		log.Printf(
			"POST /orders: starting workflow for order_id=%s",
			order.ID,
		)

		workflowRun, err := c.ExecuteWorkflow(
			r.Context(),
			client.StartWorkflowOptions{
				// Business order ID == Temporal Workflow ID.
				ID: order.ID,

				TaskQueue: "order-processing",

				// Never allow a completed workflow to be started
				// again with the same Workflow ID.
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			},
			workflow.OrderWorkflow,
			&order,
		)
		if err != nil {
			// A workflow with this ID already exists/runs or has
			// previously completed, depending on the reuse policy.
			var alreadyStartedErr *serviceerror.WorkflowExecutionAlreadyStarted
			if errors.As(err, &alreadyStartedErr) {
				log.Printf(
					"POST /orders: duplicate order_id=%s",
					order.ID,
				)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)

				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":    "order already exists",
					"order_id": order.ID,
				})
				return
			}

			log.Printf(
				"POST /orders: failed to start workflow for order_id=%s: %v",
				order.ID,
				err,
			)

			http.Error(
				w,
				"failed to start workflow: "+err.Error(),
				http.StatusInternalServerError,
			)
			return
		}

		log.Printf(
			"POST /orders: workflow started order_id=%s workflow_id=%s run_id=%s",
			order.ID,
			workflowRun.GetID(),
			workflowRun.GetRunID(),
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"order_id":    order.ID,
			"workflow_id": workflowRun.GetID(),
			"run_id":      workflowRun.GetRunID(),
		}); err != nil {
			log.Printf(
				"POST /orders: failed to encode response for order_id=%s: %v",
				order.ID,
				err,
			)
		}
	})

	// GET /orders/{id} - Check order status
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")

		log.Printf(
			"GET /orders/%s: received request",
			orderID,
		)

		desc, err := c.DescribeWorkflowExecution(
			r.Context(),
			orderID,
			"",
		)
		if err != nil {
			log.Printf(
				"GET /orders/%s: failed to describe workflow: %v",
				orderID,
				err,
			)

			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		log.Printf(
			"GET /orders/%s: workflow status=%s",
			orderID,
			desc.GetWorkflowExecutionInfo().GetStatus(),
		)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(desc); err != nil {
			log.Printf(
				"GET /orders/%s: failed to encode response: %v",
				orderID,
				err,
			)
		}
	})

	// GET /orders/{id}/result - Get workflow result
	r.Get("/orders/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")

		log.Printf(
			"GET /orders/%s/result: received request",
			orderID,
		)

		workflowRun := c.GetWorkflow(
			r.Context(),
			orderID,
			"",
		)

		var result *domain.WorkflowResult
		if err := workflowRun.Get(r.Context(), &result); err != nil {
			log.Printf(
				"GET /orders/%s/result: failed to get workflow result: %v",
				orderID,
				err,
			)

			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf(
			"GET /orders/%s/result: retrieved successfully",
			orderID,
		)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Printf(
				"GET /orders/%s/result: failed to encode response: %v",
				orderID,
				err,
			)
		}
	})

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "healthy",
		}); err != nil {
			log.Printf("GET /health: failed to encode response: %v", err)
		}
	})

	// Metrics
	r.Handle("/metrics", promhttp.Handler())

	log.Printf("REST API listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
