// cmd/server/main.go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/domain"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/observability"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/workflow"
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
	log.Printf("Connected to Temporal at %s", temporalHost)
	defer c.Close()

	// Setup REST API
	r := chi.NewRouter()

	// POST /orders - Start order workflow
	r.Post("/orders", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("POST /orders: received request")
		var order domain.Order
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			log.Printf("POST /orders: failed to decode request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if order.ID == "" {
			order.ID = fmt.Sprintf("ORD-%d", time.Now().UnixNano())
		}
		order.CreatedAt = time.Now()

		log.Printf("POST /orders: starting workflow for order_id=%s", order.ID)
		// Start workflow using the request context so cancellation propagates.
		workflowRun, err := c.ExecuteWorkflow(
			r.Context(),
			client.StartWorkflowOptions{
				ID:        order.ID,
				TaskQueue: "order-processing",
			},
			workflow.OrderWorkflow,
			&order,
		)
		if err != nil {
			log.Printf("POST /orders: failed to start workflow for order_id=%s: %v", order.ID, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("POST /orders: workflow started order_id=%s workflow_id=%s run_id=%s", order.ID, workflowRun.GetID(), workflowRun.GetRunID())

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{
			"order_id":    order.ID,
			"workflow_id": workflowRun.GetID(),
		}); err != nil {
			log.Printf("POST /orders: failed to encode response for order_id=%s: %v", order.ID, err)
		}
	})

	// GET /orders/{id} - Check order status
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")
		log.Printf("GET /orders/%s: received request", orderID)

		desc, err := c.DescribeWorkflowExecution(r.Context(), orderID, "")
		if err != nil {
			log.Printf("GET /orders/%s: failed to describe workflow: %v", orderID, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("GET /orders/%s: workflow status=%s", orderID, desc.GetWorkflowExecutionInfo().GetStatus())

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(desc); err != nil {
			log.Printf("GET /orders/%s: failed to encode response: %v", orderID, err)
		}
	})

	// GET /orders/{id}/result - Get workflow result
	r.Get("/orders/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")
		log.Printf("GET /orders/%s/result: received request", orderID)

		workflowRun := c.GetWorkflow(r.Context(), orderID, "")
		var result *domain.WorkflowResult
		if err := workflowRun.Get(r.Context(), &result); err != nil {
			log.Printf("GET /orders/%s/result: failed to get workflow result: %v", orderID, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("GET /orders/%s/result: retrieved successfully", orderID)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Printf("GET /orders/%s/result: failed to encode response: %v", orderID, err)
		}
	})

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}); err != nil {
			log.Printf("GET /health: failed to encode response: %v", err)
		}
	})

	// Metrics
	r.Handle("/metrics", promhttp.Handler())

	log.Printf("REST API listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
