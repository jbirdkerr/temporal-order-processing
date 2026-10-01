// cmd/worker/main.go

package main

import (
	"context"
	"log"
	"os"

	"github.com/jbirdkerr/temporal-order-processing/internal/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
	"github.com/jbirdkerr/temporal-order-processing/internal/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Initialize observability
	observability.InitMetrics()

	tracerShutdown, err := observability.InitTracer("order-worker")
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

	// Create worker
	w := worker.New(c, "order-processing", worker.Options{})

	// Register workflows
	w.RegisterWorkflow(workflow.OrderWorkflow)

	// Register activities
	w.RegisterActivity(activity.CheckFraudActivity)
	w.RegisterActivity(activity.ProcessPaymentActivity)
	w.RegisterActivity(activity.RefundPaymentActivity)
	w.RegisterActivity(activity.ReserveInventoryActivity)
	w.RegisterActivity(activity.ReleaseInventoryActivity)
	w.RegisterActivity(activity.ShipOrderActivity)
	w.RegisterActivity(activity.ConfirmDeliveryActivity)
	w.RegisterActivity(activity.SendNotificationActivity)

	// Start worker
	if err = w.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
	defer w.Stop()

	log.Printf("Worker started, listening on task queue: order-processing")
	select {} // Block forever
}
