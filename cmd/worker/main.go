// cmd/worker/main.go

package main

import (
	"log"

	"gitlab.com/jbirdkerr/temporal-order-processing/internal/activity"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/observability"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Initialize observability
	observability.InitMetrics()
	observability.InitTracer("temporal-order-processing")

	// Connect to Temporal
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
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
	err = w.Start()
	if err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
	defer w.Stop()

	log.Println("Worker started, listening on task queue: order-processing")
	select {} // Block forever
}
