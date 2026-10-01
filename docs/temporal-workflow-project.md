# Temporal Workflow Orchestration: Production-Grade Order Processing

A complete guide to building an observable, resilient order processing system using Temporal, Go, and k3s.

## Project Overview

**The Pitch:** "A production-grade order processing saga with observable failure recovery"

This is your actual working project at `github.com/jbirdkerr/temporal-order-processing`.

---

## Activities Implementation

### Payment Activity

```go
// internal/activity/payment.go

package activity

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"go.temporal.io/sdk/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
)

// PaymentResult represents the result of payment processing
type PaymentResult struct {
	Success       bool   `json:"success"`
	PaymentID     string `json:"payment_id"`
	FailureReason string `json:"failure_reason"`
	AttemptNumber int    `json:"attempt_number"`
}

// ProcessPaymentActivity processes a payment through a payment gateway
func ProcessPaymentActivity(ctx context.Context, order *domain.Order) (*PaymentResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Processing payment", "orderID", order.ID, "amount", order.TotalAmount)

	// Simulate payment gateway call
	select {
	case <-ctx.Done():
		logger.Warn("Payment activity cancelled", "orderID", order.ID)
		return nil, ctx.Err()
	case <-time.After(2 * time.Second):
	}

	// Simulate occasional failures (10% chance)
	if rand.Float64() < 0.1 {
		logger.Error("Payment declined", "orderID", order.ID)
		observability.PaymentsFailed.Inc()
		return &PaymentResult{
			Success:       false,
			FailureReason: "Insufficient funds",
		}, nil
	}

	paymentID := fmt.Sprintf("PAY-%s-%d", order.ID, time.Now().Unix())
	logger.Info("Payment successful", "orderID", order.ID, "paymentID", paymentID)
	observability.PaymentsSucceeded.Inc()

	return &PaymentResult{
		Success:   true,
		PaymentID: paymentID,
	}, nil
}

// RefundPaymentActivity refunds a previously authorized payment
func RefundPaymentActivity(ctx context.Context, paymentID string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Refunding payment", "paymentID", paymentID)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second):
	}

	logger.Info("Payment refunded", "paymentID", paymentID)
	observability.PaymentsRefunded.Inc()
	return nil
}
```

### Fraud Check Activity

```go
// internal/activity/fraud.go

package activity

import (
	"context"
	"math/rand"
	"time"

	"go.temporal.io/sdk/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
)

type FraudCheckResult struct {
	IsFraudulent bool    `json:"is_fraudulent"`
	Reason       string  `json:"reason,omitempty"`
	RiskScore    float64 `json:"risk_score"`
}

// CheckFraudActivity performs fraud detection
func CheckFraudActivity(ctx context.Context, order *domain.Order) (*FraudCheckResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Performing fraud check", "orderID", order.ID, "amount", order.TotalAmount)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}

	// Simulate fraud scoring based on amount
	riskScore := 0.2
	if order.TotalAmount > 5000 {
		riskScore += 0.3
	}
	if order.TotalAmount > 10000 {
		riskScore += 0.2
	}
	riskScore += rand.Float64() * 0.1

	isFraudulent := riskScore > 0.7
	reason := ""

	if isFraudulent {
		reason = "High risk score threshold exceeded"
		observability.FraudFlagged.Inc()
	} else {
		observability.FraudPassed.Inc()
	}

	logger.Info("Fraud check completed", "orderID", order.ID, "riskScore", riskScore, "fraudulent", isFraudulent)

	return &FraudCheckResult{
		IsFraudulent: isFraudulent,
		Reason:       reason,
		RiskScore:    riskScore,
	}, nil
}
```

### Inventory Activity

```go
// internal/activity/inventory.go

package activity

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"go.temporal.io/sdk/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
)

type InventoryResult struct {
	Success          bool      `json:"success"`
	RequiresApproval bool      `json:"requires_approval"`
	ApprovalReason   string    `json:"approval_reason,omitempty"`
	ReserveExpiration time.Time `json:"reserve_expiration"`
}

func ReserveInventoryActivity(ctx context.Context, order *domain.Order) (*InventoryResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Reserving inventory", "orderID", order.ID)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(1 * time.Second):
	}

	// Simulate partial availability (requires approval)
	if rand.Float64() < 0.15 {
		logger.Warn("Inventory low, requires approval", "orderID", order.ID)
		observability.InventoryApprovalRequired.Inc()
		return &InventoryResult{
			Success:          true,
			RequiresApproval: true,
			ApprovalReason:   "Low stock - manual review required",
			ReserveExpiration: time.Now().Add(24 * time.Hour),
		}, nil
	}

	// Simulate out-of-stock scenarios (5% chance)
	if rand.Float64() < 0.05 {
		logger.Error("Inventory unavailable", "orderID", order.ID)
		observability.InventoryFailed.Inc()
		return nil, errors.New("insufficient inventory")
	}

	logger.Info("Inventory reserved", "orderID", order.ID)
	observability.InventoryReserved.Inc()

	return &InventoryResult{
		Success:           true,
		RequiresApproval:  false,
		ReserveExpiration: time.Now().Add(7 * 24 * time.Hour),
	}, nil
}

func ReleaseInventoryActivity(ctx context.Context, order *domain.Order) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Releasing inventory", "orderID", order.ID)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}

	logger.Info("Inventory released", "orderID", order.ID)
	observability.InventoryReleased.Inc()
	return nil
}
```

### Shipping Activity

```go
// internal/activity/shipping.go

package activity

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"go.temporal.io/sdk/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
)

type ShipmentResult struct {
	ShipmentID        string    `json:"shipment_id"`
	TrackingID        string    `json:"tracking_id"`
	Carrier           string    `json:"carrier"`
	EstimatedDelivery time.Time `json:"estimated_delivery"`
}

type DeliveryResult struct {
	Delivered   bool      `json:"delivered"`
	DeliveredAt time.Time `json:"delivered_at"`
}

func ShipOrderActivity(ctx context.Context, order *domain.Order) (*ShipmentResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Creating shipment", "orderID", order.ID)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(2 * time.Second):
	}

	// Simulate carrier API failures (5% chance)
	if rand.Float64() < 0.05 {
		logger.Error("Shipment creation failed", "orderID", order.ID)
		observability.ShippingFailed.Inc()
		return nil, errors.New("carrier API unavailable")
	}

	carriers := []string{"FedEx", "UPS", "USPS"}
	carrier := carriers[rand.Intn(len(carriers))]
	shipmentID := fmt.Sprintf("SHIP-%s-%d", order.ID, time.Now().Unix())
	trackingID := fmt.Sprintf("TRK-%d", rand.Intn(1000000000))

	logger.Info("Shipment created", "orderID", order.ID, "shipmentID", shipmentID, "carrier", carrier)
	observability.ShippingSucceeded.Inc()

	return &ShipmentResult{
		ShipmentID:        shipmentID,
		TrackingID:        trackingID,
		Carrier:           carrier,
		EstimatedDelivery: time.Now().Add(5 * 24 * time.Hour),
	}, nil
}

func ConfirmDeliveryActivity(ctx context.Context, trackingID string) (*DeliveryResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Checking delivery status", "trackingID", trackingID)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(1 * time.Second):
	}

	delivered := rand.Float64() < 0.9 // 90% of checks show delivered

	if !delivered {
		logger.Info("Package still in transit", "trackingID", trackingID)
		return &DeliveryResult{Delivered: false}, nil
	}

	logger.Info("Package delivered", "trackingID", trackingID)
	observability.DeliveryConfirmed.Inc()

	return &DeliveryResult{
		Delivered:   true,
		DeliveredAt: time.Now(),
	}, nil
}
```

### Notification Activity

```go
// internal/activity/notification.go

package activity

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
)

func SendNotificationActivity(ctx context.Context, order *domain.Order, result *domain.WorkflowResult) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Sending notification", "orderID", order.ID, "status", result.Status)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}

	message := fmt.Sprintf("Order %s status: %s", order.ID, result.Status)
	logger.Info("Notification sent", "message", message)
	observability.NotificationSent.WithLabelValues(string(result.Status)).Inc()

	return nil
}
```

---

## Domain Models

```go
// internal/domain/order.go

package domain

import "time"

type Order struct {
	ID              string        `json:"id"`
	CustomerID      string        `json:"customer_id"`
	Items           []OrderItem   `json:"items"`
	TotalAmount     float64       `json:"total_amount"`
	ShippingAddress Address       `json:"shipping_address"`
	CreatedAt       time.Time     `json:"created_at"`
}

type OrderItem struct {
	SKU      string  `json:"sku"`
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type Address struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	State   string `json:"state"`
	ZipCode string `json:"zip_code"`
	Country string `json:"country"`
}

type OrderStatus string

const (
	OrderPending       OrderStatus = "pending"
	OrderProcessing    OrderStatus = "processing"
	OrderPaid          OrderStatus = "paid"
	OrderInventoryHeld OrderStatus = "inventory_held"
	OrderShipped       OrderStatus = "shipped"
	OrderDelivered     OrderStatus = "delivered"
	OrderFailed        OrderStatus = "failed"
	OrderCompensated   OrderStatus = "compensated"
)

type WorkflowResult struct {
	OrderID       string      `json:"order_id"`
	Status        OrderStatus `json:"status"`
	PaymentID     string      `json:"payment_id,omitempty"`
	ShipmentID    string      `json:"shipment_id,omitempty"`
	FailureReason string      `json:"failure_reason,omitempty"`
	CompletedAt   time.Time   `json:"completed_at"`
}
```

```go
// internal/domain/payment.go

package domain

import "time"

type Payment struct {
	ID            string     `json:"id"`
	OrderID       string     `json:"order_id"`
	Amount        float64    `json:"amount"`
	Method        string     `json:"method"`
	Status        string     `json:"status"`
	AuthToken     string     `json:"auth_token"`
	RefundedAt    *time.Time `json:"refunded_at,omitempty"`
	FailureReason string     `json:"failure_reason,omitempty"`
}
```

```go
// internal/domain/inventory.go

package domain

import "time"

type Inventory struct {
	SKU             string     `json:"sku"`
	ReservedAmount  int        `json:"reserved_amount"`
	ReservedUntil   time.Time  `json:"reserved_until"`
	ReleaseReason   string     `json:"release_reason,omitempty"`
	ReleasedAt      *time.Time `json:"released_at,omitempty"`
}
```

---

## REST API Server

```go
// cmd/server/main.go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
	"github.com/jbirdkerr/temporal-order-processing/internal/workflow"
)

func main() {
	// Initialize observability
	observability.InitMetrics()
	observability.InitTracer()

	// Connect to Temporal
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	defer c.Close()

	// Setup REST API
	r := chi.NewRouter()

	// POST /orders - Start order workflow
	r.Post("/orders", func(w http.ResponseWriter, r *http.Request) {
		var order domain.Order
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if order.ID == "" {
			order.ID = fmt.Sprintf("ORD-%d", time.Now().UnixNano())
		}
		order.CreatedAt = time.Now()

		// Start workflow
		workflowRun, err := c.ExecuteWorkflow(
			context.Background(),
			client.StartWorkflowOptions{
				ID:        order.ID,
				TaskQueue: "order-processing",
			},
			workflow.OrderWorkflow,
			&order,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"order_id":    order.ID,
			"workflow_id": workflowRun.GetID(),
		})
	})

	// GET /orders/{id} - Check order status
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")

		workflowRun := c.GetWorkflow(context.Background(), orderID)
		desc, err := workflowRun.Describe(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(desc)
	})

	// GET /orders/{id}/result - Get workflow result
	r.Get("/orders/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "id")

		workflowRun := c.GetWorkflow(context.Background(), orderID)
		var result *domain.WorkflowResult
		err := workflowRun.Get(context.Background(), &result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	// Metrics
	r.Handle("/metrics", promhttp.Handler())

	log.Println("REST API listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
```

---

## Worker Process

```go
// cmd/worker/main.go

package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"github.com/jbirdkerr/temporal-order-processing/internal/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
	"github.com/jbirdkerr/temporal-order-processing/internal/workflow"
)

func main() {
	// Initialize observability
	observability.InitMetrics()
	observability.InitTracer()

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
```

---

## Metrics

```go
// internal/observability/metrics.go

package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Payment metrics
	PaymentsSucceeded = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "payments_succeeded_total",
			Help: "Total successful payments",
		},
	)

	PaymentsFailed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "payments_failed_total",
			Help: "Total failed payments",
		},
	)

	PaymentsRefunded = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "payments_refunded_total",
			Help: "Total refunds issued",
		},
	)

	// Fraud metrics
	FraudPassed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "fraud_checks_passed_total",
			Help: "Total fraud checks passed",
		},
	)

	FraudFlagged = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "fraud_checks_flagged_total",
			Help: "Total fraud checks flagged",
		},
	)

	// Inventory metrics
	InventoryReserved = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "inventory_reserved_total",
			Help: "Total inventory reservations",
		},
	)

	InventoryReleased = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "inventory_released_total",
			Help: "Total inventory releases",
		},
	)

	InventoryApprovalRequired = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "inventory_approval_required_total",
			Help: "Total inventories requiring approval",
		},
	)

	InventoryFailed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "inventory_failed_total",
			Help: "Total inventory failures",
		},
	)

	// Shipping metrics
	ShippingSucceeded = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "shipping_succeeded_total",
			Help: "Total successful shipments",
		},
	)

	ShippingFailed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "shipping_failed_total",
			Help: "Total shipping failures",
		},
	)

	DeliveryConfirmed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "deliveries_confirmed_total",
			Help: "Total confirmed deliveries",
		},
	)

	// Notification metrics
	NotificationSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Total notifications sent",
		},
		[]string{"status"},
	)
)

func InitMetrics() {
	// Metrics are auto-registered via promauto
}
```

---

## Docker Compose (Local Development)

```yaml
# docker-compose.yml

version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: temporal
      POSTGRES_USER: temporal
      POSTGRES_PASSWORD: temporal
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "temporal"]
      interval: 10s
      timeout: 5s
      retries: 5

  temporal:
    image: temporalio/server:1.21.0
    environment:
      DB: postgres
      DB_PORT: 5432
      DB_USERNAME: temporal
      DB_PASSWORD: temporal
      DB_NAME: temporal
      SERVICES: "frontend,history,matching,worker"
    ports:
      - "7233:7233"
      - "8233:8233"
    depends_on:
      postgres:
        condition: service_healthy

  temporal-ui:
    image: temporalio/ui:2.27.0
    ports:
      - "8080:8080"
    environment:
      TEMPORAL_ADDRESS: temporal:7233
    depends_on:
      - temporal

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    depends_on:
      - prometheus
```

---

## Quick Start

```bash
# Start Temporal + dependencies
docker-compose up

# In another terminal, start the worker
go run cmd/worker/main.go

# In another terminal, start the REST API
go run cmd/server/main.go

# Test it
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "cust-123",
    "items": [{"sku": "sku-1", "name": "Widget", "quantity": 1, "price": 100}],
    "total_amount": 100,
    "shipping_address": {
      "street": "123 Main St",
      "city": "Portland",
      "state": "OR",
      "zip_code": "97201",
      "country": "USA"
    }
  }'

# Check order status
curl http://localhost:8080/orders/ORD-XXXXX

# View Temporal UI
open http://localhost:8080

# View Prometheus
open http://localhost:9090

# View Grafana
open http://localhost:3000
```

---

## Next: Get This Running

1. Copy the activity code into your `internal/activity/` files
2. Copy the domain code into your `internal/domain/` files
3. Update your `cmd/server/main.go` and `cmd/worker/main.go`
4. Create `docker-compose.yml` in your project root
5. Run `go mod tidy`
6. Run `docker-compose up`
7. In another terminal: `go run cmd/worker/main.go`
8. In another terminal: `go run cmd/server/main.go`
9. Test with curl

You're building the real thing now. Let me know when you hit issues!