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
