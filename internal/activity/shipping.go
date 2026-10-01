// internal/activity/shipping.go

package activity

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
	"go.temporal.io/sdk/activity"
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
