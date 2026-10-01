// internal/activity/inventory.go

package activity

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
	"go.temporal.io/sdk/activity"
)

type InventoryResult struct {
	Success           bool      `json:"success"`
	RequiresApproval  bool      `json:"requires_approval"`
	ApprovalReason    string    `json:"approval_reason,omitempty"`
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
			Success:           true,
			RequiresApproval:  true,
			ApprovalReason:    "Low stock - manual review required",
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
