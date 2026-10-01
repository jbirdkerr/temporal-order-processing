// internal/activity/payment.go

package activity

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	"github.com/jbirdkerr/temporal-order-processing/internal/observability"
	"go.temporal.io/sdk/activity"
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
