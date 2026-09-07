// internal/activity/fraud.go

package activity

import (
	"context"
	"math/rand"
	"time"

	"gitlab.com/jbirdkerr/temporal-order-processing/internal/domain"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/observability"
	"go.temporal.io/sdk/activity"
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
