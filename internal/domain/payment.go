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
