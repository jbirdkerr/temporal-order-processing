// internal/domain/inventory.go

package domain

import "time"

type Inventory struct {
	SKU            string     `json:"sku"`
	ReservedAmount int        `json:"reserved_amount"`
	ReservedUntil  time.Time  `json:"reserved_until"`
	ReleaseReason  string     `json:"release_reason,omitempty"`
	ReleasedAt     *time.Time `json:"released_at,omitempty"`
}
