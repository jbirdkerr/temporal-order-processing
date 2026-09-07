// internal/domain/order.go

package domain

import "time"

type Order struct {
	ID              string      `json:"id"`
	CustomerID      string      `json:"customer_id"`
	Items           []OrderItem `json:"items"`
	TotalAmount     float64     `json:"total_amount"`
	ShippingAddress Address     `json:"shipping_address"`
	CreatedAt       time.Time   `json:"created_at"`
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
