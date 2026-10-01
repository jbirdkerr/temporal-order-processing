package activity_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	act "github.com/jbirdkerr/temporal-order-processing/internal/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
)

// testOrder is a shared fixture used across activity tests.
var testOrder = &domain.Order{
	ID:          "ORD-TEST-001",
	CustomerID:  "CUST-001",
	TotalAmount: 150.00,
	Items: []domain.OrderItem{
		{SKU: "SKU-1", Name: "Widget", Quantity: 2, Price: 75.00},
	},
	ShippingAddress: domain.Address{
		Street:  "123 Main St",
		City:    "Portland",
		State:   "OR",
		ZipCode: "97201",
		Country: "USA",
	},
}

// newActivityEnv returns a WorkflowTestSuite environment pre-configured for
// activity testing. Each call starts a fresh environment.
func newActivityEnv(t *testing.T) *testsuite.TestActivityEnvironment {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	return env
}

// ── Fraud Check ──────────────────────────────────────────────────────────────

func TestCheckFraudActivity_LowAmount(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.CheckFraudActivity)

	order := *testOrder // copy; keep TotalAmount = 150
	val, err := env.ExecuteActivity(act.CheckFraudActivity, &order)
	require.NoError(t, err)

	var result act.FraudCheckResult
	require.NoError(t, val.Get(&result))

	// Low amount means low risk — should not be flagged as fraudulent.
	assert.False(t, result.IsFraudulent, "low-value order should pass fraud check")
	assert.Less(t, result.RiskScore, 0.7, "risk score should be below threshold")
}

func TestCheckFraudActivity_HighAmount_MayFlag(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.CheckFraudActivity)

	order := *testOrder
	order.TotalAmount = 15000 // triggers both risk-score bumps; riskScore ≥ 0.7

	val, err := env.ExecuteActivity(act.CheckFraudActivity, &order)
	require.NoError(t, err)

	var result act.FraudCheckResult
	require.NoError(t, val.Get(&result))

	// At $15k the base risk score is 0.2 + 0.3 + 0.2 = 0.7, so it should
	// always be flagged (rand adds a positive delta).
	assert.True(t, result.IsFraudulent, "high-value order should be flagged as fraudulent")
	assert.GreaterOrEqual(t, result.RiskScore, 0.7)
}

// ── Payment ──────────────────────────────────────────────────────────────────

func TestProcessPaymentActivity_Returns(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.ProcessPaymentActivity)

	val, err := env.ExecuteActivity(act.ProcessPaymentActivity, testOrder)
	require.NoError(t, err)

	var result act.PaymentResult
	require.NoError(t, val.Get(&result))

	// The activity never returns an error; only result.Success varies.
	// We can't control rand in the test env, so just assert the shape is valid.
	if result.Success {
		assert.NotEmpty(t, result.PaymentID, "successful payment must have a payment ID")
		assert.Empty(t, result.FailureReason)
	} else {
		assert.Empty(t, result.PaymentID)
		assert.NotEmpty(t, result.FailureReason)
	}
}

func TestRefundPaymentActivity(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.RefundPaymentActivity)

	// RefundPaymentActivity returns only error (no result value).
	// ExecuteActivity returns (nil, err) for void activities.
	_, err := env.ExecuteActivity(act.RefundPaymentActivity, "PAY-TEST-001")
	assert.NoError(t, err)
}

// ── Inventory ─────────────────────────────────────────────────────────────────

func TestReserveInventoryActivity_Returns(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.ReserveInventoryActivity)

	val, err := env.ExecuteActivity(act.ReserveInventoryActivity, testOrder)
	// The activity may return an error (5% chance of out-of-stock); when it
	// does we simply skip further assertions — the shape of the error path is
	// tested indirectly through the workflow saga test.
	if err != nil {
		t.Skipf("inventory randomly unavailable, skipping: %v", err)
	}

	var result act.InventoryResult
	require.NoError(t, val.Get(&result))
	assert.True(t, result.Success)
	assert.False(t, result.ReserveExpiration.IsZero(), "expiration must be set")
}

func TestReleaseInventoryActivity(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.ReleaseInventoryActivity)

	_, err := env.ExecuteActivity(act.ReleaseInventoryActivity, testOrder)
	assert.NoError(t, err)
}

// ── Shipping ──────────────────────────────────────────────────────────────────

func TestShipOrderActivity_Returns(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.ShipOrderActivity)

	val, err := env.ExecuteActivity(act.ShipOrderActivity, testOrder)
	if err != nil {
		t.Skipf("carrier API randomly unavailable, skipping: %v", err)
	}

	var result act.ShipmentResult
	require.NoError(t, val.Get(&result))
	assert.NotEmpty(t, result.ShipmentID)
	assert.NotEmpty(t, result.TrackingID)
	assert.NotEmpty(t, result.Carrier)
	assert.False(t, result.EstimatedDelivery.IsZero())
}

func TestConfirmDeliveryActivity(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.ConfirmDeliveryActivity)

	val, err := env.ExecuteActivity(act.ConfirmDeliveryActivity, "TRK-123456")
	require.NoError(t, err)

	var result act.DeliveryResult
	require.NoError(t, val.Get(&result))
	// Result shape is valid regardless of the delivered bool.
	if result.Delivered {
		assert.False(t, result.DeliveredAt.IsZero())
	}
}

// ── Notification ──────────────────────────────────────────────────────────────

func TestSendNotificationActivity_Delivered(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.SendNotificationActivity)

	wfResult := &domain.WorkflowResult{
		OrderID: testOrder.ID,
		Status:  domain.OrderDelivered,
	}

	_, err := env.ExecuteActivity(act.SendNotificationActivity, testOrder, wfResult)
	assert.NoError(t, err)
}

func TestSendNotificationActivity_Failed(t *testing.T) {
	env := newActivityEnv(t)
	env.RegisterActivity(act.SendNotificationActivity)

	wfResult := &domain.WorkflowResult{
		OrderID:       testOrder.ID,
		Status:        domain.OrderFailed,
		FailureReason: "Payment declined",
	}

	_, err := env.ExecuteActivity(act.SendNotificationActivity, testOrder, wfResult)
	assert.NoError(t, err)
}

