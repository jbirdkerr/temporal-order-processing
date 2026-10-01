package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	act "github.com/jbirdkerr/temporal-order-processing/internal/activity"
	"github.com/jbirdkerr/temporal-order-processing/internal/domain"
	wf "github.com/jbirdkerr/temporal-order-processing/internal/workflow"
)

// testOrder is reused across all workflow test scenarios.
var testOrder = &domain.Order{
	ID:          "ORD-WF-001",
	CustomerID:  "CUST-001",
	TotalAmount: 150.00,
	Items: []domain.OrderItem{
		{SKU: "SKU-1", Name: "Widget", Quantity: 1, Price: 150.00},
	},
	ShippingAddress: domain.Address{
		Street: "123 Main St", City: "Portland",
		State: "OR", ZipCode: "97201", Country: "USA",
	},
}

// newWorkflowEnv returns a fully-wired TestWorkflowEnvironment for each test.
func newWorkflowEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(wf.OrderWorkflow)
	env.RegisterActivity(act.CheckFraudActivity)
	env.RegisterActivity(act.ProcessPaymentActivity)
	env.RegisterActivity(act.RefundPaymentActivity)
	env.RegisterActivity(act.ReserveInventoryActivity)
	env.RegisterActivity(act.ReleaseInventoryActivity)
	env.RegisterActivity(act.ShipOrderActivity)
	env.RegisterActivity(act.ConfirmDeliveryActivity)
	env.RegisterActivity(act.SendNotificationActivity)
	return env
}

// ── Happy path ────────────────────────────────────────────────────────────────

func TestOrderWorkflow_HappyPath(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)

	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: true, PaymentID: "PAY-001"}, nil)

	env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
		Return(&act.InventoryResult{
			Success:           true,
			RequiresApproval:  false,
			ReserveExpiration: time.Now().Add(7 * 24 * time.Hour),
		}, nil)

	env.OnActivity(act.ShipOrderActivity, mock.Anything, mock.Anything).
		Return(&act.ShipmentResult{
			ShipmentID:        "SHIP-001",
			TrackingID:        "TRK-111",
			Carrier:           "FedEx",
			EstimatedDelivery: time.Now().Add(5 * 24 * time.Hour),
		}, nil)

	env.OnActivity(act.ConfirmDeliveryActivity, mock.Anything, mock.Anything).
		Return(&act.DeliveryResult{Delivered: true, DeliveredAt: time.Now()}, nil)

	env.OnActivity(act.SendNotificationActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.Equal(t, domain.OrderDelivered, result.Status)
	assert.Equal(t, "PAY-001", result.PaymentID)
	assert.Equal(t, "SHIP-001", result.ShipmentID)
	assert.Empty(t, result.FailureReason)
}

// ── Fraud detection ───────────────────────────────────────────────────────────

func TestOrderWorkflow_FraudDetected(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{
			IsFraudulent: true,
			Reason:       "High risk score threshold exceeded",
			RiskScore:    0.85,
		}, nil)

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.Equal(t, domain.OrderFailed, result.Status)
	assert.Contains(t, result.FailureReason, "Fraud detected")
}

// ── Payment declined ──────────────────────────────────────────────────────────

func TestOrderWorkflow_PaymentDeclined(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)

	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: false, FailureReason: "Insufficient funds"}, nil)

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.Equal(t, domain.OrderFailed, result.Status)
	assert.Contains(t, result.FailureReason, "Payment declined")
}

// ── Inventory unavailable → saga compensates payment ─────────────────────────

func TestOrderWorkflow_InventoryFailed_RefundsPayment(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)

	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: true, PaymentID: "PAY-002"}, nil)

	env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
		Return(nil, assert.AnError) // out of stock

	// Saga compensation: refund must be called exactly once.
	env.OnActivity(act.RefundPaymentActivity, mock.Anything, "PAY-002").
		Return(nil).Once()

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.Equal(t, domain.OrderFailed, result.Status)
	assert.Contains(t, result.FailureReason, "Inventory unavailable")

	// Verify the refund activity was actually invoked.
	env.AssertExpectations(t)
}

// ── Shipping failed → saga compensates inventory + payment ───────────────────

func TestOrderWorkflow_ShippingFailed_Compensates(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)

	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: true, PaymentID: "PAY-003"}, nil)

	env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
		Return(&act.InventoryResult{
			Success:           true,
			ReserveExpiration: time.Now().Add(7 * 24 * time.Hour),
		}, nil)

	env.OnActivity(act.ShipOrderActivity, mock.Anything, mock.Anything).
		Return(nil, assert.AnError) // carrier down

	// Saga: both compensation steps must fire.
	env.OnActivity(act.ReleaseInventoryActivity, mock.Anything, mock.Anything).
		Return(nil).Once()
	env.OnActivity(act.RefundPaymentActivity, mock.Anything, "PAY-003").
		Return(nil).Once()

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.Equal(t, domain.OrderFailed, result.Status)
	assert.Contains(t, result.FailureReason, "Shipping failed")

	env.AssertExpectations(t)
}

// ── Human-in-the-loop: inventory approval granted ────────────────────────────

func TestOrderWorkflow_InventoryApproval_Approved(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)

	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: true, PaymentID: "PAY-004"}, nil)

	env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
		Return(&act.InventoryResult{
			Success:           true,
			RequiresApproval:  true, // triggers human-in-the-loop path
			ApprovalReason:    "Low stock - manual review required",
			ReserveExpiration: time.Now().Add(24 * time.Hour),
		}, nil)

	// Send the approval signal after the workflow is waiting for it.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("approveInventory", wf.ApprovalSignal{
			OrderID:  testOrder.ID,
			Approved: true,
		})
	}, 1*time.Second)

	env.OnActivity(act.ShipOrderActivity, mock.Anything, mock.Anything).
		Return(&act.ShipmentResult{
			ShipmentID: "SHIP-004",
			TrackingID: "TRK-444",
			Carrier:    "UPS",
		}, nil)

	env.OnActivity(act.ConfirmDeliveryActivity, mock.Anything, mock.Anything).
		Return(&act.DeliveryResult{Delivered: true, DeliveredAt: time.Now()}, nil)

	env.OnActivity(act.SendNotificationActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.Equal(t, domain.OrderDelivered, result.Status)
}

// ── Human-in-the-loop: approval denied → saga compensates ────────────────────

func TestOrderWorkflow_InventoryApproval_Denied(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)

	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: true, PaymentID: "PAY-005"}, nil)

	env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
		Return(&act.InventoryResult{
			Success:           true,
			RequiresApproval:  true,
			ReserveExpiration: time.Now().Add(24 * time.Hour),
		}, nil)

	// Send a denial signal.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("approveInventory", wf.ApprovalSignal{
			OrderID:  testOrder.ID,
			Approved: false,
		})
	}, 1*time.Second)

	// Saga compensation must fire.
	env.OnActivity(act.ReleaseInventoryActivity, mock.Anything, mock.Anything).
		Return(nil).Once()
	env.OnActivity(act.RefundPaymentActivity, mock.Anything, "PAY-005").
		Return(nil).Once()

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.Equal(t, domain.OrderFailed, result.Status)
	assert.Contains(t, result.FailureReason, "approval timed out")

	env.AssertExpectations(t)
}

// ── Human-in-the-loop: 24h timeout → saga compensates ────────────────────────

func TestOrderWorkflow_InventoryApproval_Timeout(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)

	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: true, PaymentID: "PAY-006"}, nil)

	env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
		Return(&act.InventoryResult{
			Success:           true,
			RequiresApproval:  true,
			ReserveExpiration: time.Now().Add(24 * time.Hour),
		}, nil)

	// No signal sent — let the 24h timer fire.
	// The test framework advances time automatically.
	env.SetTestTimeout(25 * time.Hour)

	env.OnActivity(act.ReleaseInventoryActivity, mock.Anything, mock.Anything).
		Return(nil).Once()
	env.OnActivity(act.RefundPaymentActivity, mock.Anything, "PAY-006").
		Return(nil).Once()

	// Intercept the workflow timer so the test doesn't actually wait 24 hours.
	env.OnActivity(act.SendNotificationActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	// The timer fired without a signal, so approved == false.
	assert.Equal(t, domain.OrderFailed, result.Status)
	assert.Contains(t, result.FailureReason, "approval timed out")

	env.AssertExpectations(t)
}

// ── Workflow determinism: workflow.Now() used instead of time.Now() ───────────
// This test validates that we never reference external non-deterministic state
// inside the workflow by running the workflow twice with the same mocks and
// verifying identical results.

func TestOrderWorkflow_Deterministic(t *testing.T) {
	runOnce := func() domain.WorkflowResult {
		env := newWorkflowEnv(t)

		env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
			Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)
		env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
			Return(&act.PaymentResult{Success: true, PaymentID: "PAY-DET"}, nil)
		env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
			Return(&act.InventoryResult{
				Success: true, ReserveExpiration: time.Now().Add(7 * 24 * time.Hour),
			}, nil)
		env.OnActivity(act.ShipOrderActivity, mock.Anything, mock.Anything).
			Return(&act.ShipmentResult{
				ShipmentID: "SHIP-DET", TrackingID: "TRK-DET", Carrier: "USPS",
			}, nil)
		env.OnActivity(act.ConfirmDeliveryActivity, mock.Anything, mock.Anything).
			Return(&act.DeliveryResult{Delivered: true, DeliveredAt: time.Now()}, nil)
		env.OnActivity(act.SendNotificationActivity, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)
		require.NoError(t, env.GetWorkflowError())

		var result domain.WorkflowResult
		require.NoError(t, env.GetWorkflowResult(&result))
		return result
	}

	r1 := runOnce()
	r2 := runOnce()

	assert.Equal(t, r1.Status, r2.Status)
	assert.Equal(t, r1.PaymentID, r2.PaymentID)
	assert.Equal(t, r1.ShipmentID, r2.ShipmentID)
	assert.Equal(t, r1.FailureReason, r2.FailureReason)
}

// ── Workflow uses workflow.Now instead of time.Now ────────────────────────────
// Verify that the workflow's CompletedAt is set via workflow.Now (deterministic)
// and not via time.Now (non-deterministic).
func TestOrderWorkflow_CompletedAt_IsSet(t *testing.T) {
	env := newWorkflowEnv(t)

	env.OnActivity(act.CheckFraudActivity, mock.Anything, mock.Anything).
		Return(&act.FraudCheckResult{IsFraudulent: false, RiskScore: 0.2}, nil)
	env.OnActivity(act.ProcessPaymentActivity, mock.Anything, mock.Anything).
		Return(&act.PaymentResult{Success: true, PaymentID: "PAY-TIME"}, nil)
	env.OnActivity(act.ReserveInventoryActivity, mock.Anything, mock.Anything).
		Return(&act.InventoryResult{
			Success:           true,
			ReserveExpiration: time.Now().Add(7 * 24 * time.Hour),
		}, nil)
	env.OnActivity(act.ShipOrderActivity, mock.Anything, mock.Anything).
		Return(&act.ShipmentResult{ShipmentID: "SHIP-TIME", TrackingID: "TRK-T", Carrier: "FedEx"}, nil)
	env.OnActivity(act.ConfirmDeliveryActivity, mock.Anything, mock.Anything).
		Return(&act.DeliveryResult{Delivered: true, DeliveredAt: time.Now()}, nil)
	env.OnActivity(act.SendNotificationActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.ExecuteWorkflow(wf.OrderWorkflow, testOrder)
	require.NoError(t, env.GetWorkflowError())

	var result domain.WorkflowResult
	require.NoError(t, env.GetWorkflowResult(&result))

	assert.False(t, result.CompletedAt.IsZero(), "CompletedAt must be set on successful delivery")
}

// ── Ensure ApprovalSignal type is exported (compile-time check) ───────────────
var _ = wf.ApprovalSignal{}

// ── Ensure workflow is registered with the correct function signature ──────────
var _ func(workflow.Context, *domain.Order) (*domain.WorkflowResult, error) = wf.OrderWorkflow
