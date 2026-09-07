// internal/workflow/order.go

package workflow

import (
	"time"

	"gitlab.com/jbirdkerr/temporal-order-processing/internal/activity"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/domain"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// OrderWorkflow orchestrates the entire order processing pipeline
func OrderWorkflow(ctx workflow.Context, order *domain.Order) (*domain.WorkflowResult, error) {
	// Create a logger that can be used within the workflow context
	logger := workflow.GetLogger(ctx)
	logger.Info("Order workflow started", "orderID", order.ID, "amount", order.TotalAmount)

	result := &domain.WorkflowResult{
		OrderID: order.ID,
		Status:  domain.OrderProcessing,
	}

	// ==========================================
	// Step 1: Fraud Check (fast, fail-fast path)
	// ==========================================
	logger.Info("Starting fraud check", "orderID", order.ID)

	fraudCheckCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	})

	var fraudCheckResult *activity.FraudCheckResult
	err := workflow.ExecuteActivity(
		fraudCheckCtx,
		activity.CheckFraudActivity,
		order,
	).Get(ctx, &fraudCheckResult)

	if err != nil {
		logger.Error("Fraud check failed", "orderID", order.ID, "error", err)
		result.Status = domain.OrderFailed
		result.FailureReason = "Fraud check failed: " + err.Error()
		return result, nil // Don't return error; workflow completed (failed)
	}

	if fraudCheckResult.IsFraudulent {
		logger.Warn("Order flagged as fraudulent", "orderID", order.ID, "reason", fraudCheckResult.Reason)
		result.Status = domain.OrderFailed
		result.FailureReason = "Fraud detected: " + fraudCheckResult.Reason
		return result, nil
	}

	logger.Info("Fraud check passed", "orderID", order.ID)

	// ==========================================
	// Step 2: Process Payment (with retries)
	// ==========================================
	logger.Info("Processing payment", "orderID", order.ID, "amount", order.TotalAmount)

	// Exponential backoff: 1s → 2s → 4s → 8s (max 3 attempts)
	paymentCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    8 * time.Second,
			MaximumAttempts:    3,
		},
	})

	var paymentResult *activity.PaymentResult
	err = workflow.ExecuteActivity(
		paymentCtx,
		activity.ProcessPaymentActivity,
		order,
	).Get(ctx, &paymentResult)

	if err != nil {
		logger.Error("Payment failed after retries", "orderID", order.ID, "error", err)
		result.Status = domain.OrderFailed
		result.FailureReason = "Payment processing failed: " + err.Error()
		return result, nil
	}

	if !paymentResult.Success {
		logger.Error("Payment declined", "orderID", order.ID, "reason", paymentResult.FailureReason)
		result.Status = domain.OrderFailed
		result.FailureReason = "Payment declined: " + paymentResult.FailureReason
		return result, nil
	}

	result.PaymentID = paymentResult.PaymentID
	result.Status = domain.OrderPaid
	logger.Info("Payment successful", "orderID", order.ID, "paymentID", paymentResult.PaymentID)

	// ==========================================
	// Step 3: Reserve Inventory (can require approval)
	// ==========================================
	logger.Info("Attempting inventory reservation", "orderID", order.ID)

	inventoryCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1.5,
			MaximumInterval:    5 * time.Second,
			MaximumAttempts:    2,
		},
	})

	var inventoryResult *activity.InventoryResult
	err = workflow.ExecuteActivity(
		inventoryCtx,
		activity.ReserveInventoryActivity,
		order,
	).Get(ctx, &inventoryResult)

	if err != nil {
		logger.Error("Inventory reservation failed", "orderID", order.ID, "error", err)

		// Compensation: Refund payment
		logger.Info("Compensating: Refunding payment", "orderID", order.ID, "paymentID", result.PaymentID)
		compCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
		})
		workflow.ExecuteActivity(compCtx, activity.RefundPaymentActivity, result.PaymentID).Get(ctx, nil)

		result.Status = domain.OrderFailed
		result.FailureReason = "Inventory unavailable: " + err.Error()
		return result, nil
	}

	if inventoryResult.RequiresApproval {
		logger.Info("Inventory hold requires approval", "orderID", order.ID)
		result.Status = domain.OrderInventoryHeld

		// Wait for human approval via signal, with a 24-hour timeout.
		// Use a timer future raced against the signal channel via a selector.
		approvalChan := workflow.GetSignalChannel(ctx, "approveInventory")
		var approvalSignal ApprovalSignal
		approved := false

		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		timerFuture := workflow.NewTimer(timerCtx, 24*time.Hour)

		s := workflow.NewSelector(ctx)
		s.AddFuture(timerFuture, func(f workflow.Future) {
			// Timer fired — approval timed out; approved remains false.
		})
		s.AddReceive(approvalChan, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &approvalSignal)
			approved = approvalSignal.Approved
			cancelTimer() // cancel the timer since we got the signal
		})
		s.Select(ctx)

		if !approved {
			logger.Warn("Inventory approval timed out or denied", "orderID", order.ID)

			// Compensation: release inventory, refund payment
			compCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
			})
			logger.Info("Compensating: Releasing inventory", "orderID", order.ID)
			workflow.ExecuteActivity(compCtx, activity.ReleaseInventoryActivity, order).Get(ctx, nil)

			logger.Info("Compensating: Refunding payment", "orderID", order.ID)
			workflow.ExecuteActivity(compCtx, activity.RefundPaymentActivity, result.PaymentID).Get(ctx, nil)

			result.Status = domain.OrderFailed
			result.FailureReason = "Inventory approval timed out"
			return result, nil
		}

		logger.Info("Inventory approved", "orderID", order.ID)
	}

	result.Status = domain.OrderInventoryHeld
	logger.Info("Inventory reserved", "orderID", order.ID)

	// ==========================================
	// Step 4: Ship Order (with timeout)
	// ==========================================
	logger.Info("Initiating shipment", "orderID", order.ID)

	// ScheduleToCloseTimeout caps the total time across all retry attempts.
	shippingCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: 48 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    4,
		},
	})

	var shipmentResult *activity.ShipmentResult
	err = workflow.ExecuteActivity(
		shippingCtx,
		activity.ShipOrderActivity,
		order,
	).Get(ctx, &shipmentResult)

	if err != nil {
		logger.Error("Shipping failed", "orderID", order.ID, "error", err)

		// Compensation: Release inventory, refund payment
		compCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
		})
		logger.Info("Compensating: Releasing inventory", "orderID", order.ID)
		workflow.ExecuteActivity(compCtx, activity.ReleaseInventoryActivity, order).Get(ctx, nil)

		logger.Info("Compensating: Refunding payment", "orderID", order.ID)
		workflow.ExecuteActivity(compCtx, activity.RefundPaymentActivity, result.PaymentID).Get(ctx, nil)

		result.Status = domain.OrderFailed
		result.FailureReason = "Shipping failed: " + err.Error()
		return result, nil
	}

	result.ShipmentID = shipmentResult.ShipmentID
	result.Status = domain.OrderShipped
	logger.Info("Order shipped", "orderID", order.ID, "shipmentID", shipmentResult.ShipmentID)

	// ==========================================
	// Step 5: Wait for Delivery Confirmation
	// ==========================================
	logger.Info("Waiting for delivery confirmation", "orderID", order.ID)

	// Give delivery 14 days to confirm (with up to 10 polling retries).
	deliveryCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: 14 * 24 * time.Hour,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 10},
	})

	var deliveryResult *activity.DeliveryResult
	err = workflow.ExecuteActivity(
		deliveryCtx,
		activity.ConfirmDeliveryActivity,
		shipmentResult.ShipmentID,
	).Get(ctx, &deliveryResult)

	if err != nil {
		logger.Error("Delivery confirmation failed", "orderID", order.ID, "error", err)
		result.Status = domain.OrderFailed
		result.FailureReason = "Delivery confirmation failed: " + err.Error()
		return result, nil
	}

	result.Status = domain.OrderDelivered
	result.CompletedAt = workflow.Now(ctx)
	logger.Info("Order delivered", "orderID", order.ID)

	// ==========================================
	// Step 6: Send Completion Notification
	// ==========================================
	logger.Info("Sending order completion notification", "orderID", order.ID)

	notifCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	workflow.ExecuteActivity(notifCtx, activity.SendNotificationActivity, order, result).Get(ctx, nil)

	logger.Info("Order workflow completed successfully", "orderID", order.ID)
	return result, nil
}

// ApprovalSignal is used to approve inventory holds
type ApprovalSignal struct {
	OrderID  string `json:"order_id"`
	Approved bool   `json:"approved"`
}
