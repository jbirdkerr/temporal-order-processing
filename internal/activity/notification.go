// internal/activity/notification.go

package activity

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/jbirdkerr/temporal-order-processing/internal/domain"
	"gitlab.com/jbirdkerr/temporal-order-processing/internal/observability"
	"go.temporal.io/sdk/activity"
)

func SendNotificationActivity(ctx context.Context, order *domain.Order, result *domain.WorkflowResult) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Sending notification", "orderID", order.ID, "status", result.Status)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}

	message := fmt.Sprintf("Order %s status: %s", order.ID, result.Status)
	logger.Info("Notification sent", "message", message)
	observability.NotificationSent.WithLabelValues(string(result.Status)).Inc()

	return nil
}
