package connect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) notificationOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateNotification"] = replay(h, "CreateNotification",
		func(in *CreateNotificationInput) string { return in.ClientToken }, b.CreateNotification)
	ops["DescribeNotification"] = describeOp("NotificationId", "Notification", b.DescribeNotification)
	ops["UpdateNotificationContent"] = typedCtx(b.UpdateNotificationContent)
	ops["DeleteNotification"] = deleteOp("NotificationId", b.DeleteNotification)
	ops["ListNotifications"] = listOp(
		"NotificationSummaryList",
		func(r *restroute.Request, t string, m int32) ([]Notification, string, error) {
			return b.ListNotifications(instOf(r), t, m)
		},
	)
	ops["ListUserNotifications"] = listOp(
		"UserNotifications",
		func(r *restroute.Request, t string, m int32) ([]UserNotificationSummary, string, error) {
			return b.ListUserNotifications(instOf(r), r.Params["UserId"], t, m)
		},
	)
	ops["UpdateUserNotificationStatus"] = typed(b.UpdateUserNotificationStatus)
	ops["CreatePushNotificationRegistration"] = replay(h, "CreatePushNotificationRegistration",
		func(in *CreatePushNotificationRegistrationInput) string { return in.ClientToken },
		b.CreatePushNotificationRegistration)
	ops["DeletePushNotificationRegistration"] = typed(b.DeletePushNotificationRegistration)
}
