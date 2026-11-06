package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func notificationRoutes() []restroute.Route {
	const (
		list = "/notifications/{InstanceId}"
		one  = list + "/{NotificationId}"
		push = "/push-notification/{InstanceId}/registrations"
		user = "/users/{InstanceId}/{UserId}/notifications"
	)

	return []restroute.Route{
		{Method: http.MethodPut, Path: list, Op: "CreateNotification"},
		{Method: http.MethodGet, Path: list, Op: "ListNotifications"},
		{Method: http.MethodGet, Path: one, Op: "DescribeNotification"},
		{Method: http.MethodPost, Path: one, Op: "UpdateNotificationContent"},
		{Method: http.MethodDelete, Path: one, Op: "DeleteNotification"},
		{Method: http.MethodGet, Path: user, Op: "ListUserNotifications"},
		{Method: http.MethodPost, Path: user + "/{NotificationId}", Op: "UpdateUserNotificationStatus"},
		{Method: http.MethodPut, Path: push, Op: "CreatePushNotificationRegistration"},
		{Method: http.MethodDelete, Path: push + "/{RegistrationId}", Op: "DeletePushNotificationRegistration"},
	}
}
