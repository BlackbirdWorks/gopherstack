package ocinotifications

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateTopic       = "CreateTopic"
	opGetTopic          = "GetTopic"
	opListTopics        = "ListTopics"
	opUpdateTopic       = "UpdateTopic"
	opDeleteTopic       = "DeleteTopic"
	opChangeTopicComp   = "ChangeTopicCompartment"
	opPublishMessage    = "PublishMessage"
	opCreateSub         = "CreateSubscription"
	opGetSub            = "GetSubscription"
	opListSubs          = "ListSubscriptions"
	opUpdateSub         = "UpdateSubscription"
	opDeleteSub         = "DeleteSubscription"
	opChangeSubComp     = "ChangeSubscriptionCompartment"
	opGetConfirmSub     = "GetConfirmSubscription"
	opGetUnsubscription = "GetUnsubscription"
	opResendConfirm     = "ResendSubscriptionConfirmation"
	opAdminKeys         = "ManageAPIKeys"
	opAdminDeliveries   = "ListDeliveries"
	opUnknown           = "Unknown"
	adminPrefix         = "_gopherstack"
	adminDeliveriesPath = "/_gopherstack/oci/deliveries"
	topicPath           = apiBase + "/topics/{topicId}"
	subPath             = apiBase + "/subscriptions/{subscriptionId}"
	changeCompartment   = "/actions/changeCompartment"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: apiBase + "/topics", Op: opCreateTopic},
	{Method: http.MethodGet, Path: apiBase + "/topics", Op: opListTopics},
	{Method: http.MethodGet, Path: topicPath, Op: opGetTopic},
	{Method: http.MethodPut, Path: topicPath, Op: opUpdateTopic},
	{Method: http.MethodDelete, Path: topicPath, Op: opDeleteTopic},
	{Method: http.MethodPost, Path: topicPath + changeCompartment, Op: opChangeTopicComp},
	{Method: http.MethodPost, Path: topicPath + "/messages", Op: opPublishMessage},
	{Method: http.MethodPost, Path: apiBase + "/subscriptions", Op: opCreateSub},
	{Method: http.MethodGet, Path: apiBase + "/subscriptions", Op: opListSubs},
	{Method: http.MethodGet, Path: subPath, Op: opGetSub},
	{Method: http.MethodPut, Path: subPath, Op: opUpdateSub},
	{Method: http.MethodDelete, Path: subPath, Op: opDeleteSub},
	{Method: http.MethodPost, Path: subPath + changeCompartment, Op: opChangeSubComp},
	{Method: http.MethodGet, Path: subPath + "/confirmation", Op: opGetConfirmSub},
	{Method: http.MethodGet, Path: subPath + "/unsubscription", Op: opGetUnsubscription},
	{Method: http.MethodPost, Path: subPath + "/resendConfirmation", Op: opResendConfirm},
})

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if path == adminDeliveriesPath {
		return route{op: opAdminDeliveries}
	}

	if strings.HasPrefix(path, "/"+adminPrefix+"/") {
		return route{op: opAdminKeys}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
