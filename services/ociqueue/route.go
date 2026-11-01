package ociqueue

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateQueue       = "CreateQueue"
	opGetQueue          = "GetQueue"
	opListQueues        = "ListQueues"
	opUpdateQueue       = "UpdateQueue"
	opDeleteQueue       = "DeleteQueue"
	opChangeQueueComp   = "ChangeQueueCompartment"
	opPurgeQueue        = "PurgeQueue"
	opGetWorkRequest    = "GetWorkRequest"
	opListWorkRequests  = "ListWorkRequests"
	opListWorkReqErrors = "ListWorkRequestErrors"
	opListWorkReqLogs   = "ListWorkRequestLogs"
	opPutMessages       = "PutMessages"
	opGetMessages       = "GetMessages"
	opUpdateMessage     = "UpdateMessage"
	opUpdateMessages    = "UpdateMessages"
	opDeleteMessage     = "DeleteMessage"
	opDeleteMessages    = "DeleteMessages"
	opGetStats          = "GetStats"
	opListChannels      = "ListChannels"
	opAdminKeys         = "ManageAPIKeys"
	opUnknown           = "Unknown"
	adminPrefix         = "_gopherstack"
	base                = "/20210201"
	queuePath           = base + "/queues/{queueId}"
	workRequestPath     = base + "/workRequests/{workRequestId}"
	messagesPath        = queuePath + "/messages"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/queues", Op: opCreateQueue},
	{Method: http.MethodGet, Path: base + "/queues", Op: opListQueues},
	{Method: http.MethodGet, Path: queuePath, Op: opGetQueue},
	{Method: http.MethodPut, Path: queuePath, Op: opUpdateQueue},
	{Method: http.MethodDelete, Path: queuePath, Op: opDeleteQueue},
	{Method: http.MethodPost, Path: queuePath + "/actions/changeCompartment", Op: opChangeQueueComp},
	{Method: http.MethodPost, Path: queuePath + "/actions/purge", Op: opPurgeQueue},
	{Method: http.MethodGet, Path: base + "/workRequests", Op: opListWorkRequests},
	{Method: http.MethodGet, Path: workRequestPath, Op: opGetWorkRequest},
	{Method: http.MethodGet, Path: workRequestPath + "/errors", Op: opListWorkReqErrors},
	{Method: http.MethodGet, Path: workRequestPath + "/logs", Op: opListWorkReqLogs},
	{Method: http.MethodPost, Path: messagesPath, Op: opPutMessages},
	{Method: http.MethodGet, Path: messagesPath, Op: opGetMessages},
	{Method: http.MethodPut, Path: messagesPath + "/{messageReceipt}", Op: opUpdateMessage},
	{Method: http.MethodDelete, Path: messagesPath + "/{messageReceipt}", Op: opDeleteMessage},
	{Method: http.MethodPost, Path: messagesPath + "/actions/updateMessages", Op: opUpdateMessages},
	{Method: http.MethodPost, Path: messagesPath + "/actions/deleteMessages", Op: opDeleteMessages},
	{Method: http.MethodGet, Path: queuePath + "/stats", Op: opGetStats},
	{Method: http.MethodGet, Path: queuePath + "/channels", Op: opListChannels},
})

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, "/"+adminPrefix+"/") {
		return route{op: opAdminKeys}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
