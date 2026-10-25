package ocistreaming

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateStreamPool    = "CreateStreamPool"
	opGetStreamPool       = "GetStreamPool"
	opListStreamPools     = "ListStreamPools"
	opUpdateStreamPool    = "UpdateStreamPool"
	opDeleteStreamPool    = "DeleteStreamPool"
	opChangePoolComp      = "ChangeStreamPoolCompartment"
	opCreateStream        = "CreateStream"
	opGetStream           = "GetStream"
	opListStreams         = "ListStreams"
	opUpdateStream        = "UpdateStream"
	opDeleteStream        = "DeleteStream"
	opChangeStreamComp    = "ChangeStreamCompartment"
	opCreateHarness       = "CreateConnectHarness"
	opGetHarness          = "GetConnectHarness"
	opListHarnesses       = "ListConnectHarnesses"
	opUpdateHarness       = "UpdateConnectHarness"
	opDeleteHarness       = "DeleteConnectHarness"
	opChangeHarnessComp   = "ChangeConnectHarnessCompartment"
	opGetWorkRequest      = "GetWorkRequest"
	opListWorkRequests    = "ListWorkRequests"
	opListWorkReqErrors   = "ListWorkRequestErrors"
	opListWorkReqLogs     = "ListWorkRequestLogs"
	opPutMessages         = "PutMessages"
	opGetMessages         = "GetMessages"
	opCreateCursor        = "CreateCursor"
	opCreateGroupCursor   = "CreateGroupCursor"
	opGetGroup            = "GetGroup"
	opUpdateGroup         = "UpdateGroup"
	opConsumerCommit      = "ConsumerCommit"
	opConsumerHeartbeat   = "ConsumerHeartbeat"
	opAdminKeys           = "ManageAPIKeys"
	opUnknown             = "Unknown"
	adminPrefix           = "_gopherstack"
	base                  = "/20180418"
	streamPath            = base + "/streams/{streamId}"
	poolPath              = base + "/streampools/{streamPoolId}"
	harnessPath           = base + "/connectharnesses/{connectHarnessId}"
	workRequestPath       = base + "/workRequests/{workRequestId}"
	changeCompartmentPart = "/actions/changeCompartment"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/streampools", Op: opCreateStreamPool},
	{Method: http.MethodGet, Path: base + "/streampools", Op: opListStreamPools},
	{Method: http.MethodGet, Path: poolPath, Op: opGetStreamPool},
	{Method: http.MethodPut, Path: poolPath, Op: opUpdateStreamPool},
	{Method: http.MethodDelete, Path: poolPath, Op: opDeleteStreamPool},
	{Method: http.MethodPost, Path: poolPath + changeCompartmentPart, Op: opChangePoolComp},
	{Method: http.MethodPost, Path: base + "/streams", Op: opCreateStream},
	{Method: http.MethodGet, Path: base + "/streams", Op: opListStreams},
	{Method: http.MethodGet, Path: streamPath, Op: opGetStream},
	{Method: http.MethodPut, Path: streamPath, Op: opUpdateStream},
	{Method: http.MethodDelete, Path: streamPath, Op: opDeleteStream},
	{Method: http.MethodPost, Path: streamPath + changeCompartmentPart, Op: opChangeStreamComp},
	{Method: http.MethodPost, Path: base + "/connectharnesses", Op: opCreateHarness},
	{Method: http.MethodGet, Path: base + "/connectharnesses", Op: opListHarnesses},
	{Method: http.MethodGet, Path: harnessPath, Op: opGetHarness},
	{Method: http.MethodPut, Path: harnessPath, Op: opUpdateHarness},
	{Method: http.MethodDelete, Path: harnessPath, Op: opDeleteHarness},
	{Method: http.MethodPost, Path: harnessPath + changeCompartmentPart, Op: opChangeHarnessComp},
	{Method: http.MethodGet, Path: base + "/workRequests", Op: opListWorkRequests},
	{Method: http.MethodGet, Path: workRequestPath, Op: opGetWorkRequest},
	{Method: http.MethodGet, Path: workRequestPath + "/errors", Op: opListWorkReqErrors},
	{Method: http.MethodGet, Path: workRequestPath + "/logs", Op: opListWorkReqLogs},
	{Method: http.MethodPost, Path: streamPath + "/messages", Op: opPutMessages},
	{Method: http.MethodGet, Path: streamPath + "/messages", Op: opGetMessages},
	{Method: http.MethodPost, Path: streamPath + "/cursors", Op: opCreateCursor},
	{Method: http.MethodPost, Path: streamPath + "/groupCursors", Op: opCreateGroupCursor},
	{Method: http.MethodGet, Path: streamPath + "/groups/{groupName}", Op: opGetGroup},
	{Method: http.MethodPut, Path: streamPath + "/groups/{groupName}", Op: opUpdateGroup},
	{Method: http.MethodPost, Path: streamPath + "/commit", Op: opConsumerCommit},
	{Method: http.MethodPost, Path: streamPath + "/heartbeat", Op: opConsumerHeartbeat},
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
