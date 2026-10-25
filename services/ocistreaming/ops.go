package ocistreaming

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateStreamPool: (*Handler).createPool, opGetStreamPool: (*Handler).getPool,
		opListStreamPools: (*Handler).listPools, opUpdateStreamPool: (*Handler).updatePool,
		opDeleteStreamPool: (*Handler).deletePool, opChangePoolComp: (*Handler).changePoolCompartment,
		opCreateStream: (*Handler).createStream, opGetStream: (*Handler).getStream,
		opListStreams: (*Handler).listStreams, opUpdateStream: (*Handler).updateStream,
		opDeleteStream: (*Handler).deleteStream, opChangeStreamComp: (*Handler).changeStreamCompartment,
		opCreateHarness: (*Handler).createHarness, opGetHarness: (*Handler).getHarness,
		opListHarnesses: (*Handler).listHarnesses, opUpdateHarness: (*Handler).updateHarness,
		opDeleteHarness: (*Handler).deleteHarness, opChangeHarnessComp: (*Handler).changeHarnessCompartment,
		opGetWorkRequest: (*Handler).getWorkRequest, opListWorkRequests: (*Handler).listWorkRequests,
		opListWorkReqErrors: (*Handler).listWorkRequestErrors, opListWorkReqLogs: (*Handler).listWorkRequestLogs,
		opPutMessages: (*Handler).putMessages, opGetMessages: (*Handler).getMessages,
		opCreateCursor: (*Handler).createCursor, opCreateGroupCursor: (*Handler).createGroupCursor,
		opGetGroup: (*Handler).getGroup, opUpdateGroup: (*Handler).updateGroup,
		opConsumerCommit: (*Handler).consumerCommit, opConsumerHeartbeat: (*Handler).consumerHeartbeat,
	}
}

func reply[T any](q *req, w T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func listReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

// replyWR answers a create or update with the resource, its etag and the work request id.
func replyWR[T any](q *req, w T, etag, wr string, err error) error {
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, w, etag)
}

func acceptedWR(q *req, wr string, err error) error {
	if err != nil {
		return err
	}

	return q.accepted(wr)
}
