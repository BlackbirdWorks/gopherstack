package ociqueue

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const itemsKey = "items"

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateQueue: (*Handler).createQueue, opGetQueue: (*Handler).getQueue,
		opListQueues: (*Handler).listQueues, opUpdateQueue: (*Handler).updateQueue,
		opDeleteQueue: (*Handler).deleteQueue, opChangeQueueComp: (*Handler).changeQueueCompartment,
		opPurgeQueue:     (*Handler).purgeQueue,
		opGetWorkRequest: (*Handler).getWorkRequest, opListWorkRequests: (*Handler).listWorkRequests,
		opListWorkReqErrors: (*Handler).listWorkRequestErrors, opListWorkReqLogs: (*Handler).listWorkRequestLogs,
		opPutMessages: (*Handler).putMessages, opGetMessages: (*Handler).getMessages,
		opUpdateMessage: (*Handler).updateMessage, opUpdateMessages: (*Handler).updateMessages,
		opDeleteMessage: (*Handler).deleteMessage, opDeleteMessages: (*Handler).deleteMessages,
		opGetStats: (*Handler).getStats, opListChannels: (*Handler).listChannels,
	}
}

func reply[T any](q *req, w T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func acceptedWR(q *req, wr string, err error) error {
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func collectionReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, map[string]any{itemsKey: pg.Data}, "")
}
