package ocinosql

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const itemsKey = "items"

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateTable: (*Handler).createTable, opGetTable: (*Handler).getTable,
		opListTables: (*Handler).listTables, opUpdateTable: (*Handler).updateTable,
		opDeleteTable: (*Handler).deleteTable, opChangeTableComp: (*Handler).changeTableCompartment,
		opListTableUsage: (*Handler).listTableUsage,
		opCreateIndex:    (*Handler).createIndex, opGetIndex: (*Handler).getIndex,
		opListIndexes: (*Handler).listIndexes, opDeleteIndex: (*Handler).deleteIndex,
		opUpdateRow: (*Handler).updateRow, opGetRow: (*Handler).getRow, opDeleteRow: (*Handler).deleteRow,
		opQuery: (*Handler).query, opPrepareStatement: (*Handler).prepareStatement,
		opSummarizeStmt:  (*Handler).summarizeStatement,
		opGetWorkRequest: (*Handler).getWorkRequest, opListWorkRequests: (*Handler).listWorkRequests,
		opListWorkReqErrors: (*Handler).listWorkRequestErrors, opListWorkReqLogs: (*Handler).listWorkRequestLogs,
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
