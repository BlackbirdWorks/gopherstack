package ocilogging

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateLogGroup:    (*Handler).createLogGroup,
		opGetLogGroup:       (*Handler).getLogGroup,
		opListLogGroups:     (*Handler).listLogGroups,
		opUpdateLogGroup:    (*Handler).updateLogGroup,
		opDeleteLogGroup:    (*Handler).deleteLogGroup,
		opChangeGroupComp:   (*Handler).changeLogGroupCompartment,
		opCreateLog:         (*Handler).createLog,
		opGetLog:            (*Handler).getLog,
		opListLogs:          (*Handler).listLogs,
		opUpdateLog:         (*Handler).updateLog,
		opDeleteLog:         (*Handler).deleteLog,
		opChangeLogGroup:    (*Handler).changeLogLogGroup,
		opCreateAgentConfig: (*Handler).createAgentConfig,
		opGetAgentConfig:    (*Handler).getAgentConfig,
		opListAgentConfigs:  (*Handler).listAgentConfigs,
		opUpdateAgentConfig: (*Handler).updateAgentConfig,
		opDeleteAgentConfig: (*Handler).deleteAgentConfig,
		opChangeAgentComp:   (*Handler).changeAgentConfigCompartment,
		opListServices:      (*Handler).listServices,
		opGetWorkRequest:    (*Handler).getWorkRequest,
		opListWorkRequests:  (*Handler).listWorkRequests,
		opDeleteWorkRequest: (*Handler).deleteWorkRequest,
		opListWorkReqErrors: (*Handler).listWorkRequestErrors,
		opListWorkReqLogs:   (*Handler).listWorkRequestLogs,
		opPutLogs:           (*Handler).putLogs,
		opSearchLogs:        (*Handler).searchLogs,
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

// listReply writes a page as a bare JSON array, the shape every Logging list operation declares.
func listReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	data := pg.Data
	if data == nil {
		data = []T{}
	}

	return q.json(http.StatusOK, data, "")
}
