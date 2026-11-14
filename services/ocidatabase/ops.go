package ocidatabase

import (
	"net/http"
	"strconv"
	"strings"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreate: (*Handler).createDatabase, opGet: (*Handler).getDatabase, opList: (*Handler).listDatabases,
		opUpdate: (*Handler).updateDatabase, opDelete: (*Handler).deleteDatabase,
		opStart: (*Handler).startDatabase, opStop: (*Handler).stopDatabase, opRestart: (*Handler).restartDatabase,
		opRestore: (*Handler).restoreDatabase, opChangeComp: (*Handler).changeCompartment,
		opGenWallet: (*Handler).generateWallet, opGetWallet: (*Handler).getWallet,
		opUpdateWallet: (*Handler).updateWallet, opCreateBackup: (*Handler).createBackup,
		opGetBackup: (*Handler).getBackup, opListBackups: (*Handler).listBackups,
		opDeleteBackup: (*Handler).deleteBackup, opGetWR: (*Handler).getWorkRequest,
		opListWR: (*Handler).listWorkRequests, opListWRErrors: (*Handler).listWRErrors,
		opListWRLogs: (*Handler).listWRLogs,
	}
}

func (q *req) dbID() string { return q.param("autonomousDatabaseId") }

// withWork writes a 202 with the database and its work request.
func (q *req) withWork(view AdbWire, wr, etag string, err error) error {
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusAccepted, view, etag)
}

func (h *Handler) createDatabase(q *req) error {
	var in createIn
	if err := q.decode(&in); err != nil {
		return err
	}

	view, wr, etag, err := h.Backend.CreateDatabase(in, q.r.Header.Get("Opc-Retry-Token"))

	return q.withWork(view, wr, etag, err)
}

func (h *Handler) getDatabase(q *req) error {
	w, etag, err := h.Backend.GetDatabase(q.dbID())
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func queryBool(q *req, k string) *bool {
	v := q.query(k)
	if v == "" {
		return nil
	}

	b := strings.EqualFold(v, "true")

	return &b
}

func (h *Handler) listDatabases(q *req) error {
	pg, err := h.Backend.ListDatabases(DatabaseFilter{
		Compartment: q.query("compartmentId"), State: q.query("lifecycleState"),
		StateNot: q.query("lifecycleStateNotEqualTo"), Workload: q.query("dbWorkload"),
		DBVersion: q.query("dbVersion"), DisplayName: q.query("displayName"), FreeTier: queryBool(q, "isFreeTier"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) updateDatabase(q *req) error {
	var in updateIn
	if err := q.decode(&in); err != nil {
		return err
	}

	view, wr, etag, err := h.Backend.UpdateDatabase(q.dbID(), q.ifMatch(), in)

	return q.withWork(view, wr, etag, err)
}

func (h *Handler) deleteDatabase(q *req) error {
	wr, err := h.Backend.TerminateDatabase(
		q.dbID(),
		q.ifMatch(),
		strings.EqualFold(q.query("mustDeleteAssociatedLongTermBackups"), "true"),
	)
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func (h *Handler) startDatabase(q *req) error {
	view, wr, etag, err := h.Backend.StartDatabase(q.dbID(), q.ifMatch())

	return q.withWork(view, wr, etag, err)
}

func (h *Handler) stopDatabase(q *req) error {
	if len(q.body) > 0 {
		var in stopIn
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	view, wr, etag, err := h.Backend.StopDatabase(q.dbID(), q.ifMatch())

	return q.withWork(view, wr, etag, err)
}

func (h *Handler) restartDatabase(q *req) error {
	if len(q.body) > 0 {
		var in restartIn
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	view, wr, etag, err := h.Backend.RestartDatabase(q.dbID(), q.ifMatch())

	return q.withWork(view, wr, etag, err)
}

func (h *Handler) restoreDatabase(q *req) error {
	var in restoreIn
	if err := q.decode(&in); err != nil {
		return err
	}

	view, wr, etag, err := h.Backend.RestoreDatabase(q.dbID(), q.ifMatch(), in)

	return q.withWork(view, wr, etag, err)
}

func (h *Handler) changeCompartment(q *req) error {
	var in compartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, etag, err := h.Backend.ChangeCompartment(q.dbID(), q.ifMatch(), in.Compartment)
	if err != nil {
		return err
	}

	q.header("etag", etag)

	return q.accepted(wr)
}

func (h *Handler) generateWallet(q *req) error {
	var in walletIn
	if err := q.decode(&in); err != nil {
		return err
	}

	data, modified, err := h.Backend.GenerateWallet(q.dbID(), in)
	if err != nil {
		return err
	}

	q.header("Content-Type", "application/zip")
	q.header("Content-Length", strconv.Itoa(len(data)))
	q.header("Last-Modified", modified.Format(http.TimeFormat))
	q.w.WriteHeader(http.StatusOK)
	_, _ = q.w.Write(data)

	return nil
}

func (h *Handler) getWallet(q *req) error {
	w, err := h.Backend.GetWallet(q.dbID())
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, "")
}

func (h *Handler) updateWallet(q *req) error {
	var in walletUpdateIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateWallet(q.dbID(), in)
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func (h *Handler) createBackup(q *req) error {
	var in backupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	view, wr, etag, err := h.Backend.CreateBackup(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusAccepted, view, etag)
}

func (h *Handler) getBackup(q *req) error {
	w, err := h.Backend.GetBackup(q.param("autonomousDatabaseBackupId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, w.ID)
}

func (h *Handler) listBackups(q *req) error {
	pg, err := h.Backend.ListBackups(BackupFilter{
		DBID: q.query("autonomousDatabaseId"), Compartment: q.query("compartmentId"),
		State: q.query("lifecycleState"), DisplayName: q.query("displayName"), Type: q.query("type"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) deleteBackup(q *req) error {
	wr, err := h.Backend.DeleteBackup(q.param("autonomousDatabaseBackupId"), q.ifMatch())
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

func (h *Handler) getWorkRequest(q *req) error {
	w, err := h.Backend.GetWorkRequest(q.param("workRequestId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, "")
}

func (h *Handler) listWorkRequests(q *req) error {
	pg, err := h.Backend.ListWorkRequests(WorkRequestFilter{
		Compartment: q.query("compartmentId"), ResourceID: q.query("resourceId"),
	}, q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) listWRErrors(q *req) error {
	pg, err := h.Backend.WorkRequestErrors(q.param("workRequestId"), q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}

func (h *Handler) listWRLogs(q *req) error {
	pg, err := h.Backend.WorkRequestLogs(q.param("workRequestId"), q.listOpts())
	if err != nil {
		return err
	}

	return q.collection(pg.Data, pg.Next)
}
