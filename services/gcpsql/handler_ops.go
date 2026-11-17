package gcpsql

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) registerInstanceOps() {
	h.ops[opInsertInstance] = h.insertInstance
	h.ops[opGetInstance] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetInstance(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opListInstances] = h.listInstances
	h.ops[opPatchInstance] = func(c *echo.Context, rt route) error { return h.updateInstance(c, rt, false) }
	h.ops[opUpdateInstance] = func(c *echo.Context, rt route) error { return h.updateInstance(c, rt, true) }
	h.ops[opDeleteInstance] = h.deleteInstance
	h.ops[opRestartInstance] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.RestartInstance(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opCloneInstance] = h.cloneInstance
	h.ops[opPromoteReplica] = func(c *echo.Context, rt route) error {
		if failover, _ := strconv.ParseBool(c.QueryParam("failover")); failover {
			v, err := h.Backend.FailoverPromote(rt.project, rt.instance)

			return h.reply(c, v, err)
		}

		v, err := h.Backend.PromoteReplica(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opStartReplica] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.SetReplicaRunning(rt.project, rt.instance, true)

		return h.reply(c, v, err)
	}
	h.ops[opStopReplica] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.SetReplicaRunning(rt.project, rt.instance, false)

		return h.reply(c, v, err)
	}
	h.ops[opLatestRecovery] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.LatestRecoveryTime(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opFailover] = h.failover
	h.ops[opSwitchover] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.Switchover(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opExecuteSQL] = h.executeSQL
	h.ops[opTruncateLog] = h.truncateLog
	h.ops[opRotateServerCa] = h.rotateServerCa
	h.ops[opResetSslConfig] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.ResetSslConfig(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opAddServerCa] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.AddServerCa(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opListServerCas] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.ListServerCas(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
	h.ops[opExportInstance] = h.exportInstance
	h.ops[opImportInstance] = h.importInstance
	h.ops[opRestoreBackup] = h.restoreBackup
}

func (h *Handler) insertInstance(c *echo.Context, rt route) error {
	in, err := decode[InstanceInput](c)
	if err != nil {
		return err
	}

	op, err := h.Backend.InsertInstance(rt.project, in)

	return h.reply(c, op, err)
}

func (h *Handler) listInstances(c *echo.Context, rt route) error {
	pg := h.Backend.ListInstances(rt.project, c.QueryParam("pageToken"), queryInt(c, "maxResults"))

	return c.JSON(http.StatusOK, listBody("sql#instancesList", pg.Data, len(pg.Data), pg.Next))
}

func (h *Handler) updateInstance(c *echo.Context, rt route, replace bool) error {
	in, err := decode[InstanceInput](c)
	if err != nil {
		return err
	}

	op, err := h.Backend.UpdateInstance(rt.project, rt.instance, in, replace)

	return h.reply(c, op, err)
}

func (h *Handler) cloneInstance(c *echo.Context, rt route) error {
	in, _, err := decodeContext[CloneInput](c, "cloneContext")
	if err != nil {
		return err
	}

	op, err := h.Backend.CloneInstance(rt.project, rt.instance, in)

	return h.reply(c, op, err)
}

func (h *Handler) exportInstance(c *echo.Context, rt route) error {
	in, raw, err := decodeContext[ExportInput](c, "exportContext")
	if err != nil {
		return err
	}

	op, err := h.Backend.ExportInstance(rt.project, rt.instance, in, raw)

	return h.reply(c, op, err)
}

func (h *Handler) importInstance(c *echo.Context, rt route) error {
	in, raw, err := decodeContext[ImportInput](c, "importContext")
	if err != nil {
		return err
	}

	op, err := h.Backend.ImportInstance(rt.project, rt.instance, in, raw)

	return h.reply(c, op, err)
}

type restoreBackupBody struct {
	RestoreBackupContext *RestoreBackupInput `json:"restoreBackupContext"`
	Backup               string              `json:"backup"`
	BackupdrBackup       string              `json:"backupdrBackup"`
}

func (h *Handler) restoreBackup(c *echo.Context, rt route) error {
	in, err := decode[restoreBackupBody](c)
	if err != nil {
		return err
	}

	given := 0

	for _, set := range []bool{in.RestoreBackupContext != nil, in.Backup != "", in.BackupdrBackup != ""} {
		if set {
			given++
		}
	}

	switch {
	case given != 1:
		return errInvalid("Exactly one of restoreBackupContext, backup and backupdrBackup must be set.")
	case in.BackupdrBackup != "":
		return errInvalid("Backup and DR vault backups are not emulated; use backup or restoreBackupContext.")
	case in.Backup != "":
		op, rerr := h.Backend.RestoreProjectBackup(rt.project, rt.instance, in.Backup)

		return h.reply(c, op, rerr)
	}

	op, err := h.Backend.RestoreBackup(rt.project, rt.instance, *in.RestoreBackupContext)

	return h.reply(c, op, err)
}

func (h *Handler) deleteInstance(c *echo.Context, rt route) error {
	fb, err := finalBackupParams(c)
	if err != nil {
		return err
	}

	v, err := h.Backend.DeleteInstance(rt.project, rt.instance, fb)

	return h.reply(c, v, err)
}

func finalBackupParams(c *echo.Context) (FinalBackupRequest, error) {
	var req FinalBackupRequest

	if v := c.QueryParam("enableFinalBackup"); v != "" {
		on, err := strconv.ParseBool(v)
		if err != nil {
			return req, errInvalid("Invalid enableFinalBackup %q.", v)
		}

		req.Enabled = on
	}

	if v := c.QueryParam("finalBackupTtlDays"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return req, errInvalid("Invalid finalBackupTtlDays %q.", v)
		}

		req.TTLDays = n
	}

	req.Description, req.ExpiryTime = c.QueryParam("finalBackupDescription"), c.QueryParam("finalBackupExpiryTime")

	return req, nil
}

func (h *Handler) registerProjectBackupOps() {
	h.ops[opCreateBackup] = func(c *echo.Context, rt route) error {
		in, err := decode[BackupInput](c)
		if err != nil {
			return err
		}

		op, err := h.Backend.CreateBackup(rt.project, in)

		return h.reply(c, op, err)
	}
	h.ops[opGetBackup] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetBackup(rt.project, rt.name)

		return h.reply(c, v, err)
	}
	h.ops[opListBackups] = func(c *echo.Context, rt route) error {
		pg, err := h.Backend.ListBackups(rt.project, c.QueryParam("filter"), c.QueryParam("pageToken"),
			queryInt(c, "pageSize"))
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, listBodyKey("backups", pg.Data, len(pg.Data), pg.Next))
	}
	h.ops[opUpdateBackup] = func(c *echo.Context, rt route) error {
		in, err := decode[BackupInput](c)
		if err != nil {
			return err
		}

		op, err := h.Backend.UpdateBackup(rt.project, rt.name, in, c.QueryParam("updateMask"))

		return h.reply(c, op, err)
	}
	h.ops[opDeleteBackup] = func(c *echo.Context, rt route) error {
		op, err := h.Backend.DeleteBackup(rt.project, rt.name)

		return h.reply(c, op, err)
	}
}

func (h *Handler) registerAccountOps() {
	h.ops[opInsertDatabase] = func(c *echo.Context, rt route) error {
		in, err := decode[DatabaseInput](c)
		if err != nil {
			return err
		}

		op, err := h.Backend.InsertDatabase(rt.project, rt.instance, in)

		return h.reply(c, op, err)
	}
	h.ops[opGetDatabase] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetDatabase(rt.project, rt.instance, rt.name)

		return h.reply(c, v, err)
	}
	h.ops[opListDatabases] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.ListDatabases(rt.project, rt.instance)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, listBody("sql#databasesList", v, len(v), ""))
	}
	h.ops[opPatchDatabase] = h.updateDatabase
	h.ops[opUpdateDatabase] = h.updateDatabase
	h.ops[opDeleteDatabase] = func(c *echo.Context, rt route) error {
		op, err := h.Backend.DeleteDatabase(rt.project, rt.instance, rt.name)

		return h.reply(c, op, err)
	}
	h.registerUserOps()
}

func (h *Handler) updateDatabase(c *echo.Context, rt route) error {
	in, err := decode[DatabaseInput](c)
	if err != nil {
		return err
	}

	op, err := h.Backend.UpdateDatabase(rt.project, rt.instance, rt.name, in)

	return h.reply(c, op, err)
}

func (h *Handler) registerUserOps() {
	h.ops[opInsertUser] = func(c *echo.Context, rt route) error {
		in, err := decode[UserInput](c)
		if err != nil {
			return err
		}

		op, err := h.Backend.InsertUser(rt.project, rt.instance, in)

		return h.reply(c, op, err)
	}
	h.ops[opUpdateUser] = func(c *echo.Context, rt route) error {
		in, err := decode[UserInput](c)
		if err != nil {
			return err
		}

		name := orDefault(c.QueryParam("name"), in.Name)
		op, err := h.Backend.UpdateUser(rt.project, rt.instance, name, orDefault(c.QueryParam("host"), in.Host), in)

		return h.reply(c, op, err)
	}
	h.ops[opDeleteUser] = func(c *echo.Context, rt route) error {
		op, err := h.Backend.DeleteUser(rt.project, rt.instance, c.QueryParam("name"), c.QueryParam("host"))

		return h.reply(c, op, err)
	}
	h.ops[opGetUser] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetUser(rt.project, rt.instance, rt.name, c.QueryParam("host"))

		return h.reply(c, v, err)
	}
	h.ops[opListUsers] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.ListUsers(rt.project, rt.instance)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, listBody("sql#usersList", v, len(v), ""))
	}
}

func (h *Handler) registerMiscOps() {
	h.registerBackupOps()
	h.registerProjectBackupOps()
	h.ops[opDemote] = h.demote
	h.ops[opDemoteMaster] = h.demote
	h.registerCertOps()
	h.ops[opListFlags] = func(c *echo.Context, _ route) error {
		v := ListFlags(c.QueryParam("databaseVersion"))

		return c.JSON(http.StatusOK, listBody("sql#flagsList", v, len(v), ""))
	}
	h.ops[opListTiers] = func(c *echo.Context, _ route) error {
		v := ListTiers()

		return c.JSON(http.StatusOK, listBody("sql#tiersList", v, len(v), ""))
	}
	h.ops[opGetOperation] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetOperation(rt.project, rt.name)

		return h.reply(c, v, err)
	}
	h.ops[opListOperations] = func(c *echo.Context, rt route) error {
		pg := h.Backend.ListOperations(rt.project, c.QueryParam("instance"), c.QueryParam("pageToken"),
			queryInt(c, "maxResults"))

		return c.JSON(http.StatusOK, listBody("sql#operationsList", pg.Data, len(pg.Data), pg.Next))
	}
	h.ops[opCancelOperation] = func(c *echo.Context, rt route) error {
		if err := h.Backend.CancelOperation(rt.project, rt.name); err != nil {
			return err
		}

		return c.JSON(http.StatusOK, map[string]any{})
	}
}

func (h *Handler) registerBackupOps() {
	h.ops[opInsertBackupRun] = func(c *echo.Context, rt route) error {
		in, err := decode[struct {
			Description string `json:"description"`
		}](c)
		if err != nil {
			return err
		}

		op, err := h.Backend.InsertBackupRun(rt.project, rt.instance, in.Description)

		return h.reply(c, op, err)
	}
	h.ops[opGetBackupRun] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetBackupRun(rt.project, rt.instance, rt.name)

		return h.reply(c, v, err)
	}
	h.ops[opListBackupRuns] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.ListBackupRuns(rt.project, rt.instance)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, listBody("sql#backupRunsList", v, len(v), ""))
	}
	h.ops[opDeleteBackupRun] = func(c *echo.Context, rt route) error {
		op, err := h.Backend.DeleteBackupRun(rt.project, rt.instance, rt.name)

		return h.reply(c, op, err)
	}
}

type ephemeralRequest struct {
	PublicKey     string `json:"public_key"`
	ValidDuration string `json:"validDuration"`
}

func (h *Handler) registerCertOps() {
	h.ops[opInsertSslCert] = func(c *echo.Context, rt route) error {
		in, err := decode[struct {
			CommonName string `json:"commonName"`
		}](c)
		if err != nil {
			return err
		}

		res, err := h.Backend.InsertSslCert(rt.project, rt.instance, in.CommonName)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, map[string]any{
			keyKind: "sql#sslCertsInsert", "operation": res.Operation, "serverCaCert": res.CA,
			"clientCert": map[string]any{"certInfo": res.Cert, "certPrivateKey": res.Key},
		})
	}
	h.ops[opGetSslCert] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetSslCert(rt.project, rt.instance, rt.name)

		return h.reply(c, v, err)
	}
	h.ops[opListSslCerts] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.ListSslCerts(rt.project, rt.instance)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, listBody("sql#sslCertsList", v, len(v), ""))
	}
	h.ops[opDeleteSslCert] = func(c *echo.Context, rt route) error {
		op, err := h.Backend.DeleteSslCert(rt.project, rt.instance, rt.name)

		return h.reply(c, op, err)
	}
	h.ops[opCreateEphemeral] = func(c *echo.Context, rt route) error {
		in, err := decode[ephemeralRequest](c)
		if err != nil {
			return err
		}

		v, err := h.Backend.EphemeralCert(rt.project, rt.instance, in.PublicKey, in.ValidDuration)

		return h.reply(c, v, err)
	}
	h.ops[opGenerateEphemeral] = func(c *echo.Context, rt route) error {
		in, err := decode[ephemeralRequest](c)
		if err != nil {
			return err
		}

		v, err := h.Backend.EphemeralCert(rt.project, rt.instance, in.PublicKey, in.ValidDuration)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, map[string]any{"ephemeralCert": v})
	}
	h.ops[opGetConnectSettings] = func(c *echo.Context, rt route) error {
		v, err := h.Backend.GetConnectSettings(rt.project, rt.instance)

		return h.reply(c, v, err)
	}
}

type failoverBody struct {
	FailoverContext struct {
		SettingsVersion string `json:"settingsVersion"`
	} `json:"failoverContext"`
}

func (h *Handler) failover(c *echo.Context, rt route) error {
	in, err := decode[failoverBody](c)
	if err != nil {
		return err
	}

	op, err := h.Backend.Failover(rt.project, rt.instance, in.FailoverContext.SettingsVersion)

	return h.reply(c, op, err)
}

func (h *Handler) truncateLog(c *echo.Context, rt route) error {
	in, _, err := decodeContext[struct {
		LogType string `json:"logType"`
	}](c, "truncateLogContext")
	if err != nil {
		return err
	}

	op, err := h.Backend.TruncateLog(rt.project, rt.instance, in.LogType)

	return h.reply(c, op, err)
}

func (h *Handler) rotateServerCa(c *echo.Context, rt route) error {
	in, err := decode[struct {
		RotateServerCaContext struct {
			NextVersion string `json:"nextVersion"`
		} `json:"rotateServerCaContext"`
	}](c)
	if err != nil {
		return err
	}

	op, err := h.Backend.RotateServerCa(rt.project, rt.instance, in.RotateServerCaContext.NextVersion)

	return h.reply(c, op, err)
}

func (h *Handler) executeSQL(c *echo.Context, rt route) error {
	in, err := decode[ExecuteSQLInput](c)
	if err != nil {
		return err
	}

	v, err := h.Backend.ExecuteSQL(c.Request().Context(), rt.project, rt.instance, in)

	return h.reply(c, v, err)
}

type demoteBody struct {
	DemoteContext *struct {
		SourceRepresentativeInstanceName string `json:"sourceRepresentativeInstanceName"`
	} `json:"demoteContext"`
	DemoteMasterContext *struct {
		MasterInstanceName   string `json:"masterInstanceName"`
		SkipReplicationSetup bool   `json:"skipReplicationSetup"`
	} `json:"demoteMasterContext"`
}

func (h *Handler) demote(c *echo.Context, rt route) error {
	in, err := decode[demoteBody](c)
	if err != nil {
		return err
	}

	var req DemoteRequest

	switch {
	case in.DemoteContext != nil && in.DemoteMasterContext == nil:
		req.Source = in.DemoteContext.SourceRepresentativeInstanceName
	case in.DemoteMasterContext != nil && in.DemoteContext == nil:
		ctx := in.DemoteMasterContext
		req = DemoteRequest{Source: ctx.MasterInstanceName, SkipReplicationSetup: ctx.SkipReplicationSetup}
	default:
		return errInvalid("Exactly one of demoteContext (demote) and demoteMasterContext (demoteMaster) is required.")
	}

	op, err := h.Backend.DemoteInstance(rt.project, rt.instance, req)

	return h.reply(c, op, err)
}
