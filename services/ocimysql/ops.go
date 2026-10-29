package ocimysql

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateDBSystem: (*Handler).createDBSystem, opGetDBSystem: (*Handler).getDBSystem,
		opListDBSystems: (*Handler).listDBSystems, opUpdateDBSystem: (*Handler).updateDBSystem,
		opDeleteDBSystem: (*Handler).deleteDBSystem, opStartDBSystem: (*Handler).startDBSystem,
		opStopDBSystem: (*Handler).stopDBSystem, opRestartDBSystem: (*Handler).restartDBSystem,
		opCreateBackup: (*Handler).createBackup, opGetBackup: (*Handler).getBackup,
		opListBackups: (*Handler).listBackups, opUpdateBackup: (*Handler).updateBackup,
		opDeleteBackup: (*Handler).deleteBackup, opChangeBackupComp: (*Handler).changeBackupCompartment,
		opCreateChannel: (*Handler).createChannel, opGetChannel: (*Handler).getChannel,
		opListChannels: (*Handler).listChannels, opUpdateChannel: (*Handler).updateChannel,
		opDeleteChannel: (*Handler).deleteChannel, opResumeChannel: (*Handler).resumeChannel,
		opResetChannel: (*Handler).resetChannel,
		opCreateConfig: (*Handler).createConfiguration, opGetConfig: (*Handler).getConfiguration,
		opListConfigs: (*Handler).listConfigurations, opUpdateConfig: (*Handler).updateConfiguration,
		opDeleteConfig: (*Handler).deleteConfiguration,
		opListShapes:   (*Handler).listShapes, opListVersions: (*Handler).listVersions,
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

	return q.json(http.StatusOK, pg.Data, "")
}
