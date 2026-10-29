package ociapigateway

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const itemsKey = "items"

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateGateway: (*Handler).createGateway, opGetGateway: (*Handler).getGateway,
		opListGateways: (*Handler).listGateways, opUpdateGateway: (*Handler).updateGateway,
		opDeleteGateway: (*Handler).deleteGateway, opChangeGatewayComp: (*Handler).changeGatewayCompartment,
		opCreateDeployment: (*Handler).createDeployment, opGetDeployment: (*Handler).getDeployment,
		opListDeployments: (*Handler).listDeployments, opUpdateDeployment: (*Handler).updateDeployment,
		opDeleteDeployment: (*Handler).deleteDeployment, opChangeDeployComp: (*Handler).changeDeploymentCompartment,
		opCreateCert: (*Handler).createCertificate, opGetCert: (*Handler).getCertificate,
		opListCerts: (*Handler).listCertificates, opUpdateCert: (*Handler).updateCertificate,
		opDeleteCert: (*Handler).deleteCertificate, opChangeCertComp: (*Handler).changeCertificateCompartment,
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
