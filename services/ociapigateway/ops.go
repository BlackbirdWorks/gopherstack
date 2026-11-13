package ociapigateway

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const itemsKey = "items"

func buildOps() map[string]opFunc {
	ops := baseOps()

	for _, k := range lockKinds {
		ops["Add"+k.kind+"Lock"] = lockHandler(k.kind, k.param, true)
		ops["Remove"+k.kind+"Lock"] = lockHandler(k.kind, k.param, false)
	}

	return ops
}

func baseOps() map[string]opFunc {
	return map[string]opFunc{
		opCancelWorkRequest: (*Handler).cancelWorkRequest,
		opCreateAPI:         (*Handler).createAPI, opGetAPI: (*Handler).getAPI, opListAPIs: (*Handler).listAPIs,
		opUpdateAPI: (*Handler).updateAPI, opDeleteAPI: (*Handler).deleteAPI,
		opChangeAPIComp: (*Handler).changeAPICompartment, opGetAPIContent: (*Handler).getAPIContent,
		opGetAPIDeploySpec: (*Handler).getAPIDeploymentSpecification, opGetAPIValidations: (*Handler).getAPIValidations,
		opCreateSdk: (*Handler).createSDK, opGetSdk: (*Handler).getSDK, opListSdks: (*Handler).listSDKs,
		opUpdateSdk: (*Handler).updateSDK, opDeleteSdk: (*Handler).deleteSDK,
		opListSdkLangs:    (*Handler).listSdkLanguageTypes,
		opCreateUsagePlan: (*Handler).createUsagePlan, opGetUsagePlan: (*Handler).getUsagePlan,
		opListUsagePlans: (*Handler).listUsagePlans, opUpdateUsagePlan: (*Handler).updateUsagePlan,
		opDeleteUsagePlan: (*Handler).deleteUsagePlan, opChangeUPComp: (*Handler).changeUsagePlanCompartment,
		opCreateSubscriber: (*Handler).createSubscriber, opGetSubscriber: (*Handler).getSubscriber,
		opListSubscribers: (*Handler).listSubscribers, opUpdateSubscriber: (*Handler).updateSubscriber,
		opDeleteSubscriber: (*Handler).deleteSubscriber, opChangeSubComp: (*Handler).changeSubscriberCompartment,
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

// withBody decodes the request body, runs the operation and answers 202 with its work request.
func withBody[I any](q *req, fn func(in I) (string, error)) error {
	var in I
	if err := q.decode(&in); err != nil {
		return err
	}

	if r, ok := any(&in).(interface{ setRetryToken(string) }); ok {
		r.setRetryToken(q.r.Header.Get(headerRetry))
	}

	wr, err := fn(in)

	return acceptedWR(q, wr, err)
}

// created answers 202 with the new resource, its etag, location and work request.
func created[I, W any](q *req, path string, id func(W) string, fn func(I) (W, string, string, error)) error {
	var in I
	if err := q.decode(&in); err != nil {
		return err
	}

	if r, ok := any(&in).(interface{ setRetryToken(string) }); ok {
		r.setRetryToken(q.r.Header.Get(headerRetry))
	}

	w, wr, etag, err := fn(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)
	q.header("location", base+path+id(w))

	return q.json(http.StatusAccepted, w, etag)
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
