package amp

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	pathWorkspaces = "/workspaces"
	pathWorkspace  = "/workspaces/{workspaceId}"
	pathAlertMgr   = pathWorkspace + "/alertmanager/definition"
	pathDetectors  = pathWorkspace + "/anomalydetectors"
	pathDetector   = pathDetectors + "/{anomalyDetectorId}"
	pathLogging    = pathWorkspace + "/logging"
	pathQueryLog   = pathLogging + "/query"
	pathPolicy     = pathWorkspace + "/policy"
	pathNamespaces = pathWorkspace + "/rulegroupsnamespaces"
	pathNamespace  = pathNamespaces + "/{name}"
	pathScrapers   = "/scrapers"
	pathScraper    = "/scrapers/{scraperId}"
	pathScraperLog = pathScraper + "/logging-configuration"
	pathTags       = "/tags/{resourceArn}"
)

func routes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPost, Path: pathWorkspaces, Op: "CreateWorkspace"},
		{Method: http.MethodGet, Path: pathWorkspaces, Op: "ListWorkspaces"},
		{Method: http.MethodGet, Path: pathWorkspace, Op: "DescribeWorkspace"},
		{Method: http.MethodDelete, Path: pathWorkspace, Op: "DeleteWorkspace"},
		{Method: http.MethodPost, Path: pathWorkspace + "/alias", Op: "UpdateWorkspaceAlias"},
		{Method: http.MethodGet, Path: pathWorkspace + "/configuration", Op: "DescribeWorkspaceConfiguration"},
		{Method: http.MethodPatch, Path: pathWorkspace + "/configuration", Op: "UpdateWorkspaceConfiguration"},
		{Method: http.MethodPost, Path: pathAlertMgr, Op: "CreateAlertManagerDefinition"},
		{Method: http.MethodPut, Path: pathAlertMgr, Op: "PutAlertManagerDefinition"},
		{Method: http.MethodGet, Path: pathAlertMgr, Op: "DescribeAlertManagerDefinition"},
		{Method: http.MethodDelete, Path: pathAlertMgr, Op: "DeleteAlertManagerDefinition"},
		{Method: http.MethodPost, Path: pathDetectors, Op: "CreateAnomalyDetector"},
		{Method: http.MethodGet, Path: pathDetectors, Op: "ListAnomalyDetectors"},
		{Method: http.MethodGet, Path: pathDetector, Op: "DescribeAnomalyDetector"},
		{Method: http.MethodPut, Path: pathDetector, Op: "PutAnomalyDetector"},
		{Method: http.MethodDelete, Path: pathDetector, Op: "DeleteAnomalyDetector"},
		{Method: http.MethodPost, Path: pathLogging, Op: "CreateLoggingConfiguration"},
		{Method: http.MethodPut, Path: pathLogging, Op: "UpdateLoggingConfiguration"},
		{Method: http.MethodGet, Path: pathLogging, Op: "DescribeLoggingConfiguration"},
		{Method: http.MethodDelete, Path: pathLogging, Op: "DeleteLoggingConfiguration"},
		{Method: http.MethodPost, Path: pathQueryLog, Op: "CreateQueryLoggingConfiguration"},
		{Method: http.MethodPut, Path: pathQueryLog, Op: "UpdateQueryLoggingConfiguration"},
		{Method: http.MethodGet, Path: pathQueryLog, Op: "DescribeQueryLoggingConfiguration"},
		{Method: http.MethodDelete, Path: pathQueryLog, Op: "DeleteQueryLoggingConfiguration"},
		{Method: http.MethodPut, Path: pathPolicy, Op: "PutResourcePolicy"},
		{Method: http.MethodGet, Path: pathPolicy, Op: "DescribeResourcePolicy"},
		{Method: http.MethodDelete, Path: pathPolicy, Op: "DeleteResourcePolicy"},
		{Method: http.MethodPost, Path: pathNamespaces, Op: "CreateRuleGroupsNamespace"},
		{Method: http.MethodGet, Path: pathNamespaces, Op: "ListRuleGroupsNamespaces"},
		{Method: http.MethodGet, Path: pathNamespace, Op: "DescribeRuleGroupsNamespace"},
		{Method: http.MethodPut, Path: pathNamespace, Op: "PutRuleGroupsNamespace"},
		{Method: http.MethodDelete, Path: pathNamespace, Op: "DeleteRuleGroupsNamespace"},
		{Method: http.MethodPost, Path: pathScrapers, Op: "CreateScraper"},
		{Method: http.MethodGet, Path: pathScrapers, Op: "ListScrapers"},
		{Method: http.MethodGet, Path: pathScraper, Op: "DescribeScraper"},
		{Method: http.MethodPut, Path: pathScraper, Op: "UpdateScraper"},
		{Method: http.MethodDelete, Path: pathScraper, Op: "DeleteScraper"},
		{Method: http.MethodPut, Path: pathScraperLog, Op: "UpdateScraperLoggingConfiguration"},
		{Method: http.MethodGet, Path: pathScraperLog, Op: "DescribeScraperLoggingConfiguration"},
		{Method: http.MethodDelete, Path: pathScraperLog, Op: "DeleteScraperLoggingConfiguration"},
		{Method: http.MethodGet, Path: "/scraperconfiguration", Op: "GetDefaultScraperConfiguration"},
		{Method: http.MethodGet, Path: pathTags, Op: "ListTagsForResource"},
		{Method: http.MethodPost, Path: pathTags, Op: "TagResource"},
		{Method: http.MethodDelete, Path: pathTags, Op: "UntagResource"},
	}
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	ops := map[string]restroute.OpFunc{}
	h.workspaceOps(ops)
	h.docOps(ops)
	h.namespaceOps(ops)
	h.scraperOps(ops)
	h.detectorOps(ops)
	h.tagOps(ops)

	return ops
}
