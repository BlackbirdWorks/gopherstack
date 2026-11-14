package ocidns

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateZone            = "CreateZone"
	opGetZone               = "GetZone"
	opListZones             = "ListZones"
	opUpdateZone            = "UpdateZone"
	opDeleteZone            = "DeleteZone"
	opChangeZoneComp        = "ChangeZoneCompartment"
	opGetZoneContent        = "GetZoneContent"
	opCreateZoneFromFile    = "CreateZoneFromZoneFile"
	opListTransferServers   = "ListZoneTransferServers"
	opGetZoneRecords        = "GetZoneRecords"
	opPatchZoneRecords      = "PatchZoneRecords"
	opUpdateZoneRecords     = "UpdateZoneRecords"
	opGetDomainRecords      = "GetDomainRecords"
	opPatchDomainRecords    = "PatchDomainRecords"
	opUpdateDomainRecords   = "UpdateDomainRecords"
	opDeleteDomainRecords   = "DeleteDomainRecords"
	opGetRRSet              = "GetRRSet"
	opPatchRRSet            = "PatchRRSet"
	opUpdateRRSet           = "UpdateRRSet"
	opDeleteRRSet           = "DeleteRRSet"
	opCreateView            = "CreateView"
	opGetView               = "GetView"
	opListViews             = "ListViews"
	opUpdateView            = "UpdateView"
	opDeleteView            = "DeleteView"
	opChangeViewComp        = "ChangeViewCompartment"
	opGetResolver           = "GetResolver"
	opListResolvers         = "ListResolvers"
	opUpdateResolver        = "UpdateResolver"
	opChangeResolverComp    = "ChangeResolverCompartment"
	opCreateEndpoint        = "CreateResolverEndpoint"
	opGetEndpoint           = "GetResolverEndpoint"
	opListEndpoints         = "ListResolverEndpoints"
	opUpdateEndpoint        = "UpdateResolverEndpoint"
	opDeleteEndpoint        = "DeleteResolverEndpoint"
	opCreateTsigKey         = "CreateTsigKey"
	opGetTsigKey            = "GetTsigKey"
	opListTsigKeys          = "ListTsigKeys"
	opUpdateTsigKey         = "UpdateTsigKey"
	opDeleteTsigKey         = "DeleteTsigKey"
	opChangeTsigKeyComp     = "ChangeTsigKeyCompartment"
	opCreatePolicy          = "CreateSteeringPolicy"
	opGetPolicy             = "GetSteeringPolicy"
	opListPolicies          = "ListSteeringPolicies"
	opUpdatePolicy          = "UpdateSteeringPolicy"
	opDeletePolicy          = "DeleteSteeringPolicy"
	opChangePolicyComp      = "ChangeSteeringPolicyCompartment"
	opCreateAttachment      = "CreateSteeringPolicyAttachment"
	opGetAttachment         = "GetSteeringPolicyAttachment"
	opListAttachments       = "ListSteeringPolicyAttachments"
	opUpdateAttachment      = "UpdateSteeringPolicyAttachment"
	opDeleteAttachment      = "DeleteSteeringPolicyAttachment"
	opAdmin                 = "Admin"
	opUnknown               = "Unknown"
	adminPrefix             = "/_gopherstack/"
	base                    = "/" + Version
	zonePath                = base + "/zones/{zoneNameOrId}"
	recordsPath             = zonePath + "/records"
	domainPath              = recordsPath + "/{domain}"
	rrsetPath               = domainPath + "/{rtype}"
	viewPath                = base + "/views/{viewId}"
	resolverPath            = base + "/resolvers/{resolverId}"
	endpointsPath           = resolverPath + "/endpoints"
	endpointPath            = endpointsPath + "/{resolverEndpointName}"
	tsigPath                = base + "/tsigKeys/{tsigKeyId}"
	policyPath              = base + "/steeringPolicies/{steeringPolicyId}"
	attachmentPath          = base + "/steeringPolicyAttachments/{steeringPolicyAttachmentId}"
	changeCompartmentSuffix = "/actions/changeCompartment"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/zones", Op: opCreateZone},
	{Method: http.MethodGet, Path: base + "/zones", Op: opListZones},
	{Method: http.MethodGet, Path: zonePath, Op: opGetZone},
	{Method: http.MethodPut, Path: zonePath, Op: opUpdateZone},
	{Method: http.MethodDelete, Path: zonePath, Op: opDeleteZone},
	{Method: http.MethodPost, Path: base + "/zones/{zoneId}" + changeCompartmentSuffix, Op: opChangeZoneComp},
	{Method: http.MethodGet, Path: zonePath + "/content", Op: opGetZoneContent},
	{Method: http.MethodPost, Path: base + "/actions/createZoneFromZoneFile", Op: opCreateZoneFromFile},
	{Method: http.MethodGet, Path: base + "/zoneTransferServers", Op: opListTransferServers},
	{Method: http.MethodGet, Path: recordsPath, Op: opGetZoneRecords},
	{Method: http.MethodPatch, Path: recordsPath, Op: opPatchZoneRecords},
	{Method: http.MethodPut, Path: recordsPath, Op: opUpdateZoneRecords},
	{Method: http.MethodGet, Path: domainPath, Op: opGetDomainRecords},
	{Method: http.MethodPatch, Path: domainPath, Op: opPatchDomainRecords},
	{Method: http.MethodPut, Path: domainPath, Op: opUpdateDomainRecords},
	{Method: http.MethodDelete, Path: domainPath, Op: opDeleteDomainRecords},
	{Method: http.MethodGet, Path: rrsetPath, Op: opGetRRSet},
	{Method: http.MethodPatch, Path: rrsetPath, Op: opPatchRRSet},
	{Method: http.MethodPut, Path: rrsetPath, Op: opUpdateRRSet},
	{Method: http.MethodDelete, Path: rrsetPath, Op: opDeleteRRSet},
	{Method: http.MethodPost, Path: base + "/views", Op: opCreateView},
	{Method: http.MethodGet, Path: base + "/views", Op: opListViews},
	{Method: http.MethodGet, Path: viewPath, Op: opGetView},
	{Method: http.MethodPut, Path: viewPath, Op: opUpdateView},
	{Method: http.MethodDelete, Path: viewPath, Op: opDeleteView},
	{Method: http.MethodPost, Path: viewPath + changeCompartmentSuffix, Op: opChangeViewComp},
	{Method: http.MethodGet, Path: base + "/resolvers", Op: opListResolvers},
	{Method: http.MethodGet, Path: resolverPath, Op: opGetResolver},
	{Method: http.MethodPut, Path: resolverPath, Op: opUpdateResolver},
	{Method: http.MethodPost, Path: resolverPath + changeCompartmentSuffix, Op: opChangeResolverComp},
	{Method: http.MethodPost, Path: endpointsPath, Op: opCreateEndpoint},
	{Method: http.MethodGet, Path: endpointsPath, Op: opListEndpoints},
	{Method: http.MethodGet, Path: endpointPath, Op: opGetEndpoint},
	{Method: http.MethodPut, Path: endpointPath, Op: opUpdateEndpoint},
	{Method: http.MethodDelete, Path: endpointPath, Op: opDeleteEndpoint},
	{Method: http.MethodPost, Path: base + "/tsigKeys", Op: opCreateTsigKey},
	{Method: http.MethodGet, Path: base + "/tsigKeys", Op: opListTsigKeys},
	{Method: http.MethodGet, Path: tsigPath, Op: opGetTsigKey},
	{Method: http.MethodPut, Path: tsigPath, Op: opUpdateTsigKey},
	{Method: http.MethodDelete, Path: tsigPath, Op: opDeleteTsigKey},
	{Method: http.MethodPost, Path: tsigPath + changeCompartmentSuffix, Op: opChangeTsigKeyComp},
	{Method: http.MethodPost, Path: base + "/steeringPolicies", Op: opCreatePolicy},
	{Method: http.MethodGet, Path: base + "/steeringPolicies", Op: opListPolicies},
	{Method: http.MethodGet, Path: policyPath, Op: opGetPolicy},
	{Method: http.MethodPut, Path: policyPath, Op: opUpdatePolicy},
	{Method: http.MethodDelete, Path: policyPath, Op: opDeletePolicy},
	{Method: http.MethodPost, Path: policyPath + changeCompartmentSuffix, Op: opChangePolicyComp},
	{Method: http.MethodPost, Path: base + "/steeringPolicyAttachments", Op: opCreateAttachment},
	{Method: http.MethodGet, Path: base + "/steeringPolicyAttachments", Op: opListAttachments},
	{Method: http.MethodGet, Path: attachmentPath, Op: opGetAttachment},
	{Method: http.MethodPut, Path: attachmentPath, Op: opUpdateAttachment},
	{Method: http.MethodDelete, Path: attachmentPath, Op: opDeleteAttachment},
})

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()

	if strings.HasPrefix(path, adminPrefix) {
		return route{op: opAdmin}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
