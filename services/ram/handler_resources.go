package ram

import (
	"context"
	"encoding/json"
	"fmt"
)

type resourceObject struct {
	Arn                 string  `json:"arn"`
	ResourceShareArn    string  `json:"resourceShareArn"`
	Type                string  `json:"type"`
	Status              string  `json:"status"`
	ResourceRegionScope string  `json:"resourceRegionScope"`
	CreationTime        float64 `json:"creationTime"`
	LastUpdatedTime     float64 `json:"lastUpdatedTime"`
}

func toResourceObject(a *ResourceShareAssociation) resourceObject {
	resType := resourceTypeFromARN(a.AssociatedEntity)
	scope := "REGIONAL"

	return resourceObject{
		Arn:                 a.AssociatedEntity,
		ResourceShareArn:    a.ResourceShareARN,
		Type:                resType,
		Status:              a.Status,
		ResourceRegionScope: scope,
		CreationTime:        epochSeconds(a.CreationTime),
		LastUpdatedTime:     epochSeconds(a.LastUpdatedTime),
	}
}

type listResourcesRequest struct {
	MaxResults        *int32   `json:"maxResults,omitempty"`
	ResourceOwner     string   `json:"resourceOwner"`
	ResourceType      string   `json:"resourceType"`
	NextToken         string   `json:"nextToken"`
	ResourceShareArns []string `json:"resourceShareArns"`
}

type listResourcesResponse struct {
	NextToken string           `json:"nextToken,omitempty"`
	Resources []resourceObject `json:"resources"`
}

func (h *Handler) handleListResources(_ context.Context, body []byte) ([]byte, error) {
	var req listResourcesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceOwner == "" {
		return nil, fmt.Errorf("%w: resourceOwner is required", errInvalidRequest)
	}

	assocs := h.Backend.ListResources(req.ResourceOwner, req.ResourceShareArns, req.ResourceType)
	objs := make([]resourceObject, 0, len(assocs))

	for _, a := range assocs {
		objs = append(objs, toResourceObject(a))
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listResourcesResponse{NextToken: nextToken, Resources: page})
}

type listPendingInvitationResourcesRequest struct {
	MaxResults                 *int32 `json:"maxResults,omitempty"`
	ResourceShareInvitationArn string `json:"resourceShareInvitationArn"`
	NextToken                  string `json:"nextToken"`
}

type listPendingInvitationResourcesResponse struct {
	NextToken string           `json:"nextToken,omitempty"`
	Resources []resourceObject `json:"resources"`
}

func (h *Handler) handleListPendingInvitationResources(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req listPendingInvitationResourcesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareInvitationArn == "" {
		return nil, fmt.Errorf("%w: resourceShareInvitationArn is required", errInvalidRequest)
	}

	assocs, err := h.Backend.ListPendingInvitationResources(req.ResourceShareInvitationArn)
	if err != nil {
		return nil, err
	}

	objs := make([]resourceObject, 0, len(assocs))

	for _, a := range assocs {
		objs = append(objs, toResourceObject(a))
	}

	page, nextToken, pErr := ramPaginate(objs, req.NextToken, req.MaxResults)
	if pErr != nil {
		return nil, pErr
	}

	return json.Marshal(
		listPendingInvitationResourcesResponse{NextToken: nextToken, Resources: page},
	)
}

type getResourcePoliciesRequest struct {
	MaxResults   *int32   `json:"maxResults,omitempty"`
	NextToken    string   `json:"nextToken"`
	ResourceArns []string `json:"resourceArns"`
}

type getResourcePoliciesResponse struct {
	NextToken string   `json:"nextToken,omitempty"`
	Policies  []string `json:"policies"`
}

func (h *Handler) handleGetResourcePolicies(_ context.Context, body []byte) ([]byte, error) {
	var req getResourcePoliciesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	policies := h.Backend.GetResourcePolicies(req.ResourceArns)

	page, nextToken, err := ramPaginate(policies, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(getResourcePoliciesResponse{NextToken: nextToken, Policies: page})
}

type resourceTypeObject struct {
	ResourceType        string `json:"resourceType"`
	ServiceName         string `json:"serviceName"`
	ResourceRegionScope string `json:"resourceRegionScope"`
}

type listResourceTypesResponse struct {
	NextToken     string               `json:"nextToken,omitempty"`
	ResourceTypes []resourceTypeObject `json:"resourceTypes"`
}

const (
	serviceNameEC2             = "ec2"
	serviceNameRoute53Resolver = "route53resolver"
	serviceNameGlue            = "glue"
	serviceNameNetworkFirewall = "network-firewall"
	serviceNameCodeBuild       = "codebuild"
)

//nolint:gochecknoglobals // read-only table initialized once; represents the AWS-supported shareable resource types
var awsShareableResourceTypes = []resourceTypeObject{
	{
		ResourceType:        "ec2:Subnet",
		ServiceName:         serviceNameEC2,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "ec2:VPC",
		ServiceName:         serviceNameEC2,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "ec2:TransitGateway",
		ServiceName:         serviceNameEC2,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "ec2:LocalGateway",
		ServiceName:         serviceNameEC2,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "ec2:PrefixList",
		ServiceName:         serviceNameEC2,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "route53resolver:ResolverRule",
		ServiceName:         serviceNameRoute53Resolver,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "route53resolver:FirewallRuleGroup",
		ServiceName:         serviceNameRoute53Resolver,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "license-manager:LicenseConfiguration",
		ServiceName:         "license-manager",
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "codebuild:Project",
		ServiceName:         serviceNameCodeBuild,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "codebuild:ReportGroup",
		ServiceName:         serviceNameCodeBuild,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "glue:Catalog",
		ServiceName:         serviceNameGlue,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "glue:Database",
		ServiceName:         serviceNameGlue,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "glue:Table",
		ServiceName:         serviceNameGlue,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "appmesh:Mesh",
		ServiceName:         "appmesh",
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "outposts:Outpost",
		ServiceName:         "outposts",
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "resource-groups:Group",
		ServiceName:         "resource-groups",
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "ssm-contacts:Contact",
		ServiceName:         "ssm-contacts",
		ResourceRegionScope: resourceRegionScopeGlobal,
	},
	{
		ResourceType:        "ssm-incidents:ResponsePlan",
		ServiceName:         "ssm-incidents",
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "network-firewall:FirewallPolicy",
		ServiceName:         serviceNameNetworkFirewall,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "network-firewall:StatefulRuleGroup",
		ServiceName:         serviceNameNetworkFirewall,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
	{
		ResourceType:        "network-firewall:StatelessRuleGroup",
		ServiceName:         serviceNameNetworkFirewall,
		ResourceRegionScope: resourceRegionScopeRegional,
	},
}

func (h *Handler) handleListResourceTypes(_ context.Context, _ []byte) ([]byte, error) {
	return json.Marshal(listResourceTypesResponse{ResourceTypes: awsShareableResourceTypes})
}

// associatedSourceObject is the JSON representation of an AssociatedSource (RAM's wire
// shape for a "source association" -- these control which sources, e.g. an Organizations
// OU, may be used with service-principal shares). This backend always returns an empty
// list: the RAM API has no operation that creates a source association at all (confirmed
// against every api_op_*.go in the SDK module -- there is no CreateSourceAssociation or
// similar); they can only be populated by other AWS services acting behind the scenes.
// An always-empty list is therefore the correct, not a stubbed, response for a backend
// whose only surface is the RAM API itself.
type associatedSourceObject struct {
	ResourceShareArn string  `json:"resourceShareArn"`
	SourceID         string  `json:"sourceId"`
	SourceType       string  `json:"sourceType"`
	Status           string  `json:"status"`
	StatusMessage    string  `json:"statusMessage,omitempty"`
	CreationTime     float64 `json:"creationTime"`
	LastUpdatedTime  float64 `json:"lastUpdatedTime"`
}

type listSourceAssociationsResponse struct {
	NextToken          string                   `json:"nextToken,omitempty"`
	SourceAssociations []associatedSourceObject `json:"sourceAssociations"`
}

func (h *Handler) handleListSourceAssociations(_ context.Context, _ []byte) ([]byte, error) {
	return json.Marshal(listSourceAssociationsResponse{
		SourceAssociations: []associatedSourceObject{},
	})
}
