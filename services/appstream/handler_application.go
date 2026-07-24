package appstream

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Application handlers ---

type createApplicationInput struct {
	Tags        map[string]string `json:"Tags"`
	Name        string            `json:"Name"`
	DisplayName string            `json:"DisplayName"`
	Description string            `json:"Description"`
	LaunchPath  string            `json:"LaunchPath"`
	AppBlockArn string            `json:"AppBlockArn"`
	Platforms   []string          `json:"Platforms"`
}

func (h *Handler) opCreateApplication(_ context.Context, body []byte) (any, error) {
	var req createApplicationInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	app, err := h.Backend.CreateApplication(
		req.Name, req.DisplayName, req.Description, req.LaunchPath,
		req.AppBlockArn, req.Platforms, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Application": applicationToResponse(app)}, nil
}

type deleteApplicationInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteApplication(_ context.Context, body []byte) (any, error) {
	var req deleteApplicationInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteApplication(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeApplicationsInput struct {
	Arns []string `json:"Arns"`
}

func (h *Handler) opDescribeApplications(_ context.Context, body []byte) (any, error) {
	var req describeApplicationsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	apps, err := h.Backend.DescribeApplications(req.Arns)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(apps))
	for _, app := range apps {
		resp = append(resp, applicationToResponse(app))
	}

	return map[string]any{"Applications": resp}, nil
}

type updateApplicationInput struct {
	Name        string `json:"Name"`
	DisplayName string `json:"DisplayName"`
	Description string `json:"Description"`
	LaunchPath  string `json:"LaunchPath"`
}

func (h *Handler) opUpdateApplication(_ context.Context, body []byte) (any, error) {
	var req updateApplicationInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	app, err := h.Backend.UpdateApplication(req.Name, req.DisplayName, req.Description, req.LaunchPath)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Application": applicationToResponse(app)}, nil
}

func (h *Handler) opDescribeAppLicenseUsage(_ context.Context, _ []byte) (any, error) {
	usage, err := h.Backend.DescribeAppLicenseUsage()
	if err != nil {
		return nil, err
	}

	// Real DescribeAppLicenseUsageOutput carries AppLicenseUsages (plural).
	return map[string]any{"AppLicenseUsages": usage}, nil
}

// --- Application-Fleet association handlers ---

type applicationFleetInput struct {
	ApplicationArn string `json:"ApplicationArn"`
	FleetName      string `json:"FleetName"`
}

func (h *Handler) opAssociateApplicationFleet(_ context.Context, body []byte) (any, error) {
	var req applicationFleetInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.AssociateApplicationFleet(req.ApplicationArn, req.FleetName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opDisassociateApplicationFleet(_ context.Context, body []byte) (any, error) {
	var req applicationFleetInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateApplicationFleet(req.ApplicationArn, req.FleetName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeApplicationFleetAssociationsInput struct {
	ApplicationArn string `json:"ApplicationArn"`
	FleetName      string `json:"FleetName"`
}

func (h *Handler) opDescribeApplicationFleetAssociations(_ context.Context, body []byte) (any, error) {
	var req describeApplicationFleetAssociationsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	assocs, err := h.Backend.DescribeApplicationFleetAssociations(req.ApplicationArn, req.FleetName)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(assocs))
	for _, a := range assocs {
		resp = append(resp, map[string]any{
			"ApplicationArn": a.ApplicationArn,
			"FleetName":      a.FleetName,
			"State":          a.State, //nolint:goconst // existing issue.
		})
	}

	return map[string]any{"ApplicationFleetAssociations": resp}, nil
}

// --- Entitlement handlers ---

type entitlementAttributeJSON struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

type createEntitlementInput struct {
	Name          string                     `json:"Name"`
	StackName     string                     `json:"StackName"`
	Description   string                     `json:"Description"`
	AppVisibility string                     `json:"AppVisibility"`
	Attributes    []entitlementAttributeJSON `json:"Attributes"`
}

func toEntitlementAttributes(attrs []entitlementAttributeJSON) []EntitlementAttribute {
	result := make([]EntitlementAttribute, len(attrs))
	for i, a := range attrs {
		result[i] = EntitlementAttribute(a)
	}

	return result
}

func (h *Handler) opCreateEntitlement(_ context.Context, body []byte) (any, error) {
	var req createEntitlementInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	ent, err := h.Backend.CreateEntitlement(
		req.Name, req.StackName, req.Description, req.AppVisibility,
		toEntitlementAttributes(req.Attributes),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Entitlement": entitlementToResponse(ent)}, nil
}

type deleteEntitlementInput struct {
	Name      string `json:"Name"`
	StackName string `json:"StackName"`
}

func (h *Handler) opDeleteEntitlement(_ context.Context, body []byte) (any, error) {
	var req deleteEntitlementInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteEntitlement(req.Name, req.StackName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeEntitlementsInput struct {
	Name      string `json:"Name"`
	StackName string `json:"StackName"`
}

func (h *Handler) opDescribeEntitlements(_ context.Context, body []byte) (any, error) {
	var req describeEntitlementsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	ents, err := h.Backend.DescribeEntitlements(req.Name, req.StackName)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(ents))
	for _, e := range ents {
		resp = append(resp, entitlementToResponse(e))
	}

	return map[string]any{"Entitlements": resp}, nil
}

type updateEntitlementInput struct {
	Name          string                     `json:"Name"`
	StackName     string                     `json:"StackName"`
	Description   string                     `json:"Description"`
	AppVisibility string                     `json:"AppVisibility"`
	Attributes    []entitlementAttributeJSON `json:"Attributes"`
}

func (h *Handler) opUpdateEntitlement(_ context.Context, body []byte) (any, error) {
	var req updateEntitlementInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	ent, err := h.Backend.UpdateEntitlement(
		req.Name, req.StackName, req.Description, req.AppVisibility,
		toEntitlementAttributes(req.Attributes),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Entitlement": entitlementToResponse(ent)}, nil
}

type associateApplicationToEntitlementInput struct {
	ApplicationIdentifier string `json:"ApplicationIdentifier"`
	EntitlementName       string `json:"EntitlementName"`
	StackName             string `json:"StackName"`
}

func (h *Handler) opAssociateApplicationToEntitlement(_ context.Context, body []byte) (any, error) {
	var req associateApplicationToEntitlementInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.AssociateApplicationToEntitlement(
		req.ApplicationIdentifier, req.EntitlementName, req.StackName,
	); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type disassociateApplicationFromEntitlementInput struct {
	ApplicationIdentifier string `json:"ApplicationIdentifier"`
	EntitlementName       string `json:"EntitlementName"`
	StackName             string `json:"StackName"`
}

func (h *Handler) opDisassociateApplicationFromEntitlement(_ context.Context, body []byte) (any, error) {
	var req disassociateApplicationFromEntitlementInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateApplicationFromEntitlement(
		req.ApplicationIdentifier, req.EntitlementName, req.StackName,
	); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type listEntitledApplicationsInput struct {
	EntitlementName string `json:"EntitlementName"`
	StackName       string `json:"StackName"`
}

func (h *Handler) opListEntitledApplications(_ context.Context, body []byte) (any, error) {
	var req listEntitledApplicationsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	apps, err := h.Backend.ListEntitledApplications(req.EntitlementName, req.StackName)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(apps))
	for _, a := range apps {
		resp = append(resp, map[string]any{"ApplicationIdentifier": a.ApplicationIdentifier})
	}

	return map[string]any{"EntitledApplications": resp}, nil
}

// --- DirectoryConfig handlers ---

// serviceAccountCredentialsJSON mirrors the real ServiceAccountCredentials
// wire shape (both members required together).
type serviceAccountCredentialsJSON struct {
	AccountName     string `json:"AccountName"`
	AccountPassword string `json:"AccountPassword"`
}

func (j *serviceAccountCredentialsJSON) toModel() ServiceAccountCredentials {
	if j == nil {
		return ServiceAccountCredentials{}
	}

	return ServiceAccountCredentials(*j)
}

// certificateBasedAuthPropertiesJSON mirrors the real
// CertificateBasedAuthProperties wire shape.
type certificateBasedAuthPropertiesJSON struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	Status                  string `json:"Status"`
}

func (j *certificateBasedAuthPropertiesJSON) toModel() CertificateBasedAuthProperties {
	if j == nil {
		return CertificateBasedAuthProperties{}
	}

	return CertificateBasedAuthProperties(*j)
}

type createDirectoryConfigInput struct {
	ServiceAccountCredentials            *serviceAccountCredentialsJSON      `json:"ServiceAccountCredentials"`
	CertificateBasedAuthProperties       *certificateBasedAuthPropertiesJSON `json:"CertificateBasedAuthProperties"`
	DirectoryName                        string                              `json:"DirectoryName"`
	OrganizationalUnitDistinguishedNames []string                            `json:"OrganizationalUnitDistinguishedNames"`
}

func (h *Handler) opCreateDirectoryConfig(_ context.Context, body []byte) (any, error) {
	var req createDirectoryConfigInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	dc, err := h.Backend.CreateDirectoryConfig(
		req.DirectoryName, req.OrganizationalUnitDistinguishedNames,
		req.ServiceAccountCredentials.toModel(), req.CertificateBasedAuthProperties.toModel(),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DirectoryConfig": directoryConfigToResponse(dc)}, nil
}

type deleteDirectoryConfigInput struct {
	DirectoryName string `json:"DirectoryName"`
}

func (h *Handler) opDeleteDirectoryConfig(_ context.Context, body []byte) (any, error) {
	var req deleteDirectoryConfigInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteDirectoryConfig(req.DirectoryName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeDirectoryConfigsInput struct {
	DirectoryNames []string `json:"DirectoryNames"`
}

func (h *Handler) opDescribeDirectoryConfigs(_ context.Context, body []byte) (any, error) {
	var req describeDirectoryConfigsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	dcs, err := h.Backend.DescribeDirectoryConfigs(req.DirectoryNames)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(dcs))
	for _, dc := range dcs {
		resp = append(resp, directoryConfigToResponse(dc))
	}

	return map[string]any{"DirectoryConfigs": resp}, nil
}

type updateDirectoryConfigInput struct {
	ServiceAccountCredentials            *serviceAccountCredentialsJSON      `json:"ServiceAccountCredentials"`
	CertificateBasedAuthProperties       *certificateBasedAuthPropertiesJSON `json:"CertificateBasedAuthProperties"`
	DirectoryName                        string                              `json:"DirectoryName"`
	OrganizationalUnitDistinguishedNames []string                            `json:"OrganizationalUnitDistinguishedNames"`
}

func (h *Handler) opUpdateDirectoryConfig(_ context.Context, body []byte) (any, error) {
	var req updateDirectoryConfigInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	dc, err := h.Backend.UpdateDirectoryConfig(
		req.DirectoryName, req.OrganizationalUnitDistinguishedNames,
		req.ServiceAccountCredentials.toModel(), req.CertificateBasedAuthProperties.toModel(),
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DirectoryConfig": directoryConfigToResponse(dc)}, nil
}

// --- Response helpers ---

func applicationToResponse(app *Application) map[string]any {
	return map[string]any{
		"Name":        app.Name,        //nolint:goconst // existing issue.
		"Arn":         app.Arn,         //nolint:goconst // existing issue.
		"DisplayName": app.DisplayName, //nolint:goconst // existing issue.
		"Description": app.Description, //nolint:goconst // existing issue.
		"LaunchPath":  app.LaunchPath,
		"AppBlockArn": app.AppBlockArn,
		"Platforms":   app.Platforms,
		"CreatedTime": awstime.Epoch(app.CreatedTime), //nolint:goconst // existing issue.
		keyTags:       app.Tags,
	}
}

func entitlementToResponse(e *Entitlement) map[string]any {
	attrs := make([]map[string]string, len(e.Attributes))
	for i, a := range e.Attributes {
		attrs[i] = map[string]string{"Name": a.Name, "Value": a.Value}
	}

	return map[string]any{
		"Name":             e.Name,
		"StackName":        e.StackName, //nolint:goconst // existing issue.
		"Description":      e.Description,
		"AppVisibility":    e.AppVisibility,
		"Attributes":       attrs,
		"CreatedTime":      awstime.Epoch(e.CreatedTime),
		"LastModifiedTime": awstime.Epoch(e.LastModifiedAt),
	}
}

func directoryConfigToResponse(dc *DirectoryConfig) map[string]any {
	resp := map[string]any{
		"DirectoryName":                        dc.DirectoryName,
		"OrganizationalUnitDistinguishedNames": dc.OrganizationalUnitDistinguishedNames,
		"CreatedTime":                          awstime.Epoch(dc.CreatedTime),
	}

	if dc.ServiceAccountCredentials.AccountName != "" {
		resp["ServiceAccountCredentials"] = map[string]any{
			"AccountName":     dc.ServiceAccountCredentials.AccountName,
			"AccountPassword": dc.ServiceAccountCredentials.AccountPassword,
		}
	}

	certAuth := dc.CertificateBasedAuthProperties
	if certAuth.CertificateAuthorityArn != "" || certAuth.Status != "" {
		resp["CertificateBasedAuthProperties"] = map[string]any{
			"CertificateAuthorityArn": certAuth.CertificateAuthorityArn,
			"Status":                  certAuth.Status,
		}
	}

	return resp
}
