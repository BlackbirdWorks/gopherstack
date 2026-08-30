package cloudformation

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchStackSetOps handles StackSet lifecycle, instance, and organizations-access operations.
func (h *Handler) dispatchStackSetOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	if handled, err := h.dispatchStackSetCRUDOps(action, form, c); handled {
		return true, err
	}

	if handled, err := h.dispatchStackSetInstanceOps(action, form, c); handled {
		return true, err
	}

	return h.dispatchOrganizationsAccessOps(action, form, c)
}

// dispatchStackSetCRUDOps handles StackSet CRUD and basic instance operations.
func (h *Handler) dispatchStackSetCRUDOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "CreateStackSet":
		return true, h.handleCreateStackSet(form, c)
	case "UpdateStackSet":
		return true, h.handleUpdateStackSet(form, c)
	case "DeleteStackSet":
		return true, h.handleDeleteStackSet(form, c)
	case "DescribeStackSet":
		return true, h.handleDescribeStackSet(form, c)
	case "ListStackSets":
		return true, h.handleListStackSets(form, c)
	case "CreateStackInstances":
		return true, h.handleCreateStackInstances(form, c)
	case "DeleteStackInstances":
		return true, h.handleDeleteStackInstances(form, c)
	case "UpdateStackInstances":
		return true, h.handleUpdateStackInstances(form, c)
	case "ListStackInstances":
		return true, h.handleListStackInstances(form, c)
	}

	return false, nil
}

// dispatchStackSetInstanceOps handles StackSet instance detail and operation tracking.
func (h *Handler) dispatchStackSetInstanceOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "DescribeStackInstance":
		return true, h.handleDescribeStackInstance(form, c)
	case "DetectStackSetDrift":
		return true, h.handleDetectStackSetDrift(form, c)
	case "ListStackSetOperations":
		return true, h.handleListStackSetOperations(form, c)
	case "DescribeStackSetOperation":
		return true, h.handleDescribeStackSetOperation(form, c)
	case "StopStackSetOperation":
		return true, h.handleStopStackSetOperation(form, c)
	case "ListStackSetOperationResults":
		return true, h.handleListStackSetOperationResults(form, c)
	case "ListStackSetAutoDeploymentTargets":
		return true, h.handleListStackSetAutoDeploymentTargets(form, c)
	case "ImportStacksToStackSet":
		return true, h.handleImportStacksToStackSet(form, c)
	case "ListStackInstanceResourceDrifts":
		return true, h.handleListStackInstanceResourceDrifts(form, c)
	}

	return false, nil
}

// dispatchOrganizationsAccessOps handles AWS Organizations trusted-access operations.
func (h *Handler) dispatchOrganizationsAccessOps(
	action string,
	_ url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "ActivateOrganizationsAccess":
		return true, h.handleActivateOrganizationsAccess(c)
	case "DeactivateOrganizationsAccess":
		return true, h.handleDeactivateOrganizationsAccess(c)
	case "DescribeOrganizationsAccess":
		return true, h.handleDescribeOrganizationsAccess(c)
	default:
		return false, nil
	}
}

// parseStackSetOptions parses the optional CreateStackSet/UpdateStackSet form
// fields (administration/execution roles, capabilities, parameters, tags,
// permission model, OU targets, auto-deployment, managed execution) shared by
// both operations' request shapes.
func parseStackSetOptions(form url.Values) StackSetOptions {
	opts := StackSetOptions{
		AdministrationRoleARN: form.Get("AdministrationRoleARN"),
		ExecutionRoleName:     form.Get("ExecutionRoleName"),
		PermissionModel:       form.Get("PermissionModel"),
		Capabilities:          parseCapabilities(form),
		Parameters:            parseParams(form),
		Tags:                  parseTags(form),
		OrganizationalUnitIDs: parseMemberList(form, "OrganizationalUnitIds."),
	}

	if v := form.Get("AutoDeployment.Enabled"); v != "" {
		opts.AutoDeployment = &AutoDeployment{
			Enabled:                      v == boolTrue,
			RetainStacksOnAccountRemoval: form.Get("AutoDeployment.RetainStacksOnAccountRemoval") == boolTrue,
		}
	}

	if v := form.Get("ManagedExecution.Active"); v != "" {
		opts.ManagedExecution = &ManagedExecution{Active: v == boolTrue}
	}

	return opts
}

// stackInstancesErrorCode maps CreateStackInstances/DeleteStackInstances/
// UpdateStackInstances backend errors to their XML error code.
func stackInstancesErrorCode(err error) string {
	if errors.Is(err, ErrStackSetNotFound) {
		return "StackSetNotFoundException"
	}

	return "ValidationError"
}

func (h *Handler) handleCreateStackSet(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	ss, err := h.Backend.CreateStackSet(
		name, form.Get("Description"), form.Get("TemplateBody"), parseStackSetOptions(form),
	)
	if err != nil {
		return h.xmlError(c, "NameAlreadyExistsException", err.Error())
	}
	type result struct {
		StackSetID string `xml:"StackSetId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateStackSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{StackSetID: ss.StackSetID},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleUpdateStackSet(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	_, opID, err := h.Backend.UpdateStackSet(
		name, form.Get("Description"), form.Get("TemplateBody"), parseStackSetOptions(form),
	)
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type result struct {
		OperationID string `xml:"OperationId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"UpdateStackSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{OperationID: opID}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleDeleteStackSet(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	if err := h.Backend.DeleteStackSet(name); err != nil {
		if errors.Is(err, ErrStackSetNotEmpty) {
			return h.xmlError(c, "StackSetNotEmptyException", err.Error())
		}

		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"DeleteStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DeleteStackSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

// stackSetParamXML/stackSetTagXML mirror Parameter/Tag's XML shape for
// embedding inside a StackSet response (the shared Parameter/Tag structs
// carry no XML tags of their own since most call sites build their envelopes
// by hand -- see handler_stacks.go).
type stackSetParamXML struct {
	ParameterKey   string `xml:"ParameterKey"`
	ParameterValue string `xml:"ParameterValue"`
}

type stackSetTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type stackSetAutoDeploymentXML struct {
	Enabled                      bool `xml:"Enabled"`
	RetainStacksOnAccountRemoval bool `xml:"RetainStacksOnAccountRemoval"` //nolint:lll // AWS-compatible field name exceeds line limit
}

type stackSetManagedExecutionXML struct {
	Active bool `xml:"Active"`
}

// ssXML is the full DescribeStackSetResult.StackSet wire shape, field-diffed
// against aws-sdk-go-v2/service/cloudformation@v1.76.1's
// awsAwsquery_deserializeDocumentStackSet.
type ssXML struct {
	AutoDeployment        *stackSetAutoDeploymentXML   `xml:"AutoDeployment,omitempty"`
	ManagedExecution      *stackSetManagedExecutionXML `xml:"ManagedExecution,omitempty"`
	Status                string                       `xml:"Status"`
	StackSetID            string                       `xml:"StackSetId"`
	StackSetName          string                       `xml:"StackSetName"`
	Description           string                       `xml:"Description,omitempty"`
	StackSetARN           string                       `xml:"StackSetARN,omitempty"`
	AdministrationRoleARN string                       `xml:"AdministrationRoleARN,omitempty"`
	ExecutionRoleName     string                       `xml:"ExecutionRoleName,omitempty"`
	PermissionModel       string                       `xml:"PermissionModel,omitempty"`
	OrganizationalUnitIDs []string                     `xml:"OrganizationalUnitIds>member,omitempty"`
	Regions               []string                     `xml:"Regions>member,omitempty"`
	Tags                  []stackSetTagXML             `xml:"Tags>member,omitempty"`
	Parameters            []stackSetParamXML           `xml:"Parameters>member,omitempty"`
	Capabilities          []string                     `xml:"Capabilities>member,omitempty"`
}

func stackSetToXML(ss *StackSet, regions []string) ssXML {
	params := make([]stackSetParamXML, 0, len(ss.Parameters))
	for _, p := range ss.Parameters {
		params = append(params, stackSetParamXML{ParameterKey: p.ParameterKey, ParameterValue: p.ParameterValue})
	}
	tags := make([]stackSetTagXML, 0, len(ss.Tags))
	for _, t := range ss.Tags {
		tags = append(tags, stackSetTagXML(t))
	}

	x := ssXML{
		StackSetID:            ss.StackSetID,
		StackSetName:          ss.StackSetName,
		Status:                ss.Status,
		Description:           ss.Description,
		StackSetARN:           ss.StackSetARN,
		AdministrationRoleARN: ss.AdministrationRoleARN,
		ExecutionRoleName:     ss.ExecutionRoleName,
		PermissionModel:       ss.PermissionModel,
		Capabilities:          ss.Capabilities,
		Parameters:            params,
		Tags:                  tags,
		OrganizationalUnitIDs: ss.OrganizationalUnitIDs,
		Regions:               regions,
	}
	if ss.AutoDeployment != nil {
		x.AutoDeployment = &stackSetAutoDeploymentXML{
			Enabled:                      ss.AutoDeployment.Enabled,
			RetainStacksOnAccountRemoval: ss.AutoDeployment.RetainStacksOnAccountRemoval,
		}
	}
	if ss.ManagedExecution != nil {
		x.ManagedExecution = &stackSetManagedExecutionXML{Active: ss.ManagedExecution.Active}
	}

	return x
}

// describeStackSetResult/describeStackSetResponse are package-level (rather
// than function-local, like most other handlers' envelope types in this
// file) purely so govet's fieldalignment can reorder ssXML's large
// slice/pointer payload against the small XMLName/Xmlns/RequestID fields --
// it does not rewrite function-local type declarations.
type describeStackSetResult struct {
	StackSet ssXML `xml:"StackSet"`
}

type describeStackSetResponse struct {
	XMLName   xml.Name               `xml:"DescribeStackSetResponse"`
	Xmlns     string                 `xml:"xmlns,attr"`
	RequestID string                 `xml:"ResponseMetadata>RequestId"`
	Result    describeStackSetResult `xml:"DescribeStackSetResult"`
}

func (h *Handler) handleDescribeStackSet(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	ss, err := h.Backend.DescribeStackSet(name)
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}

	return writeXML(c, describeStackSetResponse{
		Xmlns:     cfnNS,
		Result:    describeStackSetResult{StackSet: stackSetToXML(ss, h.Backend.StackSetRegions(name))},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListStackSets(form url.Values, c *echo.Context) error {
	p, err := h.Backend.ListStackSets(form.Get("NextToken"), form.Get("Status"))
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type summXML struct {
		StackSetID   string `xml:"StackSetId"`
		StackSetName string `xml:"StackSetName"`
		Status       string `xml:"Status"`
	}
	members := make([]summXML, 0, len(p.Data))
	for _, s := range p.Data {
		members = append(
			members,
			summXML{StackSetID: s.StackSetID, StackSetName: s.StackSetName, Status: s.Status},
		)
	}
	type result struct {
		NextToken string    `xml:"NextToken,omitempty"`
		Summaries []summXML `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackSetsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackSetsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, Summaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

// unsupportedAccountFilterType returns the requested
// DeploymentTargets.AccountFilterType value if it's one this backend doesn't
// implement, or "" if the request should proceed. Only NONE (the union of
// Accounts and resolved OrganizationalUnitIds, this backend's only supported
// mode) passes; INTERSECTION/DIFFERENCE/UNION are rejected explicitly rather
// than silently computed as NONE (botocore cloudformation service-2.json
// AccountFilterType enum, botocore 1.43.56).
func unsupportedAccountFilterType(form url.Values) string {
	switch ft := form.Get("DeploymentTargets.AccountFilterType"); ft {
	case "", valueNone:
		return ""
	default:
		return ft
	}
}

// stackInstancesOp is CreateStackInstances or DeleteStackInstances -- same
// request shape (accounts/OU targets/regions in, an operation ID out).
type stackInstancesOp func(
	ctx context.Context, stackSetName string, accounts, ouIDs, regions []string,
) (string, error)

// handleStackInstancesOp parses the shared CreateStackInstances/
// DeleteStackInstances request shape, invokes op, and writes the shared
// {OperationId} response envelope under responseElem/resultElem.
func (h *Handler) handleStackInstancesOp(
	form url.Values, c *echo.Context, responseElem, resultElem string, op stackInstancesOp,
) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	if ft := unsupportedAccountFilterType(form); ft != "" {
		return h.xmlError(c, "ValidationError",
			fmt.Sprintf("DeploymentTargets.AccountFilterType %s is not supported", ft))
	}
	accounts := parseMemberList(form, "Accounts.")
	ouIDs := parseMemberList(form, "DeploymentTargets.OrganizationalUnitIds.")
	regions := parseMemberList(form, "Regions.")
	opID, err := op(c.Request().Context(), name, accounts, ouIDs, regions)
	if err != nil {
		return h.xmlError(c, stackInstancesErrorCode(err), err.Error())
	}
	type result struct {
		XMLName     xml.Name
		OperationID string `xml:"OperationId"`
	}
	type response struct {
		XMLName   xml.Name
		Xmlns     string `xml:"xmlns,attr"`
		Result    result
		RequestID string `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		XMLName:   xml.Name{Local: responseElem},
		Xmlns:     cfnNS,
		Result:    result{XMLName: xml.Name{Local: resultElem}, OperationID: opID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleCreateStackInstances(form url.Values, c *echo.Context) error {
	return h.handleStackInstancesOp(
		form, c, "CreateStackInstancesResponse", "CreateStackInstancesResult", h.Backend.CreateStackInstances,
	)
}

func (h *Handler) handleDeleteStackInstances(form url.Values, c *echo.Context) error {
	return h.handleStackInstancesOp(
		form, c, "DeleteStackInstancesResponse", "DeleteStackInstancesResult", h.Backend.DeleteStackInstances,
	)
}

func (h *Handler) handleUpdateStackInstances(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	if ft := unsupportedAccountFilterType(form); ft != "" {
		return h.xmlError(c, "ValidationError",
			fmt.Sprintf("DeploymentTargets.AccountFilterType %s is not supported", ft))
	}
	accounts := parseMemberList(form, "Accounts.")
	ouIDs := parseMemberList(form, "DeploymentTargets.OrganizationalUnitIds.")
	regions := parseMemberList(form, "Regions.")
	opID, err := h.Backend.UpdateStackInstances(name, accounts, ouIDs, regions)
	if err != nil {
		return h.xmlError(c, stackInstancesErrorCode(err), err.Error())
	}
	type result struct {
		OperationID string `xml:"OperationId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateStackInstancesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"UpdateStackInstancesResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{OperationID: opID}, RequestID: uuid.New().String()},
	)
}

// parseStackInstanceFilters parses Filters.member.N.{Name,Values} into a
// ListStackInstancesFilter. DETAILED_STATUS entries are ignored (see
// ListStackInstancesFilter's doc comment for why); unrecognized Name values
// are ignored too rather than rejected, matching this handler's existing
// leniency elsewhere.
func parseStackInstanceFilters(form url.Values) ListStackInstancesFilter {
	filter := ListStackInstancesFilter{
		StackInstanceAccount: form.Get("StackInstanceAccount"),
		StackInstanceRegion:  form.Get("StackInstanceRegion"),
	}
	for i := 1; ; i++ {
		name := form.Get(fmt.Sprintf("Filters.member.%d.Name", i))
		if name == "" {
			break
		}
		value := form.Get(fmt.Sprintf("Filters.member.%d.Values", i))
		switch name {
		case "DRIFT_STATUS":
			filter.DriftStatus = value
		case "LAST_OPERATION_ID":
			filter.LastOperationID = value
		}
	}

	return filter
}

func (h *Handler) handleListStackInstances(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	p, err := h.Backend.ListStackInstances(name, form.Get("NextToken"), parseStackInstanceFilters(form))
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type instXML struct {
		StackSetName         string `xml:"StackSetName,omitempty"`
		Account              string `xml:"Account,omitempty"`
		Region               string `xml:"Region,omitempty"`
		Status               string `xml:"Status,omitempty"`
		OrganizationalUnitID string `xml:"OrganizationalUnitId,omitempty"`
	}
	members := make([]instXML, 0, len(p.Data))
	for _, i := range p.Data {
		members = append(
			members,
			instXML{
				StackSetName:         i.StackSetName,
				Account:              i.Account,
				Region:               i.Region,
				Status:               i.Status,
				OrganizationalUnitID: i.OrganizationalUnitID,
			},
		)
	}
	type result struct {
		NextToken string    `xml:"NextToken,omitempty"`
		Summaries []instXML `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackInstancesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackInstancesResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, Summaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDescribeStackInstance(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	account := form.Get("StackInstanceAccount")
	region := form.Get("StackInstanceRegion")
	inst, err := h.Backend.DescribeStackInstance(name, account, region)
	if err != nil {
		if errors.Is(err, ErrStackSetNotFound) {
			return h.xmlError(c, "StackSetNotFoundException", err.Error())
		}

		return h.xmlError(c, "StackInstanceNotFoundException", err.Error())
	}
	type instXML struct {
		StackSetName         string `xml:"StackSetName,omitempty"`
		Account              string `xml:"Account,omitempty"`
		Region               string `xml:"Region,omitempty"`
		Status               string `xml:"Status,omitempty"`
		OrganizationalUnitID string `xml:"OrganizationalUnitId,omitempty"`
	}
	type result struct {
		StackInstance instXML `xml:"StackInstance"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeStackInstanceResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeStackInstanceResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: result{
			StackInstance: instXML{
				StackSetName:         inst.StackSetName,
				Account:              inst.Account,
				Region:               inst.Region,
				Status:               inst.Status,
				OrganizationalUnitID: inst.OrganizationalUnitID,
			},
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDetectStackSetDrift(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	opID, err := h.Backend.DetectStackSetDrift(name)
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type result struct {
		OperationID string `xml:"OperationId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DetectStackSetDriftResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DetectStackSetDriftResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{OperationID: opID}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleListStackSetOperations(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	p, _ := h.Backend.ListStackSetOperations(name, form.Get("NextToken"))
	type opXML struct {
		OperationID string `xml:"OperationId"`
		Action      string `xml:"Action"`
		Status      string `xml:"Status"`
	}
	members := make([]opXML, 0, len(p.Data))
	for _, op := range p.Data {
		members = append(members, opXML{
			OperationID: op.OperationID,
			Action:      op.Action,
			Status:      op.Status,
		})
	}
	type result struct {
		NextToken string  `xml:"NextToken,omitempty"`
		Summaries []opXML `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackSetOperationsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackSetOperationsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, Summaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDescribeStackSetOperation(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	opID := form.Get("OperationId")
	op, err := h.Backend.DescribeStackSetOperation(name, opID)
	if err != nil {
		if errors.Is(err, ErrStackSetNotFound) {
			return h.xmlError(c, "StackSetNotFoundException", err.Error())
		}

		return h.xmlError(c, "OperationNotFoundException", err.Error())
	}
	type result struct {
		StackSetOperation struct {
			OperationID string `xml:"OperationId"`
			Action      string `xml:"Action"`
			Status      string `xml:"Status"`
		} `xml:"StackSetOperation"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeStackSetOperationResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeStackSetOperationResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}
	r := result{}
	r.StackSetOperation.OperationID = op.OperationID
	r.StackSetOperation.Action = op.Action
	r.StackSetOperation.Status = op.Status

	return writeXML(c, response{Xmlns: cfnNS, Result: r, RequestID: uuid.New().String()})
}

func (h *Handler) handleStopStackSetOperation(form url.Values, c *echo.Context) error {
	if err := h.Backend.StopStackSetOperation(form.Get("StackSetName"), form.Get("OperationId")); err != nil {
		code := "OperationNotFoundException"
		if errors.Is(err, ErrOperationNotRunning) {
			code = "InvalidOperationException"
		}

		return h.xmlError(c, code, err.Error())
	}
	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"StopStackSetOperationResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"StopStackSetOperationResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleListStackSetAutoDeploymentTargets(form url.Values, c *echo.Context) error {
	targets, err := h.Backend.ListStackSetAutoDeploymentTargets(form.Get("StackSetName"))
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	// Real ListStackSetAutoDeploymentTargetsOutput wraps the list under
	// "Summaries", not "Targets" (cloudformation@v1.76.1 deserializers.go:
	// awsAwsquery_deserializeOpDocumentListStackSetAutoDeploymentTargetsOutput).
	type result struct {
		Targets []AutoDeploymentTarget `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackSetAutoDeploymentTargetsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackSetAutoDeploymentTargetsResult"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Targets: targets}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleImportStacksToStackSet(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	stackIDs := parseMemberList(form, "StackIds.")
	opID, err := h.Backend.ImportStacksToStackSet(name, stackIDs)
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type result struct {
		OperationID string `xml:"OperationId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ImportStacksToStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"ImportStacksToStackSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{OperationID: opID}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleListStackInstanceResourceDrifts(form url.Values, c *echo.Context) error {
	drifts, _ := h.Backend.ListStackInstanceResourceDrifts(
		form.Get("StackSetName"), form.Get("OperationId"),
		form.Get("StackInstanceAccount"), form.Get("StackInstanceRegion"),
	)
	type result struct {
		Summaries []StackResourceDrift `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackInstanceResourceDriftsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackInstanceResourceDriftsResult"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Summaries: drifts}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleActivateOrganizationsAccess(c *echo.Context) error {
	_ = h.Backend.ActivateOrganizationsAccess()
	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"ActivateOrganizationsAccessResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"ActivateOrganizationsAccessResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeactivateOrganizationsAccess(c *echo.Context) error {
	_ = h.Backend.DeactivateOrganizationsAccess()
	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"DeactivateOrganizationsAccessResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DeactivateOrganizationsAccessResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeOrganizationsAccess(c *echo.Context) error {
	status, _ := h.Backend.DescribeOrganizationsAccess()
	type result struct {
		Status string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeOrganizationsAccessResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeOrganizationsAccessResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Status: status}, RequestID: uuid.New().String()},
	)
}

// handleListStackSetOperationResults returns per-account/region operation results.
func (h *Handler) handleListStackSetOperationResults(form url.Values, c *echo.Context) error {
	stackSetName := form.Get("StackSetName")
	operationID := form.Get("OperationId")

	if stackSetName == "" || operationID == "" {
		return h.xmlError(c, "ValidationError", "StackSetName and OperationId are required")
	}

	results, err := h.Backend.ListStackSetOperationResults(stackSetName, operationID, "")
	if err != nil {
		if errors.Is(err, ErrStackSetNotFound) {
			return h.xmlError(c, "StackSetNotFoundException", err.Error())
		}

		return h.xmlError(c, "OperationNotFoundException", err.Error())
	}

	type accountGateXML struct {
		FunctionArn string `xml:"FunctionArn,omitempty"`
		Status      string `xml:"Status,omitempty"`
	}
	type resultXML struct {
		AccountGateResult *accountGateXML `xml:"AccountGateResult,omitempty"`
		Account           string          `xml:"Account,omitempty"`
		Region            string          `xml:"Region,omitempty"`
		Status            string          `xml:"Status,omitempty"`
		StatusReason      string          `xml:"StatusReason,omitempty"`
	}
	items := make([]resultXML, 0, len(results))
	for _, r := range results {
		item := resultXML{
			Account:      r.Account,
			Region:       r.Region,
			Status:       r.Status,
			StatusReason: r.StatusReason,
		}
		if r.AccountGateResult != nil {
			item.AccountGateResult = &accountGateXML{
				FunctionArn: r.AccountGateResult.FunctionArn,
				Status:      r.AccountGateResult.Status,
			}
		}
		items = append(items, item)
	}

	type response struct {
		XMLName   xml.Name `xml:"ListStackSetOperationResultsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    any      `xml:"ListStackSetOperationResultsResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}
	type resultWrapper struct {
		Summaries []resultXML `xml:"Summaries>member"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    resultWrapper{Summaries: items},
		RequestID: uuid.New().String(),
	})
}
