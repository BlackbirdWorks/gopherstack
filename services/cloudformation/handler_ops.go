package cloudformation

import (
	"encoding/xml"
	"errors"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchOps handles new extended CloudFormation operations (StackSets, types, scans, etc.).
func (h *Handler) dispatchOps(action string, form url.Values, c *echo.Context) (bool, error) {
	if handled, err := h.dispatchStackSetOps(action, form, c); handled {
		return handled, err
	}

	if handled, err := h.dispatchTemplateAndScanOps(action, form, c); handled {
		return handled, err
	}

	if handled, err := h.dispatchTypeOps(action, form, c); handled {
		return handled, err
	}

	return h.dispatchMiscOps(action, form, c)
}

// dispatchStackSetOps handles StackSet lifecycle and instance operations.
func (h *Handler) dispatchStackSetOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	if handled, err := h.dispatchStackSetCRUDOps(action, form, c); handled {
		return true, err
	}

	return h.dispatchStackSetInstanceOps(action, form, c)
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

// dispatchTemplateAndScanOps handles generated template and resource scan operations.
func (h *Handler) dispatchTemplateAndScanOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	if handled, err := h.dispatchGeneratedTemplateOps(action, form, c); handled {
		return true, err
	}

	return h.dispatchResourceScanOps(action, form, c)
}

// dispatchGeneratedTemplateOps handles generated template CRUD operations.
func (h *Handler) dispatchGeneratedTemplateOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "CreateGeneratedTemplate":
		return true, h.handleCreateGeneratedTemplate(form, c)
	case "UpdateGeneratedTemplate":
		return true, h.handleUpdateGeneratedTemplate(form, c)
	case "DeleteGeneratedTemplate":
		return true, h.handleDeleteGeneratedTemplate(form, c)
	case "DescribeGeneratedTemplate":
		return true, h.handleDescribeGeneratedTemplate(form, c)
	case "GetGeneratedTemplate":
		return true, h.handleGetGeneratedTemplate(form, c)
	case "ListGeneratedTemplates":
		return true, h.handleListGeneratedTemplates(form, c)
	}

	return false, nil
}

// dispatchResourceScanOps handles resource scan and stack refactor operations.
func (h *Handler) dispatchResourceScanOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "StartResourceScan":
		return true, h.handleStartResourceScan(form, c)
	case "DescribeResourceScan":
		return true, h.handleDescribeResourceScan(form, c)
	case "ListResourceScans":
		return true, h.handleListResourceScans(form, c)
	case "ListResourceScanResources":
		return true, h.handleListResourceScanResources(form, c)
	case "ListResourceScanRelatedResources":
		return true, h.handleListResourceScanRelatedResources(form, c)
	case "CreateStackRefactor":
		return true, h.handleCreateStackRefactor(form, c)
	case "DescribeStackRefactor":
		return true, h.handleDescribeStackRefactor(form, c)
	case "ExecuteStackRefactor":
		return true, h.handleExecuteStackRefactor(form, c)
	case "ListStackRefactors":
		return true, h.handleListStackRefactors(form, c)
	case "ListStackRefactorActions":
		return true, h.handleListStackRefactorActions(form, c)
	}

	return false, nil
}

// dispatchTypeOps handles CloudFormation type/extension registration and management.
func (h *Handler) dispatchTypeOps(action string, form url.Values, c *echo.Context) (bool, error) {
	if handled, err := h.dispatchTypeRegistrationOps(action, form, c); handled {
		return true, err
	}

	return h.dispatchTypeManagementOps(action, form, c)
}

// dispatchTypeRegistrationOps handles type registration and activation operations.
func (h *Handler) dispatchTypeRegistrationOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "ActivateType":
		return true, h.handleActivateType(form, c)
	case "DeactivateType":
		return true, h.handleDeactivateType(form, c)
	case "RegisterType":
		return true, h.handleRegisterType(form, c)
	case "DeregisterType":
		return true, h.handleDeregisterType(form, c)
	case "PublishType":
		return true, h.handlePublishType(form, c)
	case "SetTypeDefaultVersion":
		return true, h.handleSetTypeDefaultVersion(form, c)
	case "SetTypeConfiguration":
		return true, h.handleSetTypeConfiguration(form, c)
	case "BatchDescribeTypeConfigurations":
		return true, h.handleBatchDescribeTypeConfigurations(form, c)
	case "TestType":
		return true, h.handleTestType(form, c)
	}

	return false, nil
}

// dispatchTypeManagementOps handles type listing, publishing, and access management.
func (h *Handler) dispatchTypeManagementOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "ListTypes":
		return true, h.handleListTypes(form, c)
	case "ListTypeVersions":
		return true, h.handleListTypeVersions(form, c)
	case "ListTypeRegistrations":
		return true, h.handleListTypeRegistrations(form, c)
	case "DescribeTypeRegistration":
		return true, h.handleDescribeTypeRegistration(form, c)
	case "RegisterPublisher":
		return true, h.handleRegisterPublisher(form, c)
	case "DescribePublisher":
		return true, h.handleDescribePublisher(form, c)
	case "ActivateOrganizationsAccess":
		return true, h.handleActivateOrganizationsAccess(c)
	case "DeactivateOrganizationsAccess":
		return true, h.handleDeactivateOrganizationsAccess(c)
	case "DescribeOrganizationsAccess":
		return true, h.handleDescribeOrganizationsAccess(c)
	}

	return false, nil
}

func (h *Handler) dispatchMiscOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "SignalResource":
		return true, h.handleSignalResource(form, c)
	case "RollbackStack":
		return true, h.handleRollbackStack(form, c)
	case "RecordHandlerProgress":
		return true, h.handleRecordHandlerProgress(form, c)
	case "GetHookResult":
		return true, h.handleGetHookResult(form, c)
	case "ListHookResults":
		return true, h.handleListHookResults(form, c)
	case "DescribeChangeSetHooks":
		return true, h.handleDescribeChangeSetHooks(form, c)
	case "DescribeEvents":
		return true, h.handleDescribeEvents(form, c)
	case "UpdateTerminationProtection":
		return true, h.handleUpdateTerminationProtection(form, c)
	case "ValidateTemplate":
		return true, h.handleValidateTemplate(form, c)
	default:
		return false, nil
	}
}

// ---- Stack Set handlers ----

func (h *Handler) handleCreateStackSet(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	ss, err := h.Backend.CreateStackSet(name, form.Get("Description"), form.Get("TemplateBody"))
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
	if _, err := h.Backend.UpdateStackSet(name, form.Get("Description"), form.Get("TemplateBody")); err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
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
	type response struct {
		XMLName   xml.Name `xml:"DeleteStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
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
	type ssXML struct {
		StackSetID   string `xml:"StackSetId"`
		StackSetName string `xml:"StackSetName"`
		Status       string `xml:"Status"`
		Description  string `xml:"Description,omitempty"`
	}
	type result struct {
		StackSet ssXML `xml:"StackSet"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeStackSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: result{
			StackSet: ssXML{
				StackSetID:   ss.StackSetID,
				StackSetName: ss.StackSetName,
				Status:       ss.Status,
				Description:  ss.Description,
			},
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListStackSets(form url.Values, c *echo.Context) error {
	p, err := h.Backend.ListStackSets(form.Get("NextToken"))
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

func (h *Handler) handleCreateStackInstances(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	accounts := parseMemberList(form, "Accounts.")
	regions := parseMemberList(form, "Regions.")
	opID, err := h.Backend.CreateStackInstances(name, accounts, regions)
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type result struct {
		OperationID string `xml:"OperationId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateStackInstancesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateStackInstancesResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{OperationID: opID}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleDeleteStackInstances(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	accounts := parseMemberList(form, "Accounts.")
	regions := parseMemberList(form, "Regions.")
	opID, err := h.Backend.DeleteStackInstances(name, accounts, regions)
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type result struct {
		OperationID string `xml:"OperationId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DeleteStackInstancesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DeleteStackInstancesResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{OperationID: opID}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleUpdateStackInstances(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackSetName is required")
	}
	accounts := parseMemberList(form, "Accounts.")
	regions := parseMemberList(form, "Regions.")
	opID, err := h.Backend.UpdateStackInstances(name, accounts, regions)
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
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

func (h *Handler) handleListStackInstances(form url.Values, c *echo.Context) error {
	name := form.Get("StackSetName")
	p, err := h.Backend.ListStackInstances(name, form.Get("NextToken"))
	if err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type instXML struct {
		StackSetName string `xml:"StackSetName,omitempty"`
		Account      string `xml:"Account,omitempty"`
		Region       string `xml:"Region,omitempty"`
		Status       string `xml:"Status,omitempty"`
	}
	members := make([]instXML, 0, len(p.Data))
	for _, i := range p.Data {
		members = append(
			members,
			instXML{
				StackSetName: i.StackSetName,
				Account:      i.Account,
				Region:       i.Region,
				Status:       i.Status,
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
		return h.xmlError(c, "StackInstanceNotFoundException", err.Error())
	}
	type instXML struct {
		StackSetName string `xml:"StackSetName,omitempty"`
		Account      string `xml:"Account,omitempty"`
		Region       string `xml:"Region,omitempty"`
		Status       string `xml:"Status,omitempty"`
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
				StackSetName: inst.StackSetName,
				Account:      inst.Account,
				Region:       inst.Region,
				Status:       inst.Status,
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
	_ = h.Backend.StopStackSetOperation(form.Get("StackSetName"), form.Get("OperationId"))
	type response struct {
		XMLName   xml.Name `xml:"StopStackSetOperationResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleListStackSetOperationResults(form url.Values, c *echo.Context) error {
	return h.handleListStackSetOperationResultsParity(form, c)
}

func (h *Handler) handleListStackSetAutoDeploymentTargets(form url.Values, c *echo.Context) error {
	targets, _ := h.Backend.ListStackSetAutoDeploymentTargets(form.Get("StackSetName"))
	type result struct {
		Targets []AutoDeploymentTarget `xml:"Targets>member"`
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
	if err := h.Backend.ImportStacksToStackSet(name, stackIDs); err != nil {
		return h.xmlError(c, "StackSetNotFoundException", err.Error())
	}
	type response struct {
		XMLName   xml.Name `xml:"ImportStacksToStackSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
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

// ---- Generated template handlers ----

func (h *Handler) handleCreateGeneratedTemplate(form url.Values, c *echo.Context) error {
	name := form.Get("GeneratedTemplateName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "GeneratedTemplateName is required")
	}
	gt, err := h.Backend.CreateGeneratedTemplate(name, nil)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		GeneratedTemplateID string `xml:"GeneratedTemplateId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateGeneratedTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{GeneratedTemplateID: gt.GeneratedTemplateID},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleUpdateGeneratedTemplate(form url.Values, c *echo.Context) error {
	id := form.Get("GeneratedTemplateId")
	if err := h.Backend.UpdateGeneratedTemplate(id, form.Get("NewGeneratedTemplateName")); err != nil {
		return h.xmlError(c, "GeneratedTemplateNotFoundException", err.Error())
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteGeneratedTemplate(form url.Values, c *echo.Context) error {
	_ = h.Backend.DeleteGeneratedTemplate(form.Get("GeneratedTemplateId"))
	type response struct {
		XMLName   xml.Name `xml:"DeleteGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeGeneratedTemplate(form url.Values, c *echo.Context) error {
	gt, err := h.Backend.DescribeGeneratedTemplate(form.Get("GeneratedTemplateId"))
	if err != nil {
		return h.xmlError(c, "GeneratedTemplateNotFoundException", err.Error())
	}
	type result struct {
		GeneratedTemplateID   string `xml:"GeneratedTemplateId"`
		GeneratedTemplateName string `xml:"GeneratedTemplateName"`
		Status                string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeGeneratedTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, Result: result{
		GeneratedTemplateID:   gt.GeneratedTemplateID,
		GeneratedTemplateName: gt.GeneratedTemplateName,
		Status:                gt.Status,
	}, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetGeneratedTemplate(form url.Values, c *echo.Context) error {
	body, err := h.Backend.GetGeneratedTemplate(form.Get("GeneratedTemplateId"))
	if err != nil {
		return h.xmlError(c, "GeneratedTemplateNotFoundException", err.Error())
	}
	type result struct {
		TemplateBody string `xml:"TemplateBody"`
		Status       string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"GetGeneratedTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{TemplateBody: body, Status: "COMPLETE"},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListGeneratedTemplates(form url.Values, c *echo.Context) error {
	p, _ := h.Backend.ListGeneratedTemplates(form.Get("NextToken"))
	type gtXML struct {
		GeneratedTemplateID   string `xml:"GeneratedTemplateId"`
		GeneratedTemplateName string `xml:"GeneratedTemplateName"`
		Status                string `xml:"Status"`
	}
	members := make([]gtXML, 0, len(p.Data))
	for _, t := range p.Data {
		members = append(
			members,
			gtXML{
				GeneratedTemplateID:   t.GeneratedTemplateID,
				GeneratedTemplateName: t.GeneratedTemplateName,
				Status:                t.Status,
			},
		)
	}
	type result struct {
		NextToken string  `xml:"NextToken,omitempty"`
		Summaries []gtXML `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListGeneratedTemplatesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListGeneratedTemplatesResult"`
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

// ---- Resource scan handlers ----

func (h *Handler) handleStartResourceScan(_ url.Values, c *echo.Context) error {
	scanID, err := h.Backend.StartResourceScan()
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		ResourceScanID string `xml:"ResourceScanId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"StartResourceScanResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"StartResourceScanResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{ResourceScanID: scanID},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDescribeResourceScan(form url.Values, c *echo.Context) error {
	rs, err := h.Backend.DescribeResourceScan(form.Get("ResourceScanId"))
	if err != nil {
		return h.xmlError(c, "ResourceScanNotFoundException", err.Error())
	}
	type result struct {
		ResourceScanID      string  `xml:"ResourceScanId"`
		Status              string  `xml:"Status"`
		PercentageCompleted float64 `xml:"PercentageCompleted"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeResourceScanResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"DescribeResourceScanResult"`
	}

	return writeXML(c, response{Xmlns: cfnNS, Result: result{
		ResourceScanID:      rs.ResourceScanID,
		Status:              rs.Status,
		PercentageCompleted: rs.PercentageCompleted,
	}, RequestID: uuid.New().String()})
}

func (h *Handler) handleListResourceScans(form url.Values, c *echo.Context) error {
	p, _ := h.Backend.ListResourceScans(form.Get("NextToken"))
	type scanXML struct {
		ResourceScanID string `xml:"ResourceScanId"`
		Status         string `xml:"Status"`
	}
	members := make([]scanXML, 0, len(p.Data))
	for _, s := range p.Data {
		members = append(members, scanXML{ResourceScanID: s.ResourceScanID, Status: s.Status})
	}
	type result struct {
		NextToken             string    `xml:"NextToken,omitempty"`
		ResourceScanSummaries []scanXML `xml:"ResourceScanSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListResourceScansResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListResourceScansResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, ResourceScanSummaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListResourceScanResources(form url.Values, c *echo.Context) error {
	scanned, _ := h.Backend.ListResourceScanResources(form.Get("ResourceScanId"), "")
	type resourceXML = ScannedResource
	members := append([]resourceXML(nil), scanned...)
	type result struct {
		Resources []resourceXML `xml:"Resources>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListResourceScanResourcesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListResourceScanResourcesResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{Resources: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListResourceScanRelatedResources(form url.Values, c *echo.Context) error {
	related, _ := h.Backend.ListResourceScanRelatedResources(form.Get("ResourceScanId"), nil)
	// related is []string (legacy plain identifiers).
	type result struct {
		RelatedResources []string `xml:"RelatedResources>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListResourceScanRelatedResourcesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListResourceScanRelatedResourcesResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{RelatedResources: related},
			RequestID: uuid.New().String(),
		},
	)
}

// ---- Type management handlers ----

func (h *Handler) handleActivateType(form url.Values, c *echo.Context) error {
	_ = h.Backend.ActivateType(form.Get("TypeName"), form.Get("TypeArn"))
	type response struct {
		XMLName   xml.Name `xml:"ActivateTypeResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeactivateType(form url.Values, c *echo.Context) error {
	_ = h.Backend.DeactivateType(form.Get("TypeName"), form.Get("TypeArn"))
	type response struct {
		XMLName   xml.Name `xml:"DeactivateTypeResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleRegisterType(form url.Values, c *echo.Context) error {
	token, err := h.Backend.RegisterType(form.Get("TypeName"), form.Get("SchemaHandlerPackage"))
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		RegistrationToken string `xml:"RegistrationToken"`
	}
	type response struct {
		XMLName   xml.Name `xml:"RegisterTypeResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"RegisterTypeResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{RegistrationToken: token},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDeregisterType(form url.Values, c *echo.Context) error {
	_ = h.Backend.DeregisterType(form.Get("Arn"))
	type response struct {
		XMLName   xml.Name `xml:"DeregisterTypeResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handlePublishType(form url.Values, c *echo.Context) error {
	_ = h.Backend.PublishType(form.Get("TypeName"))
	type response struct {
		XMLName   xml.Name `xml:"PublishTypeResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleSetTypeDefaultVersion(form url.Values, c *echo.Context) error {
	_ = h.Backend.SetTypeDefaultVersion(form.Get("Arn"), form.Get("VersionId"))
	type response struct {
		XMLName   xml.Name `xml:"SetTypeDefaultVersionResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleSetTypeConfiguration(form url.Values, c *echo.Context) error {
	_ = h.Backend.SetTypeConfiguration(form.Get("TypeName"), form.Get("Configuration"))
	type response struct {
		XMLName   xml.Name `xml:"SetTypeConfigurationResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleBatchDescribeTypeConfigurations(form url.Values, c *echo.Context) error {
	ids := parseMemberList(form, "TypeConfigurationIdentifiers.member.")
	details, _ := h.Backend.BatchDescribeTypeConfigurations(ids)
	type result struct {
		TypeConfigurations []TypeConfigurationDetail `xml:"TypeConfigurations>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"BatchDescribeTypeConfigurationsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"BatchDescribeTypeConfigurationsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{TypeConfigurations: details},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListTypes(_ url.Values, c *echo.Context) error {
	types, _ := h.Backend.ListTypes("")
	type typeXML struct {
		TypeName string `xml:"TypeName,omitempty"`
		TypeArn  string `xml:"TypeArn,omitempty"`
		Type     string `xml:"Type,omitempty"`
	}
	members := make([]typeXML, 0, len(types))
	for _, t := range types {
		members = append(members, typeXML{TypeName: t.TypeName, TypeArn: t.TypeArn, Type: t.Type})
	}
	type result struct {
		TypeSummaries []typeXML `xml:"TypeSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListTypesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListTypesResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{TypeSummaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListTypeVersions(form url.Values, c *echo.Context) error {
	versionIDs, _ := h.Backend.ListTypeVersions(form.Get("TypeName"), form.Get("Type"))
	type versionXML struct {
		TypeArn   string `xml:"TypeArn,omitempty"`
		VersionID string `xml:"VersionId,omitempty"`
	}
	members := make([]versionXML, 0, len(versionIDs))
	typeArn := "arn:aws:cloudformation:::type/resource/" + form.Get("TypeName")
	for _, v := range versionIDs {
		members = append(members, versionXML{TypeArn: typeArn, VersionID: v})
	}
	type result struct {
		TypeVersionSummaries []versionXML `xml:"TypeVersionSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListTypeVersionsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListTypeVersionsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{TypeVersionSummaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListTypeRegistrations(form url.Values, c *echo.Context) error {
	tokens, _ := h.Backend.ListTypeRegistrations(form.Get("TypeName"), form.Get("Type"))
	type result struct {
		RegistrationTokenList []string `xml:"RegistrationTokenList>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListTypeRegistrationsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListTypeRegistrationsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{RegistrationTokenList: tokens},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDescribeTypeRegistration(form url.Values, c *echo.Context) error {
	status, err := h.Backend.DescribeTypeRegistration(form.Get("RegistrationToken"))
	if err != nil {
		return h.xmlError(c, "CFNRegistryException", err.Error())
	}
	type result struct {
		ProgressStatus string `xml:"ProgressStatus"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeTypeRegistrationResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeTypeRegistrationResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{ProgressStatus: status},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleTestType(form url.Values, c *echo.Context) error {
	token, _ := h.Backend.TestType(form.Get("TypeName"), form.Get("Arn"))
	type result struct {
		TypeVersionArn string `xml:"TypeVersionArn,omitempty"`
	}
	type response struct {
		XMLName   xml.Name `xml:"TestTypeResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"TestTypeResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{TypeVersionArn: token},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleRegisterPublisher(form url.Values, c *echo.Context) error {
	id, _ := h.Backend.RegisterPublisher(form.Get("ConnectionArn"))
	type result struct {
		PublisherID string `xml:"PublisherId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"RegisterPublisherResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"RegisterPublisherResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{PublisherID: id}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleDescribePublisher(form url.Values, c *echo.Context) error {
	status, err := h.Backend.DescribePublisher(form.Get("PublisherId"))
	if err != nil {
		return h.xmlError(c, "CFNRegistryException", err.Error())
	}
	type result struct {
		PublisherStatus string `xml:"PublisherStatus"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribePublisherResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribePublisherResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{PublisherStatus: status},
			RequestID: uuid.New().String(),
		},
	)
}

// ---- Stack refactor handlers ----

func (h *Handler) handleCreateStackRefactor(form url.Values, c *echo.Context) error {
	id, err := h.Backend.CreateStackRefactor(form.Get("Description"), nil)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		StackRefactorID string `xml:"StackRefactorId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateStackRefactorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateStackRefactorResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{StackRefactorID: id}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleDescribeStackRefactor(form url.Values, c *echo.Context) error {
	status, _ := h.Backend.DescribeStackRefactor(form.Get("StackRefactorId"))
	type result struct {
		Status string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeStackRefactorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeStackRefactorResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Status: status}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleExecuteStackRefactor(form url.Values, c *echo.Context) error {
	_ = h.Backend.ExecuteStackRefactor(form.Get("StackRefactorId"))
	type response struct {
		XMLName   xml.Name `xml:"ExecuteStackRefactorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleListStackRefactors(form url.Values, c *echo.Context) error {
	summaries, _ := h.Backend.ListStackRefactors(form.Get("NextToken"))
	type result struct {
		StackRefactorSummaries []StackRefactorSummary `xml:"StackRefactorSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackRefactorsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackRefactorsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{StackRefactorSummaries: summaries},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListStackRefactorActions(form url.Values, c *echo.Context) error {
	actions, _ := h.Backend.ListStackRefactorActions(form.Get("StackRefactorId"))
	type result struct {
		StackRefactorActions []StackRefactorAction `xml:"StackRefactorActions>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackRefactorActionsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackRefactorActionsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{StackRefactorActions: actions},
			RequestID: uuid.New().String(),
		},
	)
}

// ---- Org access handlers ----

func (h *Handler) handleActivateOrganizationsAccess(c *echo.Context) error {
	_ = h.Backend.ActivateOrganizationsAccess()
	type response struct {
		XMLName   xml.Name `xml:"ActivateOrganizationsAccessResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeactivateOrganizationsAccess(c *echo.Context) error {
	_ = h.Backend.DeactivateOrganizationsAccess()
	type response struct {
		XMLName   xml.Name `xml:"DeactivateOrganizationsAccessResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
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

// ---- Misc handlers ----

func (h *Handler) handleSignalResource(form url.Values, c *echo.Context) error {
	_ = h.Backend.SignalResource(
		form.Get("StackName"),
		form.Get("LogicalResourceId"),
		form.Get("UniqueId"),
		form.Get("Status"),
	)
	type response struct {
		XMLName   xml.Name `xml:"SignalResourceResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleRollbackStack(form url.Values, c *echo.Context) error {
	name := form.Get("StackName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	if err := h.Backend.RollbackStack(c.Request().Context(), name); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type response struct {
		XMLName   xml.Name `xml:"RollbackStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleRecordHandlerProgress(form url.Values, c *echo.Context) error {
	_ = h.Backend.RecordHandlerProgress(form.Get("BearerToken"), form.Get("OperationStatus"))
	type response struct {
		XMLName   xml.Name `xml:"RecordHandlerProgressResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetHookResult(form url.Values, c *echo.Context) error {
	status, _ := h.Backend.GetHookResult(form.Get("HookResultToken"))
	type result struct {
		HookStatus string `xml:"HookStatus"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetHookResultResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"GetHookResultResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{HookStatus: status}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleListHookResults(form url.Values, c *echo.Context) error {
	results, _ := h.Backend.ListHookResults(form.Get("HookResultToken"), form.Get("NextToken"))
	type hookXML struct {
		HookStatus string `xml:"HookStatus,omitempty"`
		ErrorCode  string `xml:"ErrorCode,omitempty"`
	}
	members := make([]hookXML, 0, len(results))
	for _, r := range results {
		members = append(members, hookXML{HookStatus: r.HookStatus, ErrorCode: r.ErrorCode})
	}
	type result struct {
		HookResults []hookXML `xml:"HookResults>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListHookResultsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListHookResultsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{HookResults: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDescribeChangeSetHooks(form url.Values, c *echo.Context) error {
	hooks, _ := h.Backend.DescribeChangeSetHooks(form.Get("StackName"), form.Get("ChangeSetName"))
	type result struct {
		Hooks []ChangeSetHook `xml:"Hooks>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeChangeSetHooksResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"DescribeChangeSetHooksResult"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Hooks: hooks}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleDescribeEvents(form url.Values, c *echo.Context) error {
	p, _ := h.Backend.DescribeEvents(form.Get("StackName"), form.Get("NextToken"))
	type evXML struct {
		EventID   string `xml:"EventId"`
		StackName string `xml:"StackName"`
		Status    string `xml:"ResourceStatus"`
	}
	members := make([]evXML, 0, len(p.Data))
	for _, e := range p.Data {
		members = append(
			members,
			evXML{EventID: e.EventID, StackName: e.StackName, Status: e.ResourceStatus},
		)
	}
	type result struct {
		NextToken   string  `xml:"NextToken,omitempty"`
		StackEvents []evXML `xml:"StackEvents>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeEventsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"DescribeEventsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, StackEvents: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleUpdateTerminationProtection(form url.Values, c *echo.Context) error {
	name := form.Get("StackName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	enable := form.Get("EnableTerminationProtection") == "true"
	if err := h.Backend.UpdateTerminationProtection(name, enable); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		StackID string `xml:"StackId,omitempty"`
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateTerminationProtectionResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"UpdateTerminationProtectionResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleValidateTemplate(form url.Values, c *echo.Context) error {
	templateBody := form.Get("TemplateBody")
	summary, err := h.Backend.ValidateTemplate(templateBody)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type paramXML struct {
		ParameterKey string `xml:"ParameterKey"`
	}
	params := make([]paramXML, 0, len(summary.Parameters))
	for _, p := range summary.Parameters {
		params = append(params, paramXML{ParameterKey: p.ParameterKey})
	}
	type result struct {
		Description  string     `xml:"Description,omitempty"`
		Parameters   []paramXML `xml:"Parameters>member,omitempty"`
		Capabilities []string   `xml:"Capabilities>member,omitempty"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ValidateTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ValidateTemplateResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{Description: summary.Description, Parameters: params},
		RequestID: uuid.New().String(),
	})
}
