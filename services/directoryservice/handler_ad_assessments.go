package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// startADAssessmentRequest mirrors the real StartADAssessmentInput, including
// the optional AssessmentConfiguration member (types.AssessmentConfiguration)
// this backend previously discarded entirely (gopherstack-10hx follow-up).
type startADAssessmentRequest struct {
	AssessmentConfiguration *assessmentConfigurationInput `json:"AssessmentConfiguration"`
	DirectoryID             string                        `json:"DirectoryId"`
}

// assessmentConfigurationInput mirrors types.AssessmentConfiguration's wire
// shape exactly (CustomerDnsIps/DnsName/InstanceIds/VpcSettings are required
// when AssessmentConfiguration is supplied at all; SecurityGroupIds is
// optional -- confirmed against the SDK's validateAssessmentConfiguration).
type assessmentConfigurationInput struct {
	DNSName          string                `json:"DnsName"`
	VPCSettings      *directoryVPCSettings `json:"VpcSettings"`
	CustomerDNSIPs   []string              `json:"CustomerDnsIps"`
	InstanceIDs      []string              `json:"InstanceIds"`
	SecurityGroupIDs []string              `json:"SecurityGroupIds"`
}

type directoryVPCSettings struct {
	VPCID     string   `json:"VpcId"`
	SubnetIDs []string `json:"SubnetIds"`
}

func (h *Handler) handleStartADAssessment(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req startADAssessmentRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	cfg, cfgErr := parseAssessmentConfiguration(req.AssessmentConfiguration)
	if cfgErr != "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", cfgErr))
	}

	assessmentID, startErr := h.Backend.StartADAssessment(h.contextWithRegion(c), req.DirectoryID, cfg)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"AssessmentId": assessmentID})
}

// parseAssessmentConfiguration validates and converts the wire-level
// AssessmentConfiguration into the backend's ADAssessmentConfiguration. A nil
// input (AssessmentConfiguration omitted entirely, a real, valid request
// shape) returns (nil, ""). Required-member validation matches the real
// SDK's validateAssessmentConfiguration exactly.
func parseAssessmentConfiguration(in *assessmentConfigurationInput) (*ADAssessmentConfiguration, string) {
	if in == nil {
		return nil, ""
	}

	if len(in.CustomerDNSIPs) == 0 {
		return nil, "AssessmentConfiguration.CustomerDnsIps is required"
	}

	if in.DNSName == "" {
		return nil, "AssessmentConfiguration.DnsName is required"
	}

	if len(in.InstanceIDs) == 0 {
		return nil, "AssessmentConfiguration.InstanceIds is required"
	}

	if in.VPCSettings == nil || in.VPCSettings.VPCID == "" || len(in.VPCSettings.SubnetIDs) == 0 {
		return nil, "AssessmentConfiguration.VpcSettings (VpcId and SubnetIds) is required"
	}

	return &ADAssessmentConfiguration{
		DNSName:          in.DNSName,
		VPCID:            in.VPCSettings.VPCID,
		CustomerDNSIPs:   in.CustomerDNSIPs,
		InstanceIDs:      in.InstanceIDs,
		SecurityGroupIDs: in.SecurityGroupIDs,
		SubnetIDs:        in.VPCSettings.SubnetIDs,
	}, ""
}

// handleDeleteADAssessment does not use handleTwoFieldOp: the real
// DeleteADAssessmentInput (api_op_DeleteADAssessment.go) is {AssessmentId}
// only, unlike every other handleTwoFieldOp consumer in this service (all of
// which genuinely require DirectoryId per their own real Input shapes).
func (h *Handler) handleDeleteADAssessment(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		AssessmentID string `json:"AssessmentId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.AssessmentID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "AssessmentId is required"))
	}

	if delErr := h.Backend.DeleteADAssessment(h.contextWithRegion(c), req.AssessmentID); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleDescribeADAssessment requires only AssessmentId: the real
// DescribeADAssessmentInput has no DirectoryId member at all (confirmed via
// its serializer, which emits only "AssessmentId"). Every real typed SDK
// client's request was previously rejected outright by this handler's own
// validation, since it could never supply the DirectoryId this handler used
// to require.
func (h *Handler) handleDescribeADAssessment(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		AssessmentID string `json:"AssessmentId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.AssessmentID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "AssessmentId is required"))
	}

	a, descErr := h.Backend.DescribeADAssessment(h.contextWithRegion(c), req.AssessmentID)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	wire := assessmentSummaryWire(a)

	// The following are real Assessment (not AssessmentSummary) members --
	// only present on DescribeADAssessment's response, confirmed against
	// types.Assessment. Omitted when empty, matching AWS's null-omission
	// convention (an assessment started without AssessmentConfiguration has
	// none of this to report).
	if len(a.SecurityGroupIDs) > 0 {
		wire["SecurityGroupIds"] = a.SecurityGroupIDs
	}

	if len(a.InstanceIDs) > 0 {
		wire["SelfManagedInstanceIds"] = a.InstanceIDs
	}

	if len(a.SubnetIDs) > 0 {
		wire["SubnetIds"] = a.SubnetIDs
	}

	if a.VPCID != "" {
		wire["VpcId"] = a.VPCID
	}

	// StatusCode/StatusReason/Version are real Assessment members this
	// backend cannot honestly populate -- see ADAssessmentInfo's doc comment.
	// Left off the wire entirely rather than emitted empty.

	// Wrapper key is "Assessment" (singular, no "AD" prefix) -- confirmed
	// against DescribeADAssessmentOutput; the fabricated "ADAssessment" key
	// this handler used to emit meant a real typed client's resp.Assessment
	// field silently decoded to nil on every call despite the backend
	// genuinely tracking the data.
	//
	// AssessmentReports (DescribeADAssessmentOutput's second top-level
	// member) is omitted: this backend tracks no per-domain-controller/
	// test-category validation results to source it from.
	return c.JSON(http.StatusOK, map[string]any{"Assessment": wire})
}

// assessmentSummaryWire builds the field set common to both DescribeADAssessment's
// Assessment and ListADAssessments' AssessmentSummary shapes (confirmed
// against types.Assessment/types.AssessmentSummary: AssessmentId, DirectoryId,
// Status, ReportType, StartTime are always present; CustomerDnsIps/DnsName/
// LastUpdateDateTime are omitted when the assessment was started without a
// real AssessmentConfiguration to report).
func assessmentSummaryWire(a *ADAssessmentInfo) map[string]any {
	wire := map[string]any{
		"AssessmentId": a.AssessmentID,
		keyDirectoryID: a.DirectoryID,
		keyStatus:      a.Status,
		"ReportType":   a.AssessType,
		keyStartTime:   awstime.Epoch(a.StartTime),
	}

	if len(a.CustomerDNSIPs) > 0 {
		wire["CustomerDnsIps"] = a.CustomerDNSIPs
	}

	if a.DNSName != "" {
		wire["DnsName"] = a.DNSName
	}

	if !a.LastUpdateDateTime.IsZero() {
		wire["LastUpdateDateTime"] = awstime.Epoch(a.LastUpdateDateTime)
	}

	return wire
}

func (h *Handler) handleListADAssessments(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	assessments, nextToken, listErr := h.Backend.ListADAssessments(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	assessList := make([]map[string]any, 0, len(assessments))
	for i := range assessments {
		assessList = append(assessList, assessmentSummaryWire(&assessments[i]))
	}

	resp := map[string]any{"Assessments": assessList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
