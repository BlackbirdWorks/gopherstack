package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// hybridUpdateInfoEntryJSON builds the real types.HybridUpdateInfoEntry wire
// shape. NewValue/PreviousValue (types.HybridUpdateValue{DnsIps,InstanceIds})
// are omitted entirely when both slices are empty, matching the real
// serializer (they are optional pointer members, only present when this
// backend actually knows a value -- i.e. for a SelfManagedInstances entry).
func hybridUpdateInfoEntryJSON(e HybridADUpdateEntry) map[string]any {
	out := map[string]any{
		keyAssessmentID:        e.AssessmentID,
		"InitiatedBy":          e.InitiatedBy,
		keyStatus:              e.Status,
		keyStartTime:           awstime.Epoch(e.StartTime),
		keyLastUpdatedDateTime: awstime.Epoch(e.LastUpdatedDateTime),
	}
	if e.StatusReason != "" {
		out["StatusReason"] = e.StatusReason
	}
	if len(e.NewDNSIPs) > 0 || len(e.NewInstanceIDs) > 0 {
		out["NewValue"] = hybridUpdateValueJSON(e.NewDNSIPs, e.NewInstanceIDs)
	}
	if len(e.PreviousDNSIPs) > 0 || len(e.PreviousInstanceIDs) > 0 {
		out["PreviousValue"] = hybridUpdateValueJSON(e.PreviousDNSIPs, e.PreviousInstanceIDs)
	}

	return out
}

func hybridUpdateValueJSON(dnsIPs, instanceIDs []string) map[string]any {
	return map[string]any{
		"DnsIps":      dnsIPs,
		"InstanceIds": instanceIDs,
	}
}

func (h *Handler) handleCreateHybridAD(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		AssessmentID string `json:"AssessmentId"`
		SecretArn    string `json:"SecretArn"`
		Tags         []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.AssessmentID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", keyAssessmentID+" is required"))
	}

	if req.SecretArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "SecretArn is required"))
	}

	tags := reqTagsToTags(req.Tags)
	d, createErr := h.Backend.CreateHybridAD(h.contextWithRegion(c), req.AssessmentID, req.SecretArn, tags)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDirectoryID: d.DirectoryID})
}

func (h *Handler) handleUpdateHybridAD(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		HybridAdministratorAccountUpdate *struct {
			SecretArn string `json:"SecretArn"`
		} `json:"HybridAdministratorAccountUpdate"`
		SelfManagedInstancesSettings *struct {
			CustomerDNSIPs []string `json:"CustomerDnsIps"`
			InstanceIDs    []string `json:"InstanceIds"`
		} `json:"SelfManagedInstancesSettings"`
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	var adminSecretArn string
	if req.HybridAdministratorAccountUpdate != nil {
		if req.HybridAdministratorAccountUpdate.SecretArn == "" {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterException", "HybridAdministratorAccountUpdate.SecretArn is required"),
			)
		}

		adminSecretArn = req.HybridAdministratorAccountUpdate.SecretArn
	}

	var dnsIPs, instanceIDs []string
	if s := req.SelfManagedInstancesSettings; s != nil {
		if s.CustomerDNSIPs == nil || s.InstanceIDs == nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp(
					"InvalidParameterException",
					"SelfManagedInstancesSettings.CustomerDnsIps and .InstanceIds are required",
				),
			)
		}

		dnsIPs = s.CustomerDNSIPs
		instanceIDs = s.InstanceIDs
	}

	assessmentID, updateErr := h.Backend.UpdateHybridAD(
		h.contextWithRegion(c), req.DirectoryID, adminSecretArn, dnsIPs, instanceIDs,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAssessmentID: assessmentID,
		keyDirectoryID:  req.DirectoryID,
	})
}

func (h *Handler) handleDescribeHybridADUpdate(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		UpdateType  string `json:"UpdateType"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	validUpdateType := req.UpdateType == "" ||
		req.UpdateType == hybridUpdateTypeAdminAccount ||
		req.UpdateType == hybridUpdateTypeSelfManaged
	if !validUpdateType {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid UpdateType"))
	}

	adminAccount, selfManaged, descErr := h.Backend.DescribeHybridADUpdate(
		h.contextWithRegion(c), req.DirectoryID, req.UpdateType,
	)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	adminAccountList := make([]map[string]any, 0, len(adminAccount))
	for _, e := range adminAccount {
		adminAccountList = append(adminAccountList, hybridUpdateInfoEntryJSON(e))
	}

	selfManagedList := make([]map[string]any, 0, len(selfManaged))
	for _, e := range selfManaged {
		selfManagedList = append(selfManagedList, hybridUpdateInfoEntryJSON(e))
	}

	// NextToken is never returned: this backend returns every matching
	// update-activity entry in a single page (no cursor pagination modeled),
	// which is SDK-valid -- NextToken is optional and its absence means "no
	// more pages," a truthful statement here since nothing was withheld.
	return c.JSON(http.StatusOK, map[string]any{
		"UpdateActivities": map[string]any{
			"HybridAdministratorAccount": adminAccountList,
			"SelfManagedInstances":       selfManagedList,
		},
	})
}
