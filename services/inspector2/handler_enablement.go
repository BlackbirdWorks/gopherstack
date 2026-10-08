package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opListAccountPermissions = "ListAccountPermissions"

	pathAccountPermissionsList = "/accountpermissions/list"

	// inheritanceModeInheritFromAdmin is UpdateConfigurationInheritance's
	// one InheritanceMode enum value (types/enums.go).
	inheritanceModeInheritFromAdmin = "INHERIT_FROM_ADMIN"
)

// handleToggle handles POST /enable and POST /disable.
func (h *Handler) handleToggle(c *echo.Context, enable bool) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ClientToken   string   `json:"clientToken"`
		ResourceTypes []string `json:"resourceTypes"`
		AccountIDs    []string `json:"accountIds"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	resp, replayErr := h.replay(opEnable, clientTokenFor(enable, req.ClientToken), req, func() (map[string]any, error) {
		return h.toggleAccounts(req.AccountIDs, req.ResourceTypes, enable), nil
	})
	if replayErr != nil {
		return h.mapError(c, replayErr)
	}

	return c.JSON(http.StatusOK, resp)
}

// clientTokenFor drops the token for Disable, whose input has no ClientToken member.
func clientTokenFor(enable bool, token string) string {
	if !enable {
		return ""
	}

	return token
}

// toggleAccounts applies an Enable/Disable to each requested account; accounts that are neither the caller's nor
// an associated member are reported in failedAccounts.
func (h *Handler) toggleAccounts(accountIDs, resourceTypes []string, enable bool) map[string]any {
	var outcome AccountOutcome

	if enable {
		outcome = h.Backend.EnableAccounts(accountIDs, resourceTypes)
	} else {
		outcome = h.Backend.DisableAccounts(accountIDs, resourceTypes)
	}

	statuses := h.Backend.GetAccountStatuses(outcome.Updated).Found
	accounts := make([]map[string]any, 0, len(statuses))

	for _, st := range statuses {
		accounts = append(accounts, map[string]any{
			keyAccountID:      st.AccountID,
			keyResourceStatus: buildResourceStatus(st),
			keyStatus:         st.Status,
		})
	}

	return map[string]any{keyAccounts: accounts, keyFailedAccounts: failedAccounts(outcome.Unknown)}
}

// failedAccounts renders FailedAccount entries for accounts the caller cannot address.
func failedAccounts(accountIDs []string) []map[string]any {
	out := make([]map[string]any, 0, len(accountIDs))

	for _, id := range accountIDs {
		out = append(out, map[string]any{
			keyAccountID:    id,
			keyErrorCode:    "RESOURCE_NOT_FOUND",
			keyErrorMessage: "account " + id + " is neither the calling account nor an associated member",
		})
	}

	return out
}

// handleBatchGetAccountStatus handles POST /status/batch/get. Unlike
// Enable/Disable (whose Account shape is a flat resourceStatus of Status
// strings), BatchGetAccountStatus returns the richer AccountState shape:
// resourceState nests a State object per resource type (status, errorCode,
// errorMessage), and the top-level state is itself a State object rather
// than a bare status string.
func (h *Handler) handleBatchGetAccountStatus(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	lookup := h.Backend.GetAccountStatuses(req.AccountIDs)
	accounts := make([]map[string]any, 0, len(lookup.Found))

	for _, status := range lookup.Found {
		accounts = append(accounts, map[string]any{
			keyAccountID:     status.AccountID,
			keyResourceState: buildResourceState(status),
			"state":          buildState(status.Status),
		})
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyAccounts: accounts, keyFailedAccounts: failedAccounts(lookup.Unknown)},
	)
}

// buildState renders an Inspector2 State object: a required status alongside
// the (unpopulated, in the absence of an error) errorCode/errorMessage
// members every State response carries.
func buildState(status string) map[string]any {
	return map[string]any{
		keyStatus:       status,
		keyErrorCode:    "",
		keyErrorMessage: nil,
	}
}

// buildResourceState constructs the resourceState map used by
// BatchGetAccountStatus, keying each resource type to its own State object.
// All five resource types are always present: the real API's ResourceState
// shape marks Ec2/Ecr as required and always populates CodeRepository/
// Lambda/LambdaCode too, and terraform-provider-aws's AccountStatuses
// (internal/service/inspector2/enabler.go) dereferences them unconditionally
// -- a response omitting any of them panics that client (gopherstack-fndhb).
func buildResourceState(status *AccountStatusResponse) map[string]any {
	return map[string]any{
		"ec2":            buildState(status.Ec2Status),
		"ecr":            buildState(status.EcrStatus),
		"lambda":         buildState(status.LambdaStatus),
		"lambdaCode":     buildState(status.LambdaCodeStatus),
		"codeRepository": buildState(status.CodeRepositoryStatus),
	}
}

// buildResourceStatus constructs the resourceStatus map.
func buildResourceStatus(status *AccountStatusResponse) map[string]any {
	return map[string]any{
		"ec2":            status.Ec2Status,
		"ecr":            status.EcrStatus,
		"lambda":         status.LambdaStatus,
		"lambdaCode":     status.LambdaCodeStatus,
		"codeRepository": status.CodeRepositoryStatus,
	}
}

// handleGetConfiguration handles POST /configuration/get.
func (h *Handler) handleGetConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountID string `json:"accountId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	cfg, getErr := h.Backend.GetConfiguration(req.AccountID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ec2Configuration": map[string]any{
			"scanModeState": map[string]any{
				"scanMode":       cfg.Ec2ScanMode,
				"scanModeStatus": scanModeStatusSuccess,
			},
		},
		"ecrConfiguration": map[string]any{
			"rescanDurationState": map[string]any{
				"rescanDuration": cfg.EcrRescanDuration,
				keyStatus:        ecrRescanDurationStatusSuccess,
				keyUpdatedAt:     nil,
			},
		},
	})
}

// handleUpdateConfiguration handles POST /configuration/update.
func (h *Handler) handleUpdateConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Ec2Configuration *struct {
			ScanMode string `json:"scanMode"`
		} `json:"ec2Configuration"`
		EcrConfiguration *struct {
			RescanDuration string `json:"rescanDuration"`
		} `json:"ecrConfiguration"`
		UpdateConfigurationInheritance *struct {
			Ec2Configuration string `json:"ec2Configuration"`
			EcrConfiguration string `json:"ecrConfiguration"`
		} `json:"updateConfigurationInheritance"`
		AccountID string `json:"accountId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	var ec2ScanMode, ecrRescanDuration string

	if req.Ec2Configuration != nil {
		ec2ScanMode = req.Ec2Configuration.ScanMode
	}

	if req.EcrConfiguration != nil {
		ecrRescanDuration = req.EcrConfiguration.RescanDuration
	}

	var resetEc2ToInherit, resetEcrToInherit bool

	if inh := req.UpdateConfigurationInheritance; inh != nil {
		resetEc2ToInherit = inh.Ec2Configuration == inheritanceModeInheritFromAdmin
		resetEcrToInherit = inh.EcrConfiguration == inheritanceModeInheritFromAdmin
	}

	updateErr := h.Backend.UpdateConfiguration(
		req.AccountID, ec2ScanMode, ecrRescanDuration, resetEc2ToInherit, resetEcrToInherit,
	)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListAccountPermissions(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Service    string `json:"service"`
		NextToken  string `json:"nextToken"`
		MaxResults int32  `json:"maxResults"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	perms, listErr := h.Backend.ListAccountPermissions(req.Service)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	if perms == nil {
		perms = []*AccountPermission{}
	}

	page, next := pageItems(
		perms,
		func(p *AccountPermission) string { return p.Service + "|" + p.Operation },
		req.MaxResults,
		req.NextToken,
	)

	return c.JSON(http.StatusOK, withPageToken(map[string]any{"permissions": page}, next))
}
