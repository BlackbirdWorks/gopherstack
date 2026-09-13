package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by governance operations (approval policies,
// DLP settings, limits profiles).
const (
	keyPolicy              = "Policy"
	keyPolicies            = "Policies"
	keyDlpSettingKey       = "DlpSetting"
	keyDlpSettingSummaries = "DlpSettingSummaries"
	keyDlpSettingID        = "DlpSettingId"
	keyProfileLower        = "profile"
	keyProfilesLower       = "profiles"
	keyArnLower            = "arn"
	keyProfileIDLower      = "profileId"
	keyClientTokenLower    = "clientToken"
	keyProfileNameLower    = "profileName"
	keyDescriptionLower    = "description"
	keyResourceLimitsLower = "resourceLimits"
	keyNextTokenLower      = "nextToken"
	keyResourceTypeLower   = "resourceType"

	// approval policy path segment indices: governance/approvalworkflows/policies/{PolicyId}.
	segApprovalPolicyID = 3

	// limits profile path segment indices:
	// governance/limits/accounts/{accountId}/profiles/{profileId}.
	segLimitsAccountID = 3
	segLimitsProfileID = 5
)

// ---- op classification ----

func isApprovalPolicyOp(op string) bool {
	switch op {
	case opCreateApprovalPolicy, opDescribeApprovalPolicy, opUpdateApprovalPolicy,
		opDeleteApprovalPolicy, opListApprovalPolicies:
		return true
	}

	return false
}

func isDlpSettingOp(op string) bool {
	switch op {
	case opCreateDlpSetting, opDescribeDlpSetting, opUpdateDlpSetting, opDeleteDlpSetting, opListDlpSettings:
		return true
	}

	return false
}

func isLimitsProfileOp(op string) bool {
	switch op {
	case opCreateLimitsProfile, opDescribeLimitsProfile, opUpdateLimitsProfile,
		opDeleteLimitsProfile, opListLimitsProfiles:
		return true
	}

	return false
}

func isGovernanceOp(op string) bool {
	return isApprovalPolicyOp(op) || isDlpSettingOp(op) || isLimitsProfileOp(op)
}

// ---- path classification ----

// classifyGovernancePaths routes /governance/... paths (ApprovalPolicy and
// LimitsProfile ops -- see quicksightGovernancePathPrefix's doc comment in
// handler.go for why these live outside the usual /accounts/{id}/... shape).
func classifyGovernancePaths(method string, segs []string, n int) (string, string) {
	if n < 2 { //nolint:mnd // minimum segment count to name a governance sub-family, not a magic value
		return opUnknown, ""
	}

	switch segs[1] {
	case pathSegApprovalWorkflows:
		return classifyApprovalPolicyPaths(method, segs, n)
	case pathSegLimits:
		return classifyLimitsProfilePaths(method, segs, n)
	}

	return opUnknown, ""
}

// classifyApprovalPolicyPaths routes
// /governance/approvalworkflows/policies[/{PolicyId}].
func classifyApprovalPolicyPaths(method string, segs []string, n int) (string, string) {
	const (
		nPoliciesRoot = 3
		nPoliciesID   = 4
	)

	if n < nPoliciesRoot || segs[2] != pathSegPolicies {
		return opUnknown, ""
	}

	switch n {
	case nPoliciesRoot:
		switch method {
		case http.MethodPost:
			return opCreateApprovalPolicy, ""
		case http.MethodGet:
			return opListApprovalPolicies, ""
		}
	case nPoliciesID:
		id := seg(segs, segApprovalPolicyID)
		switch method {
		case http.MethodGet:
			return opDescribeApprovalPolicy, id
		case http.MethodPatch:
			return opUpdateApprovalPolicy, id
		case http.MethodDelete:
			return opDeleteApprovalPolicy, id
		}
	}

	return opUnknown, ""
}

// classifyLimitsProfilePaths routes
// /governance/limits/accounts/{accountId}/profiles[/{profileId}].
func classifyLimitsProfilePaths(method string, segs []string, n int) (string, string) {
	const (
		nProfilesRoot = 5
		nProfilesID   = 6
	)

	if n < nProfilesRoot || segs[2] != pathSegAccounts || segs[4] != pathSegProfiles {
		return opUnknown, ""
	}

	accountID := seg(segs, segLimitsAccountID)

	switch n {
	case nProfilesRoot:
		switch method {
		case http.MethodPost:
			return opCreateLimitsProfile, accountID
		case http.MethodGet:
			return opListLimitsProfiles, accountID
		}
	case nProfilesID:
		id := seg(segs, segLimitsProfileID)
		switch method {
		case http.MethodGet:
			return opDescribeLimitsProfile, id
		case http.MethodPut:
			return opUpdateLimitsProfile, id
		case http.MethodDelete:
			return opDeleteLimitsProfile, id
		}
	}

	return opUnknown, ""
}

// classifyDlpSettingPaths routes
// /accounts/{AwsAccountId}/data-loss-prevention/settings[/{DlpSettingId}].
// Unlike every other resourceType classifier in resourceTypeDispatchTable,
// the instance ID here sits at segSubRes (index 4), not segSubResID: real
// AWS binds DlpSettingId into the URI even for Create (POST to the specific
// resource path, quicksight@v1.129.0 api_op_CreateDlpSetting.go's
// SplitURI), so "settings" occupies the segResID slot a sibling family would
// use for its own resource's ID.
func classifyDlpSettingPaths(method string, segs []string, n int) (string, string) {
	if n < nSegsAccountResID || seg(segs, segResID) != pathSegSettings {
		return opUnknown, ""
	}

	switch n {
	case nSegsAccountResID:
		if method == http.MethodGet {
			return opListDlpSettings, seg(segs, segAccountID)
		}
	case nSegsSubRes:
		id := seg(segs, segSubRes)
		switch method {
		case http.MethodPost:
			return opCreateDlpSetting, id
		case http.MethodGet:
			return opDescribeDlpSetting, id
		case http.MethodPut:
			return opUpdateDlpSetting, id
		case http.MethodDelete:
			return opDeleteDlpSetting, id
		}
	}

	return opUnknown, ""
}

// ---- dispatch ----

func (h *Handler) dispatchGovernance(c *echo.Context, op string) error {
	switch {
	case isApprovalPolicyOp(op):
		return h.dispatchApprovalPolicy(c, op)
	case isDlpSettingOp(op):
		return h.dispatchDlpSetting(c, op)
	case isLimitsProfileOp(op):
		return h.dispatchLimitsProfile(c, op)
	}

	return writeError(c, http.StatusNotImplemented, "UnsupportedOperationException", "operation not implemented: "+op)
}

func (h *Handler) dispatchApprovalPolicy(c *echo.Context, op string) error {
	switch op {
	case opCreateApprovalPolicy:
		return h.handleCreateApprovalPolicy(c)
	case opDescribeApprovalPolicy:
		return h.handleDescribeApprovalPolicy(c)
	case opUpdateApprovalPolicy:
		return h.handleUpdateApprovalPolicy(c)
	case opDeleteApprovalPolicy:
		return h.handleDeleteApprovalPolicy(c)
	case opListApprovalPolicies:
		return h.handleListApprovalPolicies(c)
	}

	return writeError(c, http.StatusNotImplemented, "UnsupportedOperationException", "operation not implemented: "+op)
}

func (h *Handler) dispatchDlpSetting(c *echo.Context, op string) error {
	switch op {
	case opCreateDlpSetting:
		return h.handleCreateDlpSetting(c)
	case opDescribeDlpSetting:
		return h.handleDescribeDlpSetting(c)
	case opUpdateDlpSetting:
		return h.handleUpdateDlpSetting(c)
	case opDeleteDlpSetting:
		return h.handleDeleteDlpSetting(c)
	case opListDlpSettings:
		return h.handleListDlpSettings(c)
	}

	return writeError(c, http.StatusNotImplemented, "UnsupportedOperationException", "operation not implemented: "+op)
}

func (h *Handler) dispatchLimitsProfile(c *echo.Context, op string) error {
	switch op {
	case opCreateLimitsProfile:
		return h.handleCreateLimitsProfile(c)
	case opDescribeLimitsProfile:
		return h.handleDescribeLimitsProfile(c)
	case opUpdateLimitsProfile:
		return h.handleUpdateLimitsProfile(c)
	case opDeleteLimitsProfile:
		return h.handleDeleteLimitsProfile(c)
	case opListLimitsProfiles:
		return h.handleListLimitsProfiles(c)
	}

	return writeError(c, http.StatusNotImplemented, "UnsupportedOperationException", "operation not implemented: "+op)
}

// ---- Approval Policy handlers ----

func (h *Handler) handleCreateApprovalPolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	p, err := h.Backend.CreateApprovalPolicy(
		strField(body, "PolicyId"),
		strField(body, keyName),
		strField(body, "Description"),
		strSliceField(body, "Actions"),
		strSliceField(body, "AssetTypes"),
		strSliceField(body, "ApprovalGroups"),
		applicableToFromBody(body),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyPolicy:    approvalPolicyToMap(p),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeApprovalPolicy(c *echo.Context) error {
	policyID := seg(pathSegsFromCtx(c), segApprovalPolicyID)

	p, err := h.Backend.DescribeApprovalPolicy(policyID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyPolicy:    approvalPolicyToMap(p),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateApprovalPolicy(c *echo.Context) error {
	policyID := seg(pathSegsFromCtx(c), segApprovalPolicyID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	var applicableTo *ApplicableTo
	if _, ok := body["ApplicableTo"]; ok {
		at := applicableToFromBody(body)
		applicableTo = &at
	}

	p, err := h.Backend.UpdateApprovalPolicy(
		policyID,
		strField(body, keyName),
		strField(body, "Description"),
		strSliceField(body, "Actions"),
		strSliceField(body, "AssetTypes"),
		strSliceField(body, "ApprovalGroups"),
		applicableTo,
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyPolicy:    approvalPolicyToMap(p),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteApprovalPolicy(c *echo.Context) error {
	policyID := seg(pathSegsFromCtx(c), segApprovalPolicyID)

	if err := h.Backend.DeleteApprovalPolicy(policyID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListApprovalPolicies(c *echo.Context) error {
	policies, next, err := h.Backend.ListApprovalPolicies(maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(policies))
	for _, p := range policies {
		items = append(items, approvalPolicyToMap(p))
	}

	resp := map[string]any{
		keyPolicies:  items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func approvalPolicyToMap(p *ApprovalPolicy) map[string]any {
	return map[string]any{
		"Actions":        p.Actions,
		"ApplicableTo":   applicableToToMap(p.ApplicableTo),
		"ApprovalGroups": p.ApprovalGroups,
		"AssetTypes":     p.AssetTypes,
		keyCreatedAt:     p.CreatedAt.Unix(),
		"Description":    p.Description,
		keyName:          p.Name,
		"PolicyArn":      p.Arn,
		"PolicyId":       p.PolicyID,
		keyUpdatedAt:     p.UpdatedAt.Unix(),
	}
}

func applicableToToMap(a ApplicableTo) map[string]any {
	return map[string]any{
		keyConnectorType: a.Type,
		"GroupArns":      a.GroupArns,
	}
}

func applicableToFromBody(body map[string]any) ApplicableTo {
	raw := mapField(body, "ApplicableTo")
	if raw == nil {
		return ApplicableTo{}
	}

	return ApplicableTo{
		Type:      strField(raw, "Type"),
		GroupArns: strSliceField(raw, "GroupArns"),
	}
}

// ---- DLP Setting handlers ----

func dlpSettingIDsFromCtx(c *echo.Context) (string, string) {
	segs := pathSegsFromCtx(c)

	return seg(segs, segAccountID), seg(segs, segSubRes)
}

func (h *Handler) handleCreateDlpSetting(c *echo.Context) error {
	accountID, dlpSettingID := dlpSettingIDsFromCtx(c)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	d, err := h.Backend.CreateDlpSetting(
		accountID,
		dlpSettingID,
		strField(body, keyName),
		boolField(body, "Enabled"),
		strField(body, "ProviderType"),
		strField(body, "ProviderOutageAction"),
		providerConfigFromBody(body),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrDlpSettingAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:          d.Arn,
		keyDlpSettingID: d.DlpSettingID,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleDescribeDlpSetting(c *echo.Context) error {
	accountID, dlpSettingID := dlpSettingIDsFromCtx(c)

	d, err := h.Backend.DescribeDlpSetting(accountID, dlpSettingID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDlpSettingKey: dlpSettingDetailsToMap(d),
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleUpdateDlpSetting(c *echo.Context) error {
	accountID, dlpSettingID := dlpSettingIDsFromCtx(c)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	var providerConfig *ProviderConfig
	if _, ok := body["ProviderConfig"]; ok {
		pc := providerConfigFromBody(body)
		providerConfig = &pc
	}

	d, err := h.Backend.UpdateDlpSetting(
		accountID,
		dlpSettingID,
		strField(body, keyName),
		boolPtrField(body, "Enabled"),
		strField(body, "ProviderType"),
		strField(body, "ProviderOutageAction"),
		providerConfig,
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:          d.Arn,
		keyDlpSettingID: d.DlpSettingID,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleDeleteDlpSetting(c *echo.Context) error {
	accountID, dlpSettingID := dlpSettingIDsFromCtx(c)

	arn, err := h.Backend.DeleteDlpSetting(accountID, dlpSettingID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:          arn,
		keyDlpSettingID: dlpSettingID,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleListDlpSettings(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	settings, next, err := h.Backend.ListDlpSettings(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(settings))
	for _, d := range settings {
		items = append(items, dlpSettingSummaryToMap(d))
	}

	resp := map[string]any{
		keyDlpSettingSummaries: items,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func dlpSettingDetailsToMap(d *DlpSetting) map[string]any {
	m := map[string]any{
		"Arn":                  d.Arn,
		keyCreatedAt:           d.CreatedAt.Unix(),
		keyDlpSettingID:        d.DlpSettingID,
		keyName:                d.Name,
		"ProviderOutageAction": d.ProviderOutageAction,
		"ProviderType":         d.ProviderType,
		keyStatus:              dlpSettingStatus(d.Enabled),
		keyUpdatedAt:           d.UpdatedAt.Unix(),
	}
	if d.ProviderConfig.MicrosoftPurview != nil {
		m["ProviderConfig"] = providerConfigToMap(d.ProviderConfig)
	}

	return m
}

func dlpSettingSummaryToMap(d *DlpSetting) map[string]any {
	return map[string]any{
		"Arn":           d.Arn,
		keyCreatedAt:    d.CreatedAt.Unix(),
		keyDlpSettingID: d.DlpSettingID,
		keyName:         d.Name,
		"ProviderType":  d.ProviderType,
		keyStatus:       dlpSettingStatus(d.Enabled),
		keyUpdatedAt:    d.UpdatedAt.Unix(),
	}
}

func providerConfigToMap(pc ProviderConfig) map[string]any {
	mp := pc.MicrosoftPurview
	if mp == nil {
		return nil
	}

	mappings := make([]map[string]any, 0, len(mp.LabelActionMappings))
	for _, lm := range mp.LabelActionMappings {
		mappings = append(mappings, map[string]any{
			"Action":    lm.Action,
			"LabelId":   lm.LabelID,
			"LabelName": lm.LabelName,
		})
	}

	return map[string]any{
		"MicrosoftPurview": map[string]any{
			"Credentials":         map[string]any{"SecretArn": mp.SecretArn},
			"LabelActionMappings": mappings,
			"UnmappedAction":      mp.UnmappedAction,
		},
	}
}

func providerConfigFromBody(body map[string]any) ProviderConfig {
	raw := mapField(body, "ProviderConfig")
	if raw == nil {
		return ProviderConfig{}
	}

	mp := mapField(raw, "MicrosoftPurview")
	if mp == nil {
		return ProviderConfig{}
	}

	creds := mapField(mp, "Credentials")

	rawMappings, _ := mp["LabelActionMappings"].([]any)
	mappings := make([]LabelActionMapping, 0, len(rawMappings))
	for _, item := range rawMappings {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		mappings = append(mappings, LabelActionMapping{
			Action:    strField(m, "Action"),
			LabelID:   strField(m, "LabelId"),
			LabelName: strField(m, "LabelName"),
		})
	}

	return ProviderConfig{
		MicrosoftPurview: &MicrosoftPurviewProviderConfig{
			SecretArn:           strField(creds, "SecretArn"),
			UnmappedAction:      strField(mp, "UnmappedAction"),
			LabelActionMappings: mappings,
		},
	}
}

func boolField(body map[string]any, key string) bool {
	b, _ := body[key].(bool)

	return b
}

func boolPtrField(body map[string]any, key string) *bool {
	v, ok := body[key]
	if !ok {
		return nil
	}
	b, _ := v.(bool)

	return &b
}

// ---- Limits Profile handlers ----

func limitsProfileAccountIDFromCtx(c *echo.Context) string {
	return seg(pathSegsFromCtx(c), segLimitsAccountID)
}

func limitsProfileIDFromCtx(c *echo.Context) string {
	return seg(pathSegsFromCtx(c), segLimitsProfileID)
}

func (h *Handler) handleCreateLimitsProfile(c *echo.Context) error {
	accountID := limitsProfileAccountIDFromCtx(c)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	p, err := h.Backend.CreateLimitsProfile(
		accountID,
		strField(body, keyClientTokenLower),
		strField(body, keyProfileNameLower),
		strField(body, keyDescriptionLower),
		resourceLimitsFromBody(body, keyResourceLimitsLower),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArnLower:       p.Arn,
		keyProfileIDLower: p.ProfileID,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeLimitsProfile(c *echo.Context) error {
	accountID := limitsProfileAccountIDFromCtx(c)
	profileID := limitsProfileIDFromCtx(c)

	p, err := h.Backend.DescribeLimitsProfile(accountID, profileID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyProfileLower: limitsProfileToMap(p),
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleUpdateLimitsProfile(c *echo.Context) error {
	accountID := limitsProfileAccountIDFromCtx(c)
	profileID := limitsProfileIDFromCtx(c)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	p, err := h.Backend.UpdateLimitsProfile(
		accountID,
		profileID,
		strField(body, keyProfileNameLower),
		strField(body, keyDescriptionLower),
		resourceLimitsFromBody(body, keyResourceLimitsLower),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArnLower:  p.Arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteLimitsProfile(c *echo.Context) error {
	accountID := limitsProfileAccountIDFromCtx(c)
	profileID := limitsProfileIDFromCtx(c)

	arn, err := h.Backend.DeleteLimitsProfile(accountID, profileID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArnLower:  arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListLimitsProfiles(c *echo.Context) error {
	accountID := limitsProfileAccountIDFromCtx(c)
	resourceType := queryParam(c, keyResourceTypeLower)

	profiles, next, err := h.Backend.ListLimitsProfiles(accountID, resourceType, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(profiles))
	for _, p := range profiles {
		items = append(items, limitsProfileToMap(p))
	}

	resp := map[string]any{
		keyProfilesLower: items,
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	}
	if next != "" {
		resp[keyNextTokenLower] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func limitsProfileToMap(p *LimitsProfile) map[string]any {
	limits := make(map[string]any, len(p.ResourceLimits))
	for k, v := range p.ResourceLimits {
		limits[k] = map[string]any{"maxValue": v.MaxValue, "unit": v.Unit}
	}

	return map[string]any{
		"accountId":            p.AccountID,
		keyArnLower:            p.Arn,
		"createdAt":            p.CreatedAt.Unix(),
		keyDescriptionLower:    p.Description,
		keyProfileIDLower:      p.ProfileID,
		keyProfileNameLower:    p.ProfileName,
		keyResourceLimitsLower: limits,
		"updatedAt":            p.UpdatedAt.Unix(),
	}
}

func resourceLimitsFromBody(body map[string]any, key string) map[string]ProfileLimitValue {
	raw := mapField(body, key)
	if len(raw) == 0 {
		return nil
	}

	out := make(map[string]ProfileLimitValue, len(raw))
	for k, v := range raw {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}

		out[k] = ProfileLimitValue{
			MaxValue: int64Field(m, "maxValue"),
			Unit:     strField(m, "unit"),
		}
	}

	return out
}
