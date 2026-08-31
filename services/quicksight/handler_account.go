package quicksight

import (
	"errors"
	"net/http"
	"sync"

	"github.com/labstack/echo/v5"
)

// JSON response/request keys and query params used only by the account-level
// configuration operations (account settings, subscription, customization,
// IP restriction, public sharing, key registration, default Q Business
// application, Q personalization, Q Search, and Dashboards Q&A).
const (
	keyAccountSettings                 = "AccountSettings"
	keyAccountName                     = "AccountName"
	keyEdition                         = "Edition"
	keyDefaultNamespaceField           = "DefaultNamespace"
	keyNotificationEmail               = "NotificationEmail"
	keyPublicSharingEnabled            = "PublicSharingEnabled"
	keyTerminationProtectionEnabled    = "TerminationProtectionEnabled"
	keyAuthenticationMethod            = "AuthenticationMethod"
	keySignupResponse                  = "SignupResponse"
	keyUserLoginName                   = "userLoginName"
	keyIAMUser                         = "IAMUser"
	keyAccountInfo                     = "AccountInfo"
	keyAuthenticationType              = "AuthenticationType"
	keyAccountSubscriptionStatus       = "AccountSubscriptionStatus"
	keyAccountCustomization            = "AccountCustomization"
	keyDefaultTheme                    = "DefaultTheme"
	keyDefaultEmailCustomizationTmpl   = "DefaultEmailCustomizationTemplate"
	keyAwsAccountID                    = "AwsAccountId"
	keyNamespaceField                  = "Namespace"
	keyIPRestrictionRuleMap            = "IpRestrictionRuleMap"
	keyVPCIDRestrictionRuleMap         = "VpcIdRestrictionRuleMap"
	keyVPCEndpointIDRestrictionRuleMap = "VpcEndpointIdRestrictionRuleMap"
	keyEnabled                         = "Enabled"
	keyKeyRegistration                 = "KeyRegistration"
	keySuccessfulKeyRegistration       = "SuccessfulKeyRegistration"
	keyFailedKeyRegistration           = "FailedKeyRegistration"
	keyKeyArn                          = "KeyArn"
	keyDefaultKey                      = "DefaultKey"
	keyApplicationID                   = "ApplicationId"
	keyPersonalizationMode             = "PersonalizationMode"
	keyQSearchStatus                   = "QSearchStatus"
	keyDashboardsQAStatus              = "DashboardsQAStatus"
	keyPurchaseMode                    = "PurchaseMode"

	queryParamNamespace = "namespace"

	// queryParamDefaultKeyOnly is DescribeKeyRegistrationInput.
	// DefaultKeyOnly's wire binding (quicksight@v1.123.1 serializers.go:
	// encoder.SetQuery("default-key-only")).
	queryParamDefaultKeyOnly = "default-key-only"

	// queryParamResolved is DescribeAccountCustomizationInput.Resolved's
	// wire binding (quicksight@v1.123.1 serializers.go:
	// encoder.SetQuery("resolved"), only emitted when true).
	queryParamResolved = "resolved"
)

func isAccountConfigOp(op string) bool {
	switch op {
	case opDescribeAccountSettings, opUpdateAccountSettings,
		opCreateAccountSubscription, opDescribeAccountSubscription, opDeleteAccountSubscription,
		opCreateAccountCustomization, opDescribeAccountCustomization,
		opUpdateAccountCustomization, opDeleteAccountCustomization,
		opDescribeIpRestriction, opUpdateIpRestriction,
		opUpdatePublicSharingSettings,
		opDescribeKeyRegistration, opUpdateKeyRegistration,
		opDescribeDefaultQBiz, opUpdateDefaultQBiz, opDeleteDefaultQBiz,
		opDescribeQPersonalization, opUpdateQPersonalization,
		opDescribeQSearchConfig, opUpdateQSearchConfig,
		opDescribeDashboardsQAConfiguration, opUpdateDashboardsQAConfiguration,
		opUpdateSPICECapacity:
		return true
	}

	return false
}

// accountConfigDispatchTable lazily builds the op-name -> handler-method
// lookup for the whole account/config op cluster exactly once. A
// map[string]method lookup (rather than a flat switch) keeps
// dispatchAccountConfig itself trivially simple regardless of how many
// account-config ops exist -- mirrors the onceOpTable pattern in
// services/apigatewayv2/handler.go.
//
//nolint:gochecknoglobals // read-only package-level lookup table, built once via sync.OnceValue
var accountConfigDispatchTable = sync.OnceValue(func() map[string]func(*Handler, *echo.Context) error {
	return map[string]func(*Handler, *echo.Context) error{
		opDescribeAccountSettings:           (*Handler).handleDescribeAccountSettings,
		opUpdateAccountSettings:             (*Handler).handleUpdateAccountSettings,
		opCreateAccountSubscription:         (*Handler).handleCreateAccountSubscription,
		opDescribeAccountSubscription:       (*Handler).handleDescribeAccountSubscription,
		opDeleteAccountSubscription:         (*Handler).handleDeleteAccountSubscription,
		opCreateAccountCustomization:        (*Handler).handleCreateAccountCustomization,
		opDescribeAccountCustomization:      (*Handler).handleDescribeAccountCustomization,
		opUpdateAccountCustomization:        (*Handler).handleUpdateAccountCustomization,
		opDeleteAccountCustomization:        (*Handler).handleDeleteAccountCustomization,
		opDescribeIpRestriction:             (*Handler).handleDescribeIPRestriction,
		opUpdateIpRestriction:               (*Handler).handleUpdateIPRestriction,
		opUpdatePublicSharingSettings:       (*Handler).handleUpdatePublicSharingSettings,
		opDescribeKeyRegistration:           (*Handler).handleDescribeKeyRegistration,
		opUpdateKeyRegistration:             (*Handler).handleUpdateKeyRegistration,
		opDescribeDefaultQBiz:               (*Handler).handleDescribeDefaultQBiz,
		opUpdateDefaultQBiz:                 (*Handler).handleUpdateDefaultQBiz,
		opDeleteDefaultQBiz:                 (*Handler).handleDeleteDefaultQBiz,
		opDescribeQPersonalization:          (*Handler).handleDescribeQPersonalization,
		opUpdateQPersonalization:            (*Handler).handleUpdateQPersonalization,
		opDescribeQSearchConfig:             (*Handler).handleDescribeQSearchConfig,
		opUpdateQSearchConfig:               (*Handler).handleUpdateQSearchConfig,
		opDescribeDashboardsQAConfiguration: (*Handler).handleDescribeDashboardsQA,
		opUpdateDashboardsQAConfiguration:   (*Handler).handleUpdateDashboardsQA,
		opUpdateSPICECapacity:               (*Handler).handleUpdateSPICECapacity,
	}
})

func (h *Handler) dispatchAccountConfig(c *echo.Context, op string) error {
	if fn, ok := accountConfigDispatchTable()[op]; ok {
		return fn(h, c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

// ---- Account Settings ----

func accountSettingsToMap(s *AccountSettings) map[string]any {
	return map[string]any{
		keyAccountName:                  s.AccountName,
		keyEdition:                      s.Edition,
		keyDefaultNamespaceField:        s.DefaultNamespace,
		keyNotificationEmail:            s.NotificationEmail,
		keyPublicSharingEnabled:         s.PublicSharingEnabled,
		keyTerminationProtectionEnabled: s.TerminationProtectionEnabled,
	}
}

func (h *Handler) handleDescribeAccountSettings(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	s, err := h.Backend.DescribeAccountSettings(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAccountSettings: accountSettingsToMap(s),
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleUpdateAccountSettings(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	var terminationProtection *bool
	if v, present := body[keyTerminationProtectionEnabled]; present {
		b, _ := v.(bool)
		terminationProtection = &b
	}

	_, err = h.Backend.UpdateAccountSettings(
		accountID,
		strField(body, keyDefaultNamespaceField),
		strField(body, keyNotificationEmail),
		terminationProtection,
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Account Subscription ----

func (h *Handler) handleCreateAccountSubscription(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	accountName := strField(body, keyAccountName)

	s, err := h.Backend.CreateAccountSubscription(
		accountID,
		accountName,
		strField(body, keyEdition),
		strField(body, keyAuthenticationMethod),
		strField(body, keyNotificationEmail),
	)
	if err != nil {
		if errors.Is(err, ErrAccountSubscriptionAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySignupResponse: map[string]any{
			keyAccountName:   s.AccountName,
			keyUserLoginName: s.AccountName,
			keyIAMUser:       true,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeAccountSubscription(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	s, err := h.Backend.DescribeAccountSubscription(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAccountInfo: map[string]any{
			keyAccountName:               s.AccountName,
			keyEdition:                   s.Edition,
			keyNotificationEmail:         s.NotificationEmail,
			keyAuthenticationType:        s.AuthenticationType,
			keyAccountSubscriptionStatus: s.AccountSubscriptionStatus,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteAccountSubscription(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	if err := h.Backend.DeleteAccountSubscription(accountID); err != nil {
		if errors.Is(err, ErrAccountTerminationProtectionEnabled) {
			return writeError(c, http.StatusBadRequest, "PreconditionNotMetException", err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Account Customization ----

// accountCustomizationFromBody extracts DefaultTheme/DefaultEmailCustomizationTemplate
// fields, accepting either the AWS-shaped body (fields nested under an
// "AccountCustomization" object) or a flat body with the fields at the top level.
func accountCustomizationFromBody(body map[string]any) (string, string) {
	src := body
	if wrapped, ok := body[keyAccountCustomization].(map[string]any); ok {
		src = wrapped
	}

	return strField(src, keyDefaultTheme), strField(src, keyDefaultEmailCustomizationTmpl)
}

func accountCustomizationToMap(c *AccountCustomization) map[string]any {
	return map[string]any{
		keyDefaultTheme:                  c.DefaultTheme,
		keyDefaultEmailCustomizationTmpl: c.DefaultEmailCustomizationTemplate,
	}
}

func (h *Handler) handleCreateAccountCustomization(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)
	namespace := queryParam(c, queryParamNamespace)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	defaultTheme, defaultEmailTemplate := accountCustomizationFromBody(body)

	cust, err := h.Backend.CreateAccountCustomization(accountID, namespace, defaultTheme, defaultEmailTemplate)
	if err != nil {
		if errors.Is(err, ErrAccountCustomizationAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAccountCustomization: accountCustomizationToMap(cust),
		keyAwsAccountID:         accountID,
		keyNamespaceField:       namespace,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	})
}

func (h *Handler) handleDescribeAccountCustomization(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)
	namespace := queryParam(c, queryParamNamespace)
	resolved := queryParam(c, queryParamResolved) == queryValueTrue

	cust, err := h.Backend.DescribeAccountCustomization(accountID, namespace, resolved)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAccountCustomization: accountCustomizationToMap(cust),
		keyAwsAccountID:         accountID,
		keyNamespaceField:       namespace,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	})
}

func (h *Handler) handleUpdateAccountCustomization(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)
	namespace := queryParam(c, queryParamNamespace)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	defaultTheme, defaultEmailTemplate := accountCustomizationFromBody(body)

	cust, err := h.Backend.UpdateAccountCustomization(accountID, namespace, defaultTheme, defaultEmailTemplate)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAccountCustomization: accountCustomizationToMap(cust),
		keyAwsAccountID:         accountID,
		keyNamespaceField:       namespace,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	})
}

func (h *Handler) handleDeleteAccountCustomization(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)
	namespace := queryParam(c, queryParamNamespace)

	if err := h.Backend.DeleteAccountCustomization(accountID, namespace); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- IP Restriction ----

func (h *Handler) handleDescribeIPRestriction(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	r, err := h.Backend.DescribeIPRestriction(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyIPRestrictionRuleMap:            r.RuleMap,
		keyVPCIDRestrictionRuleMap:         r.VPCIDRuleMap,
		keyVPCEndpointIDRestrictionRuleMap: r.VPCEndpointIDRuleMap,
		keyEnabled:                         r.Enabled,
		keyAwsAccountID:                    accountID,
		keyRequestID:                       reqIDPlaceholder,
		keyStatus:                          http.StatusOK,
	})
}

func stringMapFromBody(body map[string]any, key string) map[string]string {
	raw, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}

	out := make(map[string]string, len(raw))
	for k, v := range raw {
		s, _ := v.(string)
		out[k] = s
	}

	return out
}

func (h *Handler) handleUpdateIPRestriction(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	var enabled *bool
	if v, present := body[keyEnabled]; present {
		b, _ := v.(bool)
		enabled = &b
	}

	_, err = h.Backend.UpdateIPRestriction(
		accountID,
		stringMapFromBody(body, keyIPRestrictionRuleMap),
		stringMapFromBody(body, keyVPCIDRestrictionRuleMap),
		stringMapFromBody(body, keyVPCEndpointIDRestrictionRuleMap),
		enabled,
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAwsAccountID: accountID,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

// ---- Public Sharing ----

func (h *Handler) handleUpdatePublicSharingSettings(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	enabled, _ := body[keyPublicSharingEnabled].(bool)

	if err = h.Backend.UpdatePublicSharingSettings(accountID, enabled); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Key Registration ----

// DescribeKeyRegistrationOutput (quicksight@v1.123.1 deserializers.go's
// awsRestjson1_deserializeOpDocumentDescribeKeyRegistrationOutput) wraps the
// list under "KeyRegistration", not "RegisteredCustomerManagedKeys" (that's
// the name of the array's own item type, types.RegisteredCustomerManagedKey).
func (h *Handler) handleDescribeKeyRegistration(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	keys, err := h.Backend.DescribeKeyRegistration(accountID, queryParam(c, queryParamDefaultKeyOnly) == queryValueTrue)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyKeyRegistration: registeredKeysToList(keys),
		keyAwsAccountID:    accountID,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func registeredKeysToList(keys []RegisteredCustomerManagedKey) []map[string]any {
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{keyKeyArn: k.KeyArn, keyDefaultKey: k.DefaultKey})
	}

	return out
}

func registeredKeysFromBody(body map[string]any) []RegisteredCustomerManagedKey {
	raw, _ := body[keyKeyRegistration].([]any)
	out := make([]RegisteredCustomerManagedKey, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		defaultKey, _ := m[keyDefaultKey].(bool)
		out = append(out, RegisteredCustomerManagedKey{
			KeyArn:     strField(m, keyKeyArn),
			DefaultKey: defaultKey,
		})
	}

	return out
}

func (h *Handler) handleUpdateKeyRegistration(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	keys, err := h.Backend.UpdateKeyRegistration(accountID, registeredKeysFromBody(body))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySuccessfulKeyRegistration: registeredKeysToList(keys),
		keyFailedKeyRegistration:     []any{},
		keyRequestID:                 reqIDPlaceholder,
		keyStatus:                    http.StatusOK,
	})
}

// ---- Default Q Business Application ----

// DescribeDefaultQBusinessApplicationOutput (quicksight@v1.123.1
// deserializers.go's awsRestjson1_deserializeOpDocumentDescribeDefaultQBusinessApplicationOutput)
// is flat -- ApplicationId/RequestId only, no "DefaultQBusinessApplication"
// wrapper and no Namespace echo (Namespace is Input-side only).
func (h *Handler) handleDescribeDefaultQBiz(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)
	namespace := queryParam(c, queryParamNamespace)

	a, err := h.Backend.DescribeDefaultQBusinessApplication(accountID, namespace)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApplicationID: a.ApplicationID,
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleUpdateDefaultQBiz(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)
	namespace := queryParam(c, queryParamNamespace)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if bodyNS := strField(body, keyNamespaceField); bodyNS != "" {
		namespace = bodyNS
	}

	if _, err = h.Backend.UpdateDefaultQBusinessApplication(
		accountID, strField(body, keyApplicationID), namespace,
	); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteDefaultQBiz(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)
	namespace := queryParam(c, queryParamNamespace)

	if err := h.Backend.DeleteDefaultQBusinessApplication(accountID, namespace); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Q Personalization ----

func (h *Handler) handleDescribeQPersonalization(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	mode, err := h.Backend.DescribeQPersonalizationConfiguration(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyPersonalizationMode: mode,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	})
}

func (h *Handler) handleUpdateQPersonalization(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	mode, err := h.Backend.UpdateQPersonalizationConfiguration(
		accountID, strField(body, keyPersonalizationMode),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyPersonalizationMode: mode,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	})
}

// ---- Q Search Configuration ----

func (h *Handler) handleDescribeQSearchConfig(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	status, err := h.Backend.DescribeQuickSightQSearchConfiguration(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyQSearchStatus: status,
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleUpdateQSearchConfig(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	status, err := h.Backend.UpdateQuickSightQSearchConfiguration(
		accountID, strField(body, keyQSearchStatus),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyQSearchStatus: status,
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

// ---- Dashboards Q&A Configuration ----

func (h *Handler) handleDescribeDashboardsQA(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	status, err := h.Backend.DescribeDashboardsQAConfiguration(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDashboardsQAStatus: status,
		keyRequestID:          reqIDPlaceholder,
		keyStatus:             http.StatusOK,
	})
}

func (h *Handler) handleUpdateDashboardsQA(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if _, err = h.Backend.UpdateDashboardsQAConfiguration(
		accountID, strField(body, keyDashboardsQAStatus),
	); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// handleUpdateSPICECapacity validates and stores accountID's SPICE capacity
// purchase mode. The real UpdateSPICECapacityConfiguration response carries
// no data beyond RequestId/Status, so there is nothing else to echo back.
func (h *Handler) handleUpdateSPICECapacity(c *echo.Context) error {
	accountID := seg(pathSegsFromCtx(c), segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if updateErr := h.Backend.UpdateSPICECapacityConfiguration(
		accountID, strField(body, keyPurchaseMode),
	); updateErr != nil {
		return httpErr(c, updateErr)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// classifyAccountSubscriptionPaths routes /account/{accountId} paths.
func classifyAccountSubscriptionPaths(method string, segs []string, _ int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch method {
	case http.MethodPost:
		return opCreateAccountSubscription, accountID
	case http.MethodGet:
		return opDescribeAccountSubscription, accountID
	case http.MethodDelete:
		return opDeleteAccountSubscription, accountID
	}

	return opUnknown, ""
}

// classifyCustomizationPaths routes /accounts/{id}/customizations/... paths.
func classifyCustomizationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodPost:
			return opCreateAccountCustomization, accountID
		case http.MethodGet:
			return opDescribeAccountCustomization, accountID
		case http.MethodPut:
			return opUpdateAccountCustomization, accountID
		case http.MethodDelete:
			return opDeleteAccountCustomization, accountID
		}
	}

	return opUnknown, ""
}

// classifyAccountCustomPermissionPaths routes /accounts/{id}/custom-permission/... paths.
func classifyAccountCustomPermissionPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeAccountCustomPerm, accountID
		case http.MethodPut:
			return opUpdateAccountCustomPerm, accountID
		case http.MethodDelete:
			return opDeleteAccountCustomPerm, accountID
		}
	}

	return opUnknown, ""
}

// classifyAccountSettingsPaths routes /accounts/{id}/settings/... paths.
func classifyAccountSettingsPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeAccountSettings, accountID
		case http.MethodPut:
			return opUpdateAccountSettings, accountID
		}
	}

	return opUnknown, ""
}

// classifyDashboardsQAPaths routes /accounts/{id}/dashboards-qa-configuration/... paths.
func classifyDashboardsQAPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeDashboardsQAConfiguration, accountID
		case http.MethodPut:
			return opUpdateDashboardsQAConfiguration, accountID
		}
	}

	return opUnknown, ""
}

// classifyDefaultQBizPaths routes /accounts/{id}/default-qbusiness-application/... paths.
func classifyDefaultQBizPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeDefaultQBiz, accountID
		case http.MethodPut:
			return opUpdateDefaultQBiz, accountID
		case http.MethodDelete:
			return opDeleteDefaultQBiz, accountID
		}
	}

	return opUnknown, ""
}

// classifyIPRestrictionPaths routes /accounts/{id}/ip-restriction/... paths.
func classifyIPRestrictionPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeIpRestriction, accountID
		case http.MethodPost:
			return opUpdateIpRestriction, accountID
		}
	}

	return opUnknown, ""
}

// classifyKeyRegistrationPaths routes /accounts/{id}/key-registration/... paths.
func classifyKeyRegistrationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeKeyRegistration, accountID
		case http.MethodPost:
			return opUpdateKeyRegistration, accountID
		}
	}

	return opUnknown, ""
}

// classifyQPersonalizationPaths routes /accounts/{id}/q-personalization-configuration/... paths.
func classifyQPersonalizationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeQPersonalization, accountID
		case http.MethodPut:
			return opUpdateQPersonalization, accountID
		}
	}

	return opUnknown, ""
}

// classifyQSearchConfigPaths routes /accounts/{id}/quicksight-q-search-configuration/... paths.
func classifyQSearchConfigPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeQSearchConfig, accountID
		case http.MethodPut:
			return opUpdateQSearchConfig, accountID
		}
	}

	return opUnknown, ""
}

// ---- Account Custom Permission ----

// isAccountCustomPermOp reports whether op is one of the account-level
// Describe/Update/DeleteAccountCustomPermission operations (applying a named
// custom permissions profile to an entire account), as distinct from
// isCustomPermOp's CustomPermissions-profile CRUD family.
func isAccountCustomPermOp(op string) bool {
	switch op {
	case opDescribeAccountCustomPerm, opUpdateAccountCustomPerm, opDeleteAccountCustomPerm:
		return true
	}

	return false
}

func (h *Handler) dispatchAccountCustomPerm(c *echo.Context, op string) error {
	switch op {
	case opDescribeAccountCustomPerm:
		return h.handleDescribeAccountCustomPerm(c)
	case opUpdateAccountCustomPerm:
		return h.handleUpdateAccountCustomPerm(c)
	case opDeleteAccountCustomPerm:
		return h.handleDeleteAccountCustomPerm(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleDescribeAccountCustomPerm(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	name, err := h.Backend.DescribeAccountCustomPermission(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"CustomPermissionsName": name,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	})
}

func (h *Handler) handleUpdateAccountCustomPerm(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if updateErr := h.Backend.UpdateAccountCustomPermission(
		accountID, strField(body, "CustomPermissionsName"),
	); updateErr != nil {
		return httpErr(c, updateErr)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteAccountCustomPerm(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	if err := h.Backend.DeleteAccountCustomPermission(accountID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}
