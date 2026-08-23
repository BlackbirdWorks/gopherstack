package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// subscriptionPath returns the /account/{id} (singular) path used by the
// AccountSubscription operations.
func subscriptionPath() string {
	return fmt.Sprintf("/account/%s", testAccountID)
}

// ---- Account Settings round-trip ----

func TestQuickSight_AccountSettings_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "DescribeAccountSettings returns AWS defaults when never set",
			method:   http.MethodGet,
			path:     accountPath("/settings"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				s, ok := body["AccountSettings"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "STANDARD", s["Edition"])
				assert.Equal(t, "default", s["DefaultNamespace"])
				assert.Empty(t, s["NotificationEmail"])
				assert.Equal(t, false, s["TerminationProtectionEnabled"])
			},
		},
		{
			name:   "DescribeAccountSettings reflects prior update",
			method: http.MethodGet,
			path:   accountPath("/settings"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPut, accountPath("/settings"), map[string]any{
					"NotificationEmail":            "ops@example.com",
					"DefaultNamespace":             "custom-ns",
					"TerminationProtectionEnabled": true,
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				s, ok := body["AccountSettings"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "ops@example.com", s["NotificationEmail"])
				assert.Equal(t, "custom-ns", s["DefaultNamespace"])
				assert.Equal(t, true, s["TerminationProtectionEnabled"])
			},
		},
		{
			name:   "PublicSharingEnabled reflects UpdatePublicSharingSettings",
			method: http.MethodGet,
			path:   accountPath("/settings"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPut, accountPath("/public-sharing-settings"), map[string]any{
					"PublicSharingEnabled": true,
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				s, ok := body["AccountSettings"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, s["PublicSharingEnabled"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// ---- Account Subscription round-trip ----

func TestQuickSight_AccountSubscription_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "DescribeAccountSubscription missing returns 404",
			method:   http.MethodGet,
			path:     subscriptionPath(),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "CreateAccountSubscription then Describe reflects it",
			method: http.MethodGet,
			path:   subscriptionPath(),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, subscriptionPath(), map[string]any{
					"AccountName": "acme", "Edition": "ENTERPRISE", "NotificationEmail": "a@b.com",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				info, ok := body["AccountInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "acme", info["AccountName"])
				assert.Equal(t, "ENTERPRISE", info["Edition"])
				assert.Equal(t, "a@b.com", info["NotificationEmail"])
				assert.NotEmpty(t, info["AccountSubscriptionStatus"])
			},
		},
		{
			name:   "CreateAccountSubscription duplicate returns 409",
			method: http.MethodPost,
			path:   subscriptionPath(),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, subscriptionPath(), map[string]any{"Edition": "STANDARD"})
			},
			body:     map[string]any{"Edition": "STANDARD"},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DeleteAccountSubscription then Describe returns 404",
			method: http.MethodGet,
			path:   subscriptionPath(),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, subscriptionPath(), map[string]any{"Edition": "STANDARD"})
				doRequest(t, h, http.MethodDelete, subscriptionPath(), nil)
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteAccountSubscription missing returns 404",
			method:   http.MethodDelete,
			path:     subscriptionPath(),
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// ---- Account Customization round-trip ----

func TestQuickSight_AccountCustomization_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "DescribeAccountCustomization missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/customizations"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "CreateAccountCustomization then Describe reflects it",
			method: http.MethodGet,
			path:   accountPath("/customizations"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/customizations"), map[string]any{
					"AccountCustomization": map[string]any{
						"DefaultTheme": "arn:aws:quicksight::aws:theme/MIDNIGHT",
					},
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				c, ok := body["AccountCustomization"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "arn:aws:quicksight::aws:theme/MIDNIGHT", c["DefaultTheme"])
			},
		},
		{
			name:   "CreateAccountCustomization duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/customizations"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/customizations"), map[string]any{
					"AccountCustomization": map[string]any{},
				})
			},
			body:     map[string]any{"AccountCustomization": map[string]any{}},
			wantCode: http.StatusConflict,
		},
		{
			name:   "UpdateAccountCustomization then Describe reflects the change",
			method: http.MethodGet,
			path:   accountPath("/customizations"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/customizations"), map[string]any{
					"AccountCustomization": map[string]any{"DefaultTheme": "old-theme"},
				})
				doRequest(t, h, http.MethodPut, accountPath("/customizations"), map[string]any{
					"AccountCustomization": map[string]any{"DefaultTheme": "new-theme"},
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				c, ok := body["AccountCustomization"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "new-theme", c["DefaultTheme"])
			},
		},
		{
			name:     "UpdateAccountCustomization missing returns 404",
			method:   http.MethodPut,
			path:     accountPath("/customizations"),
			body:     map[string]any{"AccountCustomization": map[string]any{"DefaultTheme": "x"}},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteAccountCustomization then Describe returns 404",
			method: http.MethodGet,
			path:   accountPath("/customizations"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/customizations"), map[string]any{
					"AccountCustomization": map[string]any{},
				})
				doRequest(t, h, http.MethodDelete, accountPath("/customizations"), nil)
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteAccountCustomization missing returns 404",
			method:   http.MethodDelete,
			path:     accountPath("/customizations"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "Namespace-scoped customization is independent of account-level",
			method: http.MethodGet,
			path:   accountPath("/customizations") + "?namespace=ns1",
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/customizations"), map[string]any{
					"AccountCustomization": map[string]any{"DefaultTheme": "account-level"},
				})
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// ---- IP Restriction round-trip ----

func TestQuickSight_IPRestriction_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "DescribeIpRestriction returns disabled defaults when never set",
			method:   http.MethodGet,
			path:     accountPath("/ip-restriction"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, false, body["Enabled"])
				assert.Empty(t, body["IpRestrictionRuleMap"])
			},
		},
		{
			name:   "UpdateIpRestriction then Describe reflects the change",
			method: http.MethodGet,
			path:   accountPath("/ip-restriction"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/ip-restriction"), map[string]any{
					"Enabled": true,
					"IpRestrictionRuleMap": map[string]any{
						"10.0.0.0/24": "office",
					},
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, true, body["Enabled"])
				ruleMap, ok := body["IpRestrictionRuleMap"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "office", ruleMap["10.0.0.0/24"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// ---- Key Registration round-trip ----

func TestQuickSight_KeyRegistration_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "DescribeKeyRegistration returns empty when never set",
			method:   http.MethodGet,
			path:     accountPath("/key-registration"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Empty(t, body["KeyRegistration"])
			},
		},
		{
			name:   "UpdateKeyRegistration then Describe reflects the change",
			method: http.MethodGet,
			path:   accountPath("/key-registration"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/key-registration"), map[string]any{
					"KeyRegistration": []any{
						map[string]any{"KeyArn": "arn:aws:kms:us-east-1:000000000000:key/abc", "DefaultKey": true},
					},
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				keys, ok := body["KeyRegistration"].([]any)
				require.True(t, ok)
				require.Len(t, keys, 1)
				k, ok := keys[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc", k["KeyArn"])
				assert.Equal(t, true, k["DefaultKey"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// ---- Default Q Business Application round-trip ----

func TestQuickSight_DefaultQBusinessApplication_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "DescribeDefaultQBusinessApplication returns empty when never set",
			method:   http.MethodGet,
			path:     accountPath("/default-qbusiness-application"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Empty(t, body["ApplicationId"])
			},
		},
		{
			name:   "UpdateDefaultQBusinessApplication then Describe reflects it",
			method: http.MethodGet,
			path:   accountPath("/default-qbusiness-application"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPut, accountPath("/default-qbusiness-application"), map[string]any{
					"ApplicationId": "qbiz-123",
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "qbiz-123", body["ApplicationId"])
			},
		},
		{
			name:     "DeleteDefaultQBusinessApplication missing returns 404",
			method:   http.MethodDelete,
			path:     accountPath("/default-qbusiness-application"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteDefaultQBusinessApplication then Describe returns empty again",
			method: http.MethodGet,
			path:   accountPath("/default-qbusiness-application"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPut, accountPath("/default-qbusiness-application"), map[string]any{
					"ApplicationId": "qbiz-123",
				})
				doRequest(t, h, http.MethodDelete, accountPath("/default-qbusiness-application"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Empty(t, body["ApplicationId"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

// ---- Q Personalization / Q Search / Dashboards Q&A toggle round-trips ----

func TestQuickSight_ToggleConfigurations_RoundTrip(t *testing.T) {
	t.Parallel()

	type toggleCase struct {
		describePath string
		updatePath   string
		statusKey    string
	}

	cases := []toggleCase{
		{
			describePath: accountPath("/q-personalization-configuration"),
			updatePath:   accountPath("/q-personalization-configuration"),
			statusKey:    "PersonalizationMode",
		},
		{
			describePath: accountPath("/quicksight-q-search-configuration"),
			updatePath:   accountPath("/quicksight-q-search-configuration"),
			statusKey:    "QSearchStatus",
		},
		{
			describePath: accountPath("/dashboards-qa-configuration"),
			updatePath:   accountPath("/dashboards-qa-configuration"),
			statusKey:    "DashboardsQAStatus",
		},
	}

	for _, tc := range cases {
		t.Run(tc.statusKey, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// 1. Describe default is DISABLED.
			rec := doRequest(t, h, http.MethodGet, tc.describePath, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			body := parseBody(t, rec)
			assert.Equal(t, "DISABLED", body[tc.statusKey])

			// 2. Update to ENABLED.
			rec = doRequest(t, h, http.MethodPut, tc.updatePath, map[string]any{tc.statusKey: "ENABLED"})
			assert.Equal(t, http.StatusOK, rec.Code)

			// 3. Describe now returns ENABLED.
			rec = doRequest(t, h, http.MethodGet, tc.describePath, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			body = parseBody(t, rec)
			assert.Equal(t, "ENABLED", body[tc.statusKey])

			// 4. Invalid status value is rejected.
			rec = doRequest(t, h, http.MethodPut, tc.updatePath, map[string]any{tc.statusKey: "BOGUS"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestQuickSight_AccountPaths_Sanity(t *testing.T) {
	t.Parallel()
	assert.Equal(t, fmt.Sprintf("/accounts/%s/settings", testAccountID), accountPath("/settings"))
}

// ---- Account-level settings tests ---- //nolint:godot // existing issue.
func TestQuickSight_AccountSettings(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create account customization",
			method:     http.MethodPost,
			path:       accountPath("/customizations"),
			body:       map[string]any{"AccountCustomization": map[string]any{}},
			wantStatus: http.StatusOK,
			wantKey:    "AccountCustomization",
		},
		{
			name:       "describe account customization",
			method:     http.MethodGet,
			path:       accountPath("/customizations"),
			wantStatus: http.StatusOK,
			wantKey:    "AccountCustomization",
		},
		{
			name:       "update account customization",
			method:     http.MethodPut,
			path:       accountPath("/customizations"),
			body:       map[string]any{"DefaultTheme": "arn:aws:quicksight::aws:theme/MIDNIGHT"},
			wantStatus: http.StatusOK,
			wantKey:    "AccountCustomization",
		},
		{
			name:       "describe account settings",
			method:     http.MethodGet,
			path:       accountPath("/settings"),
			wantStatus: http.StatusOK,
			wantKey:    "AccountSettings",
		},
		{
			name:       "update account settings",
			method:     http.MethodPut,
			path:       accountPath("/settings"),
			body:       map[string]any{"NotificationEmail": "test@example.com"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe ip restriction",
			method:     http.MethodGet,
			path:       accountPath("/ip-restriction"),
			wantStatus: http.StatusOK,
			wantKey:    "IpRestrictionRuleMap",
		},
		{
			name:       "update ip restriction",
			method:     http.MethodPost,
			path:       accountPath("/ip-restriction"),
			body:       map[string]any{"Enabled": true},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe key registration",
			method:     http.MethodGet,
			path:       accountPath("/key-registration"),
			wantStatus: http.StatusOK,
			wantKey:    "KeyRegistration",
		},
		{
			name:       "update key registration",
			method:     http.MethodPost,
			path:       accountPath("/key-registration"),
			body:       map[string]any{"KeyRegistration": []any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update public sharing settings",
			method:     http.MethodPut,
			path:       accountPath("/public-sharing-settings"),
			body:       map[string]any{"PublicSharingEnabled": true},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe q personalization",
			method:     http.MethodGet,
			path:       accountPath("/q-personalization-configuration"),
			wantStatus: http.StatusOK,
			wantKey:    "PersonalizationMode",
		},
		{
			name:       "update q personalization",
			method:     http.MethodPut,
			path:       accountPath("/q-personalization-configuration"),
			body:       map[string]any{"PersonalizationMode": "DISABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe q search config",
			method:     http.MethodGet,
			path:       accountPath("/quicksight-q-search-configuration"),
			wantStatus: http.StatusOK,
			wantKey:    "QSearchStatus",
		},
		{
			name:       "update q search config",
			method:     http.MethodPut,
			path:       accountPath("/quicksight-q-search-configuration"),
			body:       map[string]any{"QSearchStatus": "DISABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update spice capacity",
			method:     http.MethodPost,
			path:       accountPath("/spice-capacity-configuration"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe default qbusiness",
			method:     http.MethodGet,
			path:       accountPath("/default-qbusiness-application"),
			wantStatus: http.StatusOK,
			wantKey:    "ApplicationId",
		},
		{
			name:       "update default qbusiness",
			method:     http.MethodPut,
			path:       accountPath("/default-qbusiness-application"),
			body:       map[string]any{"ApplicationId": "qbiz1"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update app token grant",
			method:     http.MethodPut,
			path:       accountPath("/application-with-token-exchange-grant"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:   "get identity context",
			method: http.MethodPost,
			path:   accountPath("/identity-context"),
			body: map[string]any{
				"UserIdentifier": map[string]any{
					"UserArn": "arn:aws:quicksight:us-east-1:000000000000:user/default/u1",
				},
			},
			wantStatus: http.StatusOK,
			wantKey:    "Context",
		},
		{
			name:       "delete account customization",
			method:     http.MethodDelete,
			path:       accountPath("/customizations"),
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status for "+tc.name)
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}

func accountSingularPath(sub string) string {
	return fmt.Sprintf("/account/%s%s", testAccountID, sub)
}

// ---- Account Subscription tests ---- //nolint:godot // existing issue.
func TestQuickSight_AccountSubscription(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create account subscription",
			method:     http.MethodPost,
			path:       accountSingularPath(""),
			body:       map[string]any{"Edition": "ENTERPRISE"},
			wantStatus: http.StatusOK,
			wantKey:    "SignupResponse",
		},
		{
			name:       "describe account subscription",
			method:     http.MethodGet,
			path:       accountSingularPath(""),
			wantStatus: http.StatusOK,
			wantKey:    "AccountInfo",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}
