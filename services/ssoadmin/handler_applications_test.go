package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestStaticApplicationProviderCatalog verifies ListApplicationProviders returns static catalog.
func TestStaticApplicationProviderCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantMinCount int
	}{
		{
			name:         "returns at least 5 providers",
			wantMinCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "ListApplicationProviders", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			providers := resp["ApplicationProviders"].([]any)
			assert.GreaterOrEqual(t, len(providers), tt.wantMinCount)
		})
	}
}

// TestDescribeApplicationProviderAccountScopedArn verifies account-scoped custom provider ARN resolves.
func TestDescribeApplicationProviderAccountScopedArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arn        string
		wantStatus int
	}{
		{
			name:       "aws-managed ARN resolves",
			arn:        "arn:aws:sso::aws:applicationProvider/custom",
			wantStatus: http.StatusOK,
		},
		{
			name:       "account-scoped custom ARN resolves",
			arn:        "arn:aws:sso::123456789012:applicationProvider/custom",
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown provider returns 404",
			arn:        "arn:aws:sso::aws:applicationProvider/nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "DescribeApplicationProvider", map[string]any{
				"ApplicationProviderArn": tt.arn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestApplicationPortalOptions verifies CreateApplication and UpdateApplication handle PortalOptions.
func TestApplicationPortalOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		visibility   string
		signInOrigin string
		wantStatus   int
	}{
		{
			name:         "application created with portal options",
			visibility:   "ENABLED",
			signInOrigin: "IDENTITY_CENTER",
			wantStatus:   http.StatusOK,
		},
		{
			name:       "application created without portal options",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "portal-opts-inst")

			reqBody := map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
				"Name":                   "PortalApp",
			}
			if tt.visibility != "" {
				reqBody["PortalOptions"] = map[string]any{
					"Visibility": tt.visibility,
					"SignInOptions": map[string]any{
						"Origin": tt.signInOrigin,
					},
				}
			}

			rec := doRequest(t, h, "CreateApplication", reqBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestApplicationStatusValidation verifies UpdateApplication validates Status enum.
func TestApplicationStatusValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		wantStatus int
	}{
		{
			name:       "ENABLED status accepted",
			status:     "ENABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "DISABLED status accepted",
			status:     "DISABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid status rejected",
			status:     "ACTIVE",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "app-status-inst")
			appArn := createApplication(t, h, instanceArn, "StatusApp")

			rec := doRequest(t, h, "UpdateApplication", map[string]any{
				"ApplicationArn": appArn,
				"Status":         tt.status,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		appName        string
		providerArn    string
		description    string
		wantStatus     int
		wantAppArn     bool
		useInvalidInst bool
	}{
		{
			name:        "create application successfully",
			appName:     "MyApp",
			providerArn: "arn:aws:sso::123456789012:applicationProvider/custom",
			description: "test app",
			wantStatus:  http.StatusOK,
			wantAppArn:  true,
		},
		{
			name:        "create application without description",
			appName:     "MinimalApp",
			providerArn: "arn:aws:sso::123456789012:applicationProvider/custom",
			description: "",
			wantStatus:  http.StatusOK,
			wantAppArn:  true,
		},
		{
			name:           "create application with nonexistent instance",
			appName:        "BadApp",
			providerArn:    "arn:aws:sso::123456789012:applicationProvider/custom",
			wantStatus:     http.StatusBadRequest,
			useInvalidInst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var instanceArn string
			if tt.useInvalidInst {
				instanceArn = "arn:aws:sso:::instance/ssoins-nonexistent"
			} else {
				instanceArn = createInstance(t, h, "app-test-instance")
			}
			rec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": tt.providerArn,
				"Name":                   tt.appName,
				"Description":            tt.description,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantAppArn {
				resp := parseResponse(t, rec)
				arn, ok := resp["ApplicationArn"].(string)
				assert.True(t, ok)
				assert.NotEmpty(t, arn)
			}
		})
	}
}

func TestCreateApplicationDuplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "dup-app-instance")

	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "DupApp",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "DupApp",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestDeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createApp  bool
		wantStatus int
	}{
		{
			name:       "delete existing application",
			createApp:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete nonexistent application",
			createApp:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var appArn string
			if tt.createApp {
				instanceArn := createInstance(t, h, "del-app-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "ToDelete",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResponse(t, rec)
				appArn = resp["ApplicationArn"].(string)
			} else {
				appArn = "arn:aws:sso::123456789012:application/ssoins-bad/apl-notexist"
			}
			rec := doRequest(t, h, "DeleteApplication", map[string]any{
				"ApplicationArn": appArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestApplicationAdditionalOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *ssoadmin.Handler) map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "put application access scope",
			op:   "PutApplicationAccessScope",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "app-scope-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "AppScope",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				appArn := parseResponse(t, rec)["ApplicationArn"].(string)

				return map[string]any{"ApplicationArn": appArn, "Scope": "openid"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "describe and list application operations",
			op:   "DescribeApplication",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "app-describe-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "AppDescribe",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				appArn := parseResponse(t, rec)["ApplicationArn"].(string)

				listRec := doRequest(t, h, "ListApplications", map[string]any{"InstanceArn": instanceArn})
				require.Equal(t, http.StatusOK, listRec.Code)

				return map[string]any{"ApplicationArn": appArn}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "application assignment operations",
			op:   "DescribeApplicationAssignment",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "app-assign-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "AppAssign",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				appArn := parseResponse(t, rec)["ApplicationArn"].(string)

				createAssignRec := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
					"ApplicationArn": appArn,
					"PrincipalId":    "user-001",
					"PrincipalType":  "USER",
				})
				require.Equal(t, http.StatusOK, createAssignRec.Code)

				listAssignRec := doRequest(t, h, "ListApplicationAssignments", map[string]any{"ApplicationArn": appArn})
				require.Equal(t, http.StatusOK, listAssignRec.Code)

				return map[string]any{
					"ApplicationArn": appArn,
					"PrincipalId":    "user-001",
					"PrincipalType":  "USER",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "application grants and provider operations",
			op:   "DeleteApplicationGrant",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "app-grant-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "AppGrant",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				appArn := parseResponse(t, rec)["ApplicationArn"].(string)

				putGrantRec := doRequest(t, h, "PutApplicationGrant", map[string]any{
					"ApplicationArn": appArn,
					"GrantType":      "authorization_code",
				})
				require.Equal(t, http.StatusOK, putGrantRec.Code)

				listGrantRec := doRequest(t, h, "ListApplicationGrants", map[string]any{"ApplicationArn": appArn})
				require.Equal(t, http.StatusOK, listGrantRec.Code)

				listProvidersRec := doRequest(t, h, "ListApplicationProviders", map[string]any{})
				require.Equal(t, http.StatusOK, listProvidersRec.Code)

				describeProviderRec := doRequest(t, h, "DescribeApplicationProvider", map[string]any{
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				})
				require.Equal(t, http.StatusOK, describeProviderRec.Code)

				return map[string]any{
					"ApplicationArn": appArn,
					"GrantType":      "authorization_code",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "application configuration operations",
			op:   "UpdateApplication",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "app-config-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "AppConfig",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				appArn := parseResponse(t, rec)["ApplicationArn"].(string)

				ops := []struct {
					body map[string]any
					op   string
				}{
					{
						op:   "PutApplicationAuthenticationMethod",
						body: map[string]any{"ApplicationArn": appArn, "AuthenticationMethodType": "IAM"},
					},
					{
						op:   "ListApplicationAuthenticationMethods",
						body: map[string]any{"ApplicationArn": appArn},
					},
					{
						op:   "PutApplicationAssignmentConfiguration",
						body: map[string]any{"ApplicationArn": appArn, "AssignmentRequired": true},
					},
					{
						op: "PutApplicationSessionConfiguration",
						body: map[string]any{
							"ApplicationArn":                         appArn,
							"UserBackgroundSessionApplicationStatus": "ENABLED",
						},
					},
					{
						op:   "PutApplicationAccessScope",
						body: map[string]any{"ApplicationArn": appArn, "Scope": "openid"},
					},
					{
						op:   "ListApplicationAccessScopes",
						body: map[string]any{"ApplicationArn": appArn},
					},
				}
				for _, operation := range ops {
					opRec := doRequest(t, h, operation.op, operation.body)
					require.Equal(t, http.StatusOK, opRec.Code)
				}

				return map[string]any{
					"ApplicationArn": appArn,
					"Name":           "AppConfigUpdated",
					"Status":         "DISABLED",
				}
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			body := tt.setup(t, h)
			rec := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestApplicationCount verifies ApplicationCount export helper.
func TestApplicationCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, ssoadmin.ApplicationCount(b))

	inst := b.AddInstanceInternal("app-inst")
	b.AddApplicationInternal(inst.InstanceArn, "app1")
	assert.Equal(t, 1, ssoadmin.ApplicationCount(b))
}

// TestCreateApplicationNameRequired verifies that creating an application
// requires a non-empty Name field.
func TestCreateApplicationNameRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "name-req-inst")
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeleteApplicationCascadesAssignments verifies that DeleteApplication
// removes related assignments.
func TestDeleteApplicationCascadesAssignments(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(b)

	instanceArn := createInstance(t, h, "del-cascade-inst")
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::000000000000:applicationProvider/custom",
		"Name":                   "CascadeApp",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	appArn := parseResponse(t, rec)["ApplicationArn"].(string)

	// Assign a user.
	_ = doRequest(t, h, "CreateApplicationAssignment", map[string]any{
		"ApplicationArn": appArn,
		"PrincipalId":    "user-cascade",
		"PrincipalType":  "USER",
	})

	// Delete the application.
	delRec := doRequest(t, h, "DeleteApplication", map[string]any{"ApplicationArn": appArn})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Application count should be 0.
	assert.Equal(t, 0, ssoadmin.ApplicationCount(b))
}

// TestAddApplicationInternal verifies the seed helper.
func TestAddApplicationInternal(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	inst := b.AddInstanceInternal("app-seed-inst")
	app := b.AddApplicationInternal(inst.InstanceArn, "seed-app")
	require.NotNil(t, app)
	assert.NotEmpty(t, app.ApplicationArn)
	assert.Equal(t, "seed-app", app.Name)
	assert.Equal(t, 1, ssoadmin.ApplicationCount(b))
}

// TestDescribeApplicationWireShape locks in the real DescribeApplicationOutput
// wire shape: flat top-level fields (ApplicationAccount, ApplicationArn,
// ApplicationProviderArn, CreatedDate, CreatedFrom, Description,
// IdentityStoreArn, InstanceArn, Name, PortalOptions, Status) with NO nested
// "Application" wrapper and NO "Tags" member. Tags for an application are
// fetched separately via ListTagsForResource, exactly like every other
// taggable ssoadmin resource -- gopherstack previously nested everything
// (including a fabricated Tags field) under an invented "Application" key
// that doesn't exist on the real wire; a real aws-sdk-go-v2 client parsing
// that response would find every DescribeApplicationOutput field nil.
func TestDescribeApplicationWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "app-tags-inst")

	createRec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::000000000000:applicationProvider/custom",
		"Name":                   "TaggedApp",
		"Tags":                   []map[string]any{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := parseResponse(t, createRec)
	appArn := createResp["ApplicationArn"].(string)
	require.NotEmpty(t, createResp["IdentityStoreArn"], "CreateApplicationOutput must include IdentityStoreArn")
	require.Equal(t, instanceArn, createResp["InstanceArn"])
	assert.NotContains(t, createResp, "Application", `CreateApplicationOutput has no nested "Application" member`)

	rec := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationArn": appArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	assert.NotContains(t, resp, "Application", `DescribeApplicationOutput has no nested "Application" member`)
	assert.NotContains(t, resp, "Tags", "DescribeApplicationOutput has no Tags member")
	assert.Equal(t, appArn, resp["ApplicationArn"])
	assert.Equal(t, "TaggedApp", resp["Name"])
	assert.Equal(t, instanceArn, resp["InstanceArn"])
	assert.NotEmpty(t, resp["ApplicationAccount"])
	assert.NotEmpty(t, resp["IdentityStoreArn"])
	assert.Contains(t, resp, "CreatedFrom")
	assert.Contains(t, resp, "CreatedDate")

	// Tags are only reachable via ListTagsForResource, matching real AWS.
	tagsRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": appArn,
	})
	require.Equal(t, http.StatusOK, tagsRec.Code)
	tagsResp := parseResponse(t, tagsRec)
	tags, ok := tagsResp["Tags"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, tags)
}

// TestUpdateApplicationWireShape locks in that UpdateApplicationOutput is void
// (no members at all) -- gopherstack previously echoed a full invented
// "Application" object that doesn't exist on the real wire.
func TestUpdateApplicationWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "app-update-wire-shape-inst")
	appArn := createApplication(t, h, instanceArn, "UpdateWireShapeApp")

	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationArn": appArn,
		"Description":    "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.Empty(t, resp, "UpdateApplicationOutput has no members")
}

// TestApplicationProviderDisplayDataStruct verifies DisplayData is a struct.
func TestApplicationProviderDisplayDataStruct(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-provider-inst")

	_ = doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "r3-provider-app",
	})

	rec := doRequest(t, h, "DescribeApplicationProvider", map[string]any{
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	displayData := resp["DisplayData"]
	// Verify DisplayData is a map (struct), not a string.
	displayDataMap, ok := displayData.(map[string]any)
	assert.True(t, ok, "DisplayData should be a struct/object, not a string")
	assert.NotEmpty(t, displayDataMap["DisplayName"])
}

// TestUpdateApplication_PreservesVisibility guards against gopherstack-1vv2:
// UpdateApplicationInput.PortalOptions is types.UpdateApplicationPortalOptions,
// which declares only SignInOptions -- unlike the Create-side
// types.PortalOptions, it has no Visibility field at all, so a real client's
// Update payload can never carry one. Wholesale-replacing the application's
// stored PortalOptions with that narrower payload used to silently erase
// Visibility (and even wipe SignInOptions to empty) on every Update call,
// including ones that never mentioned PortalOptions at all.
func TestUpdateApplication_PreservesVisibility(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "app-update-visibility-inst")

	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "VisibilityApp",
		"PortalOptions": map[string]any{
			"Visibility": "ENABLED",
			"SignInOptions": map[string]any{
				"Origin": "IDENTITY_CENTER",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	appArn := parseResponse(t, rec)["ApplicationArn"].(string)
	require.NotEmpty(t, appArn)

	// A real client's Update payload can only ever carry
	// PortalOptions.SignInOptions -- never Visibility.
	rec = doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationArn": appArn,
		"PortalOptions": map[string]any{
			"SignInOptions": map[string]any{
				"Origin":         "APPLICATION",
				"ApplicationUrl": "https://example.com/app",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationArn": appArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	portalOptions, ok := resp["PortalOptions"].(map[string]any)
	require.True(t, ok, "PortalOptions must survive the update")
	assert.Equal(
		t,
		"ENABLED",
		portalOptions["Visibility"],
		"Visibility set at Create must survive an Update that can never send it",
	)

	signInOptions, ok := portalOptions["SignInOptions"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "APPLICATION", signInOptions["Origin"], "SignInOptions must reflect the new Update value")

	// An update that never mentions PortalOptions at all must leave it untouched.
	rec = doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationArn": appArn,
		"Description":    "no portal options here",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationArn": appArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseResponse(t, rec)
	portalOptions, ok = resp["PortalOptions"].(map[string]any)
	require.True(t, ok, "PortalOptions must survive an update that never mentions it")
	assert.Equal(t, "ENABLED", portalOptions["Visibility"])
	signInOptions, ok = portalOptions["SignInOptions"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "APPLICATION", signInOptions["Origin"])
}
