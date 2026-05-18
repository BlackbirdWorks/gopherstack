package verifiedpermissions_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

// helpers -------------------------------------------------------------------

func newRefinement1Backend() *verifiedpermissions.InMemoryBackend {
	return verifiedpermissions.NewInMemoryBackend("123456789012", "us-east-1")
}

func seedPolicyStore(
	t *testing.T,
	b *verifiedpermissions.InMemoryBackend,
	desc string,
) *verifiedpermissions.PolicyStore {
	t.Helper()

	ps, err := b.CreatePolicyStore(desc, nil, "OFF", "")
	require.NoError(t, err)

	return ps
}

// export_test.go helpers ----------------------------------------------------

func TestRefinement1_ExportHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*verifiedpermissions.InMemoryBackend)
		name      string
		wantPS    int
		wantPol   int
		wantTmpl  int
		wantIS    int
		wantSch   int
		wantARN   int
		wantOpsGE int
	}{
		{
			name:      "empty backend",
			setup:     func(*verifiedpermissions.InMemoryBackend) {},
			wantPS:    0,
			wantPol:   0,
			wantTmpl:  0,
			wantIS:    0,
			wantSch:   0,
			wantARN:   0,
			wantOpsGE: 30,
		},
		{
			name: "one policy store with policy and template",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				ps, _ := b.CreatePolicyStore("desc", nil, "OFF", "")
				_, _ = b.CreatePolicy(
					ps.PolicyStoreID,
					verifiedpermissions.CreatePolicyParams{
						PolicyType: "STATIC",
						Statement:  "permit(principal,action,resource);",
					},
				)
				_, _ = b.CreatePolicyTemplate(ps.PolicyStoreID, "tmpl", "permit(principal,action,resource);")
				_, _ = b.CreateIdentitySource(
					ps.PolicyStoreID,
					"User",
					verifiedpermissions.IdentitySourceConfig{
						UserPoolArn: "arn:aws:cognito-idp:us-east-1:123456789012:userpool/pool",
					},
				)
				_, _ = b.PutSchema(ps.PolicyStoreID, `{"ns":{}}`)
			},
			wantPS:    1,
			wantPol:   1,
			wantTmpl:  1,
			wantIS:    1,
			wantSch:   1,
			wantARN:   4,
			wantOpsGE: 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend()
			tt.setup(b)
			h := verifiedpermissions.NewHandler(b)

			assert.Equal(t, tt.wantPS, verifiedpermissions.PolicyStoreCount(b))
			assert.Equal(t, tt.wantPol, verifiedpermissions.PolicyCount(b))
			assert.Equal(t, tt.wantTmpl, verifiedpermissions.PolicyTemplateCount(b))
			assert.Equal(t, tt.wantIS, verifiedpermissions.IdentitySourceCount(b))
			assert.Equal(t, tt.wantSch, verifiedpermissions.SchemaCount(b))
			assert.Equal(t, tt.wantARN, verifiedpermissions.ARNIndexSize(b))
			assert.GreaterOrEqual(t, verifiedpermissions.HandlerOpsLen(h), tt.wantOpsGE)
		})
	}
}

// AccountID -----------------------------------------------------------------

func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := verifiedpermissions.NewInMemoryBackend("111222333444", "eu-west-1")
	assert.Equal(t, "111222333444", b.AccountID())
}

// Handler.Reset -------------------------------------------------------------

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend()
	_, _ = b.CreatePolicyStore("store", nil, "OFF", "")

	require.Equal(t, 1, verifiedpermissions.PolicyStoreCount(b))

	h := verifiedpermissions.NewHandler(b)
	h.Reset()

	assert.Equal(t, 0, verifiedpermissions.PolicyStoreCount(b))
}

// ARN index O(1) tag ops ----------------------------------------------------

func TestRefinement1_ARNIndexTagOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn      func(b *verifiedpermissions.InMemoryBackend, psArn string) error
		name    string
		wantErr bool
	}{
		{
			name: "tag resource with valid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, arn string) error {
				return b.TagResource(arn, map[string]string{"k": "v"})
			},
			wantErr: false,
		},
		{
			name: "untag resource with valid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, arn string) error {
				return b.UntagResource(arn, []string{"k"})
			},
			wantErr: false,
		},
		{
			name: "tag resource with invalid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, _ string) error {
				return b.TagResource("arn:bad", map[string]string{"k": "v"})
			},
			wantErr: true,
		},
		{
			name: "list tags with valid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, arn string) error {
				_, err := b.ListTagsForResource(arn)

				return err
			},
			wantErr: false,
		},
		{
			name: "list tags with invalid ARN",
			fn: func(b *verifiedpermissions.InMemoryBackend, _ string) error {
				_, err := b.ListTagsForResource("arn:bad")

				return err
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend()
			ps := seedPolicyStore(t, b, "tag store")

			err := tt.fn(b, ps.Arn)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Seed helpers ---------------------------------------------------------------

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*verifiedpermissions.InMemoryBackend)
		name   string
		wantPS int
		wantP  int
		wantT  int
		wantIS int
	}{
		{
			name: "AddPolicyStoreInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					Description:   "seeded",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
			},
			wantPS: 1,
		},
		{
			name: "AddPolicyInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
				b.AddPolicyInternal(&verifiedpermissions.Policy{
					PolicyID:      "pol-seed",
					PolicyStoreID: "ps-seed",
					PolicyType:    "STATIC",
					Statement:     "permit(principal,action,resource);",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
			},
			wantPS: 1,
			wantP:  1,
		},
		{
			name: "AddPolicyTemplateInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
				b.AddPolicyTemplateInternal(&verifiedpermissions.PolicyTemplate{
					PolicyTemplateID: "tmpl-seed",
					PolicyStoreID:    "ps-seed",
					Statement:        "permit(principal,action,resource);",
					CreatedDate:      time.Now(),
					LastUpdated:      time.Now(),
				})
			},
			wantPS: 1,
			wantT:  1,
		},
		{
			name: "AddIdentitySourceInternal",
			setup: func(b *verifiedpermissions.InMemoryBackend) {
				b.AddPolicyStoreInternal(&verifiedpermissions.PolicyStore{
					PolicyStoreID: "ps-seed",
					Arn:           "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store/ps-seed",
					CreatedDate:   time.Now(),
					LastUpdated:   time.Now(),
				})
				b.AddIdentitySourceInternal(&verifiedpermissions.IdentitySource{
					IdentitySourceID: "is-seed",
					PolicyStoreID:    "ps-seed",
					UserPoolArn:      "arn:aws:cognito-idp:us-east-1:123456789012:userpool/pool",
					CreatedDate:      time.Now(),
					LastUpdated:      time.Now(),
				})
			},
			wantPS: 1,
			wantIS: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend()
			tt.setup(b)

			assert.Equal(t, tt.wantPS, verifiedpermissions.PolicyStoreCount(b))
			assert.Equal(t, tt.wantP, verifiedpermissions.PolicyCount(b))
			assert.Equal(t, tt.wantT, verifiedpermissions.PolicyTemplateCount(b))
			assert.Equal(t, tt.wantIS, verifiedpermissions.IdentitySourceCount(b))
		})
	}
}

// IsAuthorized --------------------------------------------------------------

func TestRefinement1_IsAuthorized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policyStoreID string
		wantDecision  string
		wantErr       bool
	}{
		{
			name:         "allow on existing store",
			wantErr:      false,
			wantDecision: "DENY",
		},
		{
			name:          "not found on missing store",
			policyStoreID: "missing-store",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend()
			ps := seedPolicyStore(t, b, "auth store")

			id := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				id = tt.policyStoreID
			}

			decision, err := b.IsAuthorized(id, verifiedpermissions.AuthorizationRequest{
				PrincipalEntityType: "User",
				PrincipalEntityID:   "alice",
				ActionType:          "Action",
				ActionID:            "view",
				ResourceEntityType:  "Document",
				ResourceEntityID:    "doc1",
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDecision, decision.Decision)
			assert.NotNil(t, decision.DeterminingPolicies)
			assert.NotNil(t, decision.Errors)
		})
	}
}

// IsAuthorizedWithToken -----------------------------------------------------

func TestRefinement1_IsAuthorizedWithToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policyStoreID string
		wantDecision  string
		wantErr       bool
	}{
		{
			name:         "allow on existing store",
			wantErr:      false,
			wantDecision: "DENY",
		},
		{
			name:          "not found on missing store",
			policyStoreID: "missing-store",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend()
			ps := seedPolicyStore(t, b, "token auth store")

			id := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				id = tt.policyStoreID
			}

			decision, err := b.IsAuthorizedWithToken(id, verifiedpermissions.AuthorizationRequest{
				ActionType: "Action",
				ActionID:   "view",
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDecision, decision.Decision)
			assert.NotNil(t, decision.DeterminingPolicies)
			assert.NotNil(t, decision.Errors)
		})
	}
}

// UpdateIdentitySource -------------------------------------------------------

func TestRefinement1_UpdateIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		policyStoreID    string
		identitySourceID string
		userPoolArn      string
		openIDIssuer     string
		principalType    string
		wantErr          bool
	}{
		{
			name:          "update cognito user pool",
			userPoolArn:   "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
			principalType: "NewUser",
			wantErr:       false,
		},
		{
			name:         "update openid connect",
			openIDIssuer: "https://new.issuer.example.com",
			wantErr:      false,
		},
		{
			name:             "not found identity source",
			identitySourceID: "missing-is",
			wantErr:          true,
		},
		{
			name:          "not found policy store",
			policyStoreID: "missing-ps",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend()
			ps := seedPolicyStore(t, b, "is update store")

			is, err := b.CreateIdentitySource(
				ps.PolicyStoreID,
				"User",
				verifiedpermissions.IdentitySourceConfig{
					UserPoolArn: "arn:aws:cognito-idp:us-east-1:123456789012:userpool/original",
				},
			)
			require.NoError(t, err)

			psID := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				psID = tt.policyStoreID
			}

			isID := is.IdentitySourceID
			if tt.identitySourceID != "" {
				isID = tt.identitySourceID
			}

			updated, err := b.UpdateIdentitySource(
				psID,
				isID,
				tt.principalType,
				verifiedpermissions.IdentitySourceConfig{
					UserPoolArn: tt.userPoolArn,
					Issuer:      tt.openIDIssuer,
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.userPoolArn != "" {
				assert.Equal(t, tt.userPoolArn, updated.UserPoolArn)
			}

			if tt.openIDIssuer != "" {
				assert.Equal(t, tt.openIDIssuer, updated.OpenIDIssuer)
			}

			if tt.principalType != "" {
				assert.Equal(t, tt.principalType, updated.PrincipalEntityType)
			}
		})
	}
}

// DeletePolicyStore cascades ARN index removal --------------------------------

func TestRefinement1_DeletePolicyStore_ARNIndexCleaned(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend()
	ps := seedPolicyStore(t, b, "to delete")

	require.Equal(t, 1, verifiedpermissions.ARNIndexSize(b))

	err := b.DeletePolicyStore(ps.PolicyStoreID)
	require.NoError(t, err)

	assert.Equal(t, 0, verifiedpermissions.ARNIndexSize(b))
}

// Persistence: Snapshot/Restore with ARN index rebuild ----------------------

func TestRefinement1_SnapshotRestoreARNIndex(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend()
	ps := seedPolicyStore(t, b, "snap store")
	_ = b.TagResource(ps.Arn, map[string]string{"env": "test"})

	data := b.Snapshot()
	require.NotNil(t, data)

	b2 := newRefinement1Backend()
	err := b2.Restore(data)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, verifiedpermissions.ARNIndexSize(b2), 1)

	tags, err := b2.ListTagsForResource(ps.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

// BatchIsAuthorized output includes DeterminingPolicies/Errors ----------------

func TestRefinement1_BatchIsAuthorizedOutputFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policyStoreID string
		requests      int
		wantErr       bool
	}{
		{
			name:     "single request",
			requests: 1,
			wantErr:  false,
		},
		{
			name:     "empty requests",
			requests: 0,
			wantErr:  false,
		},
		{
			name:          "unknown policy store",
			policyStoreID: "missing",
			requests:      1,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinement1Backend()
			ps := seedPolicyStore(t, b, "batch store")

			psID := ps.PolicyStoreID
			if tt.policyStoreID != "" {
				psID = tt.policyStoreID
			}

			reqs := make([]verifiedpermissions.AuthorizationRequest, tt.requests)
			for i := range reqs {
				reqs[i] = verifiedpermissions.AuthorizationRequest{ActionType: "Action", ActionID: "view"}
			}

			decisions, err := b.BatchIsAuthorized(psID, reqs)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, decisions, tt.requests)

			for _, d := range decisions {
				assert.Contains(t, []string{"ALLOW", "DENY"}, d.Decision)
				assert.NotNil(t, d.DeterminingPolicies)
				assert.NotNil(t, d.Errors)
			}
		})
	}
}

// handler HTTP tests: IsAuthorized -------------------------------------------

func TestRefinement1_Handler_IsAuthorized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name: "allow on existing store",
			body: map[string]any{
				"policyStoreId": "__REPLACE__",
				"action":        map[string]any{"actionType": "Action", "actionId": "view"},
			},
			wantStatus: http.StatusOK,
			wantField:  "decision",
		},
		{
			name: "missing policyStoreId",
			body: map[string]any{
				"action": map[string]any{"actionType": "Action", "actionId": "view"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown store",
			body: map[string]any{
				"policyStoreId": "unknown-store",
				"action":        map[string]any{"actionType": "Action", "actionId": "view"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			ps := doVPRequest(
				t,
				h,
				"CreatePolicyStore",
				map[string]any{"description": "store", "validationSettings": map[string]any{"mode": "OFF"}},
			)
			require.Equal(t, http.StatusOK, ps.Code)

			var psResp map[string]any
			_ = json.NewDecoder(ps.Body).Decode(&psResp)

			if v, ok := tt.body["policyStoreId"]; ok && v == "__REPLACE__" {
				tt.body["policyStoreId"] = psResp["policyStoreId"]
			}

			rec := doVPRequest(t, h, "IsAuthorized", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.Contains(t, resp, tt.wantField)
			}
		})
	}
}

// handler HTTP tests: IsAuthorizedWithToken -----------------------------------

func TestRefinement1_Handler_IsAuthorizedWithToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "allow with access token",
			body: map[string]any{
				"policyStoreId": "__REPLACE__",
				"accessToken":   "eyJfake.token.here",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing token",
			body: map[string]any{
				"policyStoreId": "__REPLACE__",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing policyStoreId",
			body: map[string]any{
				"accessToken": "eyJfake.token.here",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			ps := doVPRequest(
				t,
				h,
				"CreatePolicyStore",
				map[string]any{"description": "store", "validationSettings": map[string]any{"mode": "OFF"}},
			)
			require.Equal(t, http.StatusOK, ps.Code)

			var psResp map[string]any
			_ = json.NewDecoder(ps.Body).Decode(&psResp)

			if v, ok := tt.body["policyStoreId"]; ok && v == "__REPLACE__" {
				tt.body["policyStoreId"] = psResp["policyStoreId"]
			}

			rec := doVPRequest(t, h, "IsAuthorizedWithToken", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// handler HTTP tests: UpdateIdentitySource ------------------------------------

func TestRefinement1_Handler_UpdateIdentitySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildBody  func(psID, isID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "update cognito config",
			buildBody: func(psID, isID string) map[string]any {
				return map[string]any{
					"policyStoreId":    psID,
					"identitySourceId": isID,
					"updateConfiguration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
						},
					},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing policyStoreId",
			buildBody: func(_, isID string) map[string]any {
				return map[string]any{
					"identitySourceId": isID,
					"updateConfiguration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
						},
					},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing identitySourceId",
			buildBody: func(psID, _ string) map[string]any {
				return map[string]any{
					"policyStoreId": psID,
					"updateConfiguration": map[string]any{
						"cognitoUserPoolConfiguration": map[string]any{
							"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/newpool",
						},
					},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing updateConfiguration",
			buildBody: func(psID, isID string) map[string]any {
				return map[string]any{
					"policyStoreId":    psID,
					"identitySourceId": isID,
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)

			psRec := doVPRequest(
				t,
				h,
				"CreatePolicyStore",
				map[string]any{"description": "store", "validationSettings": map[string]any{"mode": "OFF"}},
			)
			require.Equal(t, http.StatusOK, psRec.Code)

			var psResp map[string]any
			require.NoError(t, json.NewDecoder(psRec.Body).Decode(&psResp))

			psID := psResp["policyStoreId"].(string)

			isRec := doVPRequest(t, h, "CreateIdentitySource", map[string]any{
				"policyStoreId":       psID,
				"principalEntityType": "User",
				"configuration": map[string]any{
					"cognitoUserPoolConfiguration": map[string]any{
						"userPoolArn": "arn:aws:cognito-idp:us-east-1:123456789012:userpool/original",
					},
				},
			})
			require.Equal(t, http.StatusOK, isRec.Code)

			var isResp map[string]any
			require.NoError(t, json.NewDecoder(isRec.Body).Decode(&isResp))

			isID := isResp["identitySourceId"].(string)

			rec := doVPRequest(t, h, "UpdateIdentitySource", tt.buildBody(psID, isID))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ErrNilAppContext -----------------------------------------------------------

func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &verifiedpermissions.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, verifiedpermissions.ErrNilAppContext)
}

// ErrValidation wired into handleError ----------------------------------------

func TestRefinement1_HandleError_ValidationException(t *testing.T) {
	t.Parallel()

	h := newTestVPHandler(t)

	// Passing raw malformed JSON triggers a JSON syntax error → ValidationException (400).
	rec := doVPRequestRaw(t, h, "GetPolicyStore", []byte("{bad json}"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// Persistence: round-trip policies + templates --------------------------------

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend()
	ps := seedPolicyStore(t, b, "persist store")

	_, _ = b.CreatePolicy(
		ps.PolicyStoreID,
		verifiedpermissions.CreatePolicyParams{PolicyType: "STATIC", Statement: "permit(principal,action,resource);"},
	)
	_, _ = b.CreatePolicyTemplate(ps.PolicyStoreID, "tmpl", "permit(principal,action,resource);")
	_, _ = b.PutSchema(ps.PolicyStoreID, `{"ns":{}}`)
	_, _ = b.CreateIdentitySource(
		ps.PolicyStoreID,
		"User",
		verifiedpermissions.IdentitySourceConfig{
			UserPoolArn: "arn:aws:cognito-idp:us-east-1:123456789012:userpool/pool",
		},
	)

	data := b.Snapshot()
	require.NotNil(t, data)

	b2 := newRefinement1Backend()
	require.NoError(t, b2.Restore(data))

	assert.Equal(t, 1, verifiedpermissions.PolicyStoreCount(b2))
	assert.Equal(t, 1, verifiedpermissions.PolicyCount(b2))
	assert.Equal(t, 1, verifiedpermissions.PolicyTemplateCount(b2))
	assert.Equal(t, 1, verifiedpermissions.SchemaCount(b2))
	assert.Equal(t, 1, verifiedpermissions.IdentitySourceCount(b2))
	assert.Equal(t, 4, verifiedpermissions.ARNIndexSize(b2)) // store + policy + template + identity source
}

// Handler ops length matches GetSupportedOperations --------------------------

func TestRefinement1_OpsLength(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend()
	h := verifiedpermissions.NewHandler(b)
	ops := h.GetSupportedOperations()

	assert.GreaterOrEqual(t, len(ops), 30)

	// Verify all new ops are present.
	for _, op := range []string{"IsAuthorized", "IsAuthorizedWithToken", "UpdateIdentitySource"} {
		assert.Contains(t, ops, op)
	}
}

// BatchGetPolicy with empty results and errors arrays -------------------------

func TestRefinement1_BatchGetPolicy_EmptyArrays(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend()
	ps := seedPolicyStore(t, b, "batch store")

	result := b.BatchGetPolicy([]verifiedpermissions.BatchGetPolicyItem{
		{PolicyStoreID: ps.PolicyStoreID, PolicyID: "nonexistent"},
	})

	assert.Empty(t, result.Results)
	assert.Len(t, result.Errors, 1)
}

// BatchIsAuthorizedWithToken output DeterminingPolicies/Errors ----------------

func TestRefinement1_BatchIsAuthorizedWithToken_OutputFields(t *testing.T) {
	t.Parallel()

	b := newRefinement1Backend()
	ps := seedPolicyStore(t, b, "token batch store")

	decisions, err := b.BatchIsAuthorizedWithToken(ps.PolicyStoreID, []verifiedpermissions.AuthorizationRequest{
		{ActionType: "Action", ActionID: "view"},
	})
	require.NoError(t, err)
	require.Len(t, decisions, 1)

	assert.Contains(t, []string{"ALLOW", "DENY"}, decisions[0].Decision)
	assert.NotNil(t, decisions[0].DeterminingPolicies)
	assert.NotNil(t, decisions[0].Errors)
}
