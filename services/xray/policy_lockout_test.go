package xray_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func TestPutResourcePolicy_LockoutPrevention(t *testing.T) {
	t.Parallel()

	const caller = "arn:aws:iam::000000000000:user/alice"

	deny := func(principal, action, extra string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":` + principal +
			`,"Action":` + action + extra + `,"Resource":"*"}]}`
	}

	tests := []struct {
		name       string
		doc        string
		wantType   string
		wantStatus int
		bypass     bool
	}{
		{
			name:       "deny caller",
			doc:        deny(`{"AWS":"`+caller+`"}`, `"xray:PutResourcePolicy"`, ""),
			wantStatus: 400,
			wantType:   "LockoutPreventionException",
		},
		{
			name:       "deny everyone wildcard action",
			doc:        deny(`"*"`, `["xray:*"]`, ""),
			wantStatus: 400,
			wantType:   "LockoutPreventionException",
		},
		{
			name:       "deny account root",
			doc:        deny(`{"AWS":"arn:aws:iam::000000000000:root"}`, `"xray:PutResourcePolicy"`, ""),
			wantStatus: 400,
			wantType:   "LockoutPreventionException",
		},
		{name: "bypass", doc: deny(`"*"`, `"xray:*"`, ""), bypass: true, wantStatus: 200},
		{
			name:       "other principal",
			doc:        deny(`{"AWS":"arn:aws:iam::000000000000:user/bob"}`, `"xray:PutResourcePolicy"`, ""),
			wantStatus: 200,
		},
		{name: "other action", doc: deny(`"*"`, `"xray:GetTraceSummaries"`, ""), wantStatus: 200},
		{
			name:       "conditional deny",
			doc:        deny(`"*"`, `"xray:*"`, `,"Condition":{"Bool":{"aws:SecureTransport":"false"}}`),
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			payload, err := json.Marshal(map[string]any{
				"PolicyName": "p", "PolicyDocument": tt.doc, "BypassPolicyLockoutCheck": tt.bypass,
			})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/PutResourcePolicy", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			req.RequestURI = "/PutResourcePolicy"
			req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
				Account: "000000000000", Partition: "aws", Principal: &awsmeta.Principal{Arn: caller},
			}))

			rec := httptest.NewRecorder()
			c := echo.New().NewContext(req, rec)
			c.SetRequest(req)
			require.NoError(t, h.Handler()(c))

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}
