package shield_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

func newTestHandler(t *testing.T) *shield.Handler {
	t.Helper()

	return shield.NewHandler(shield.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doShieldRequest(
	t *testing.T,
	h *shield.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSShield_20160616."+target)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Shield", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateSubscription")
	assert.Contains(t, ops, "DescribeSubscription")
	assert.Contains(t, ops, "GetSubscriptionState")
	assert.Contains(t, ops, "CreateProtection")
	assert.Contains(t, ops, "DescribeProtection")
	assert.Contains(t, ops, "DeleteProtection")
	assert.Contains(t, ops, "ListProtections")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "UntagResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "shield", h.ChaosServiceName())
}

func TestHandler_CreateSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
		{
			name:       "idempotent when already subscribed",
			wantStatus: http.StatusOK,
		},
	}

	h := newTestHandler(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doShieldRequest(t, h, "CreateSubscription", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DescribeSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name: "no subscription returns not found",
			setup: func(_ *shield.Handler) {
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "after subscription returns details",
			setup: func(h *shield.Handler) {
				_ = h.Backend.CreateSubscription()
			},
			wantStatus: http.StatusOK,
			wantField:  "Subscription",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doShieldRequest(t, h, "DescribeSubscription", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				assert.Contains(t, result, tt.wantField)
			}
		})
	}
}

func TestHandler_GetSubscriptionState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*shield.Handler)
		wantState string
	}{
		{
			name:      "inactive when no subscription",
			setup:     func(_ *shield.Handler) {},
			wantState: "INACTIVE",
		},
		{
			name: "active after subscription",
			setup: func(h *shield.Handler) {
				_ = h.Backend.CreateSubscription()
			},
			wantState: "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doShieldRequest(t, h, "GetSubscriptionState", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var result map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantState, result["SubscriptionState"])
		})
	}
}

func TestHandler_CreateProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":        "my-protection",
				"ResourceArn": "arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-12345678",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name: "missing name returns error",
			body: map[string]any{
				"ResourceArn": "arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-12345678",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resource arn returns error",
			body: map[string]any{
				"Name": "my-protection",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doShieldRequest(t, h, "CreateProtection", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var result map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				assert.NotEmpty(t, result["ProtectionId"])
			}
		})
	}
}

func TestHandler_DescribeProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "by protection id",
			setup: func(h *shield.Handler) string {
				p, _ := h.Backend.CreateProtection("my-protection", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionId": id}
			},
			wantStatus: http.StatusOK,
			wantName:   "my-protection",
		},
		{
			name: "not found",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"ProtectionId": "nonexistent"}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing id and arn returns error",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "DescribeProtection", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				prot, ok := result["Protection"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, prot["Name"])
			}
		})
	}
}

func TestHandler_DeleteProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				p, _ := h.Backend.CreateProtection("my-protection", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionId": id}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"ProtectionId": "nonexistent"}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing id returns error",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "DeleteProtection", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListProtections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*shield.Handler)
		name      string
		wantCount int
	}{
		{
			name:      "empty list",
			setup:     func(_ *shield.Handler) {},
			wantCount: 0,
		},
		{
			name: "two protections",
			setup: func(h *shield.Handler) {
				_, _ = h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)
				_, _ = h.Backend.CreateProtection("p2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2", nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doShieldRequest(t, h, "ListProtections", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			protections, ok := result["Protections"].([]any)
			require.True(t, ok)
			assert.Len(t, protections, tt.wantCount)
		})
	}
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				p, _ := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ResourceARN": id,
					"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing resource arn",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{
					"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "TagResource", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				p, _ := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1",
					map[string]string{"env": "prod"})

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"ResourceARN": id}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing resource arn",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "ListTagsForResource", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				p, _ := h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1",
					map[string]string{"env": "prod"})

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"ResourceARN": id,
					"TagKeys":     []string{"env"},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing resource arn",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"TagKeys": []string{"env"}}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "UntagResource", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doShieldRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, "shield", h.ChaosServiceName())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &shield.Provider{}
	assert.Equal(t, "Shield", p.Name())
}

func TestBackend_ListProtections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "empty",
			setup:   func(_ *shield.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "one protection",
			setup: func(b *shield.InMemoryBackend) {
				_, _ = b.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			list := b.ListProtections()
			assert.Len(t, list, tt.wantLen)
		})
	}
}
