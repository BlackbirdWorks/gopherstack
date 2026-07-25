package bedrock_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Guardrail CRUD tests --- //nolint:godot // existing issue.
func TestHandler_CreateGuardrail(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		input      map[string]any
		wantFields map[string]string
		name       string
		wantStatus int
	}{
		{
			name: "valid guardrail",
			input: map[string]any{
				"name":                    "test-guardrail",
				"description":             "A test guardrail",
				"blockedInputMessaging":   "blocked input",
				"blockedOutputsMessaging": "blocked output",
			},
			wantStatus: http.StatusOK,
			wantFields: map[string]string{
				"guardrailId":  "",
				"guardrailArn": "",
				"version":      "DRAFT",
			},
		},
		{
			name:       "empty body",
			input:      nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			if tt.input == nil {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/guardrails", bytes.NewReader([]byte("invalid json")))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			rec := doRequest(t, h, http.MethodPost, "/guardrails", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["guardrailId"])
				assert.NotEmpty(t, out["guardrailArn"])
				assert.Equal(t, "DRAFT", out["version"])
			}
		})
	}
}

func TestHandler_GetGuardrail(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*bedrock.Handler) string
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "existing guardrail",
			setup: func(h *bedrock.Handler) string {
				g, err := h.Backend.CreateGuardrail("test", "desc", "blocked-in", "blocked-out", nil)
				if err != nil {
					panic(err)
				}

				return g.GuardrailID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent guardrail",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodGet, "/guardrails/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, id, out["guardrailId"])
				assert.Equal(t, "READY", out["status"])
			}
		})
	}
}

func TestHandler_ListGuardrails(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup func(*bedrock.Handler)
		name  string
		want  int
	}{
		{
			name:  "empty",
			setup: func(*bedrock.Handler) {},
			want:  0,
		},
		{
			name: "two guardrails",
			setup: func(h *bedrock.Handler) {
				_, err := h.Backend.CreateGuardrail("g1", "", "", "", nil)
				require.NoError(t, err)
				_, err = h.Backend.CreateGuardrail("g2", "", "", "", nil)
				require.NoError(t, err)
			},
			want: 2,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			tt.setup(h)

			rec := doRequest(t, h, http.MethodGet, "/guardrails", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			guardrails := out["guardrails"].([]any)
			assert.Len(t, guardrails, tt.want)
		})
	}
}

func TestHandler_UpdateGuardrail(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*bedrock.Handler) string
		input      map[string]any
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "update existing",
			setup: func(h *bedrock.Handler) string {
				g, err := h.Backend.CreateGuardrail("test", "old desc", "", "", nil)
				if err != nil {
					panic(err)
				}

				return g.GuardrailID
			},
			input: map[string]any{
				"name":        "test",
				"description": "new desc",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update non-existent",
			id:         "nonexistent",
			input:      map[string]any{"name": "test"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPut, "/guardrails/"+id, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteGuardrail(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*bedrock.Handler) string
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "delete existing",
			setup: func(h *bedrock.Handler) string {
				g, err := h.Backend.CreateGuardrail("test", "", "", "", nil)
				if err != nil {
					panic(err)
				}

				return g.GuardrailID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodDelete, "/guardrails/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetGuardrailByARN(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	g, err := h.Backend.CreateGuardrail("arn-test", "", "", "", nil)
	require.NoError(t, err)

	// Look up by ARN
	encodedARN := url.PathEscape(g.GuardrailArn)
	rec := doRequest(t, h, http.MethodGet, "/guardrails/"+encodedARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListGuardrailsPagination(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create more than one page of guardrails (bedrockDefaultPageSize=100).
	for i := range 105 {
		_, err := h.Backend.CreateGuardrail(
			fmt.Sprintf("guardrail-%04d", i),
			"", "", "", nil,
		)
		require.NoError(t, err)
	}

	// First page.
	rec := doRequest(t, h, http.MethodGet, "/guardrails", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	guardrails := out["guardrails"].([]any)
	assert.Len(t, guardrails, 100)
	nextToken, ok := out["nextToken"].(string)
	require.True(t, ok, "nextToken should be present")
	assert.NotEmpty(t, nextToken)

	// Second page using the token.
	rec2 := doRequest(t, h, http.MethodGet, "/guardrails?nextToken="+url.QueryEscape(nextToken), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)

	guardrails2 := out2["guardrails"].([]any)
	assert.Len(t, guardrails2, 5)
	assert.Empty(t, out2["nextToken"])
}

func TestHandler_CreateGuardrailVersion(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler) string
		input      map[string]any
		name       string
		id         string
		wantStatus int
		wantFields bool
	}{
		{
			name: "create version for existing guardrail",
			setup: func(t *testing.T, h *bedrock.Handler) string {
				t.Helper()

				g, err := h.Backend.CreateGuardrail("ver-guardrail", "desc", "", "", nil)
				require.NoError(t, err)

				return g.GuardrailID
			},
			input:      map[string]any{"description": "v1 snapshot"},
			wantStatus: http.StatusOK,
			wantFields: true,
		},
		{
			name:       "guardrail not found",
			id:         "nonexistent",
			input:      map[string]any{},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/guardrails/"+id, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFields {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["guardrailId"])
				assert.NotEmpty(t, out["version"])
			}
		})
	}
}

func TestHandler_CreateGuardrailNameRequired(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateGuardrailTagsReturned(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "tagged-guardrail",
		"tags": []map[string]string{
			{"key": "env", "value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	id := createOut["guardrailId"].(string)

	// Tags should be present in GET response.
	rec2 := doRequest(t, h, http.MethodGet, "/guardrails/"+id, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var getOut map[string]any
	mustUnmarshal(t, rec2, &getOut)
	require.NotEmpty(t, getOut["tags"])
	tags := getOut["tags"].([]any)
	assert.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["key"])
	assert.Equal(t, "test", tag["value"])
}

func TestHandler_UpdateGuardrailNameChange(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "original-name"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	id := createOut["guardrailId"].(string)

	// Rename the guardrail.
	rec2 := doRequest(t, h, http.MethodPut, "/guardrails/"+id, map[string]any{
		"name": "renamed-guardrail",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Old name should be available again.
	rec3 := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "original-name"})
	assert.Equal(t, http.StatusOK, rec3.Code)

	// New name should conflict if created again.
	rec4 := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "renamed-guardrail"})
	assert.Equal(t, http.StatusConflict, rec4.Code)
}

func TestHandler_ListGuardrailsFilter(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	g, err := h.Backend.CreateGuardrail("filter-target", "", "", "", nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateGuardrail("other-guardrail", "", "", "", nil)
	require.NoError(t, err)

	tests := []struct {
		name       string
		identifier string
		wantCount  int
	}{
		{"filter by id", g.GuardrailID, 1},
		{"filter by arn", g.GuardrailArn, 1},
		{"filter by name", "filter-target", 1},
		{"filter nonexistent", "does-not-exist", 0},
		{"no filter", "", 2},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			path := "/guardrails"
			if tt.identifier != "" {
				path += "?guardrailIdentifier=" + url.QueryEscape(tt.identifier)
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			assert.Len(t, out["guardrails"].([]any), tt.wantCount)
		})
	}
}

func TestHandler_GuardrailVersionPersisted(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	g, err := h.Backend.CreateGuardrail("versioned-guardrail", "", "", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/guardrails/"+g.GuardrailID, map[string]any{
		"description": "first version",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var v1Out map[string]any
	mustUnmarshal(t, rec, &v1Out)
	version1 := v1Out["version"].(string)
	assert.NotEmpty(t, version1)

	// Create a second version.
	rec2 := doRequest(t, h, http.MethodPost, "/guardrails/"+g.GuardrailID, map[string]any{
		"description": "second version",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var v2Out map[string]any
	mustUnmarshal(t, rec2, &v2Out)
	version2 := v2Out["version"].(string)
	assert.NotEmpty(t, version2)
	assert.NotEqual(t, version1, version2)
}

func TestBatch1_Guardrail_ContentPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	policies := &bedrock.GuardrailPolicies{
		ContentPolicy: &bedrock.GuardrailContentPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContentFilter{
				{Type: "HATE", InputStrength: "HIGH", OutputStrength: "MEDIUM"},
				{Type: "VIOLENCE", InputStrength: "MEDIUM", OutputStrength: "LOW"},
			},
		},
	}

	g, err := b.CreateGuardrail("content-policy-guard", "test guardrail", "", "", nil, policies)
	require.NoError(t, err)
	require.NotNil(t, g.Policies)
	require.NotNil(t, g.Policies.ContentPolicy)
	assert.Len(t, g.Policies.ContentPolicy.FiltersConfig, 2)
	assert.Equal(t, "HATE", g.Policies.ContentPolicy.FiltersConfig[0].Type)
	assert.Equal(t, "HIGH", g.Policies.ContentPolicy.FiltersConfig[0].InputStrength)

	// Fetch it back.
	fetched, err := b.GetGuardrail(g.GuardrailID)
	require.NoError(t, err)
	require.NotNil(t, fetched.Policies)
	require.NotNil(t, fetched.Policies.ContentPolicy)
	assert.Equal(t, "VIOLENCE", fetched.Policies.ContentPolicy.FiltersConfig[1].Type)
	assert.Equal(t, "LOW", fetched.Policies.ContentPolicy.FiltersConfig[1].OutputStrength)
}

func TestBatch1_Guardrail_ContentPolicy_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name":        "http-content-guard",
		"description": "content filter test",
		"contentPolicyConfig": map[string]any{
			"filtersConfig": []map[string]any{
				{"type": "SEXUAL", "inputStrength": "HIGH", "outputStrength": "HIGH"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	guardrailID := createOut["guardrailId"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	contentPolicy := getOut["contentPolicy"].(map[string]any)
	filters := contentPolicy["filtersConfig"].([]any)
	require.Len(t, filters, 1)
	filter := filters[0].(map[string]any)
	assert.Equal(t, "SEXUAL", filter["type"])
	assert.Equal(t, "HIGH", filter["inputStrength"])
}

func TestBatch1_Guardrail_TopicPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	policies := &bedrock.GuardrailPolicies{
		TopicPolicy: &bedrock.GuardrailTopicPolicyConfig{
			TopicsConfig: []bedrock.GuardrailTopic{
				{
					Name:       "financial-advice",
					Definition: "Any investment or financial advice",
					Examples:   []string{"Should I buy this stock?", "What is the best ETF?"},
					Type:       "DENY",
				},
				{
					Name:       "medical-advice",
					Definition: "Any clinical or medical recommendations",
					Type:       "DENY",
				},
			},
		},
	}

	g, err := b.CreateGuardrail("topic-policy-guard", "", "", "", nil, policies)
	require.NoError(t, err)
	require.NotNil(t, g.Policies.TopicPolicy)
	assert.Len(t, g.Policies.TopicPolicy.TopicsConfig, 2)
	assert.Equal(t, "financial-advice", g.Policies.TopicPolicy.TopicsConfig[0].Name)
	assert.Equal(t, "DENY", g.Policies.TopicPolicy.TopicsConfig[0].Type)
	assert.Len(t, g.Policies.TopicPolicy.TopicsConfig[0].Examples, 2)

	fetched, err := b.GetGuardrail(g.GuardrailID)
	require.NoError(t, err)
	assert.Equal(t, "medical-advice", fetched.Policies.TopicPolicy.TopicsConfig[1].Name)
}

func TestBatch1_Guardrail_TopicPolicy_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "http-topic-guard",
		"topicPolicyConfig": map[string]any{
			"topicsConfig": []map[string]any{
				{
					"name":       "legal-advice",
					"definition": "Legal counsel or law-specific guidance",
					"type":       "DENY",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	guardrailID := createOut["guardrailId"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	topicPolicy := getOut["topicPolicy"].(map[string]any)
	topics := topicPolicy["topicsConfig"].([]any)
	require.Len(t, topics, 1)
	assert.Equal(t, "legal-advice", topics[0].(map[string]any)["name"])
}

func TestBatch1_Guardrail_WordPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	policies := &bedrock.GuardrailPolicies{
		WordPolicy: &bedrock.GuardrailWordPolicyConfig{
			WordsConfig: []bedrock.GuardrailWordConfig{
				{Text: "badword1"},
				{Text: "badword2"},
			},
			ManagedWordListsConfig: []bedrock.GuardrailManagedWordList{
				{Type: "PROFANITY"},
			},
		},
	}

	g, err := b.CreateGuardrail("word-policy-guard", "", "", "", nil, policies)
	require.NoError(t, err)
	require.NotNil(t, g.Policies.WordPolicy)
	assert.Len(t, g.Policies.WordPolicy.WordsConfig, 2)
	assert.Equal(t, "badword1", g.Policies.WordPolicy.WordsConfig[0].Text)
	assert.Len(t, g.Policies.WordPolicy.ManagedWordListsConfig, 1)
	assert.Equal(t, "PROFANITY", g.Policies.WordPolicy.ManagedWordListsConfig[0].Type)
}

func TestBatch1_Guardrail_WordPolicy_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "http-word-guard",
		"wordPolicyConfig": map[string]any{
			"wordsConfig": []map[string]any{
				{"text": "forbidden"},
			},
			"managedWordListsConfig": []map[string]any{
				{"type": "PROFANITY"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	guardrailID := createOut["guardrailId"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	wordPolicy := getOut["wordPolicy"].(map[string]any)
	words := wordPolicy["wordsConfig"].([]any)
	require.Len(t, words, 1)
	assert.Equal(t, "forbidden", words[0].(map[string]any)["text"])
	managed := wordPolicy["managedWordListsConfig"].([]any)
	assert.Len(t, managed, 1)
}

func TestBatch1_Guardrail_SensitiveInformationPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	policies := &bedrock.GuardrailPolicies{
		SensitiveInformationPolicy: &bedrock.GuardrailSensitiveInformationPolicyConfig{
			PiiEntitiesConfig: []bedrock.GuardrailPIIEntity{
				{Type: "EMAIL", Action: "ANONYMIZE"},
				{Type: "PHONE", Action: "BLOCK"},
			},
			RegexesConfig: []bedrock.GuardrailRegexConfig{
				{
					Name:    "SSN",
					Pattern: `\d{3}-\d{2}-\d{4}`,
					Action:  "ANONYMIZE",
				},
			},
		},
	}

	g, err := b.CreateGuardrail("sensitive-info-guard", "", "", "", nil, policies)
	require.NoError(t, err)
	sip := g.Policies.SensitiveInformationPolicy
	require.NotNil(t, sip)
	assert.Len(t, sip.PiiEntitiesConfig, 2)
	assert.Equal(t, "EMAIL", sip.PiiEntitiesConfig[0].Type)
	assert.Equal(t, "ANONYMIZE", sip.PiiEntitiesConfig[0].Action)
	assert.Equal(t, "BLOCK", sip.PiiEntitiesConfig[1].Action)
	assert.Len(t, sip.RegexesConfig, 1)
	assert.Equal(t, "SSN", sip.RegexesConfig[0].Name)

	fetched, err := b.GetGuardrail(g.GuardrailID)
	require.NoError(t, err)
	assert.Len(t, fetched.Policies.SensitiveInformationPolicy.PiiEntitiesConfig, 2)
	assert.Len(t, fetched.Policies.SensitiveInformationPolicy.RegexesConfig, 1)
}

func TestBatch1_Guardrail_SensitiveInformationPolicy_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "http-sensitive-guard",
		"sensitiveInformationPolicyConfig": map[string]any{
			"piiEntitiesConfig": []map[string]any{
				{"type": "CREDIT_DEBIT_CARD_NUMBER", "action": "BLOCK"},
			},
			"regexesConfig": []map[string]any{
				{"name": "AccountNum", "pattern": `ACC-\d+`, "action": "ANONYMIZE"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	guardrailID := createOut["guardrailId"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	sip := getOut["sensitiveInformationPolicy"].(map[string]any)
	piiEntities := sip["piiEntitiesConfig"].([]any)
	require.Len(t, piiEntities, 1)
	assert.Equal(t, "CREDIT_DEBIT_CARD_NUMBER", piiEntities[0].(map[string]any)["type"])
	regexes := sip["regexesConfig"].([]any)
	require.Len(t, regexes, 1)
	assert.Equal(t, "AccountNum", regexes[0].(map[string]any)["name"])
}

func TestBatch1_Guardrail_ContextualGroundingPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	policies := &bedrock.GuardrailPolicies{
		ContextualGroundingPolicy: &bedrock.GuardrailContextualGroundingPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContextualGroundingFilter{
				{Type: "GROUNDING", Threshold: 0.7},
				{Type: "RELEVANCE", Threshold: 0.5},
			},
		},
	}

	g, err := b.CreateGuardrail("contextual-guard", "", "", "", nil, policies)
	require.NoError(t, err)
	cgp := g.Policies.ContextualGroundingPolicy
	require.NotNil(t, cgp)
	assert.Len(t, cgp.FiltersConfig, 2)
	assert.Equal(t, "GROUNDING", cgp.FiltersConfig[0].Type)
	assert.InDelta(t, 0.7, cgp.FiltersConfig[0].Threshold, 0.001)

	fetched, err := b.GetGuardrail(g.GuardrailID)
	require.NoError(t, err)
	assert.InDelta(
		t,
		0.5,
		fetched.Policies.ContextualGroundingPolicy.FiltersConfig[1].Threshold,
		0.001,
	)
}

func TestBatch1_Guardrail_ContextualGroundingPolicy_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "http-contextual-guard",
		"contextualGroundingPolicyConfig": map[string]any{
			"filtersConfig": []map[string]any{
				{"type": "GROUNDING", "threshold": 0.8},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	guardrailID := createOut["guardrailId"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	cgp := getOut["contextualGroundingPolicy"].(map[string]any)
	filters := cgp["filtersConfig"].([]any)
	require.Len(t, filters, 1)
	f := filters[0].(map[string]any)
	assert.Equal(t, "GROUNDING", f["type"])
	assert.InDelta(t, 0.8, f["threshold"].(float64), 0.001)
}

func TestBatch1_Guardrail_AllPolicies_Combined(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	policies := &bedrock.GuardrailPolicies{
		ContentPolicy: &bedrock.GuardrailContentPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContentFilter{
				{Type: "HATE", InputStrength: "HIGH", OutputStrength: "HIGH"},
			},
		},
		TopicPolicy: &bedrock.GuardrailTopicPolicyConfig{
			TopicsConfig: []bedrock.GuardrailTopic{
				{Name: "politics", Definition: "Political content", Type: "DENY"},
			},
		},
		WordPolicy: &bedrock.GuardrailWordPolicyConfig{
			WordsConfig: []bedrock.GuardrailWordConfig{{Text: "slur"}},
		},
		SensitiveInformationPolicy: &bedrock.GuardrailSensitiveInformationPolicyConfig{
			PiiEntitiesConfig: []bedrock.GuardrailPIIEntity{{Type: "NAME", Action: "ANONYMIZE"}},
		},
		ContextualGroundingPolicy: &bedrock.GuardrailContextualGroundingPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContextualGroundingFilter{
				{Type: "GROUNDING", Threshold: 0.6},
			},
		},
	}

	g, err := b.CreateGuardrail(
		"all-policies-guard",
		"combined test",
		"blocked-in",
		"blocked-out",
		nil,
		policies,
	)
	require.NoError(t, err)
	assert.NotNil(t, g.Policies.ContentPolicy)
	assert.NotNil(t, g.Policies.TopicPolicy)
	assert.NotNil(t, g.Policies.WordPolicy)
	assert.NotNil(t, g.Policies.SensitiveInformationPolicy)
	assert.NotNil(t, g.Policies.ContextualGroundingPolicy)

	fetched, err := b.GetGuardrail(g.GuardrailID)
	require.NoError(t, err)
	assert.NotNil(t, fetched.Policies.ContentPolicy)
	assert.NotNil(t, fetched.Policies.TopicPolicy)
	assert.NotNil(t, fetched.Policies.WordPolicy)
	assert.NotNil(t, fetched.Policies.SensitiveInformationPolicy)
	assert.NotNil(t, fetched.Policies.ContextualGroundingPolicy)
}

func TestBatch1_Guardrail_UpdatePolicies(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	initialPolicies := &bedrock.GuardrailPolicies{
		ContentPolicy: &bedrock.GuardrailContentPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContentFilter{
				{Type: "HATE", InputStrength: "LOW", OutputStrength: "LOW"},
			},
		},
	}

	g, err := b.CreateGuardrail("update-policy-guard", "", "", "", nil, initialPolicies)
	require.NoError(t, err)
	assert.Equal(t, "LOW", g.Policies.ContentPolicy.FiltersConfig[0].InputStrength)

	updatedPolicies := &bedrock.GuardrailPolicies{
		ContentPolicy: &bedrock.GuardrailContentPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContentFilter{
				{Type: "HATE", InputStrength: "HIGH", OutputStrength: "HIGH"},
			},
		},
	}

	updated, err := b.UpdateGuardrail(g.GuardrailID, "", "", "", "", updatedPolicies)
	require.NoError(t, err)
	assert.Equal(t, "HIGH", updated.Policies.ContentPolicy.FiltersConfig[0].InputStrength)
}

func TestBatch1_Guardrail_UpdatePolicies_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "http-update-policy-guard",
		"wordPolicyConfig": map[string]any{
			"wordsConfig": []map[string]any{{"text": "original"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	guardrailID := createOut["guardrailId"].(string)

	recUpdate := doRequest(t, h, http.MethodPut, "/guardrails/"+guardrailID, map[string]any{
		"name":        "http-update-policy-guard",
		"description": "updated",
		"wordPolicyConfig": map[string]any{
			"wordsConfig": []map[string]any{{"text": "updated"}},
		},
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	wordPolicy := getOut["wordPolicy"].(map[string]any)
	words := wordPolicy["wordsConfig"].([]any)
	assert.Equal(t, "updated", words[0].(map[string]any)["text"])
}

func TestBatch1_Guardrail_NoPolicies_NilField(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGuardrail("no-policy-guard", "", "", "", nil)
	require.NoError(t, err)
	assert.Nil(t, g.Policies)
}

func TestBatch1_Guardrail_CopySemantics_Isolation(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	policies := &bedrock.GuardrailPolicies{
		ContentPolicy: &bedrock.GuardrailContentPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContentFilter{
				{Type: "HATE", InputStrength: "HIGH", OutputStrength: "HIGH"},
			},
		},
	}

	g1, err := b.CreateGuardrail("isolation-guard", "", "", "", nil, policies)
	require.NoError(t, err)

	// Mutate the returned value — should not affect stored state.
	g1.Policies.ContentPolicy.FiltersConfig[0].InputStrength = "NONE"

	g2, err := b.GetGuardrail(g1.GuardrailID)
	require.NoError(t, err)
	assert.Equal(
		t,
		"HIGH",
		g2.Policies.ContentPolicy.FiltersConfig[0].InputStrength,
		"stored copy should not be mutated",
	)
}

func TestBatch1_Guardrail_AddPoliciesViaUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "add-policies-later",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	guardrailID := createOut["guardrailId"].(string)

	// Initially no policies.
	recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet.Code)
	var getOut1 map[string]any
	mustUnmarshal(t, recGet, &getOut1)
	assert.Nil(t, getOut1["contentPolicy"])

	// Update to add a content policy.
	recUpdate := doRequest(t, h, http.MethodPut, "/guardrails/"+guardrailID, map[string]any{
		"name": "add-policies-later",
		"contentPolicyConfig": map[string]any{
			"filtersConfig": []map[string]any{
				{"type": "INSULTS", "inputStrength": "MEDIUM", "outputStrength": "MEDIUM"},
			},
		},
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recGet2 := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID, nil)
	require.Equal(t, http.StatusOK, recGet2.Code)
	var getOut2 map[string]any
	mustUnmarshal(t, recGet2, &getOut2)
	require.NotNil(t, getOut2["contentPolicy"])
	cp := getOut2["contentPolicy"].(map[string]any)
	filters := cp["filtersConfig"].([]any)
	assert.Equal(t, "INSULTS", filters[0].(map[string]any)["type"])
}

func TestAccuracy_GuardrailVersion_PerGuardrailCounter(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	g1, err := b.CreateGuardrail("guardrail-alpha", "", "", "", nil)
	require.NoError(t, err)

	g2, err := b.CreateGuardrail("guardrail-beta", "", "", "", nil)
	require.NoError(t, err)

	v1a, err := b.CreateGuardrailVersion(g1.GuardrailID, "v1 of alpha")
	require.NoError(t, err)
	assert.Equal(t, "1", v1a.Version)

	v1b, err := b.CreateGuardrailVersion(g2.GuardrailID, "v1 of beta")
	require.NoError(t, err)
	assert.Equal(t, "1", v1b.Version, "each guardrail should start at version 1")

	v2a, err := b.CreateGuardrailVersion(g1.GuardrailID, "v2 of alpha")
	require.NoError(t, err)
	assert.Equal(t, "2", v2a.Version)
}

// TestAccuracy_GuardrailVersion_UpdateAfterPublishAllowed verifies AWS's actual
// behavior: UpdateGuardrail always edits the mutable DRAFT and succeeds regardless of
// how many numbered versions have been published from it. Published (numbered) versions
// are immutable snapshots and are unaffected by later DRAFT edits.
func TestAccuracy_GuardrailVersion_UpdateAfterPublishAllowed(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	g, err := b.CreateGuardrail("versioned-guard", "", "", "", nil)
	require.NoError(t, err)

	v1, err := b.CreateGuardrailVersion(g.GuardrailID, "first version")
	require.NoError(t, err)

	// Updating DRAFT after a version is published must succeed.
	updated, err := b.UpdateGuardrail(g.GuardrailID, "new-name", "new-description", "", "")
	require.NoError(t, err, "update of DRAFT after publishing a version should succeed")
	assert.Equal(t, "new-name", updated.Name)

	// The already-published version stays a frozen snapshot of the pre-update name.
	snapshot, err := b.GetGuardrailVersion(g.GuardrailID, v1.Version)
	require.NoError(t, err)
	assert.Equal(t, "versioned-guard", snapshot.Name, "published version must not reflect later DRAFT edits")
}

func TestAccuracy_GuardrailVersion_HTTPCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantVersion  string
		versionCount int
	}{
		{name: "first version", versionCount: 1, wantVersion: "1"},
		{name: "second version", versionCount: 2, wantVersion: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail("guard-"+tt.name, "", "", "", nil)
			require.NoError(t, err)

			var lastOut map[string]any
			for i := range tt.versionCount {
				rec := doRequest(t, h, http.MethodPost,
					"/guardrails/"+g.GuardrailID,
					map[string]any{"description": fmt.Sprintf("v%d", i+1)})
				require.Equal(t, http.StatusOK, rec.Code)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lastOut))
			}

			assert.Equal(t, tt.wantVersion, lastOut["version"])
			assert.Equal(t, g.GuardrailID, lastOut["guardrailId"])
		})
	}
}

func TestAccuracy_Guardrail_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/guardrails/nonexistent-guardrail", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_Guardrail_UpdatePreservesAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialName string
		newName     string
		newDesc     string
	}{
		{
			name:        "rename guardrail",
			initialName: "original-name",
			newName:     "updated-name",
			newDesc:     "updated description",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail(tt.initialName, "", "", "", nil)
			require.NoError(t, err)

			rec := doRequest(
				t, h, http.MethodPut, "/guardrails/"+g.GuardrailID,
				map[string]any{
					"name":        tt.newName,
					"description": tt.newDesc,
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			recGet := doRequest(t, h, http.MethodGet, "/guardrails/"+g.GuardrailID, nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, tt.newName, out["name"])
			assert.Equal(t, tt.newDesc, out["description"])
		})
	}
}

func TestAccuracy_Tags_OnGuardrailViaListTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []bedrock.Tag
		tagCount int
	}{
		{
			name:     "two tags on guardrail",
			tagCount: 2,
			tags:     []bedrock.Tag{{Key: "team", Value: "ml"}, {Key: "env", Value: "staging"}},
		},
		{
			name:     "single tag on guardrail",
			tagCount: 1,
			tags:     []bedrock.Tag{{Key: "owner", Value: "alice"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail("tagged-guard-"+tt.name, "", "", "", tt.tags)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/listTagsForResource",
				map[string]any{"resourceARN": g.GuardrailArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			returnedTags := out["tags"].([]any)
			assert.Len(t, returnedTags, tt.tagCount)
		})
	}
}

func TestAccuracy_Tags_TagGuardrailAfterCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		val  string
	}{
		{name: "add single tag", key: "added", val: "yes"},
		{name: "add env tag", key: "env", val: "prod"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			g, err := b.CreateGuardrail("tag-later-"+tt.name, "", "", "", nil)
			require.NoError(t, err)

			recTag := doRequest(
				t, h, http.MethodPost, "/tagResource",
				map[string]any{
					"resourceARN": g.GuardrailArn,
					"tags":        []map[string]any{{"key": tt.key, "value": tt.val}},
				},
			)
			assert.Equal(t, http.StatusOK, recTag.Code)

			recList := doRequest(t, h, http.MethodPost, "/listTagsForResource",
				map[string]any{"resourceARN": g.GuardrailArn})
			require.Equal(t, http.StatusOK, recList.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
			returnedTags := out["tags"].([]any)
			assert.Len(t, returnedTags, 1)
			assert.Equal(t, tt.key, returnedTags[0].(map[string]any)["key"])
			assert.Equal(t, tt.val, returnedTags[0].(map[string]any)["value"])
		})
	}
}

func TestAccuracy_GuardrailVersion_PoliciesPreservedInVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create guardrail with content policy
	rec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{
		"name": "versioned-policy-guardrail",
		"contentPolicyConfig": map[string]any{
			"filtersConfig": []map[string]any{
				{"type": "HATE", "inputStrength": "HIGH", "outputStrength": "HIGH"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	guardrailID := out["guardrailId"].(string)

	// Create a version
	versionRec := doRequest(t, h, http.MethodPost, "/guardrails/"+guardrailID,
		map[string]any{"description": "stable version"})
	require.Equal(t, http.StatusOK, versionRec.Code)

	var verOut map[string]any
	require.NoError(t, json.Unmarshal(versionRec.Body.Bytes(), &verOut))
	version := verOut["version"].(string)
	assert.NotEqual(t, "DRAFT", version)

	// Get the specific version — it should have policies
	getVerRec := doRequest(t, h, http.MethodGet, "/guardrails/"+guardrailID+"?guardrailVersion="+version, nil)
	require.Equal(t, http.StatusOK, getVerRec.Code)

	var verDetailOut map[string]any
	require.NoError(t, json.Unmarshal(getVerRec.Body.Bytes(), &verDetailOut))
	assert.NotNil(t, verDetailOut["contentPolicy"], "version should preserve policies")
}
