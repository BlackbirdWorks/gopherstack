package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// ─────────────────────────────────────────────────────────────────────────────
// Foundation model accuracy: inferenceTypesSupported, customizationsSupported,
// responseStreamingSupported fields.
// ─────────────────────────────────────────────────────────────────────────────

func TestBatch1_ListFoundationModels_InferenceTypesSupported(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	summaries := out["modelSummaries"].([]any)
	require.NotEmpty(t, summaries)

	// Every seeded model should have inferenceTypesSupported.
	for i, raw := range summaries {
		m := raw.(map[string]any)
		types, ok := m["inferenceTypesSupported"]
		assert.True(t, ok, "model[%d] missing inferenceTypesSupported", i)
		assert.NotEmpty(t, types, "model[%d] inferenceTypesSupported should not be empty", i)
	}
}

func TestBatch1_GetFoundationModel_InferenceTypesSupported(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-text-express-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	types := details["inferenceTypesSupported"].([]any)
	assert.Contains(t, typesAsStrings(types), "ON_DEMAND")
	assert.Contains(t, typesAsStrings(types), "PROVISIONED")

	customizations := details["customizationsSupported"].([]any)
	assert.Contains(t, typesAsStrings(customizations), "FINE_TUNING")

	streaming, ok := details["responseStreamingSupported"].(bool)
	assert.True(t, ok, "responseStreamingSupported should be bool")
	assert.True(t, streaming)
}

func TestBatch1_GetFoundationModel_EmbeddingModel_InferenceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-embed-text-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	types := typesAsStrings(details["inferenceTypesSupported"].([]any))
	assert.Contains(t, types, "ON_DEMAND")
	assert.NotContains(t, types, "PROVISIONED", "embedding model should not support PROVISIONED")

	streaming := details["responseStreamingSupported"].(bool)
	assert.False(t, streaming, "embedding model should not support streaming")
}

func TestBatch1_GetFoundationModel_Claude3Sonnet_ImageInputModality(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/foundation-models/anthropic.claude-3-sonnet-20240229-v1:0",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	inputMods := typesAsStrings(details["inputModalities"].([]any))
	assert.Contains(t, inputMods, "TEXT")
	assert.Contains(t, inputMods, "IMAGE")
}

func TestBatch1_GetFoundationModel_Llama3_InferenceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/meta.llama3-8b-instruct-v1:0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	types := typesAsStrings(details["inferenceTypesSupported"].([]any))
	assert.Contains(t, types, "ON_DEMAND")

	customizations := typesAsStrings(details["customizationsSupported"].([]any))
	assert.Contains(t, customizations, "FINE_TUNING")
}

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail content policy roundtrip.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail topic policy roundtrip.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail word policy roundtrip.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail sensitive information policy roundtrip.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail contextual grounding policy roundtrip.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail: all policies combined.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail update: policies are replaced by UpdateGuardrail.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail: no-policies creates nil policies field.
// ─────────────────────────────────────────────────────────────────────────────

func TestBatch1_Guardrail_NoPolicies_NilField(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGuardrail("no-policy-guard", "", "", "", nil)
	require.NoError(t, err)
	assert.Nil(t, g.Policies)
}

// ─────────────────────────────────────────────────────────────────────────────
// EvaluationJob: model config roundtrip.
// ─────────────────────────────────────────────────────────────────────────────

func TestBatch1_EvaluationJob_ModelConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	opts := &bedrock.CreateEvaluationJobInput{
		EvaluatorConfig: &bedrock.EvaluationModelConfig{
			ModelIdentifier: "anthropic.claude-3-sonnet-20240229-v1:0",
		},
		InferenceConfig: &bedrock.EvaluationInferenceConfig{
			Models: []bedrock.EvaluationInferenceModelConfig{
				{ModelIdentifier: "amazon.titan-text-express-v1"},
			},
		},
		EvalConfig: []bedrock.EvaluationTaskConfig{
			{
				TaskType: "Summarization",
				Dataset:  &bedrock.EvaluationDataset{Name: "squad-v2"},
				MetricNames: []bedrock.EvaluationMetricConfig{
					{MetricName: "Builtin.Rouge"},
				},
			},
		},
	}

	job, err := b.CreateEvaluationJob("model-eval-job", nil, opts)
	require.NoError(t, err)
	require.NotNil(t, job.EvaluatorConfig)
	assert.Equal(t, "anthropic.claude-3-sonnet-20240229-v1:0", job.EvaluatorConfig.ModelIdentifier)
	require.NotNil(t, job.InferenceConfig)
	require.Len(t, job.InferenceConfig.Models, 1)
	assert.Equal(t, "amazon.titan-text-express-v1", job.InferenceConfig.Models[0].ModelIdentifier)
	require.Len(t, job.EvaluationConfig, 1)
	assert.Equal(t, "Summarization", job.EvaluationConfig[0].TaskType)
	assert.Equal(t, "squad-v2", job.EvaluationConfig[0].Dataset.Name)
	assert.Len(t, job.EvaluationConfig[0].MetricNames, 1)
	assert.Equal(t, "Builtin.Rouge", job.EvaluationConfig[0].MetricNames[0].MetricName)
}

func TestBatch1_EvaluationJob_RAGConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	opts := &bedrock.CreateEvaluationJobInput{
		EvaluatorConfig: &bedrock.EvaluationModelConfig{
			ModelIdentifier: "anthropic.claude-v2",
		},
		InferenceConfig: &bedrock.EvaluationInferenceConfig{
			RAG: &bedrock.EvaluationRAGConfig{
				KnowledgeBaseID: "kb-0001",
			},
		},
	}

	job, err := b.CreateEvaluationJob("rag-eval-job", nil, opts)
	require.NoError(t, err)
	require.NotNil(t, job.InferenceConfig)
	require.NotNil(t, job.InferenceConfig.RAG)
	assert.Equal(t, "kb-0001", job.InferenceConfig.RAG.KnowledgeBaseID)
}

func TestBatch1_EvaluationJob_ModelConfig_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "http-model-eval",
		"evaluatorConfig": map[string]any{
			"modelIdentifier": "anthropic.claude-3-sonnet-20240229-v1:0",
		},
		"inferenceConfig": map[string]any{
			"models": []map[string]any{
				{"modelIdentifier": "amazon.titan-text-express-v1"},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)

	recGet := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, jobARN, getOut["jobArn"])
	assert.Equal(t, "InProgress", getOut["status"])

	evaluatorConfig := getOut["evaluatorConfig"].(map[string]any)
	assert.Equal(t, "anthropic.claude-3-sonnet-20240229-v1:0", evaluatorConfig["modelIdentifier"])

	inferenceConfig := getOut["inferenceConfig"].(map[string]any)
	models := inferenceConfig["models"].([]any)
	require.Len(t, models, 1)
	assert.Equal(t, "amazon.titan-text-express-v1", models[0].(map[string]any)["modelIdentifier"])
}

func TestBatch1_EvaluationJob_RAGConfig_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "http-rag-eval",
		"evaluatorConfig": map[string]any{
			"modelIdentifier": "anthropic.claude-v2",
		},
		"inferenceConfig": map[string]any{
			"ragConfig": map[string]any{
				"knowledgeBaseId": "kb-test-001",
			},
		},
		"jobDescription": "RAG evaluation test",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, "RAG evaluation test", getOut["jobDescription"])

	inferenceConfig := getOut["inferenceConfig"].(map[string]any)
	ragConfig := inferenceConfig["ragConfig"].(map[string]any)
	assert.Equal(t, "kb-test-001", ragConfig["knowledgeBaseId"])
}

func TestBatch1_EvaluationJob_WithEvalConfig_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "http-full-eval",
		"evaluatorConfig": map[string]any{
			"modelIdentifier": "anthropic.claude-v2",
		},
		"inferenceConfig": map[string]any{
			"models": []map[string]any{
				{"modelIdentifier": "meta.llama3-8b-instruct-v1:0"},
			},
		},
		"evaluationConfig": []map[string]any{
			{
				"taskType": "QuestionAndAnswer",
				"dataset": map[string]any{
					"name": "my-qa-dataset",
					"datasetLocation": map[string]any{
						"s3Uri": "s3://my-bucket/qa-data",
					},
				},
				"metricNames": []map[string]any{
					{"metricName": "Builtin.Accuracy"},
					{"metricName": "Builtin.Robustness"},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)

	evalConfigs := getOut["evaluationConfig"].([]any)
	require.Len(t, evalConfigs, 1)
	ec := evalConfigs[0].(map[string]any)
	assert.Equal(t, "QuestionAndAnswer", ec["taskType"])
	dataset := ec["dataset"].(map[string]any)
	assert.Equal(t, "my-qa-dataset", dataset["name"])
	metrics := ec["metricNames"].([]any)
	assert.Len(t, metrics, 2)
}

// ─────────────────────────────────────────────────────────────────────────────
// EvaluationJob: list returns correct fields.
// ─────────────────────────────────────────────────────────────────────────────

func TestBatch1_EvaluationJob_ListEvaluationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "list-eval-1"})
	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "list-eval-2"})

	rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobs := out["jobSummaries"].([]any)
	assert.Len(t, jobs, 2)

	for _, raw := range jobs {
		j := raw.(map[string]any)
		assert.NotEmpty(t, j["jobArn"])
		assert.NotEmpty(t, j["jobName"])
		assert.NotEmpty(t, j["status"])
		assert.NotEmpty(t, j["creationTime"])
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail copy semantics: modifying returned value does not affect stored state.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Foundation model: no spurious fields when guardrail returned via HTTP.
// ─────────────────────────────────────────────────────────────────────────────

func TestBatch1_FoundationModel_ResponseStreamingSupported_Bool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Titan text supports streaming.
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-text-express-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)
	rawBytes, err := json.Marshal(details)
	require.NoError(t, err)

	var typed struct {
		InferenceTypesSupported    []string `json:"inferenceTypesSupported"`
		ResponseStreamingSupported bool     `json:"responseStreamingSupported"`
	}
	require.NoError(t, json.Unmarshal(rawBytes, &typed))
	assert.True(t, typed.ResponseStreamingSupported)
	assert.NotEmpty(t, typed.InferenceTypesSupported)

	// Embedding model does not support streaming.
	rec2 := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-embed-text-v1", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	details2 := out2["modelDetails"].(map[string]any)
	rawBytes2, err := json.Marshal(details2)
	require.NoError(t, err)

	var typed2 struct {
		ResponseStreamingSupported bool `json:"responseStreamingSupported"`
	}
	require.NoError(t, json.Unmarshal(rawBytes2, &typed2))
	assert.False(t, typed2.ResponseStreamingSupported)
}

// ─────────────────────────────────────────────────────────────────────────────
// Guardrail: create without policies, update to add content policy via HTTP.
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// helpers.
// ─────────────────────────────────────────────────────────────────────────────

func typesAsStrings(raw []any) []string {
	out := make([]string, len(raw))
	for i, v := range raw {
		out[i] = v.(string)
	}

	return out
}
