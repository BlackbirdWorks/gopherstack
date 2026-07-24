package comprehend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ClassifierMetadata / RecognizerMetadata / TrainingStartTime / TrainingEndTime ---
//
// Real DocumentClassifierProperties/EntityRecognizerProperties only carry
// ClassifierMetadata/RecognizerMetadata and TrainingStartTime/TrainingEndTime
// once training has actually completed (status TRAINED); see
// resourceMap's doc comment in handler_resources.go.

func TestDocumentClassifierMetadataPresentWhenTrained(t *testing.T) {
	t.Parallel()

	h := newHandler()
	created := request(t, h, "CreateDocumentClassifier", map[string]any{
		"DocumentClassifierName": "metadata-clf", "LanguageCode": "en",
	})
	arn := created["DocumentClassifierArn"].(string)

	// The emulator fast-forwards classifier training straight to TRAINED
	// (see initialResourceStatus in store.go), so metadata must be present
	// on the very first Describe.
	described := request(t, h, "DescribeDocumentClassifier", map[string]any{"DocumentClassifierArn": arn})
	props := described["DocumentClassifierProperties"].(map[string]any)
	assert.Equal(t, "TRAINED", props["Status"])
	assert.NotEmpty(t, props["TrainingStartTime"], "TrainingStartTime must be set once TRAINED")
	assert.NotEmpty(t, props["TrainingEndTime"], "TrainingEndTime must be set once TRAINED")

	metadata, ok := props["ClassifierMetadata"].(map[string]any)
	require.True(t, ok, "ClassifierMetadata must be present once TRAINED")
	assert.Contains(t, metadata, "NumberOfLabels")
	assert.Contains(t, metadata, "NumberOfTestDocuments")
	assert.Contains(t, metadata, "NumberOfTrainedDocuments")

	evalMetrics, ok := metadata["EvaluationMetrics"].(map[string]any)
	require.True(t, ok, "EvaluationMetrics must be present")
	for _, field := range []string{
		"Accuracy", "F1Score", "HammingLoss", "MicroF1Score", "MicroPrecision", "MicroRecall", "Precision", "Recall",
	} {
		assert.Contains(t, evalMetrics, field)
	}

	assert.NotContains(t, props, "RecognizerMetadata", "a classifier must not carry recognizer-only fields")
}

func TestEntityRecognizerMetadataPresentWhenTrained(t *testing.T) {
	t.Parallel()

	h := newHandler()
	created := request(t, h, "CreateEntityRecognizer", map[string]any{
		"RecognizerName": "metadata-rec", "LanguageCode": "en",
		"InputDataConfig": map[string]any{
			"EntityTypes": []any{map[string]any{"Type": "PERSON"}, map[string]any{"Type": "LOCATION"}},
		},
	})
	arn := created["EntityRecognizerArn"].(string)

	described := request(t, h, "DescribeEntityRecognizer", map[string]any{"EntityRecognizerArn": arn})
	props := described["EntityRecognizerProperties"].(map[string]any)
	assert.Equal(t, "TRAINED", props["Status"])
	assert.NotEmpty(t, props["TrainingStartTime"])
	assert.NotEmpty(t, props["TrainingEndTime"])

	metadata, ok := props["RecognizerMetadata"].(map[string]any)
	require.True(t, ok, "RecognizerMetadata must be present once TRAINED")
	assert.Contains(t, metadata, "NumberOfTestDocuments")
	assert.Contains(t, metadata, "NumberOfTrainedDocuments")
	assert.Contains(t, metadata, "EvaluationMetrics")

	entityTypes, ok := metadata["EntityTypes"].([]any)
	require.True(t, ok, "EntityTypes must be a list")
	require.Len(t, entityTypes, 2, "must reflect the InputDataConfig.EntityTypes supplied at creation")
	first := entityTypes[0].(map[string]any)
	assert.Equal(t, "PERSON", first["Type"])
	assert.Contains(t, first, "NumberOfTrainMentions")
	assert.Contains(t, first, "EvaluationMetrics")

	assert.NotContains(t, props, "ClassifierMetadata", "a recognizer must not carry classifier-only fields")
}

// TestDocumentClassifierSummariesGroupVersionsByName verifies
// ListDocumentClassifierSummaries groups every version created under the
// same DocumentClassifierName into a single summary row with an aggregated
// NumberOfVersions, rather than one row per stored resource -- versions are
// created by calling CreateDocumentClassifier again with the same name and
// a new VersionName (see resourceSpecs' doc comment in handler_resources.go).
func TestDocumentClassifierSummariesGroupVersionsByName(t *testing.T) {
	t.Parallel()

	h := newHandler()
	request(t, h, "CreateDocumentClassifier", map[string]any{
		"DocumentClassifierName": "grouped-clf", "LanguageCode": "en",
	})
	request(t, h, "CreateDocumentClassifier", map[string]any{
		"DocumentClassifierName": "grouped-clf", "LanguageCode": "en", "VersionName": "v2",
	})
	request(t, h, "CreateDocumentClassifier", map[string]any{
		"DocumentClassifierName": "other-clf", "LanguageCode": "en",
	})

	summaries := request(t, h, "ListDocumentClassifierSummaries", nil)
	list, ok := summaries["DocumentClassifierSummariesList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 2, "one row per distinct name, not per stored resource")

	byName := make(map[string]map[string]any, len(list))
	for _, raw := range list {
		row := raw.(map[string]any)
		byName[row["DocumentClassifierName"].(string)] = row
	}
	require.Contains(t, byName, "grouped-clf")
	require.Contains(t, byName, "other-clf")
	assert.InEpsilon(t, float64(2), byName["grouped-clf"]["NumberOfVersions"], 0)
	assert.InEpsilon(t, float64(1), byName["other-clf"]["NumberOfVersions"], 0)
}
