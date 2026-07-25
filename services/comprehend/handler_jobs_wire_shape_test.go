package comprehend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Per-job-family wire-shape field-diff ---
//
// Real *Properties shapes are NOT uniform across the 9 async job families
// (see jobMap's doc comment in handler_jobs.go); this locks in that only
// the fields each family's real shape actually carries appear on the wire.

func TestDocumentClassificationJobWireShape(t *testing.T) {
	t.Parallel()

	h := newHandler()
	started := request(t, h, "StartDocumentClassificationJob", map[string]any{
		"JobName": "wire-shape-doc-clf", "FlywheelArn": "arn:aws:comprehend:us-east-1:123456789012:flywheel/fw",
		"VolumeKmsKeyId": "1234abcd-12ab-34cd-56ef-1234567890ab",
	})
	desc := request(t, h, "DescribeDocumentClassificationJob", map[string]any{"JobId": started["JobId"]})
	props := desc["DocumentClassificationJobProperties"].(map[string]any)

	assert.Equal(t, "arn:aws:comprehend:us-east-1:123456789012:flywheel/fw", props["FlywheelArn"])
	assert.Equal(t, "1234abcd-12ab-34cd-56ef-1234567890ab", props["VolumeKmsKeyId"])
	assert.NotContains(t, props, fieldLanguageCodeKey, "DocumentClassificationJobProperties has no LanguageCode field")
	assert.NotContains(t, props, "EntityRecognizerArn")
}

func TestEntitiesDetectionJobWireShape(t *testing.T) {
	t.Parallel()

	h := newHandler()
	started := request(t, h, "StartEntitiesDetectionJob", map[string]any{
		"JobName": "wire-shape-entities", "LanguageCode": "en",
		"EntityRecognizerArn": "arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/er",
		"FlywheelArn":         "arn:aws:comprehend:us-east-1:123456789012:flywheel/fw",
	})
	desc := request(t, h, "DescribeEntitiesDetectionJob", map[string]any{"JobId": started["JobId"]})
	props := desc["EntitiesDetectionJobProperties"].(map[string]any)

	assert.Equal(t, "en", props[fieldLanguageCodeKey])
	assert.Equal(t, "arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/er", props["EntityRecognizerArn"])
	assert.Equal(t, "arn:aws:comprehend:us-east-1:123456789012:flywheel/fw", props["FlywheelArn"])
	assert.NotContains(t, props, "DocumentClassifierArn")
}

func TestPiiEntitiesDetectionJobWireShape(t *testing.T) {
	t.Parallel()

	h := newHandler()
	started := request(t, h, "StartPiiEntitiesDetectionJob", map[string]any{
		"JobName": "wire-shape-pii", "LanguageCode": "en", "Mode": "ONLY_OFFSETS",
		"RedactionConfig": map[string]any{"PiiEntityTypes": []any{"SSN"}},
	})
	desc := request(t, h, "DescribePiiEntitiesDetectionJob", map[string]any{"JobId": started["JobId"]})
	props := desc["PiiEntitiesDetectionJobProperties"].(map[string]any)

	assert.Equal(t, "ONLY_OFFSETS", props["Mode"])
	redaction, ok := props["RedactionConfig"].(map[string]any)
	require.True(t, ok, "RedactionConfig must be present")
	assert.Contains(t, redaction, "PiiEntityTypes")
	assert.NotContains(t, props, "VolumeKmsKeyId", "PiiEntitiesDetectionJobProperties has no VolumeKmsKeyId field")
	assert.NotContains(t, props, "VpcConfig", "PiiEntitiesDetectionJobProperties has no VpcConfig field")
}

func TestTopicsDetectionJobWireShape(t *testing.T) {
	t.Parallel()

	h := newHandler()
	started := request(t, h, "StartTopicsDetectionJob", map[string]any{
		"JobName": "wire-shape-topics", "NumberOfTopics": float64(10),
	})
	desc := request(t, h, "DescribeTopicsDetectionJob", map[string]any{"JobId": started["JobId"]})
	props := desc["TopicsDetectionJobProperties"].(map[string]any)

	assert.InEpsilon(t, float64(10), props["NumberOfTopics"], 0)
	assert.NotContains(t, props, fieldLanguageCodeKey, "TopicsDetectionJobProperties has no LanguageCode field")
}

func TestEventsDetectionJobWireShape(t *testing.T) {
	t.Parallel()

	h := newHandler()
	started := request(t, h, "StartEventsDetectionJob", map[string]any{
		"JobName": "wire-shape-events", "LanguageCode": "en",
		"TargetEventTypes": []any{"BANKRUPTCY", "EMPLOYMENT"},
	})
	desc := request(t, h, "DescribeEventsDetectionJob", map[string]any{"JobId": started["JobId"]})
	props := desc["EventsDetectionJobProperties"].(map[string]any)

	assert.Equal(t, "en", props[fieldLanguageCodeKey])
	assert.ElementsMatch(t, []any{"BANKRUPTCY", "EMPLOYMENT"}, props["TargetEventTypes"])
	assert.NotContains(t, props, "VolumeKmsKeyId", "EventsDetectionJobProperties has no VolumeKmsKeyId field")
}

// fieldLanguageCodeKey avoids colliding with the unexported package-level
// fieldLanguageCode constant, which is not visible from the comprehend_test
// package.
const fieldLanguageCodeKey = "LanguageCode"
