package comprehend_test

import (
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- TooManyTagsException: 50-tag-per-resource limit ---

func manyTagsFrom(start, n int) []any {
	tags := make([]any, 0, n)
	for i := range n {
		tags = append(tags, map[string]any{"Key": fmt.Sprintf("k%d", start+i), "Value": "v"})
	}

	return tags
}

func manyTags(n int) []any { return manyTagsFrom(0, n) }

func TestTooManyTagsOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "create_document_classifier",
			action: "CreateDocumentClassifier",
			body:   map[string]any{"DocumentClassifierName": "too-many-tags", "LanguageCode": "en"},
		},
		{
			name:   "start_sentiment_job",
			action: "StartSentimentDetectionJob",
			body:   map[string]any{"JobName": "too-many-tags", "LanguageCode": "en"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)
			body["Tags"] = manyTags(51)

			rec := rawRequest(t, newHandler(), tt.action, toJSON(t, body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, "TooManyTagsException", m["__type"])
		})
	}
}

// TestTooManyTagsOnCreate_ResourceNotCreated verifies that a rejected
// TooManyTagsException create leaves no partial resource behind (real AWS
// validates tags before creating anything).
func TestTooManyTagsOnCreate_ResourceNotCreated(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := rawRequest(t, h, "CreateDocumentClassifier", toJSON(t, map[string]any{
		"DocumentClassifierName": "rejected-clf", "LanguageCode": "en", "Tags": manyTags(51),
	}))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	listed := request(t, h, "ListDocumentClassifiers", nil)
	assert.Empty(t, listed["DocumentClassifierPropertiesList"])
}

func TestTooManyTagsOnTagResource(t *testing.T) {
	t.Parallel()

	h := newHandler()
	created := request(t, h, "CreateDocumentClassifier", map[string]any{
		"DocumentClassifierName": "tag-limit-clf", "LanguageCode": "en", "Tags": manyTags(49),
	})
	arn := created["DocumentClassifierArn"].(string)

	// 49 existing (k0..k48) + 2 new, distinct keys (k100, k101) = 51,
	// exceeding the 50-tag-per-resource limit.
	rec := rawRequest(t, h, "TagResource", toJSON(t, map[string]any{
		"ResourceArn": arn, "Tags": manyTagsFrom(100, 2),
	}))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := decodeBody(t, rec)
	assert.Equal(t, "TooManyTagsException", m["__type"])

	// The resource's existing 49 tags must be untouched by the rejected call.
	tags := request(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	assert.Len(t, tags["Tags"], 49)
}

// --- KmsKeyValidationException ---

func TestKmsKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "create_classifier_bad_model_kms_key",
			action: "CreateDocumentClassifier",
			body: map[string]any{
				"DocumentClassifierName": "bad-kms-clf", "LanguageCode": "en",
				"ModelKmsKeyId": "not-a-valid-key",
			},
		},
		{
			name:   "start_job_bad_volume_kms_key",
			action: "StartSentimentDetectionJob",
			body: map[string]any{
				"JobName": "bad-kms-job", "LanguageCode": "en",
				"VolumeKmsKeyId": "not-a-valid-key",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, toJSON(t, tt.body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, "KmsKeyValidationException", m["__type"])
		})
	}
}

func TestKmsKeyValidationAcceptsValidShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		keyID string
	}{
		{name: "bare_uuid", keyID: "1234abcd-12ab-34cd-56ef-1234567890ab"},
		{
			name:  "key_arn",
			keyID: "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
		},
		{name: "alias_arn", keyID: "arn:aws:kms:us-west-2:111122223333:alias/my-key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := request(t, newHandler(), "CreateDocumentClassifier", map[string]any{
				"DocumentClassifierName": "ok-kms-" + tt.name, "LanguageCode": "en",
				"ModelKmsKeyId": tt.keyID,
			})
			assert.NotEmpty(t, m["DocumentClassifierArn"])
		})
	}
}
