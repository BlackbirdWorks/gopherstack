package comprehend_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- Error paths ---

func TestErrorsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "describe_missing_classifier",
			action: "DescribeDocumentClassifier",
			body:   `{"DocumentClassifierArn":"arn:aws:comprehend:us-east-1:000000000000:document-classifier/nope"}`,
		},
		{
			name:   "describe_missing_entity_recognizer",
			action: "DescribeEntityRecognizer",
			body:   `{"EntityRecognizerArn":"arn:aws:comprehend:us-east-1:000000000000:entity-recognizer/nope"}`,
		},
		{
			name:   "describe_missing_endpoint",
			action: "DescribeEndpoint",
			body:   `{"EndpointArn":"arn:aws:comprehend:us-east-1:000000000000:endpoint/nope"}`,
		},
		{
			name:   "describe_missing_job",
			action: "DescribeSentimentDetectionJob",
			body:   `{"JobId":"no-such-job-id"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, "ResourceNotFoundException", m["__type"])
			assert.NotEmpty(t, m["message"])
		})
	}
}

func TestErrorsInvalidRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "detect_sentiment_empty_text",
			action: "DetectSentiment",
			body:   `{"Text":"","LanguageCode":"en"}`,
		},
		{
			name:   "detect_entities_whitespace_text",
			action: "DetectEntities",
			body:   `{"Text":"   ","LanguageCode":"en"}`,
		},
		{
			name:   "create_classifier_missing_name",
			action: "CreateDocumentClassifier",
			body:   `{"DocumentClassifierName":"","LanguageCode":"en"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := decodeBody(t, rec)
			assert.Equal(t, "InvalidRequestException", m["__type"])
		})
	}
}
