package comprehend_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNestedConfigValidation(t *testing.T) {
	t.Parallel()

	vpc := func(sg, subnets []any) map[string]any {
		cfg := map[string]any{}
		if sg != nil {
			cfg["SecurityGroupIds"] = sg
		}

		if subnets != nil {
			cfg["Subnets"] = subnets
		}

		return cfg
	}

	tests := []struct {
		body   map[string]any
		name   string
		action string
		valid  bool
	}{
		{
			name: "vpc_ok", action: "CreateDocumentClassifier", valid: true,
			body: map[string]any{
				"DocumentClassifierName": "vpc-ok",
				"VpcConfig":              vpc([]any{"sg-1"}, []any{"subnet-1"}),
			},
		},
		{
			name: "vpc_missing_subnets", action: "CreateDocumentClassifier",
			body: map[string]any{"DocumentClassifierName": "vpc-nosub", "VpcConfig": vpc([]any{"sg-1"}, nil)},
		},
		{
			name: "vpc_empty_groups", action: "CreateEntityRecognizer",
			body: map[string]any{
				"RecognizerName": "vpc-nosg",
				"VpcConfig":      vpc([]any{}, []any{"subnet-1"}),
			},
		},
		{
			name: "job_vpc_missing_groups", action: "StartSentimentDetectionJob",
			body: map[string]any{
				"JobName": "job-vpc", "LanguageCode": "en",
				"VpcConfig": vpc(nil, []any{"subnet-1"}),
			},
		},
		{
			name: "redaction_ok", action: "StartPiiEntitiesDetectionJob", valid: true,
			body: map[string]any{
				"JobName": "red-ok", "LanguageCode": "en", "Mode": "ONLY_REDACTION",
				"RedactionConfig": map[string]any{
					"MaskMode": "MASK", "MaskCharacter": "*", "PiiEntityTypes": []any{"SSN"},
				},
			},
		},
		{
			name: "redaction_bad_mask_mode", action: "StartPiiEntitiesDetectionJob",
			body: map[string]any{
				"JobName": "red-mode", "LanguageCode": "en", "Mode": "ONLY_REDACTION",
				"RedactionConfig": map[string]any{"MaskMode": "BLUR"},
			},
		},
		{
			name: "redaction_bad_entity_type", action: "StartPiiEntitiesDetectionJob",
			body: map[string]any{
				"JobName": "red-type", "LanguageCode": "en", "Mode": "ONLY_REDACTION",
				"RedactionConfig": map[string]any{"PiiEntityTypes": []any{"NOT_A_TYPE"}},
			},
		},
		{
			name: "redaction_long_mask_char", action: "StartPiiEntitiesDetectionJob",
			body: map[string]any{
				"JobName": "red-char", "LanguageCode": "en", "Mode": "ONLY_REDACTION",
				"RedactionConfig": map[string]any{"MaskCharacter": "**"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, toJSON(t, tt.body))
			if tt.valid {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, "InvalidRequestException", decodeBody(t, rec)["__type"])
		})
	}
}

func TestDocumentBytesInput(t *testing.T) {
	t.Parallel()

	enc := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	tests := []struct {
		body       map[string]any
		name       string
		action     string
		wantEntity string
		wantOK     bool
	}{
		{
			name: "classify_text_bytes", action: "ClassifyDocument", wantOK: true,
			body: map[string]any{"EndpointArn": "arn:x", "Bytes": enc("money and finance")},
		},
		{
			name: "text_and_bytes", action: "ClassifyDocument",
			body: map[string]any{"EndpointArn": "arn:x", "Text": "a", "Bytes": enc("b")},
		},
		{
			name: "pdf_bytes", action: "ClassifyDocument",
			body: map[string]any{"EndpointArn": "arn:x", "Bytes": enc("%PDF-1.7 data")},
		},
		{
			name: "bad_base64", action: "ClassifyDocument",
			body: map[string]any{"EndpointArn": "arn:x", "Bytes": "***"},
		},
		{
			name: "reader_config_missing_action", action: "ClassifyDocument",
			body: map[string]any{
				"EndpointArn": "arn:x", "Bytes": enc("hi"),
				"DocumentReaderConfig": map[string]any{"DocumentReadMode": "SERVICE_DEFAULT"},
			},
		},
		{
			name: "reader_config_analyze_needs_features", action: "DetectEntities",
			body: map[string]any{
				"LanguageCode": "en", "Bytes": enc("hi"),
				"DocumentReaderConfig": map[string]any{"DocumentReadAction": "TEXTRACT_ANALYZE_DOCUMENT"},
			},
		},
		{
			name: "reader_config_ok", action: "DetectEntities", wantOK: true,
			body: map[string]any{
				"LanguageCode": "en", "Bytes": enc("Alice went home"),
				"DocumentReaderConfig": map[string]any{
					"DocumentReadAction": "TEXTRACT_ANALYZE_DOCUMENT",
					"FeatureTypes":       []any{"TABLES"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := rawRequest(t, newHandler(), tt.action, toJSON(t, tt.body))
			if tt.wantOK {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, "InvalidRequestException", decodeBody(t, rec)["__type"])
		})
	}
}
