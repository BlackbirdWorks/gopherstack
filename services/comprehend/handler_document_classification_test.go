package comprehend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ContainsPiiEntities and ClassifyDocument ---

func TestContainsPiiEntitiesFieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		text    string
		wantPii bool
	}{
		{name: "with_pii", text: "My SSN is 123-45-6789", wantPii: true},
		{name: "without_pii", text: "The weather is nice today", wantPii: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := request(t, newHandler(), "ContainsPiiEntities", map[string]any{"Text": tt.text, "LanguageCode": "en"})
			labels, ok := m["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")
			if tt.wantPii {
				assert.NotEmpty(t, labels, "text with PII should have non-empty Labels")
			} else {
				assert.Empty(t, labels, "text without PII should have empty Labels")
			}
		})
	}
}

// TestContainsPiiEntitiesLabelTypes verifies that ContainsPiiEntities returns
// Labels with specific PII entity type names (EMAIL, SSN) rather than the
// generic "PII" label that the old code produced.
func TestContainsPiiEntitiesLabelTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		wantTypes []string
		wantEmpty bool
	}{
		{
			name:      "email_only",
			text:      "Contact us at user@example.com for support.",
			wantTypes: []string{"EMAIL"},
		},
		{
			name:      "ssn_only",
			text:      "SSN on file: 123-45-6789.",
			wantTypes: []string{"SSN"},
		},
		{
			name:      "email_and_ssn",
			text:      "Email user@example.com and SSN 987-65-4321.",
			wantTypes: []string{"EMAIL", "SSN"},
		},
		{
			name:      "no_pii",
			text:      "The weather is sunny today.",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := request(t, newHandler(), "ContainsPiiEntities", map[string]any{"Text": tt.text, "LanguageCode": "en"})
			labels, ok := m["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")

			if tt.wantEmpty {
				assert.Empty(t, labels, "no PII in text — Labels must be empty")

				return
			}

			require.Len(t, labels, len(tt.wantTypes), "label count must match detected PII types")
			gotNames := make([]string, 0, len(labels))
			for _, raw := range labels {
				label := raw.(map[string]any)
				name, nameOK := label["Name"].(string)
				require.True(t, nameOK, "each label must have a Name field")
				assert.NotEqual(t, "PII", name, "label Name must be a specific type, not generic PII")
				gotNames = append(gotNames, name)
			}
			for _, want := range tt.wantTypes {
				assert.Contains(t, gotNames, want, "expected PII type %q in Labels", want)
			}
		})
	}
}

func TestClassifyDocumentFieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		wantClass string
	}{
		{name: "finance_text", text: "The finance sector money report", wantClass: "FINANCE"},
		{name: "sports_text", text: "The sports game was exciting", wantClass: "SPORTS"},
		{name: "tech_text", text: "The tech computer innovation", wantClass: "TECHNOLOGY"},
		{name: "other_text", text: "Random unclassified content here", wantClass: "OTHER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			arn := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/clf"
			m := request(t, newHandler(), "ClassifyDocument", map[string]any{"Text": tt.text, "EndpointArn": arn})

			classes, ok := m["Classes"].([]any)
			require.True(t, ok, "Classes must be a list")
			require.NotEmpty(t, classes)

			cls := classes[0].(map[string]any)
			assert.Contains(t, cls, "Name", "class must have Name field")
			assert.Contains(t, cls, "Score", "class must have Score field")
			assert.Equal(t, tt.wantClass, cls["Name"])

			labels, ok := m["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")
			assert.NotEmpty(t, labels)
		})
	}
}
