package azureeventgrid_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

func mappingBody(props map[string]any) map[string]any {
	return map[string]any{"inputSchemaMappingType": "Json", "properties": props}
}

func TestCustomInputSchema(t *testing.T) {
	t.Parallel()

	payload := `{"myEventTypeField":"Created","resource":"Users/1","resourceData":{"a":1},"when":"2026-10-20T12:00:00Z"}`

	tests := []struct {
		mapping  map[string]any
		check    func(t *testing.T, got map[string]any)
		name     string
		delivery string
		body     string
		wantErr  bool
	}{
		{
			name: "mapped fields and defaults",
			mapping: mappingBody(map[string]any{
				"eventType":   map[string]any{"sourceField": "myEventTypeField"},
				"subject":     map[string]any{"sourceField": "resource", "defaultValue": "fallback"},
				"dataVersion": map[string]any{"defaultValue": "1.0"},
				"eventTime":   map[string]any{"sourceField": "when"},
			}),
			body: payload,
			check: func(t *testing.T, got map[string]any) {
				t.Helper()
				assert.Equal(t, "Created", got["eventType"])
				assert.Equal(t, "Users/1", got["subject"])
				assert.Equal(t, "1.0", got["dataVersion"])
				assert.Equal(t, "2026-10-20T12:00:00Z", got["eventTime"])
				assert.Equal(t, topicRID, got["topic"])
				assert.Equal(t, "1", got["metadataVersion"])
				assert.NotEmpty(t, got["id"])
				assert.Equal(t, "Users/1", got["data"].(map[string]any)["resource"])
			},
		},
		{
			name: "default used when source field is absent",
			mapping: mappingBody(map[string]any{
				"eventType":   map[string]any{"defaultValue": "Thing"},
				"subject":     map[string]any{"sourceField": "nope", "defaultValue": "DefaultSubject"},
				"dataVersion": map[string]any{"defaultValue": "2"},
			}),
			body: payload,
			check: func(t *testing.T, got map[string]any) {
				t.Helper()
				assert.Equal(t, "DefaultSubject", got["subject"])
				assert.Equal(t, "Thing", got["eventType"])
			},
		},
		{
			name:     "custom input delivery hands over the original payload",
			delivery: azureeventgrid.SchemaCustomInput,
			mapping: mappingBody(map[string]any{
				"eventType":   map[string]any{"sourceField": "myEventTypeField"},
				"subject":     map[string]any{"defaultValue": "s"},
				"dataVersion": map[string]any{"defaultValue": "1"},
			}),
			body: payload,
			check: func(t *testing.T, got map[string]any) {
				t.Helper()
				assert.Equal(t, "Created", got["myEventTypeField"])
				assert.NotContains(t, got, "metadataVersion")
			},
		},
		{
			name:    "required property missing",
			mapping: mappingBody(map[string]any{"eventType": map[string]any{"sourceField": "myEventTypeField"}}),
			body:    payload,
			wantErr: true,
		},
		{name: "not an object", body: `[1]`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, rec := newBackend(t)
			_, err := b.PutTopicMapped("c1", topicRID, azureeventgrid.SchemaCustom, tt.mapping)
			require.NoError(t, err)

			props := queueDest("q")
			if tt.delivery != "" {
				props["eventDeliverySchema"] = tt.delivery
			}

			_, err = b.PutSubscription(t.Context(), "c1", "s1", props)
			require.NoError(t, err)

			events, err := azureeventgrid.ParseCustomEvents([]byte(tt.body))
			if err == nil {
				err = b.Publish("c1", events)
			}

			if tt.wantErr {
				require.ErrorIs(t, err, azureeventgrid.ErrInvalidEvent)

				return
			}

			require.NoError(t, err)
			require.Equal(t, 1, b.RunDue(t.Context()))

			var got map[string]any

			require.Len(t, rec.all(), 1)
			require.NoError(t, json.Unmarshal([]byte(rec.all()[0].text), &got))
			tt.check(t, got)
		})
	}
}

func TestInputSchemaMappingValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mapping map[string]any
		name    string
		schema  string
		wantErr bool
	}{
		{name: "valid", schema: azureeventgrid.SchemaCustom,
			mapping: mappingBody(map[string]any{"subject": map[string]any{"defaultValue": "x"}})},
		{name: "mapping on eventgrid schema", schema: azureeventgrid.SchemaEventGrid,
			mapping: mappingBody(map[string]any{"id": map[string]any{"sourceField": "x"}}), wantErr: true},
		{name: "wrong type", schema: azureeventgrid.SchemaCustom,
			mapping: map[string]any{"inputSchemaMappingType": "Xml"}, wantErr: true},
		{name: "subject needs source or default", schema: azureeventgrid.SchemaCustom,
			mapping: mappingBody(map[string]any{"subject": map[string]any{}}), wantErr: true},
		{name: "id takes no default", schema: azureeventgrid.SchemaCustom,
			mapping: mappingBody(map[string]any{"id": map[string]any{"defaultValue": "x"}}), wantErr: true},
		{name: "unknown schema", schema: "Nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			_, err := b.PutTopicMapped("c1", topicRID, tt.schema, tt.mapping)

			if tt.wantErr {
				require.ErrorIs(t, err, azureeventgrid.ErrInvalidSpec)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCustomTopicDeliverySchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		delivery string
		wantErr  bool
	}{
		{name: "default", delivery: ""},
		{name: "event grid", delivery: azureeventgrid.SchemaEventGrid},
		{name: "custom input", delivery: azureeventgrid.SchemaCustomInput},
		{name: "cloud events", delivery: azureeventgrid.SchemaCloudEvent, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			_, err := b.PutTopicMapped("c1", topicRID, azureeventgrid.SchemaCustom, nil)
			require.NoError(t, err)

			props := queueDest("q")
			props["eventDeliverySchema"] = tt.delivery
			_, err = b.PutSubscription(t.Context(), "c1", "s1", props)

			if tt.wantErr {
				require.ErrorIs(t, err, azureeventgrid.ErrInvalidSpec)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPublishCustomSchemaOverHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		status int
		want   int
	}{
		{name: "mapped event", body: `[{"kind":"Created","what":"x"}]`, status: http.StatusOK, want: 1},
		{name: "unmapped required field", body: `[{"what":"x"}]`, status: http.StatusBadRequest},
		{name: "event grid shape is not required", body: `{"kind":"Created"}`, status: http.StatusOK, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			_, err := e.backend.PutTopicMapped("c2", topicRID, azureeventgrid.SchemaCustom, mappingBody(map[string]any{
				"eventType":   map[string]any{"sourceField": "kind"},
				"subject":     map[string]any{"defaultValue": "s"},
				"dataVersion": map[string]any{"defaultValue": "1"},
			}))
			require.NoError(t, err)

			_, err = e.backend.PutSubscription(t.Context(), "c2", "s", queueDest("q"))
			require.NoError(t, err)

			resp := post(t, e.srv.Client(), e.srv.URL+"/api/events", "c2.localhost", nil, tt.body)
			assert.Equal(t, tt.status, resp.StatusCode)
			assert.Equal(t, tt.want, azureeventgrid.PendingCount(e.backend, "c2", "s"))
		})
	}
}
