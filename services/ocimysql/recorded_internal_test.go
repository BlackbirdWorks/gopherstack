package ocimysql

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordedFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra map[string]jsontext.Value
		name  string
		want  []string
	}{
		{name: "none", extra: nil},
		{
			name:  "rest on",
			extra: map[string]jsontext.Value{"rest": jsontext.Value(`{"status":"ENABLED"}`)},
			want:  []string{"rest"},
		},
		{name: "rest off", extra: map[string]jsontext.Value{"rest": jsontext.Value(`{"status":"DISABLED"}`)}},
		{
			name: "console and read endpoint",
			extra: map[string]jsontext.Value{
				"databaseConsole": jsontext.Value(`{"status":"ENABLED"}`),
				"readEndpoint":    jsontext.Value(`{"isEnabled":true}`),
			},
			want: []string{"databaseConsole", "readEndpoint"},
		},
		{
			name: "own certificate",
			extra: map[string]jsontext.Value{
				"secureConnections": jsontext.Value(`{"certificateGenerationType":"BYOC"}`),
			},
			want: []string{"secureConnections"},
		},
		{
			name: "system certificate",
			extra: map[string]jsontext.Value{
				"secureConnections": jsontext.Value(`{"certificateGenerationType":"SYSTEM"}`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, recordedFeatures(tt.extra))
		})
	}
}
