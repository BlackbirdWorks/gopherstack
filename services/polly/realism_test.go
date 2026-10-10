package polly_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

func TestRealism_RequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantType string
		wantMsg  string
	}{
		{
			name: "describe_voices_language_code", method: http.MethodGet, path: "/v1/voices?LanguageCode=xx",
			wantType: "ValidationException", wantMsg: `invalid LanguageCode "xx"`,
		},
		{
			name: "describe_voices_next_token", method: http.MethodGet, path: "/v1/voices?NextToken=garbage",
			wantType: "InvalidNextTokenException", wantMsg: "invalid NextToken",
		},
		{
			name: "validation_has_no_code_prefix", method: http.MethodGet, path: "/v1/voices?Engine=quantum",
			wantType: "ValidationException", wantMsg: `invalid Engine "quantum"`,
		},
		{
			name: "lexicon_not_found_has_no_code_prefix", method: http.MethodGet, path: "/v1/lexicons/nope",
			wantType: "LexiconNotFoundException", wantMsg: `lexicon "nope"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := request(t, newHandler(), tt.method, tt.path, tt.body)
			require.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			out := responseMap(t, rec)
			assert.Equal(t, tt.wantType, out["__type"])
			assert.Contains(t, out["message"], tt.wantMsg)
			assert.NotContains(t, out["message"], "Exception: ")
		})
	}
}

func TestRealism_VoicesLanguageCodeAccepted(t *testing.T) {
	t.Parallel()

	rec := request(t, newHandler(), http.MethodGet, "/v1/voices?LanguageCode=en-US", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, responseMap(t, rec)["Voices"])
}

func TestRealism_TaskLifecycleDelay(t *testing.T) {
	t.Parallel()

	const delay = 20 * time.Second

	tests := []struct {
		name    string
		sleeps  []time.Duration
		want    []string
		viaList bool
	}{
		{
			name: "via_get", sleeps: []time.Duration{0, delay, delay},
			want: []string{"scheduled", "inProgress", "completed"},
		},
		{
			name: "via_list", viaList: true, sleeps: []time.Duration{0, delay, delay},
			want: []string{"scheduled", "inProgress", "completed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
				b.SetLifecycleDelay(delay)
				h := polly.NewHandler(b)
				id, _ := startTask(t, h, "hello")

				for i, want := range tt.want {
					time.Sleep(tt.sleeps[i])

					var got string
					if tt.viaList {
						tasks := responseMap(t, request(t, h, http.MethodGet, "/v1/synthesisTasks", nil))["SynthesisTasks"].([]any)
						require.Len(t, tasks, 1)
						got = tasks[0].(map[string]any)["TaskStatus"].(string)
					} else {
						rec := request(t, h, http.MethodGet, "/v1/synthesisTasks/"+id, nil)
						got = responseMap(t, rec)["SynthesisTask"].(map[string]any)["TaskStatus"].(string)
					}

					assert.Equal(t, want, got, "stage %d", i)
				}
			})
		})
	}
}
