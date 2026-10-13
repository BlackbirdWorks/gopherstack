package fis_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestSetLifecycleDelay_StretchesInitiating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
	}{
		{name: "default", delay: 0, want: "completed"},
		{name: "stretched", delay: time.Minute, want: "initiating"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newTestHandler(t)
				h.SetLifecycleDelay(tt.delay)

				rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", map[string]any{
					"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
					"stopConditions": []map[string]any{{"source": "none"}},
					"targets":        map[string]any{},
					"actions":        map[string]any{},
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				var tpl struct {
					ExperimentTemplate struct {
						ID string `json:"id"`
					} `json:"experimentTemplate"`
				}

				mustJSON(t, rec, &tpl)

				rec = doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
					"experimentTemplateId": tpl.ExperimentTemplate.ID,
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				var exp struct {
					Experiment struct {
						ID string `json:"id"`
					} `json:"experiment"`
				}

				mustJSON(t, rec, &exp)

				time.Sleep(3*fis.LifecycleDelayForTest + time.Millisecond)
				synctest.Wait()

				rec = doRequest(t, h, http.MethodGet, "/experiments/"+exp.Experiment.ID, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var got struct {
					Experiment struct {
						State struct {
							Status string `json:"status"`
						} `json:"state"`
					} `json:"experiment"`
				}

				mustJSON(t, rec, &got)
				assert.Equal(t, tt.want, got.Experiment.State.Status)

				h.SetLifecycleDelay(0)
				doRequest(t, h, http.MethodPost, "/experiments/"+exp.Experiment.ID+"/stop", nil)
				h.Shutdown(t.Context())
				synctest.Wait()
			})
		})
	}
}
