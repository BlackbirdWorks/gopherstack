package fis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

type fakeTargetResolver struct {
	arns []string
}

func (f fakeTargetResolver) ResolveTargets(
	context.Context, string, map[string]string, []fis.ExperimentTemplateTargetFilter,
) []string {
	return f.arns
}

func TestStartExperiment_TargetResolution(t *testing.T) {
	t.Parallel()

	const (
		inAccount  = "arn:aws:ec2:us-east-1:000000000000:instance/i-1"
		otherAcct  = "arn:aws:ec2:us-east-1:111111111111:instance/i-2"
		explicit   = "arn:aws:ec2:us-east-1:000000000000:instance/i-3"
		emptyMsg   = "Target resolution returned empty set"
		statusFail = "failed"
		statusDone = "completed"
	)

	tests := []struct {
		name         string
		options      map[string]any
		resolved     []string
		explicit     []string
		wantStatus   string
		wantReason   string
		wantActions  string
		wantProvider []string
	}{
		{
			name: "default-fails-on-empty", resolved: nil,
			wantStatus: statusFail, wantReason: emptyMsg,
		},
		{
			name:       "fail-mode",
			options:    map[string]any{"emptyTargetResolutionMode": "fail"},
			wantStatus: statusFail, wantReason: emptyMsg,
		},
		{
			name:       "skip-mode-skips-action",
			options:    map[string]any{"emptyTargetResolutionMode": "skip"},
			wantStatus: statusDone, wantActions: "skipped",
		},
		{
			name:       "resolved-reaches-provider",
			resolved:   []string{inAccount},
			wantStatus: statusDone, wantActions: "completed", wantProvider: []string{inAccount},
		},
		{
			name:       "merged-with-explicit-arns",
			resolved:   []string{inAccount, explicit},
			explicit:   []string{explicit},
			wantStatus: statusDone, wantActions: "completed", wantProvider: []string{explicit, inAccount},
		},
		{
			name:       "single-account-drops-foreign-arns",
			options:    map[string]any{"accountTargeting": "single-account"},
			resolved:   []string{otherAcct},
			wantStatus: statusFail, wantReason: emptyMsg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			mock := &fis.MockFISActionProvider{
				Definitions: []service.FISActionDefinition{
					{ActionID: "aws:test:resolve", TargetType: "aws:ec2:instance"},
				},
			}
			h.SetActionProviders([]service.FISActionProvider{mock})
			h.SetTargetResolver(fakeTargetResolver{arns: tt.resolved})

			target := map[string]any{
				"resourceType":  "aws:ec2:instance",
				"selectionMode": "ALL",
				"resourceTags":  map[string]string{"env": "chaos"},
			}
			if tt.explicit != nil {
				target["resourceArns"] = tt.explicit
			}

			body := map[string]any{
				"roleArn":        "arn:aws:iam::000000000000:role/FISRole",
				"stopConditions": []map[string]any{{"source": "none"}},
				"targets":        map[string]any{"Instances": target},
				"actions": map[string]any{
					"act": map[string]any{
						"actionId": "aws:test:resolve", "targets": map[string]string{"Instances": "Instances"},
					},
				},
			}
			if tt.options != nil {
				body["experimentOptions"] = tt.options
			}

			rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

			var tpl struct {
				ExperimentTemplate struct {
					ID string `json:"id"`
				} `json:"experimentTemplate"`
			}
			mustJSON(t, rec, &tpl)

			rec = doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
				"experimentTemplateId": tpl.ExperimentTemplate.ID,
			})
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

			var started struct {
				Experiment struct {
					ID string `json:"id"`
				} `json:"experiment"`
			}
			mustJSON(t, rec, &started)

			exp := pollExperimentUntilTerminal(t, h, started.Experiment.ID)

			var status struct {
				Status string `json:"status"`
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(exp["status"], &status))
			assert.Equal(t, tt.wantStatus, status.Status)
			assert.Equal(t, tt.wantReason, status.Reason)

			if tt.wantActions != "" {
				var actions map[string]struct {
					State struct {
						Status string `json:"status"`
					} `json:"state"`
				}
				require.NoError(t, json.Unmarshal(exp["actions"], &actions))
				assert.Equal(t, tt.wantActions, actions["act"].State.Status)
			}

			if tt.wantProvider == nil {
				assert.Empty(t, mock.Execs)

				return
			}

			require.Len(t, mock.Execs, 1)
			assert.ElementsMatch(t, tt.wantProvider, mock.Execs[0].Targets)
		})
	}
}
