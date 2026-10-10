package glue_test

import (
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestConditionalTrigger_Fires(t *testing.T) {
	t.Parallel()

	jobCond := func(job, state string) glue.TriggerCondition {
		return glue.TriggerCondition{JobName: job, State: state, LogicalOperator: "EQUALS"}
	}

	tests := []struct {
		pred       *glue.TriggerPredicate
		name       string
		startJobs  []string
		wantPreds  []string
		wantRuns   int
		manual     bool
		deactivate bool
	}{
		{
			name: "any_job_succeeded",
			pred: &glue.TriggerPredicate{
				Logical:    "ANY",
				Conditions: []glue.TriggerCondition{jobCond("j1", "SUCCEEDED")},
			},
			startJobs: []string{"j1"},
			wantRuns:  1,
			wantPreds: []string{"j1"},
		},
		{
			name: "and_waits_for_both",
			pred: &glue.TriggerPredicate{
				Logical:    "AND",
				Conditions: []glue.TriggerCondition{jobCond("j1", "SUCCEEDED"), jobCond("j2", "SUCCEEDED")},
			},
			startJobs: []string{"j1", "j2"},
			wantRuns:  1,
			wantPreds: []string{"j1", "j2"},
		},
		{
			name: "and_one_side_never_runs",
			pred: &glue.TriggerPredicate{
				Logical:    "AND",
				Conditions: []glue.TriggerCondition{jobCond("j1", "SUCCEEDED"), jobCond("j2", "SUCCEEDED")},
			},
			startJobs: []string{"j1"},
		},
		{
			name: "state_mismatch",
			pred: &glue.TriggerPredicate{
				Logical:    "ANY",
				Conditions: []glue.TriggerCondition{jobCond("j1", "FAILED")},
			},
			startJobs: []string{"j1"},
		},
		{
			name: "manual_run_does_not_fire",
			pred: &glue.TriggerPredicate{
				Logical:    "ANY",
				Conditions: []glue.TriggerCondition{jobCond("j1", "SUCCEEDED")},
			},
			startJobs: []string{"j1"},
			manual:    true,
		},
		{
			name: "deactivated_does_not_fire",
			pred: &glue.TriggerPredicate{
				Logical:    "ANY",
				Conditions: []glue.TriggerCondition{jobCond("j1", "SUCCEEDED")},
			},
			startJobs:  []string{"j1"},
			deactivate: true,
		},
		{
			name: "crawler_succeeded",
			pred: &glue.TriggerPredicate{
				Logical: "ANY",
				Conditions: []glue.TriggerCondition{
					{CrawlerName: "c1", CrawlState: "SUCCEEDED", LogicalOperator: "EQUALS"},
				},
			},
			startJobs: []string{"c1"},
			wantRuns:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := glue.NewInMemoryBackend(testAccountID, testRegion)
				defer b.Close()

				h := glue.NewHandler(b)

				for _, j := range []string{"j1", "j2", "j3"} {
					_, err := b.CreateJob(glue.Job{
						Name: j, Role: "arn:aws:iam::000000000000:role/glue", Command: glue.JobCommand{Name: "glueetl"},
					})
					require.NoError(t, err)
				}

				_, err := b.CreateCrawler("c1", "arn:aws:iam::000000000000:role/glue", "", glue.CrawlerTarget{}, nil)
				require.NoError(t, err)

				var actions []glue.TriggerAction
				for _, n := range tt.startJobs {
					if n == "c1" {
						actions = append(actions, glue.TriggerAction{CrawlerName: n})
					} else {
						actions = append(actions, glue.TriggerAction{JobName: n})
					}
				}

				_, err = b.CreateTrigger(glue.Trigger{
					Name: "dependent", Type: "CONDITIONAL", Predicate: tt.pred, StartOnCreation: true,
					Actions: []glue.TriggerAction{{JobName: "j3"}},
				}, nil)
				require.NoError(t, err)

				if tt.deactivate {
					require.NoError(t, b.StopTrigger("dependent"))
				}

				if tt.manual {
					for _, n := range tt.startJobs {
						_, err = b.StartJobRun(n, nil)
						require.NoError(t, err)
					}
				} else {
					_, err = b.CreateTrigger(glue.Trigger{Name: "root", Type: "ON_DEMAND", Actions: actions}, nil)
					require.NoError(t, err)
					require.NoError(t, b.StartTrigger("root"))
				}

				time.Sleep(2 * time.Second)
				_, err = b.GetJobRuns("j1")
				require.NoError(t, err)

				runs, err := b.GetJobRuns("j3")
				require.NoError(t, err)
				require.Len(t, runs, tt.wantRuns)

				if tt.wantRuns == 0 {
					return
				}

				assert.Equal(t, "dependent", runs[0].TriggerName)

				rec := doGlueRequest(t, h, "GetJobRun", map[string]any{
					"JobName": "j3", "RunId": runs[0].ID, "PredecessorsIncluded": true,
				})
				type pred struct {
					JobName string `json:"JobName"`
					RunID   string `json:"RunId"`
				}

				var out struct {
					JobRun struct {
						PredecessorRuns []pred `json:"PredecessorRuns"`
					} `json:"JobRun"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				var got []string
				for _, p := range out.JobRun.PredecessorRuns {
					got = append(got, p.JobName)
					assert.NotEmpty(t, p.RunID)
				}

				assert.ElementsMatch(t, tt.wantPreds, got)

				rec = doGlueRequest(t, h, "GetJobRun", map[string]any{"JobName": "j3", "RunId": runs[0].ID})
				assert.NotContains(t, rec.Body.String(), "PredecessorRuns")
			})
		})
	}
}
