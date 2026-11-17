package cloudformation_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

const (
	updTemplateV1 = `{"AWSTemplateFormatVersion":"2010-09-09","Parameters":{"Env":{"Type":"String",` +
		`"Default":"x","AllowedValues":["x","y","z"]}},` +
		`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`
	updTemplateV2 = `{"AWSTemplateFormatVersion":"2010-09-09","Parameters":{"Env":{"Type":"String",` +
		`"Default":"x","AllowedValues":["x","y","z"]}},` +
		`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}},` +
		`"Outputs":{"EnvOut":{"Value":{"Ref":"Env"}}}}`
	updTemplateStrict = `{"AWSTemplateFormatVersion":"2010-09-09","Parameters":{"Env":{"Type":"String",` +
		`"Default":"x","AllowedValues":["x"]}},` +
		`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`

	updAcct1   = "111111111111"
	updAcct2   = "222222222222"
	updSetName = "upd-set"
	updRegion1 = "us-east-1"
	updRegion2 = "us-west-2"
)

func envParam(v string) []cloudformation.Parameter {
	return []cloudformation.Parameter{{ParameterKey: "Env", ParameterValue: v}}
}

func newUpdateFixture(t *testing.T, accounts, regions []string) *cloudformation.InMemoryBackend {
	t.Helper()

	b := newBackend()
	_, err := b.CreateStackSet(updSetName, "", updTemplateV1, cloudformation.StackSetOptions{Parameters: envParam("x")})
	require.NoError(t, err)

	_, err = b.CreateStackInstances(t.Context(), updSetName, accounts, nil, regions, "")
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	return b
}

func instanceStack(t *testing.T, b *cloudformation.InMemoryBackend, account string) *cloudformation.Stack {
	t.Helper()

	inst, err := b.DescribeStackInstance(updSetName, account, updRegion1)
	require.NoError(t, err)

	stack, err := b.DescribeStack(inst.StackID)
	require.NoError(t, err)

	return stack
}

func stackEnv(stack *cloudformation.Stack) string {
	for _, p := range stack.Parameters {
		if p.ParameterKey == "Env" {
			return p.ParameterValue
		}
	}

	return ""
}

func TestUpdateStackSet_UpdatesInstanceStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTemplate map[string]string
		wantEnv      map[string]string
		wantStatus   map[string]string
		name         string
		template     string
		params       []cloudformation.Parameter
		opts         []cloudformation.StackSetOpOption
	}{
		{
			name: "all_instances", template: updTemplateV2, params: envParam("y"),
			wantTemplate: map[string]string{"111111111111": updTemplateV2, "222222222222": updTemplateV2},
			wantEnv:      map[string]string{"111111111111": "y", "222222222222": "y"},
			wantStatus:   map[string]string{"111111111111": "CURRENT", "222222222222": "CURRENT"},
		},
		{
			name: "targeted_subset", template: updTemplateV2,
			opts: []cloudformation.StackSetOpOption{
				cloudformation.WithInstanceTargets([]string{"111111111111"}, nil, []string{updRegion1}, ""),
			},
			wantTemplate: map[string]string{"111111111111": updTemplateV2, "222222222222": updTemplateV1},
			wantEnv:      map[string]string{"111111111111": "x", "222222222222": "x"},
			wantStatus:   map[string]string{"111111111111": "CURRENT", "222222222222": "OUTDATED"},
		},
		{
			name:         "no_template_or_parameter_change",
			wantTemplate: map[string]string{"111111111111": updTemplateV1, "222222222222": updTemplateV1},
			wantEnv:      map[string]string{"111111111111": "x", "222222222222": "x"},
			wantStatus:   map[string]string{"111111111111": "CURRENT", "222222222222": "CURRENT"},
		},
		{
			name: "use_previous_parameter_value", template: updTemplateV2,
			params:       []cloudformation.Parameter{{ParameterKey: "Env", UsePreviousValue: true}},
			wantTemplate: map[string]string{"111111111111": updTemplateV2, "222222222222": updTemplateV2},
			wantEnv:      map[string]string{"111111111111": "x", "222222222222": "x"},
			wantStatus:   map[string]string{"111111111111": "CURRENT", "222222222222": "CURRENT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newUpdateFixture(t, []string{"111111111111", "222222222222"}, []string{updRegion1})

			_, opID, err := b.UpdateStackSet(
				updSetName, "", tt.template, cloudformation.StackSetOptions{Parameters: tt.params}, tt.opts...,
			)
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			op, err := b.DescribeStackSetOperation(updSetName, opID)
			require.NoError(t, err)
			assert.Equal(t, "SUCCEEDED", op.Status)

			for acct, want := range tt.wantTemplate {
				stack := instanceStack(t, b, acct)
				assert.Equal(t, want, stack.TemplateBody, "template of %s", acct)
				assert.Equal(t, tt.wantEnv[acct], stackEnv(stack), "Env of %s", acct)

				inst, ierr := b.DescribeStackInstance(updSetName, acct, updRegion1)
				require.NoError(t, ierr)
				assert.Equal(t, tt.wantStatus[acct], inst.Status, "status of %s", acct)
			}
		})
	}
}

func TestUpdateStackSet_ReplacesInstanceLastOperation(t *testing.T) {
	t.Parallel()

	b := newUpdateFixture(t, []string{"111111111111"}, []string{updRegion1})

	_, opID, err := b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{})
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	inst, err := b.DescribeStackInstance(updSetName, "111111111111", updRegion1)
	require.NoError(t, err)
	assert.Equal(t, opID, inst.LastOperationID)
	assert.Equal(t, "SUCCEEDED", inst.DetailedStatus)

	results, err := b.ListStackSetOperationResults(updSetName, opID, 0, "", nil)
	require.NoError(t, err)
	require.Len(t, results.Data, 1)
	assert.Equal(t, "SUCCEEDED", results.Data[0].Status)
}

type overrideStep struct {
	overrides     *[]cloudformation.Parameter
	wantErr       error
	wantEnv       map[string]string
	wantOverrides []cloudformation.Parameter
	setParams     []cloudformation.Parameter
}

func envOf(e1, e2 string) map[string]string { return map[string]string{updAcct1: e1, updAcct2: e2} }

func TestUpdateStackInstances_ParameterOverrides(t *testing.T) {
	t.Parallel()

	const (
		a1 = updAcct1
		a2 = updAcct2
	)

	y := cloudformation.Parameter{ParameterKey: "Env", ParameterValue: "y"}

	tests := []struct {
		name  string
		steps []overrideStep
	}{
		{name: "set_override", steps: []overrideStep{
			{
				overrides:     new([]cloudformation.Parameter{y}),
				wantEnv:       envOf("y", "x"),
				wantOverrides: []cloudformation.Parameter{y},
			},
		}},
		{name: "empty_list_reverts", steps: []overrideStep{
			{
				overrides:     new([]cloudformation.Parameter{y}),
				wantEnv:       envOf("y", "x"),
				wantOverrides: []cloudformation.Parameter{y},
			},
			{overrides: new([]cloudformation.Parameter{}), wantEnv: envOf("x", "x")},
		}},
		{name: "unspecified_keeps", steps: []overrideStep{
			{
				overrides:     new([]cloudformation.Parameter{y}),
				wantEnv:       envOf("y", "x"),
				wantOverrides: []cloudformation.Parameter{y},
			},
			{wantEnv: envOf("y", "x"), wantOverrides: []cloudformation.Parameter{y}},
		}},
		{name: "use_previous_value", steps: []overrideStep{
			{
				overrides:     new([]cloudformation.Parameter{y}),
				wantEnv:       envOf("y", "x"),
				wantOverrides: []cloudformation.Parameter{y},
			},
			{
				overrides:     new([]cloudformation.Parameter{{ParameterKey: "Env", UsePreviousValue: true}}),
				wantEnv:       envOf("y", "x"),
				wantOverrides: []cloudformation.Parameter{y},
			},
		}},
		{name: "set_update_keeps_override", steps: []overrideStep{
			{
				overrides:     new([]cloudformation.Parameter{y}),
				wantEnv:       envOf("y", "x"),
				wantOverrides: []cloudformation.Parameter{y},
			},
			{setParams: envParam("z"), wantEnv: envOf("y", "z"), wantOverrides: []cloudformation.Parameter{y}},
		}},
		{name: "unknown_parameter", steps: []overrideStep{
			{
				overrides: new([]cloudformation.Parameter{{ParameterKey: "Nope", ParameterValue: "1"}}),
				wantErr:   cloudformation.ErrInvalidParameterOverrides,
				wantEnv:   envOf("x", "x"),
			},
		}},
		{name: "value_and_use_previous", steps: []overrideStep{
			{
				overrides: new([]cloudformation.Parameter{
					{ParameterKey: "Env", ParameterValue: "y", UsePreviousValue: true},
				}),
				wantErr: cloudformation.ErrInvalidParameterOverrides,
				wantEnv: envOf("x", "x"),
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newUpdateFixture(t, []string{a1, a2}, []string{updRegion1})

			for i, step := range tt.steps {
				if step.setParams != nil {
					_, _, err := b.UpdateStackSet(
						updSetName,
						"",
						"",
						cloudformation.StackSetOptions{Parameters: step.setParams},
					)
					require.NoError(t, err, "step %d", i)
				} else {
					var opts []cloudformation.StackSetOpOption
					if step.overrides != nil {
						opts = append(opts, cloudformation.WithParameterOverrides(*step.overrides))
					}

					_, err := b.UpdateStackInstances(updSetName, []string{a1}, nil, []string{updRegion1}, "", opts...)
					if step.wantErr != nil {
						require.ErrorIs(t, err, step.wantErr, "step %d", i)
					} else {
						require.NoError(t, err, "step %d", i)
					}
				}

				b.WaitForStackSetOperations()

				for acct, want := range step.wantEnv {
					assert.Equal(t, want, stackEnv(instanceStack(t, b, acct)), "step %d Env of %s", i, acct)
				}

				inst, err := b.DescribeStackInstance(updSetName, a1, updRegion1)
				require.NoError(t, err)
				assert.ElementsMatch(t, step.wantOverrides, inst.ParameterOverrides, "step %d overrides", i)
			}
		})
	}
}

func TestUpdateStackInstances_FailedUpdateKeepsOverrides(t *testing.T) {
	t.Parallel()

	b := newUpdateFixture(t, []string{"111111111111"}, []string{updRegion1})

	_, _, err := b.UpdateStackSet(
		updSetName,
		"",
		updTemplateV1,
		cloudformation.StackSetOptions{Parameters: envParam("x")},
	)
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	_, err = b.UpdateStackInstances(updSetName, []string{"111111111111"}, nil, []string{updRegion1}, "",
		cloudformation.WithParameterOverrides(envParam("y")))
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	_, opID, err := b.UpdateStackSet(updSetName, "", updTemplateStrict, cloudformation.StackSetOptions{})
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	op, err := b.DescribeStackSetOperation(updSetName, opID)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", op.Status)

	inst, err := b.DescribeStackInstance(updSetName, "111111111111", updRegion1)
	require.NoError(t, err)
	assert.Equal(t, "OUTDATED", inst.Status)
	assert.NotEmpty(t, inst.StatusReason)
	assert.Equal(t, opID, inst.LastOperationID)
	assert.Equal(t, envParam("y"), inst.ParameterOverrides)
}

func TestUpdateStackSet_FailureAccounting(t *testing.T) {
	t.Parallel()

	accounts := []string{"111111111111", "222222222222", "333333333333"}

	tests := []struct {
		prefs       *cloudformation.OperationPreferences
		name        string
		wantOp      string
		wantResults []string
		wantStatus  []string
		wantFailed  int
	}{
		{
			name: "zero_tolerance_halts", prefs: nil, wantOp: "FAILED",
			wantResults: []string{"SUCCEEDED", "FAILED", "CANCELLED"},
			wantStatus:  []string{"CURRENT", "OUTDATED", "OUTDATED"}, wantFailed: 1,
		},
		{
			name: "tolerance_count", prefs: &cloudformation.OperationPreferences{FailureToleranceCount: int32p(1)},
			wantOp:      "SUCCEEDED",
			wantResults: []string{"SUCCEEDED", "FAILED", "SUCCEEDED"},
			wantStatus:  []string{"CURRENT", "OUTDATED", "CURRENT"}, wantFailed: 1,
		},
		{
			name:        "tolerance_percentage",
			prefs:       &cloudformation.OperationPreferences{FailureTolerancePercentage: int32p(50)},
			wantOp:      "SUCCEEDED",
			wantResults: []string{"SUCCEEDED", "FAILED", "SUCCEEDED"},
			wantStatus:  []string{"CURRENT", "OUTDATED", "CURRENT"},
			wantFailed:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newUpdateFixture(t, accounts, []string{updRegion1})
			_, err := b.UpdateStackInstances(updSetName, []string{accounts[1]}, nil, []string{updRegion1}, "",
				cloudformation.WithParameterOverrides(envParam("y")))
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			opts := []cloudformation.StackSetOpOption{cloudformation.WithOperationPreferences(tt.prefs)}
			_, opID, err := b.UpdateStackSet(
				updSetName,
				"",
				updTemplateStrict,
				cloudformation.StackSetOptions{},
				opts...)
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			op, err := b.DescribeStackSetOperation(updSetName, opID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantOp, op.Status)
			assert.Equal(t, tt.wantFailed, op.FailedCount)

			assert.Equal(t, tt.wantResults, opResultStatuses(t, b, updSetName, opID))

			for i, acct := range accounts {
				inst, ierr := b.DescribeStackInstance(updSetName, acct, updRegion1)
				require.NoError(t, ierr)
				assert.Equal(t, tt.wantStatus[i], inst.Status, "status of %s", acct)
			}
		})
	}
}

func TestUpdateStackSet_RegionOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantRegion map[string]string
		name       string
		order      []string
	}{
		{
			name: "default_order", order: nil,
			wantRegion: map[string]string{updRegion1: "SUCCEEDED", updRegion2: "FAILED"},
		},
		{
			name: "failing_region_first", order: []string{updRegion2, updRegion1},
			wantRegion: map[string]string{updRegion1: "CANCELLED", updRegion2: "FAILED"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			acct := "111111111111"
			b := newUpdateFixture(t, []string{acct}, []string{updRegion1, updRegion2})

			_, err := b.UpdateStackInstances(updSetName, []string{acct}, nil, []string{updRegion2}, "",
				cloudformation.WithParameterOverrides(envParam("y")))
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			prefs := &cloudformation.OperationPreferences{RegionOrder: tt.order}
			_, opID, err := b.UpdateStackSet(updSetName, "", updTemplateStrict, cloudformation.StackSetOptions{},
				cloudformation.WithOperationPreferences(prefs))
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			results, err := b.ListStackSetOperationResults(updSetName, opID, 0, "", nil)
			require.NoError(t, err)

			got := map[string]string{}
			for _, r := range results.Data {
				got[r.Region] = r.Status
			}

			assert.Equal(t, tt.wantRegion, got)
		})
	}
}

func TestStackSetOperations_OperationID(t *testing.T) {
	t.Parallel()

	const acct = "111111111111"

	tests := []struct {
		run  func(t *testing.T, b *cloudformation.InMemoryBackend)
		name string
	}{
		{name: "replay_returns_same_operation", run: func(t *testing.T, b *cloudformation.InMemoryBackend) {
			t.Helper()

			id := cloudformation.WithOperationID("op-1")
			first, err := b.UpdateStackInstances(updSetName, []string{acct}, nil, []string{updRegion1}, "", id)
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			second, err := b.UpdateStackInstances(updSetName, []string{acct}, nil, []string{updRegion1}, "", id)
			require.NoError(t, err)
			assert.Equal(t, "op-1", first)
			assert.Equal(t, first, second)

			ops, err := b.ListStackSetOperations(updSetName, 0, "")
			require.NoError(t, err)
			assert.Len(t, ops.Data, 2, "create plus one update")
		}},
		{name: "reuse_by_different_request_rejected", run: func(t *testing.T, b *cloudformation.InMemoryBackend) {
			t.Helper()

			id := cloudformation.WithOperationID("op-2")
			_, _, err := b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{}, id)
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			_, _, err = b.UpdateStackSet(updSetName, "", updTemplateStrict, cloudformation.StackSetOptions{}, id)
			require.ErrorIs(t, err, cloudformation.ErrOperationIDAlreadyExists)

			ss, err := b.DescribeStackSet(updSetName)
			require.NoError(t, err)
			assert.JSONEq(t, updTemplateV2, ss.TemplateBody, "rejected request must not change the set")
		}},
		{name: "update_set_replay_does_not_reapply", run: func(t *testing.T, b *cloudformation.InMemoryBackend) {
			t.Helper()

			id := cloudformation.WithOperationID("op-3")
			_, first, err := b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{}, id)
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			_, second, err := b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{}, id)
			require.NoError(t, err)
			assert.Equal(t, first, second)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newUpdateFixture(t, []string{acct}, []string{updRegion1}))
		})
	}
}

func TestStackSetOperations_Errors(t *testing.T) {
	t.Parallel()

	const acct = "111111111111"

	tests := []struct {
		call    func(b *cloudformation.InMemoryBackend) error
		wantErr error
		name    string
	}{
		{name: "update_instances_unknown_instance", wantErr: cloudformation.ErrStackInstanceNotFound,
			call: func(b *cloudformation.InMemoryBackend) error {
				_, err := b.UpdateStackInstances(updSetName, []string{"999999999999"}, nil, []string{updRegion1}, "")

				return err
			}},
		{name: "update_instances_unknown_set", wantErr: cloudformation.ErrStackSetNotFound,
			call: func(b *cloudformation.InMemoryBackend) error {
				_, err := b.UpdateStackInstances("nope", []string{acct}, nil, []string{updRegion1}, "")

				return err
			}},
		{name: "update_set_accounts_without_regions", wantErr: cloudformation.ErrInvalidOperation,
			call: func(b *cloudformation.InMemoryBackend) error {
				_, _, err := b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{},
					cloudformation.WithInstanceTargets([]string{acct}, nil, nil, ""))

				return err
			}},
		{name: "update_set_unknown_instance", wantErr: cloudformation.ErrStackInstanceNotFound,
			call: func(b *cloudformation.InMemoryBackend) error {
				_, _, err := b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{},
					cloudformation.WithInstanceTargets([]string{acct}, nil, []string{updRegion2}, ""))

				return err
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newUpdateFixture(t, []string{acct}, []string{updRegion1}))
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestStackSetOperations_ConcurrentAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantAdmit   int32
		wantBlocked int32
		managed     bool
	}{
		{name: "unmanaged_rejects_concurrent", managed: false, wantAdmit: 1, wantBlocked: 7},
		{name: "managed_queues_all", managed: true, wantAdmit: 8, wantBlocked: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newBackend()
				b.SetStackSetBatchDelay(time.Minute)

				_, err := b.CreateStackSet(updSetName, "", updTemplateV1, cloudformation.StackSetOptions{
					ManagedExecution: &cloudformation.ManagedExecution{Active: tt.managed},
				})
				require.NoError(t, err)

				var admitted, blocked, other atomic.Int32

				var wg sync.WaitGroup

				for range 8 {
					wg.Go(func() {
						_, cerr := b.CreateStackInstances(
							t.Context(),
							updSetName,
							[]string{"111111111111", "222222222222"},
							nil,
							[]string{updRegion1},
							"",
						)

						switch {
						case cerr == nil:
							admitted.Add(1)
						case errors.Is(cerr, cloudformation.ErrOperationInProgress):
							blocked.Add(1)
						default:
							other.Add(1)
						}
					})
				}

				wg.Wait()
				assert.Equal(t, tt.wantAdmit, admitted.Load())
				assert.Equal(t, tt.wantBlocked, blocked.Load())
				assert.Zero(t, other.Load())

				time.Sleep(time.Hour)
				synctest.Wait()
				b.WaitForStackSetOperations()
			})
		})
	}
}

func TestUpdateStackInstances_PersistsOverridesAndOperationID(t *testing.T) {
	t.Parallel()

	const acct = "111111111111"

	b := newUpdateFixture(t, []string{acct}, []string{updRegion1})
	opID, err := b.UpdateStackInstances(updSetName, []string{acct}, nil, []string{updRegion1}, "",
		cloudformation.WithParameterOverrides(envParam("y")), cloudformation.WithOperationID("persist-op"))
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	fresh := newBackend()
	require.NoError(t, fresh.Restore(t.Context(), b.Snapshot(t.Context())))

	inst, err := fresh.DescribeStackInstance(updSetName, acct, updRegion1)
	require.NoError(t, err)
	assert.Equal(t, envParam("y"), inst.ParameterOverrides)

	replay, err := fresh.UpdateStackInstances(updSetName, []string{acct}, nil, []string{updRegion1}, "",
		cloudformation.WithParameterOverrides(envParam("y")), cloudformation.WithOperationID("persist-op"))
	require.NoError(t, err)
	assert.Equal(t, opID, replay)

	_, err = fresh.UpdateStackInstances(updSetName, []string{acct}, nil, []string{updRegion1}, "",
		cloudformation.WithParameterOverrides(envParam("z")), cloudformation.WithOperationID("persist-op"))
	require.ErrorIs(t, err, cloudformation.ErrOperationIDAlreadyExists)
}

func TestRealClient_StackSetUpdateWire(t *testing.T) {
	t.Parallel()

	const acct = "111111111111"

	b := newUpdateFixture(t, []string{acct}, []string{updRegion1})
	client := newTestClientForBackend(t, b)
	ctx := t.Context()

	upd, err := client.UpdateStackInstances(ctx, &cfnsdk.UpdateStackInstancesInput{
		StackSetName:       aws.String(updSetName),
		Accounts:           []string{acct},
		Regions:            []string{updRegion1},
		OperationId:        aws.String("wire-op"),
		ParameterOverrides: []types.Parameter{{ParameterKey: aws.String("Env"), ParameterValue: aws.String("z")}},
	})
	require.NoError(t, err)
	assert.Equal(t, "wire-op", aws.ToString(upd.OperationId))

	desc, err := client.DescribeStackInstance(ctx, &cfnsdk.DescribeStackInstanceInput{
		StackSetName: aws.String(
			updSetName,
		),
		StackInstanceAccount: aws.String(acct),
		StackInstanceRegion:  aws.String(updRegion1),
	})
	require.NoError(t, err)
	require.Len(t, desc.StackInstance.ParameterOverrides, 1)
	assert.Equal(t, "Env", aws.ToString(desc.StackInstance.ParameterOverrides[0].ParameterKey))
	assert.Equal(t, "z", aws.ToString(desc.StackInstance.ParameterOverrides[0].ParameterValue))
	assert.Equal(t, "wire-op", aws.ToString(desc.StackInstance.LastOperationId))
	assert.Equal(t, types.StackInstanceDetailedStatusSucceeded, desc.StackInstance.StackInstanceStatus.DetailedStatus)

	setOut, err := client.UpdateStackSet(ctx, &cfnsdk.UpdateStackSetInput{
		StackSetName: aws.String(updSetName), TemplateBody: aws.String(updTemplateV2),
		Accounts: []string{acct}, Regions: []string{updRegion1}, OperationId: aws.String("wire-set-op"),
	})
	require.NoError(t, err)
	assert.Equal(t, "wire-set-op", aws.ToString(setOut.OperationId))
	assert.JSONEq(t, updTemplateV2, instanceStack(t, b, acct).TemplateBody)

	errCases := []struct {
		call func() error
		want any
		name string
	}{
		{name: "operation_id_reuse", want: new(*types.OperationIdAlreadyExistsException), call: func() error {
			_, e := client.UpdateStackSet(ctx, &cfnsdk.UpdateStackSetInput{
				StackSetName: aws.String(
					updSetName,
				),
				TemplateBody: aws.String(updTemplateStrict),
				OperationId:  aws.String("wire-set-op"),
			})

			return e
		}},
		{name: "instance_not_found", want: new(*types.StackInstanceNotFoundException), call: func() error {
			_, e := client.UpdateStackInstances(ctx, &cfnsdk.UpdateStackInstancesInput{
				StackSetName: aws.String(updSetName), Accounts: []string{"999999999999"}, Regions: []string{updRegion1},
			})

			return e
		}},
		{name: "invalid_operation", want: new(*types.InvalidOperationException), call: func() error {
			_, e := client.UpdateStackSet(ctx, &cfnsdk.UpdateStackSetInput{
				StackSetName: aws.String(updSetName), TemplateBody: aws.String(updTemplateV2), Accounts: []string{acct},
			})

			return e
		}},
	}

	for _, tc := range errCases {
		err = tc.call()
		require.Error(t, err, tc.name)
		assert.ErrorAs(t, err, tc.want, tc.name)
	}
}
