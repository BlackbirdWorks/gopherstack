package emr_test

import (
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

func TestCancelSteps_StepCancellationOption_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		option  emrtypes.StepCancellationOption
		wantErr bool
	}{
		{name: "unset"},
		{name: "send_interrupt", option: emrtypes.StepCancellationOptionSendInterrupt},
		{name: "terminate_process", option: emrtypes.StepCancellationOptionTerminateProcess},
		{name: "unknown", option: emrtypes.StepCancellationOption("KILL"), wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEMRClient(t, emr.NewHandler(emr.NewInMemoryBackend(testAccountID, testRegion)))
			ctx := t.Context()

			run, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
				Name:      awssdk.String("cancel-opt"),
				Instances: &emrtypes.JobFlowInstancesConfig{},
			})
			require.NoError(t, err)

			added, err := client.AddJobFlowSteps(ctx, &emrsdk.AddJobFlowStepsInput{
				JobFlowId: run.JobFlowId,
				Steps: []emrtypes.StepConfig{{
					Name:          awssdk.String("s"),
					HadoopJarStep: &emrtypes.HadoopJarStepConfig{Jar: awssdk.String("s3://b/j.jar")},
				}},
			})
			require.NoError(t, err)

			out, err := client.CancelSteps(ctx, &emrsdk.CancelStepsInput{
				ClusterId:              run.JobFlowId,
				StepIds:                added.StepIds,
				StepCancellationOption: tc.option,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Len(t, out.CancelStepsInfoList, 1)
			assert.Equal(t, added.StepIds[0], awssdk.ToString(out.CancelStepsInfoList[0].StepId))
		})
	}
}
