package omics_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	omicstypes "github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

func TestLifecycleDelay_AsyncResources(t *testing.T) {
	t.Parallel()

	const delay = 10 * time.Second

	tests := []struct {
		run  func(t *testing.T, b *omics.InMemoryBackend)
		name string
	}{
		{
			name: "workflow",
			run: func(t *testing.T, b *omics.InMemoryBackend) {
				t.Helper()

				wf, err := b.CreateWorkflow(omics.CreateWorkflowInput{Name: "w", Engine: "WDL"})
				require.NoError(t, err)

				for range 3 {
					got, gErr := b.GetWorkflow(wf.ID)
					require.NoError(t, gErr)
					assert.Equal(t, "CREATING", got.Status)
				}

				time.Sleep(delay)
				synctest.Wait()

				got, err := b.GetWorkflow(wf.ID)
				require.NoError(t, err)
				assert.Equal(t, "ACTIVE", got.Status)
			},
		},
		{
			name: "run",
			run: func(t *testing.T, b *omics.InMemoryBackend) {
				t.Helper()

				run, err := b.StartRun(
					omics.StartRunInput{WorkflowID: "w", RoleARN: "arn:aws:iam::000000000000:role/r"},
				)
				require.NoError(t, err)

				for _, want := range []string{"PENDING", "STARTING", "RUNNING", "COMPLETED"} {
					got, gErr := b.GetRun(run.ID)
					require.NoError(t, gErr)
					assert.Equal(t, want, got.Status)

					time.Sleep(delay)
					synctest.Wait()
				}

				got, err := b.GetRun(run.ID)
				require.NoError(t, err)
				assert.NotNil(t, got.StartTime)
				assert.NotNil(t, got.StopTime)
			},
		},
		{
			name: "read set import job",
			run: func(t *testing.T, b *omics.InMemoryBackend) {
				t.Helper()

				store, err := b.CreateSequenceStore(omics.CreateSequenceStoreInput{Name: "s"})
				require.NoError(t, err)

				job, err := b.StartReadSetImportJob(store.ID, "arn:aws:iam::000000000000:role/r", nil)
				require.NoError(t, err)
				assert.Equal(t, "SUBMITTED", job.Status)
				assert.Nil(t, job.CompletionTime)

				for _, want := range []string{"SUBMITTED", "IN_PROGRESS", "COMPLETED"} {
					got, gErr := b.GetReadSetImportJob(store.ID, job.ID)
					require.NoError(t, gErr)
					assert.Equal(t, want, got.Status)

					time.Sleep(delay)
					synctest.Wait()
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := omics.NewInMemoryBackend("123456789012", "us-east-1")
				b.SetLifecycleDelay(delay)
				tt.run(t, b)
			})
		})
	}
}

func TestInputValidation_SDK(t *testing.T) {
	t.Parallel()

	client := newTestOmicsClient(t, omics.NewHandler(omics.NewInMemoryBackend("123456789012", "us-east-1")))

	tests := []struct {
		call func() error
		name string
	}{
		{
			name: "unknown engine",
			call: func() error {
				_, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
					Name: aws.String(
						"w",
					),
					Engine:        omicstypes.WorkflowEngine("BOGUS"),
					DefinitionUri: aws.String("s3://b/k"),
				})

				return err
			},
		},
		{
			name: "name too long",
			call: func() error {
				long := make([]byte, 129)
				for i := range long {
					long[i] = 'a'
				}

				_, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
					Name: aws.String(
						string(long),
					),
					Engine:        omicstypes.WorkflowEngineWdl,
					DefinitionUri: aws.String("s3://b/k"),
				})

				return err
			},
		},
		{
			name: "bad networking mode",
			call: func() error {
				_, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
					WorkflowId: aws.String("w"), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
					OutputUri: aws.String("s3://b/o"), NetworkingMode: omicstypes.NetworkingMode("PUBLIC"),
				})

				return err
			},
		},
		{
			name: "bad page token",
			call: func() error {
				_, err := client.ListWorkflows(
					t.Context(),
					&omicssdk.ListWorkflowsInput{StartingToken: aws.String("zzz")},
				)

				return err
			},
		},
		{
			name: "max results too large",
			call: func() error {
				_, err := client.ListRuns(t.Context(), &omicssdk.ListRunsInput{MaxResults: aws.Int32(1000)})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var apiErr smithy.APIError

			require.ErrorAs(t, tt.call(), &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "ValidationException")
		})
	}
}

func TestListWorkflows_OpaqueTokenWalk(t *testing.T) {
	t.Parallel()

	b := omics.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestOmicsClient(t, omics.NewHandler(b))

	for _, n := range []string{"a", "b", "c", "d", "e"} {
		_, err := b.CreateWorkflow(omics.CreateWorkflowInput{Name: n, Engine: "WDL"})
		require.NoError(t, err)
	}

	seen := map[string]bool{}

	var token *string

	for {
		out, err := client.ListWorkflows(t.Context(), &omicssdk.ListWorkflowsInput{
			MaxResults: aws.Int32(2), StartingToken: token,
		})
		require.NoError(t, err)

		for _, w := range out.Items {
			assert.False(t, seen[aws.ToString(w.Id)])
			seen[aws.ToString(w.Id)] = true
		}

		if aws.ToString(out.NextToken) == "" {
			break
		}

		assert.NotContains(t, seen, aws.ToString(out.NextToken), "token must not be a bare resource id")

		token = out.NextToken
	}

	assert.Len(t, seen, 5)
}

func TestNotFoundMessage_NoCodePrefix(t *testing.T) {
	t.Parallel()

	client := newTestOmicsClient(t, omics.NewHandler(omics.NewInMemoryBackend("123456789012", "us-east-1")))

	_, err := client.GetWorkflow(t.Context(), &omicssdk.GetWorkflowInput{Id: aws.String("1234567")})

	var nf *omicstypes.ResourceNotFoundException

	require.ErrorAs(t, err, &nf)
	assert.Equal(t, "workflow 1234567 not found", aws.ToString(nf.Message))
}
