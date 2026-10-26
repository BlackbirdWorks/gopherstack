package cleanrooms_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

type exportEnv struct {
	backend *cleanrooms.InMemoryBackend
	client  *cleanroomssdk.Client
	memID   string
	queryID string
}

func exportResultConfig() *crtypes.AnalysisLogExportResultConfiguration {
	return &crtypes.AnalysisLogExportResultConfiguration{
		OutputConfiguration: &crtypes.AnalysisLogExportOutputConfiguration{
			S3: &crtypes.AnalysisLogExportS3OutputConfiguration{
				Bucket:    aws.String("logs-bucket"),
				KeyPrefix: aws.String("p/"),
			},
		},
	}
}

func newExportEnv(t *testing.T, abilities []crtypes.MemberAbility, delay time.Duration) *exportEnv {
	t.Helper()

	backend := cleanrooms.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	backend.SetProtectedWorkDelay(delay)
	client := newRoundTripClient(t, cleanrooms.NewHandler(backend))
	ctx := t.Context()

	collab, err := client.CreateCollaboration(ctx, &cleanroomssdk.CreateCollaborationInput{
		Name: aws.String("collab"), CreatorDisplayName: aws.String("creator"),
		CreatorMemberAbilities: abilities, Members: []crtypes.MemberSpecification{},
		QueryLogStatus: crtypes.CollaborationQueryLogStatusDisabled,
	})
	require.NoError(t, err)

	memID := aws.ToString(collab.Collaboration.MembershipId)
	q, err := client.StartProtectedQuery(ctx, &cleanroomssdk.StartProtectedQueryInput{
		MembershipIdentifier: aws.String(memID), Type: crtypes.ProtectedQueryTypeSql,
		SqlParameters: &crtypes.ProtectedQuerySQLParameters{QueryString: aws.String("SELECT 1")},
	})
	require.NoError(t, err)

	return &exportEnv{backend: backend, client: client, memID: memID, queryID: aws.ToString(q.ProtectedQuery.Id)}
}

func (e *exportEnv) start(ctx context.Context, queryID string) (*cleanroomssdk.StartAnalysisLogExportOutput, error) {
	return e.client.StartAnalysisLogExport(ctx, &cleanroomssdk.StartAnalysisLogExportInput{
		MembershipIdentifier: aws.String(e.memID), AnalysisId: aws.String(queryID),
		AnalysisType: crtypes.LogExportAnalysisTypeProtectedQuery, ResultConfiguration: exportResultConfig(),
	})
}

func exportAbilities() []crtypes.MemberAbility {
	return []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery, crtypes.MemberAbilityCanExportQueryAnalysisLog}
}

func TestAnalysisLogExport_Lifecycle(t *testing.T) {
	t.Parallel()

	env := newExportEnv(t, exportAbilities(), 0)
	ctx := t.Context()

	started, err := env.start(ctx, env.queryID)
	require.NoError(t, err)

	exp := started.AnalysisLogExport
	require.NotNil(t, exp)
	assert.NotEmpty(t, aws.ToString(exp.AnalysisLogExportId))
	assert.Equal(t, env.queryID, aws.ToString(exp.AnalysisId))
	assert.Equal(t, env.memID, aws.ToString(exp.MembershipId))
	assert.Equal(t, crtypes.LogExportAnalysisTypeProtectedQuery, exp.AnalysisType)
	assert.Equal(t, crtypes.AnalysisLogExportStatusInProgress, exp.Status)
	assert.Equal(t, "logs-bucket", aws.ToString(exp.ResultConfiguration.OutputConfiguration.S3.Bucket))
	assert.Equal(t, "p/", aws.ToString(exp.ResultConfiguration.OutputConfiguration.S3.KeyPrefix))
	require.NotNil(t, exp.CreateTime)
	require.NotNil(t, exp.UpdateTime)

	got, err := env.client.GetAnalysisLogExport(ctx, &cleanroomssdk.GetAnalysisLogExportInput{
		MembershipIdentifier: aws.String(env.memID), AnalysisLogExportIdentifier: exp.AnalysisLogExportId,
	})
	require.NoError(t, err)
	assert.Equal(t, crtypes.AnalysisLogExportStatusSuccess, got.AnalysisLogExport.Status)
	assert.Nil(t, got.AnalysisLogExport.Error)

	listed, err := env.client.ListAnalysisLogExports(ctx, &cleanroomssdk.ListAnalysisLogExportsInput{
		MembershipIdentifier: aws.String(env.memID),
	})
	require.NoError(t, err)
	require.Len(t, listed.AnalysisLogExports, 1)
	assert.Equal(t, exp.AnalysisLogExportId, listed.AnalysisLogExports[0].AnalysisLogExportId)
	assert.Equal(t, crtypes.AnalysisLogExportStatusSuccess, listed.AnalysisLogExports[0].Status)
}

func TestAnalysisLogExport_ListFilters(t *testing.T) {
	t.Parallel()

	env := newExportEnv(t, exportAbilities(), 0)
	ctx := t.Context()

	for range 3 {
		_, err := env.start(ctx, env.queryID)
		require.NoError(t, err)
	}

	tests := []struct {
		name     string
		analysis string
		status   crtypes.AnalysisLogExportStatus
		want     int
	}{
		{name: "all", want: 3},
		{name: "by analysis", analysis: env.queryID, want: 3},
		{name: "other analysis", analysis: "00000000-0000-0000-0000-000000000000", want: 0},
		{name: "status success", status: crtypes.AnalysisLogExportStatusSuccess, want: 3},
		{name: "status failed", status: crtypes.AnalysisLogExportStatusFailed, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &cleanroomssdk.ListAnalysisLogExportsInput{
				MembershipIdentifier: aws.String(env.memID),
				Status:               tc.status,
			}
			if tc.analysis != "" {
				in.AnalysisIdentifier = aws.String(tc.analysis)
			}

			out, err := env.client.ListAnalysisLogExports(ctx, in)
			require.NoError(t, err)
			assert.Len(t, out.AnalysisLogExports, tc.want)
		})
	}

	var seen int

	p := cleanroomssdk.NewListAnalysisLogExportsPaginator(env.client, &cleanroomssdk.ListAnalysisLogExportsInput{
		MembershipIdentifier: aws.String(env.memID), MaxResults: aws.Int32(2),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		require.NoError(t, err)

		seen += len(page.AnalysisLogExports)
	}

	assert.Equal(t, 3, seen)
}

func TestAnalysisLogExport_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run       func(ctx context.Context, e *exportEnv) error
		name      string
		code      string
		abilities []crtypes.MemberAbility
		delay     time.Duration
	}{
		{
			name: "start without ability", code: "AccessDeniedException",
			abilities: []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
			run: func(ctx context.Context, e *exportEnv) error {
				_, err := e.start(ctx, e.queryID)

				return err
			},
		},
		{
			name: "start unknown membership", code: "ResourceNotFoundException", abilities: exportAbilities(),
			run: func(ctx context.Context, e *exportEnv) error {
				e.memID = "00000000-0000-0000-0000-000000000000"
				_, err := e.start(ctx, e.queryID)

				return err
			},
		},
		{
			name: "start unknown query", code: "ResourceNotFoundException", abilities: exportAbilities(),
			run: func(ctx context.Context, e *exportEnv) error {
				_, err := e.start(ctx, "00000000-0000-0000-0000-000000000000")

				return err
			},
		},
		{
			name: "start running query", code: "ConflictException", abilities: exportAbilities(), delay: time.Hour,
			run: func(ctx context.Context, e *exportEnv) error {
				_, err := e.start(ctx, e.queryID)

				return err
			},
		},
		{
			name: "start without bucket", code: "ValidationException", abilities: exportAbilities(),
			run: func(ctx context.Context, e *exportEnv) error {
				_, err := e.client.StartAnalysisLogExport(ctx, &cleanroomssdk.StartAnalysisLogExportInput{
					MembershipIdentifier: aws.String(e.memID), AnalysisId: aws.String(e.queryID),
					AnalysisType: crtypes.LogExportAnalysisTypeProtectedQuery,
					ResultConfiguration: &crtypes.AnalysisLogExportResultConfiguration{
						OutputConfiguration: &crtypes.AnalysisLogExportOutputConfiguration{
							S3: &crtypes.AnalysisLogExportS3OutputConfiguration{Bucket: aws.String("")},
						},
					},
				})

				return err
			},
		},
		{
			name: "get unknown export", code: "ResourceNotFoundException", abilities: exportAbilities(),
			run: func(ctx context.Context, e *exportEnv) error {
				_, err := e.client.GetAnalysisLogExport(ctx, &cleanroomssdk.GetAnalysisLogExportInput{
					MembershipIdentifier:        aws.String(e.memID),
					AnalysisLogExportIdentifier: aws.String("00000000-0000-0000-0000-000000000000"),
				})

				return err
			},
		},
		{
			name: "list unknown membership", code: "ResourceNotFoundException", abilities: exportAbilities(),
			run: func(ctx context.Context, e *exportEnv) error {
				_, err := e.client.ListAnalysisLogExports(ctx, &cleanroomssdk.ListAnalysisLogExportsInput{
					MembershipIdentifier: aws.String("00000000-0000-0000-0000-000000000000"),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newExportEnv(t, tc.abilities, tc.delay)

			err := tc.run(t.Context(), env)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.code)
		})
	}
}

func TestAnalysisLogExport_Persistence(t *testing.T) {
	t.Parallel()

	env := newExportEnv(t, exportAbilities(), 0)
	started, err := env.start(t.Context(), env.queryID)
	require.NoError(t, err)

	restored := cleanrooms.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	require.NoError(t, restored.Restore(t.Context(), env.backend.Snapshot(t.Context())))
	client := newRoundTripClient(t, cleanrooms.NewHandler(restored))

	got, err := client.GetAnalysisLogExport(t.Context(), &cleanroomssdk.GetAnalysisLogExportInput{
		MembershipIdentifier:        aws.String(env.memID),
		AnalysisLogExportIdentifier: started.AnalysisLogExport.AnalysisLogExportId,
	})
	require.NoError(t, err)
	assert.Equal(t, env.queryID, aws.ToString(got.AnalysisLogExport.AnalysisId))
	assert.Equal(
		t,
		"logs-bucket",
		aws.ToString(got.AnalysisLogExport.ResultConfiguration.OutputConfiguration.S3.Bucket),
	)
}
