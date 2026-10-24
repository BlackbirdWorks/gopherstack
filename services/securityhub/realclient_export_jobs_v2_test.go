package securityhub_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	types "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

const (
	exportBucket = "findings-export"
	exportKMSArn = "arn:aws:kms:us-east-1:000000000000:key/1234abcd-12ab-34cd-56ef-1234567890ab"
)

type exportSiblings struct{ s3 *s3backend.S3Handler }

func (s exportSiblings) GetS3Handler() service.Registerable { return s.s3 }

type exportRig struct {
	backend *securityhub.InMemoryBackend
	client  *securityhubsdk.Client
	s3      *s3backend.S3Handler
}

func newExportRig(t *testing.T) exportRig {
	t.Helper()

	sib := exportSiblings{s3: s3backend.NewHandler(s3backend.NewInMemoryBackend(nil))}
	_, err := sib.s3.Backend.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(exportBucket)})
	require.NoError(t, err)

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	backend.SetAppConfig(sib)
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err = client.EnableSecurityHub(
		t.Context(),
		&securityhubsdk.EnableSecurityHubInput{EnableDefaultStandards: aws.Bool(false)},
	)
	require.NoError(t, err)

	for _, f := range []struct{ id, title, label string }{{"f-1", "alpha", "HIGH"}, {"f-2", "beta", "LOW"}} {
		out, importErr := client.BatchImportFindings(t.Context(), &securityhubsdk.BatchImportFindingsInput{
			Findings: []types.AwsSecurityFinding{{
				SchemaVersion: aws.String("2018-10-08"),
				Id:            aws.String(f.id),
				ProductArn:    aws.String("arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default"),
				GeneratorId:   aws.String("g"),
				AwsAccountId:  aws.String("000000000000"),
				Types:         []string{"Software and Configuration Checks"},
				CreatedAt:     aws.String("2026-01-01T00:00:00.000Z"),
				UpdatedAt:     aws.String("2026-01-01T00:00:00.000Z"),
				Severity:      &types.Severity{Label: types.SeverityLabel(f.label)},
				Title:         aws.String(f.title),
				Description:   aws.String("d"),
				Resources:     []types.Resource{{Type: aws.String("Other"), Id: aws.String("r")}},
			}},
		})
		require.NoError(t, importErr)
		require.Zero(t, aws.ToInt32(out.FailedCount))
	}

	return exportRig{backend: backend, client: client, s3: sib.s3}
}

func exportDest(bucket string) *types.ExportDestinationMemberS3 {
	return &types.ExportDestinationMemberS3{Value: types.S3ExportDestination{
		BucketArn: aws.String(
			"arn:aws:s3:::" + bucket,
		),
		KmsKeyArn:    aws.String(exportKMSArn),
		ObjectPrefix: aws.String("out"),
	}}
}

func selectable(fields []string) []types.FindingsSelectableField {
	out := make([]types.FindingsSelectableField, 0, len(fields))
	for _, f := range fields {
		out = append(out, types.FindingsSelectableField(f))
	}

	return out
}

func csvOutput(fields ...string) *types.ExportOutputMemberFindings {
	return &types.ExportOutputMemberFindings{Value: types.FindingsOutput{
		Format: types.FindingsExportFormatCsv, SelectedFields: selectable(fields),
		Filters: &types.OcsfFindingFilters{CompositeFilters: []types.CompositeFilter{{
			StringFilters: []types.OcsfStringFilter{
				{
					FieldName: types.OcsfStringFieldFindingInfoTitle,
					Filter: &types.StringFilter{
						Comparison: types.StringFilterComparisonEquals,
						Value:      aws.String("alpha"),
					},
				},
			},
		}}},
	}}
}

func readExportObject(t *testing.T, rig exportRig, key string) string {
	t.Helper()

	out, err := rig.s3.Backend.GetObject(
		t.Context(),
		&awss3.GetObjectInput{Bucket: aws.String(exportBucket), Key: aws.String(key)},
	)
	require.NoError(t, err)

	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return string(data)
}

func TestRealClient_ExportJobsV2(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, rig exportRig)
		name string
	}{
		{name: "csv export writes filtered findings to s3", run: func(t *testing.T, rig exportRig) {
			t.Helper()

			ctx := t.Context()
			started, err := rig.client.StartExportJobV2(ctx, &securityhubsdk.StartExportJobV2Input{
				Name: aws.String("daily"), Destination: exportDest(exportBucket),
				OutputConfiguration: csvOutput("finding_info.title", "severity"),
			})
			require.NoError(t, err)
			id := aws.ToString(started.ExportJobId)
			require.NotEmpty(t, id)

			got, err := rig.client.GetExportJobV2(
				ctx,
				&securityhubsdk.GetExportJobV2Input{ExportJobId: started.ExportJobId},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ExportStatusSucceeded, got.Status)
			assert.Equal(t, types.ExportDataTypeFindings, got.DataType)
			assert.Equal(t, "daily", aws.ToString(got.Name))
			assert.NotNil(t, got.StartedAt)
			assert.NotNil(t, got.EndedAt)

			dest, ok := got.Destination.(*types.ExportDestinationMemberS3)
			require.True(t, ok)
			assert.Equal(t, "arn:aws:s3:::"+exportBucket, aws.ToString(dest.Value.BucketArn))
			assert.Equal(t, "out", aws.ToString(dest.Value.ObjectPrefix))

			out, ok := got.OutputConfiguration.(*types.ExportOutputMemberFindings)
			require.True(t, ok)
			assert.Equal(t, types.FindingsExportFormatCsv, out.Value.Format)
			assert.Equal(t, selectable([]string{"finding_info.title", "severity"}), out.Value.SelectedFields)
			require.NotNil(t, out.Value.Filters)
			assert.Len(t, out.Value.Filters.CompositeFilters, 1)

			assert.Equal(
				t,
				"finding_info.title,severity\nalpha,HIGH\n",
				readExportObject(t, rig, "out/"+id+"/findings.csv"),
			)
		}},
		{name: "ocsf json export writes every finding", run: func(t *testing.T, rig exportRig) {
			t.Helper()

			started, err := rig.client.StartExportJobV2(t.Context(), &securityhubsdk.StartExportJobV2Input{
				Destination: exportDest(exportBucket),
				OutputConfiguration: &types.ExportOutputMemberFindings{Value: types.FindingsOutput{
					Format: types.FindingsExportFormatOcsfJson,
				}},
			})
			require.NoError(t, err)

			body := readExportObject(t, rig, "out/"+aws.ToString(started.ExportJobId)+"/findings.json")
			assert.Len(t, strings.Split(strings.TrimSpace(body), "\n"), 2)
		}},
		{name: "missing bucket fails the job", run: func(t *testing.T, rig exportRig) {
			t.Helper()

			started, err := rig.client.StartExportJobV2(t.Context(), &securityhubsdk.StartExportJobV2Input{
				Destination: exportDest("no-such-bucket"), OutputConfiguration: csvOutput("severity"),
			})
			require.NoError(t, err)

			got, err := rig.client.GetExportJobV2(
				t.Context(),
				&securityhubsdk.GetExportJobV2Input{ExportJobId: started.ExportJobId},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ExportStatusFailed, got.Status)
			assert.Equal(t, types.ExportFailureCodeResourceNotFound, got.FailureCode)
			assert.Contains(t, aws.ToString(got.FailureMessage), "no-such-bucket")
		}},
		{name: "client token replays", run: func(t *testing.T, rig exportRig) {
			t.Helper()

			in := &securityhubsdk.StartExportJobV2Input{
				ClientToken: aws.String(
					"tok-1",
				),
				Destination:         exportDest(exportBucket),
				OutputConfiguration: csvOutput("severity"),
			}
			first, err := rig.client.StartExportJobV2(t.Context(), in)
			require.NoError(t, err)

			second, err := rig.client.StartExportJobV2(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.ExportJobId), aws.ToString(second.ExportJobId))

			list, err := rig.client.ListExportJobsV2(t.Context(), &securityhubsdk.ListExportJobsV2Input{})
			require.NoError(t, err)
			assert.Len(t, list.Items, 1)
		}},
		{name: "list filters and paginates", run: func(t *testing.T, rig exportRig) {
			t.Helper()

			ctx := t.Context()
			for range 3 {
				_, err := rig.client.StartExportJobV2(ctx, &securityhubsdk.StartExportJobV2Input{
					Destination: exportDest(exportBucket), OutputConfiguration: csvOutput("severity"),
				})
				require.NoError(t, err)
			}
			_, err := rig.client.StartExportJobV2(ctx, &securityhubsdk.StartExportJobV2Input{
				Destination: exportDest("no-such-bucket"), OutputConfiguration: csvOutput("severity"),
			})
			require.NoError(t, err)

			failed, err := rig.client.ListExportJobsV2(
				ctx,
				&securityhubsdk.ListExportJobsV2Input{Status: types.ExportStatusFailed},
			)
			require.NoError(t, err)
			assert.Len(t, failed.Items, 1)

			seen := map[string]bool{}
			var token *string
			pages := 0
			for {
				page, listErr := rig.client.ListExportJobsV2(ctx, &securityhubsdk.ListExportJobsV2Input{
					DataType: types.ExportDataTypeFindings, MaxResults: aws.Int32(3), NextToken: token,
				})
				require.NoError(t, listErr)

				for _, item := range page.Items {
					seen[aws.ToString(item.ExportJobId)] = true
					sum, ok := item.OutputConfiguration.(*types.ExportOutputSummaryMemberFindings)
					require.True(t, ok)
					assert.Equal(t, types.FindingsExportFormatCsv, sum.Value.Format)
				}

				pages++
				if token = page.NextToken; token == nil {
					break
				}
			}

			assert.Len(t, seen, 4)
			assert.Equal(t, 2, pages)
		}},
		{name: "cancel", run: func(t *testing.T, rig exportRig) {
			t.Helper()

			ctx := t.Context()
			done, err := rig.client.StartExportJobV2(ctx, &securityhubsdk.StartExportJobV2Input{
				Destination: exportDest(exportBucket), OutputConfiguration: csvOutput("severity"),
			})
			require.NoError(t, err)

			_, err = rig.client.CancelExportJobV2(
				ctx,
				&securityhubsdk.CancelExportJobV2Input{ExportJobId: done.ExportJobId},
			)
			var conflict *types.ConflictException
			require.ErrorAs(t, err, &conflict)

			_, err = rig.client.CancelExportJobV2(
				ctx,
				&securityhubsdk.CancelExportJobV2Input{ExportJobId: aws.String("nope")},
			)
			var missing *types.ResourceNotFoundException
			require.ErrorAs(t, err, &missing)
		}},
		{name: "get unknown job", run: func(t *testing.T, rig exportRig) {
			t.Helper()

			_, err := rig.client.GetExportJobV2(
				t.Context(),
				&securityhubsdk.GetExportJobV2Input{ExportJobId: aws.String("nope")},
			)
			var missing *types.ResourceNotFoundException
			require.ErrorAs(t, err, &missing)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.run(t, newExportRig(t))
		})
	}
}

func TestRealClient_StartExportJobV2Validation(t *testing.T) {
	t.Parallel()

	jsonOut := func(fields ...string) *types.ExportOutputMemberFindings {
		return &types.ExportOutputMemberFindings{Value: types.FindingsOutput{
			Format: types.FindingsExportFormatOcsfJson, SelectedFields: selectable(fields),
		}}
	}
	dest := func(bucket, key string) *types.ExportDestinationMemberS3 {
		return &types.ExportDestinationMemberS3{Value: types.S3ExportDestination{
			BucketArn: aws.String(bucket), KmsKeyArn: aws.String(key),
		}}
	}

	cases := []struct {
		dest    types.ExportDestination
		out     types.ExportOutput
		name    string
		wantMsg string
	}{
		{
			name:    "csv needs fields",
			dest:    exportDest(exportBucket),
			out:     csvOutput(),
			wantMsg: "SelectedFields is required for CSV",
		},
		{
			name:    "json forbids fields",
			dest:    exportDest(exportBucket),
			out:     jsonOut("severity"),
			wantMsg: "SelectedFields is not supported for OCSF_JSON",
		},
		{
			name:    "too many fields",
			dest:    exportDest(exportBucket),
			out:     csvOutput(manyFields(51)...),
			wantMsg: "at most 50",
		},
		{
			name:    "unknown field",
			dest:    exportDest(exportBucket),
			out:     csvOutput("no.such.field"),
			wantMsg: "unsupported SelectedFields entry",
		},
		{
			name:    "bucket arn",
			dest:    dest("not-an-arn", exportKMSArn),
			out:     jsonOut(),
			wantMsg: "BucketArn must be an S3 bucket ARN",
		},
		{
			name:    "kms key id rejected",
			dest:    dest("arn:aws:s3:::b", "1234abcd"),
			out:     jsonOut(),
			wantMsg: "KmsKeyArn must be a full single-Region KMS key ARN",
		},
		{
			name: "multi region key", dest: dest("arn:aws:s3:::b", "arn:aws:kms:us-east-1:000000000000:key/mrk-1234"),
			out: jsonOut(), wantMsg: "KmsKeyArn must be a full single-Region KMS key ARN",
		},
		{
			name: "other region key", dest: dest("arn:aws:s3:::b", "arn:aws:kms:eu-west-1:000000000000:key/1234abcd"),
			out: jsonOut(), wantMsg: "KmsKeyArn must be in region us-east-1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rig := newExportRig(t)
			_, err := rig.client.StartExportJobV2(t.Context(), &securityhubsdk.StartExportJobV2Input{
				Destination: tc.dest, OutputConfiguration: tc.out,
			})

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
			assert.Contains(t, ve.ErrorMessage(), tc.wantMsg)

			list, listErr := rig.client.ListExportJobsV2(t.Context(), &securityhubsdk.ListExportJobsV2Input{})
			require.NoError(t, listErr)
			assert.Empty(t, list.Items, "a rejected request creates no job")
		})
	}
}

func manyFields(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "severity"
	}

	return out
}

func TestExportJobsV2_SnapshotRestore(t *testing.T) {
	t.Parallel()

	rig := newExportRig(t)
	started, err := rig.client.StartExportJobV2(t.Context(), &securityhubsdk.StartExportJobV2Input{
		Name: aws.String(
			"persisted",
		),
		Destination:         exportDest(exportBucket),
		OutputConfiguration: csvOutput("severity"),
	})
	require.NoError(t, err)

	fresh := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), rig.backend.Snapshot(t.Context())))

	client := newTestSecurityHubClient(t, securityhub.NewHandler(fresh))
	got, err := client.GetExportJobV2(
		t.Context(),
		&securityhubsdk.GetExportJobV2Input{ExportJobId: started.ExportJobId},
	)
	require.NoError(t, err)
	assert.Equal(t, "persisted", aws.ToString(got.Name))
	assert.Equal(t, types.ExportStatusSucceeded, got.Status)
}
