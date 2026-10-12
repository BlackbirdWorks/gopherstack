package main

import (
	"archive/zip"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	cptypes "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
)

func pipelineSiteZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	for name, body := range map[string]string{"index.html": "<h1>hi</h1>", "css/site.css": "body{}"} {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func s3Declaration(
	stage, name, category, provider string,
	cfg map[string]string,
	in, out string,
) cptypes.StageDeclaration {
	a := cptypes.ActionDeclaration{
		Name: aws.String(name),
		ActionTypeId: &cptypes.ActionTypeId{
			Category: cptypes.ActionCategory(category), Owner: cptypes.ActionOwnerAws,
			Provider: aws.String(provider), Version: aws.String("1"),
		},
		Configuration: cfg,
	}
	if in != "" {
		a.InputArtifacts = []cptypes.InputArtifact{{Name: aws.String(in)}}
	}

	if out != "" {
		a.OutputArtifacts = []cptypes.OutputArtifact{{Name: aws.String(out)}}
	}

	return cptypes.StageDeclaration{Name: aws.String(stage), Actions: []cptypes.ActionDeclaration{a}}
}

func TestCodePipelineS3CodeBuildPipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantActions  map[string]cptypes.ActionExecutionStatus
		name         string
		wantExec     cptypes.PipelineExecutionStatus
		wantErrCode  string
		wantErrMsg   string
		putSource    bool
		stopBuild    bool
		wantDeployed bool
		plainCopy    bool
	}{
		{
			name: "success", putSource: true, wantExec: cptypes.PipelineExecutionStatusSucceeded,
			wantActions: map[string]cptypes.ActionExecutionStatus{
				"Source": cptypes.ActionExecutionStatusSucceeded,
				"Build":  cptypes.ActionExecutionStatusSucceeded,
				"Deploy": cptypes.ActionExecutionStatusSucceeded,
			},
			wantDeployed: true,
		},
		{
			name: "plain copy", putSource: true, plainCopy: true, wantExec: cptypes.PipelineExecutionStatusSucceeded,
			wantActions: map[string]cptypes.ActionExecutionStatus{
				"Source": cptypes.ActionExecutionStatusSucceeded,
				"Build":  cptypes.ActionExecutionStatusSucceeded,
				"Deploy": cptypes.ActionExecutionStatusSucceeded,
			},
			wantDeployed: true,
		},
		{
			name: "failed build", putSource: true, stopBuild: true, wantExec: cptypes.PipelineExecutionStatusFailed,
			wantActions: map[string]cptypes.ActionExecutionStatus{
				"Source": cptypes.ActionExecutionStatusSucceeded,
				"Build":  cptypes.ActionExecutionStatusFailed,
			},
			wantErrCode: "JobFailed", wantErrMsg: "STOPPED",
		},
		{
			name: "missing source", wantExec: cptypes.PipelineExecutionStatusFailed,
			wantActions: map[string]cptypes.ActionExecutionStatus{
				"Source": cptypes.ActionExecutionStatusFailed,
			},
			wantErrCode: "ConfigurationError", wantErrMsg: "does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			ctx := t.Context()
			s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
			cbc := codebuild.NewFromConfig(fx.cfg)
			cpc := codepipeline.NewFromConfig(fx.cfg)

			for _, b := range []string{"src-bkt", "art-bkt", "site-bkt"} {
				_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)})
				require.NoError(t, err)
			}

			if tt.putSource {
				_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String("src-bkt"), Key: aws.String("app.zip"),
					Body: bytes.NewReader(pipelineSiteZip(t)),
				})
				require.NoError(t, err)
			}

			_, err := cbc.CreateProject(ctx, &codebuild.CreateProjectInput{
				Name:        aws.String("bp"),
				ServiceRole: aws.String("arn:aws:iam::000000000000:role/cb"),
				Source:      &cbtypes.ProjectSource{Type: cbtypes.SourceTypeCodepipeline},
				Artifacts:   &cbtypes.ProjectArtifacts{Type: cbtypes.ArtifactsTypeCodepipeline},
				Environment: &cbtypes.ProjectEnvironment{
					Type: cbtypes.EnvironmentTypeLinuxContainer, ComputeType: cbtypes.ComputeTypeBuildGeneral1Small,
					Image: aws.String("aws/codebuild/standard:7.0"),
				},
			})
			require.NoError(t, err)

			deployCfg := map[string]string{"BucketName": "site-bkt", "Extract": "true", "ObjectKey": "site"}
			wantKeys := []string{"site/index.html", "site/css/site.css"}

			if tt.plainCopy {
				deployCfg = map[string]string{"BucketName": "site-bkt", "Extract": "false", "ObjectKey": "bundle.zip"}
				wantKeys = []string{"bundle.zip"}
			}

			_, err = cpc.CreatePipeline(ctx, &codepipeline.CreatePipelineInput{Pipeline: &cptypes.PipelineDeclaration{
				Name:    aws.String("site"),
				RoleArn: aws.String("arn:aws:iam::000000000000:role/cp"),
				ArtifactStore: &cptypes.ArtifactStore{
					Type: cptypes.ArtifactStoreTypeS3, Location: aws.String("art-bkt"),
				},
				Stages: []cptypes.StageDeclaration{
					s3Declaration(
						"Source",
						"Source",
						"Source",
						"S3",
						map[string]string{
							"S3Bucket":             "src-bkt",
							"S3ObjectKey":          "app.zip",
							"PollForSourceChanges": "false",
						},
						"",
						"SrcOut",
					),
					s3Declaration("Build", "Build", "Build", "CodeBuild",
						map[string]string{"ProjectName": "bp"}, "SrcOut", "BuildOut"),
					s3Declaration("Deploy", "Deploy", "Deploy", "S3",
						deployCfg, "BuildOut", ""),
				},
			}})
			require.NoError(t, err)

			started, err := cpc.StartPipelineExecution(
				ctx,
				&codepipeline.StartPipelineExecutionInput{Name: aws.String("site")},
			)
			require.NoError(t, err)

			execID := started.PipelineExecutionId

			actions := func() map[string]cptypes.ActionExecutionDetail {
				out, lErr := cpc.ListActionExecutions(ctx, &codepipeline.ListActionExecutionsInput{
					PipelineName: aws.String(
						"site",
					),
					Filter: &cptypes.ActionExecutionFilter{PipelineExecutionId: execID},
				})
				require.NoError(t, lErr)

				m := map[string]cptypes.ActionExecutionDetail{}
				for _, d := range out.ActionExecutionDetails {
					m[aws.ToString(d.ActionName)] = d
				}

				return m
			}

			if tt.putSource {
				build, ok := actions()["Build"]
				require.True(t, ok)
				assert.Equal(
					t,
					cptypes.ActionExecutionStatusInProgress,
					build.Status,
					"build action waits for the build",
				)
				require.NotNil(t, build.Output)
				buildID := aws.ToString(build.Output.ExecutionResult.ExternalExecutionId)
				require.NotEmpty(t, buildID)

				got, gErr := cbc.BatchGetBuilds(ctx, &codebuild.BatchGetBuildsInput{Ids: []string{buildID}})
				require.NoError(t, gErr)
				require.Len(t, got.Builds, 1)
				assert.Contains(t, aws.ToString(got.Builds[0].Source.Location), "art-bkt/site/SrcOut/")

				if tt.stopBuild {
					_, err = cbc.StopBuild(ctx, &codebuild.StopBuildInput{Id: aws.String(buildID)})
					require.NoError(t, err)
				} else {
					cbH, _ := serviceByName(fx.services)["CodeBuild"].(*codebuildbackend.Handler)
					codebuildbackend.NewJanitor(cbH.Backend, 0, 0).SweepOnce(ctx)
				}
			}

			require.Eventually(t, func() bool {
				out, gErr := cpc.GetPipelineExecution(ctx, &codepipeline.GetPipelineExecutionInput{
					PipelineName: aws.String("site"), PipelineExecutionId: execID,
				})

				return gErr == nil && out.PipelineExecution.Status == tt.wantExec
			}, 10*time.Second, 50*time.Millisecond)

			got := actions()
			require.Len(t, got, len(tt.wantActions))

			for name, want := range tt.wantActions {
				assert.Equal(t, want, got[name].Status, name)
			}

			if tt.wantErrCode != "" {
				failed := got["Source"]
				if tt.putSource {
					failed = got["Build"]
				}

				require.NotNil(t, failed.Output)
				require.NotNil(t, failed.Output.ExecutionResult)
				require.NotNil(t, failed.Output.ExecutionResult.ErrorDetails)
				assert.Equal(t, tt.wantErrCode, aws.ToString(failed.Output.ExecutionResult.ErrorDetails.Code))
				assert.Contains(t, aws.ToString(failed.Output.ExecutionResult.ErrorDetails.Message), tt.wantErrMsg)
			}

			for _, key := range wantKeys {
				obj, gErr := s3c.GetObject(
					ctx,
					&s3.GetObjectInput{Bucket: aws.String("site-bkt"), Key: aws.String(key)},
				)
				if !tt.wantDeployed {
					require.Error(t, gErr, key)

					continue
				}

				require.NoError(t, gErr, key)

				body, _ := io.ReadAll(obj.Body)
				assert.NotEmpty(t, body)
			}
		})
	}
}
