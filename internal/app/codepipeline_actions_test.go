package app

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	cptypes "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cfnPipelineTemplate = `{"Parameters":{"Env":{"Type":"String","Default":"dev"}},` +
	`"Resources":{"Topic":{"Type":"AWS::SNS::Topic"}},` +
	`"Outputs":{"EnvOut":{"Value":{"Ref":"Env"}}}}`

func pipelineZip(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, body := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

type actionPipelineEnv struct {
	fx  *sfnFixture
	s3c *s3.Client
	cpc *codepipeline.Client
}

func newActionPipelineEnv(t *testing.T) *actionPipelineEnv {
	t.Helper()

	fx := newSFNFixture(t)
	env := &actionPipelineEnv{
		fx:  fx,
		s3c: s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }),
		cpc: codepipeline.NewFromConfig(fx.cfg),
	}

	for _, b := range []string{"src-bkt", "art-bkt", "out-bkt"} {
		_, err := env.s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(b)})
		require.NoError(t, err)
	}

	return env
}

func (e *actionPipelineEnv) putSource(t *testing.T, files map[string]string) {
	t.Helper()

	_, err := e.s3c.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("src-bkt"), Key: aws.String("app.zip"), Body: bytes.NewReader(pipelineZip(t, files)),
	})
	require.NoError(t, err)
}

func (e *actionPipelineEnv) createPipeline(t *testing.T, name string, stages ...cptypes.StageDeclaration) error {
	t.Helper()

	_, err := e.cpc.CreatePipeline(
		t.Context(),
		&codepipeline.CreatePipelineInput{Pipeline: &cptypes.PipelineDeclaration{
			Name:    aws.String(name),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/cp"),
			ArtifactStore: &cptypes.ArtifactStore{
				Type: cptypes.ArtifactStoreTypeS3, Location: aws.String("art-bkt"),
			},
			Stages: stages,
		}},
	)

	return err
}

func s3SourceStage(out string) cptypes.StageDeclaration {
	return s3Declaration("Source", "Source", "Source", "S3",
		map[string]string{"S3Bucket": "src-bkt", "S3ObjectKey": "app.zip", "PollForSourceChanges": "false"}, "", out)
}

// run starts the pipeline and waits for a terminal execution status.
func (e *actionPipelineEnv) run(
	t *testing.T,
	name string,
) (cptypes.PipelineExecutionStatus, map[string]cptypes.ActionExecutionDetail) {
	t.Helper()

	ctx := t.Context()

	started, err := e.cpc.StartPipelineExecution(ctx, &codepipeline.StartPipelineExecutionInput{Name: aws.String(name)})
	require.NoError(t, err)

	var status cptypes.PipelineExecutionStatus

	require.Eventually(t, func() bool {
		out, gErr := e.cpc.GetPipelineExecution(ctx, &codepipeline.GetPipelineExecutionInput{
			PipelineName: aws.String(name), PipelineExecutionId: started.PipelineExecutionId,
		})
		if gErr != nil {
			return false
		}

		status = out.PipelineExecution.Status

		return status != cptypes.PipelineExecutionStatusInProgress
	}, 15*time.Second, 50*time.Millisecond)

	list, err := e.cpc.ListActionExecutions(ctx, &codepipeline.ListActionExecutionsInput{
		PipelineName: aws.String(name),
		Filter:       &cptypes.ActionExecutionFilter{PipelineExecutionId: started.PipelineExecutionId},
	})
	require.NoError(t, err)

	actions := map[string]cptypes.ActionExecutionDetail{}
	for _, d := range list.ActionExecutionDetails {
		actions[aws.ToString(d.ActionName)] = d
	}

	return status, actions
}

func (e *actionPipelineEnv) object(t *testing.T, key string) string {
	t.Helper()

	out, err := e.s3c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String("out-bkt"), Key: aws.String(key)})
	require.NoError(t, err)

	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return string(body)
}

func extractStage(in string) cptypes.StageDeclaration {
	return s3Declaration("Publish", "Publish", "Deploy", "S3",
		map[string]string{"BucketName": "out-bkt", "Extract": "true", "ObjectKey": "out"}, in, "")
}

func errorMessage(d cptypes.ActionExecutionDetail) string {
	if d.Output == nil || d.Output.ExecutionResult == nil || d.Output.ExecutionResult.ErrorDetails == nil {
		return ""
	}

	return aws.ToString(d.Output.ExecutionResult.ErrorDetails.Message)
}

func TestCodePipelineCloudFormationDeploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		files       map[string]string
		cfg         map[string]string
		name        string
		wantOutput  string
		wantErr     string
		wantStatus  cptypes.PipelineExecutionStatus
		wantStack   bool
		wantDeleted bool
		seedStack   bool
	}{
		{
			name:  "create update writes outputs",
			files: map[string]string{"template.json": cfnPipelineTemplate, "params.json": `{"env":"prod"}`},
			cfg: map[string]string{
				"ActionMode": "CREATE_UPDATE", "StackName": "cp-stack", "TemplatePath": "SrcOut::template.json",
				"OutputFileName": "StackOutputs.json", "Capabilities": "CAPABILITY_IAM",
				"ParameterOverrides": `{"Env":{"Fn::GetParam":["SrcOut","params.json","env"]}}`,
			},
			wantStatus: cptypes.PipelineExecutionStatusSucceeded,
			wantOutput: `{"EnvOut":"prod"}`,
			wantStack:  true,
		},
		{
			name: "template configuration parameters",
			files: map[string]string{
				"template.json": cfnPipelineTemplate,
				"conf.json":     `{"Parameters":{"Env":"staging"}}`,
			},
			cfg: map[string]string{
				"ActionMode": "CREATE_UPDATE", "StackName": "cp-stack", "TemplatePath": "SrcOut::template.json",
				"TemplateConfiguration": "SrcOut::conf.json", "OutputFileName": "StackOutputs.json",
			},
			wantStatus: cptypes.PipelineExecutionStatusSucceeded,
			wantOutput: `{"EnvOut":"staging"}`,
			wantStack:  true,
		},
		{
			name:  "invalid template fails with reason",
			files: map[string]string{"template.json": `{"Resources":{"X":{"Type":"AWS::Fake::Nothing"}}}`},
			cfg: map[string]string{
				"ActionMode": "CREATE_UPDATE", "StackName": "cp-stack", "TemplatePath": "SrcOut::template.json",
			},
			wantStatus: cptypes.PipelineExecutionStatusFailed,
			wantErr:    "Fake",
		},
		{
			name:  "missing template file fails",
			files: map[string]string{"other.json": `{}`},
			cfg: map[string]string{
				"ActionMode": "CREATE_UPDATE", "StackName": "cp-stack", "TemplatePath": "SrcOut::template.json",
			},
			wantStatus: cptypes.PipelineExecutionStatusFailed,
			wantErr:    "template.json",
		},
		{
			name:  "delete only removes stack",
			files: map[string]string{"template.json": cfnPipelineTemplate},
			cfg: map[string]string{
				"ActionMode": "DELETE_ONLY", "StackName": "cp-stack",
			},
			seedStack:   true,
			wantStatus:  cptypes.PipelineExecutionStatusSucceeded,
			wantDeleted: true,
		},
		{
			name:  "delete only absent stack succeeds",
			files: map[string]string{"template.json": cfnPipelineTemplate},
			cfg: map[string]string{
				"ActionMode": "DELETE_ONLY", "StackName": "cp-stack",
			},
			wantStatus:  cptypes.PipelineExecutionStatusSucceeded,
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newActionPipelineEnv(t)
			cfn := cloudformation.NewFromConfig(env.fx.cfg)
			env.putSource(t, tt.files)

			if tt.seedStack {
				_, err := cfn.CreateStack(t.Context(), &cloudformation.CreateStackInput{
					StackName: aws.String("cp-stack"), TemplateBody: aws.String(cfnPipelineTemplate),
				})
				require.NoError(t, err)
			}

			deploy := s3Declaration("Deploy", "Deploy", "Deploy", "CloudFormation", tt.cfg, "SrcOut", "CfnOut")
			stages := []cptypes.StageDeclaration{s3SourceStage("SrcOut"), deploy}

			if tt.wantOutput != "" {
				stages = append(stages, extractStage("CfnOut"))
			} else {
				stages[1].Actions[0].OutputArtifacts = nil
			}

			require.NoError(t, env.createPipeline(t, "cfn", stages...))

			status, actions := env.run(t, "cfn")
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantErr != "" {
				assert.Equal(t, cptypes.ActionExecutionStatusFailed, actions["Deploy"].Status)
				assert.Contains(t, errorMessage(actions["Deploy"]), tt.wantErr)
			}

			if tt.wantStack {
				out, err := cfn.DescribeStacks(
					t.Context(),
					&cloudformation.DescribeStacksInput{StackName: aws.String("cp-stack")},
				)
				require.NoError(t, err)
				require.Len(t, out.Stacks, 1)
				assert.Equal(t, "CREATE_COMPLETE", string(out.Stacks[0].StackStatus))
				assert.Equal(t, tt.wantOutput, env.object(t, "out/StackOutputs.json"))
				assert.Equal(
					t,
					tt.wantOutput == `{"EnvOut":"prod"}`,
					actions["Deploy"].Output.OutputVariables["EnvOut"] == "prod",
				)
			}

			if tt.wantDeleted {
				ls, err := cfn.ListStacks(t.Context(), &cloudformation.ListStacksInput{})
				require.NoError(t, err)

				for _, s := range ls.StackSummaries {
					assert.NotEqual(t, "CREATE_COMPLETE", string(s.StackStatus), "stack must not stay live")
				}

				if tt.seedStack {
					require.NotEmpty(t, ls.StackSummaries)
					assert.Equal(t, "DELETE_COMPLETE", string(ls.StackSummaries[0].StackStatus))
				}
			}
		})
	}
}

func TestCodePipelineCloudFormationChangeSets(t *testing.T) {
	t.Parallel()

	env := newActionPipelineEnv(t)
	cfn := cloudformation.NewFromConfig(env.fx.cfg)
	env.putSource(t, map[string]string{"template.json": cfnPipelineTemplate})

	base := map[string]string{"StackName": "cs-stack", "ChangeSetName": "cs", "TemplatePath": "SrcOut::template.json"}
	replace := mergeStringMaps(base, map[string]string{"ActionMode": "CHANGE_SET_REPLACE"})
	execute := mergeStringMaps(base, map[string]string{"ActionMode": "CHANGE_SET_EXECUTE", "OutputFileName": "o.json"})

	require.NoError(t, env.createPipeline(t, "cs",
		s3SourceStage("SrcOut"),
		s3Declaration("Plan", "Plan", "Deploy", "CloudFormation", replace, "SrcOut", ""),
		s3Declaration("Apply", "Apply", "Deploy", "CloudFormation", execute, "SrcOut", "CfnOut"),
		extractStage("CfnOut"),
	))

	status, actions := env.run(t, "cs")
	assert.Equal(t, cptypes.PipelineExecutionStatusSucceeded, status)
	assert.Equal(t, cptypes.ActionExecutionStatusSucceeded, actions["Apply"].Status)
	assert.JSONEq(t, `{"EnvOut":"dev"}`, env.object(t, "out/o.json"))

	out, err := cfn.DescribeStacks(t.Context(), &cloudformation.DescribeStacksInput{StackName: aws.String("cs-stack")})
	require.NoError(t, err)
	require.Len(t, out.Stacks, 1)
	assert.Equal(t, "CREATE_COMPLETE", string(out.Stacks[0].StackStatus))
}

func mergeStringMaps(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	maps.Copy(out, a)
	maps.Copy(out, b)

	return out
}

func TestCodePipelineECSDeploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		defs       string
		wantStatus cptypes.PipelineExecutionStatus
		wantErr    string
		wantImage  string
	}{
		{
			name:       "new revision with new image",
			defs:       `[{"name":"web","imageUri":"nginx:2.0"}]`,
			wantStatus: cptypes.PipelineExecutionStatusSucceeded,
			wantImage:  "nginx:2.0",
		},
		{
			name:       "unknown container fails",
			defs:       `[{"name":"nope","imageUri":"nginx:2.0"}]`,
			wantStatus: cptypes.PipelineExecutionStatusFailed,
			wantErr:    "not in task definition",
		},
		{
			name:       "malformed image definitions fail",
			defs:       `{"name":"web"}`,
			wantStatus: cptypes.PipelineExecutionStatusFailed,
			wantErr:    "image definitions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newActionPipelineEnv(t)
			ecsc := ecs.NewFromConfig(env.fx.cfg)
			ctx := t.Context()

			_, err := ecsc.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("c1")})
			require.NoError(t, err)

			_, err = ecsc.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
				Family: aws.String("app"),
				ContainerDefinitions: []ecstypes.ContainerDefinition{
					{Name: aws.String("web"), Image: aws.String("nginx:1.0"), Memory: aws.Int32(128)},
				},
			})
			require.NoError(t, err)

			_, err = ecsc.CreateService(ctx, &ecs.CreateServiceInput{
				Cluster: aws.String("c1"), ServiceName: aws.String("svc"), TaskDefinition: aws.String("app:1"),
				DesiredCount: aws.Int32(1),
			})
			require.NoError(t, err)

			env.putSource(t, map[string]string{"imagedefinitions.json": tt.defs})

			require.NoError(t, env.createPipeline(t, "ecs",
				s3SourceStage("SrcOut"),
				s3Declaration("Deploy", "Deploy", "Deploy", "ECS",
					map[string]string{"ClusterName": "c1", "ServiceName": "svc"}, "SrcOut", ""),
			))

			status, actions := env.run(t, "ecs")
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantErr != "" {
				assert.Contains(t, errorMessage(actions["Deploy"]), tt.wantErr)

				return
			}

			svc, err := ecsc.DescribeServices(ctx, &ecs.DescribeServicesInput{
				Cluster: aws.String("c1"), Services: []string{"svc"},
			})
			require.NoError(t, err)
			require.Len(t, svc.Services, 1)
			assert.Contains(t, aws.ToString(svc.Services[0].TaskDefinition), "task-definition/app:2")

			td, err := ecsc.DescribeTaskDefinition(
				ctx,
				&ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String("app:2")},
			)
			require.NoError(t, err)
			require.Len(t, td.TaskDefinition.ContainerDefinitions, 1)
			assert.Equal(t, tt.wantImage, aws.ToString(td.TaskDefinition.ContainerDefinitions[0].Image))

			old, err := ecsc.DescribeTaskDefinition(
				ctx,
				&ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String("app:1")},
			)
			require.NoError(t, err)
			assert.Equal(t, "nginx:1.0", aws.ToString(old.TaskDefinition.ContainerDefinitions[0].Image))
		})
	}
}

func TestCodePipelineECRSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tag        string
		wantStatus cptypes.PipelineExecutionStatus
		wantErr    string
	}{
		{name: "default tag latest", tag: "", wantStatus: cptypes.PipelineExecutionStatusSucceeded},
		{name: "explicit tag", tag: "v1", wantStatus: cptypes.PipelineExecutionStatusSucceeded},
		{
			name:       "missing tag fails",
			tag:        "nope",
			wantStatus: cptypes.PipelineExecutionStatusFailed,
			wantErr:    "does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newActionPipelineEnv(t)
			ecrc := ecr.NewFromConfig(env.fx.cfg)
			ctx := t.Context()

			repo, err := ecrc.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String("img")})
			require.NoError(t, err)

			for _, tag := range []string{"latest", "v1"} {
				_, err = ecrc.PutImage(ctx, &ecr.PutImageInput{
					RepositoryName: aws.String(
						"img",
					), ImageTag: aws.String(tag), ImageManifest: aws.String(`{"schemaVersion":2}`),
				})
				require.NoError(t, err)
			}

			cfg := map[string]string{"RepositoryName": "img"}
			if tt.tag != "" {
				cfg["ImageTag"] = tt.tag
			}

			require.NoError(t, env.createPipeline(t, "ecr",
				s3Declaration("Source", "Source", "Source", "ECR", cfg, "", "ImgOut"),
				extractStage("ImgOut"),
			))

			status, actions := env.run(t, "ecr")
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantErr != "" {
				assert.Contains(t, errorMessage(actions["Source"]), tt.wantErr)

				return
			}

			var detail map[string]any
			require.NoError(t, json.Unmarshal([]byte(env.object(t, "out/imageDetail.json")), &detail))

			imgs, err := ecrc.DescribeImages(ctx, &ecr.DescribeImagesInput{
				RepositoryName: aws.String(
					"img",
				), ImageIds: []ecrtypes.ImageIdentifier{{ImageTag: aws.String("latest")}},
			})
			require.NoError(t, err)
			require.NotEmpty(t, imgs.ImageDetails)

			digest := aws.ToString(imgs.ImageDetails[0].ImageDigest)
			assert.Equal(t, digest, detail["ImageDigest"])
			assert.Equal(t, aws.ToString(repo.Repository.RepositoryUri)+"@"+digest, detail["ImageURI"])
			assert.Equal(t, "img", detail["RepositoryName"])
			assert.Equal(t, "1.0", detail["Version"])
			assert.Equal(t, aws.ToString(repo.Repository.RegistryId), detail["RegistryId"])
			assert.Contains(t, detail, "ImagePushedAt")
			assert.Contains(t, detail, "ImageSizeInBytes")
			assert.NotEmpty(t, detail["ImageTags"])
			assert.Equal(t, digest, actions["Source"].Output.OutputVariables["ImageDigest"])
		})
	}
}

func TestCodePipelineCodeCommitSource(t *testing.T) {
	t.Parallel()

	env := newActionPipelineEnv(t)
	ccc := codecommit.NewFromConfig(env.fx.cfg)
	ctx := t.Context()

	_, err := ccc.CreateRepository(ctx, &codecommit.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	put, err := ccc.PutFile(ctx, &codecommit.PutFileInput{
		RepositoryName: aws.String("repo"), BranchName: aws.String("main"), FilePath: aws.String("dir/app.txt"),
		FileContent: []byte("hello"), CommitMessage: aws.String("first"),
	})
	require.NoError(t, err)

	require.NoError(t, env.createPipeline(t, "cc",
		s3Declaration("Source", "Source", "Source", "CodeCommit",
			map[string]string{"RepositoryName": "repo", "BranchName": "main"}, "", "Code"),
		extractStage("Code"),
	))

	status, actions := env.run(t, "cc")
	assert.Equal(t, cptypes.PipelineExecutionStatusSucceeded, status)
	assert.Equal(t, "hello", env.object(t, "out/dir/app.txt"))
	assert.Equal(t, aws.ToString(put.CommitId), actions["Source"].Output.OutputVariables["CommitId"])
}

func TestCodePipelineUnsupportedBuiltinActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		category     string
		provider     string
		version      string
		cfg          map[string]string
		wantCreate   string
		wantExecFail string
	}{
		{
			name:       "unknown provider rejected",
			category:   "Deploy",
			provider:   "Teleport",
			version:    "1",
			wantCreate: "InvalidActionDeclarationException",
		},
		{
			name:       "unknown version rejected",
			category:   "Deploy",
			provider:   "ECS",
			version:    "9",
			wantCreate: "InvalidActionDeclarationException",
		},
		{
			name: "bad cfn action mode rejected", category: "Deploy", provider: "CloudFormation", version: "1",
			cfg: map[string]string{
				"ActionMode": "BOGUS",
				"StackName":  "s",
			}, wantCreate: "InvalidActionDeclarationException",
		},
		{
			name: "valid but unimplemented fails", category: "Deploy", provider: "ElasticBeanstalk", version: "1",
			cfg:          map[string]string{"ApplicationName": "a", "EnvironmentName": "e"},
			wantExecFail: "not supported by this emulator",
		},
		{
			name: "codestar connection fails", category: "Source", provider: "CodeStarSourceConnection", version: "1",
			cfg: map[string]string{
				"ConnectionArn":    "arn:aws:codeconnections:us-east-1:000000000000:connection/abc",
				"FullRepositoryId": "o/r",
				"BranchName":       "main",
			},
			wantExecFail: "third-party repository content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newActionPipelineEnv(t)
			decl := s3Declaration("Stage", "Act", tt.category, tt.provider, tt.cfg, "", "")
			decl.Actions[0].ActionTypeId.Version = aws.String(tt.version)

			err := env.createPipeline(t, "p", decl)
			if tt.wantCreate != "" {
				require.ErrorContains(t, err, tt.wantCreate)

				return
			}

			require.NoError(t, err)

			status, actions := env.run(t, "p")
			assert.Equal(t, cptypes.PipelineExecutionStatusFailed, status)
			assert.Equal(t, cptypes.ActionExecutionStatusFailed, actions["Act"].Status)
			assert.Contains(t, errorMessage(actions["Act"]), tt.wantExecFail)
		})
	}
}
