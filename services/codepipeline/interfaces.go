package codepipeline

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// CodeBuildStarter is the subset of CodeBuild operations CodePipeline needs
// to run a Build/CodeBuild action, wired via SetCodeBuildBackend. When
// unset, Build actions complete instantly with no cross-service call,
// matching this backend's original behavior.
type CodeBuildStarter interface {
	// StartBuild starts a build for the named project in the region ctx carries.
	// An error (e.g. the project does not exist) fails the pipeline action.
	StartBuild(ctx context.Context, projectName string) error
}

// LambdaInvoker is the subset of Lambda operations CodePipeline needs to run
// an Invoke/Lambda action, wired via SetLambdaBackend. When unset, Invoke
// actions complete instantly with no cross-service call, matching this
// backend's original behavior. Same shape as the LambdaInvoker interface
// repeated across sns, stepfunctions/asl, eventbridge, etc. --
// lambda.InMemoryBackend's InvokeFunction (invocationType aliased to string)
// satisfies it directly with no adapter.
type LambdaInvoker interface {
	InvokeFunction(
		ctx context.Context,
		name, invocationType string,
		payload []byte,
	) ([]byte, int, error)
}

// CodeDeployStarter is the subset of CodeDeploy operations CodePipeline
// needs to run a Deploy/CodeDeploy action, wired via SetCodeDeployBackend.
// When unset, Deploy actions complete instantly with no cross-service call,
// matching this backend's original behavior.
type CodeDeployStarter interface {
	// CreateDeployment starts a deployment in the region ctx carries. An error
	// (e.g. either does not exist) fails the pipeline action.
	CreateDeployment(ctx context.Context, applicationName, deploymentGroupName string) error
}

// BuildRequest is what a Build/CodeBuild action hands CodeBuild.
type BuildRequest struct {
	ProjectName string
	// SourceLocation is "bucket/key" of the primary input artifact, empty when none is staged.
	SourceLocation    string
	BuildspecOverride string
}

// CodeBuildRunner is the CodeBuildStarter extension that lets a Build action
// wait on the build's final status. A starter that also implements it is run
// asynchronously: the action stays InProgress until BuildStatus is terminal.
type CodeBuildRunner interface {
	CodeBuildStarter
	// RunBuild starts a build and returns its ID.
	RunBuild(ctx context.Context, req BuildRequest) (string, error)
	// BuildStatus returns the build's current status (e.g. IN_PROGRESS, SUCCEEDED, FAILED, STOPPED).
	BuildStatus(ctx context.Context, buildID string) (string, error)
}

// ArtifactS3 is the S3 surface Source/S3 and Deploy/S3 actions and the
// pipeline artifact store need, wired via SetArtifactS3.
type ArtifactS3 interface {
	GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}
