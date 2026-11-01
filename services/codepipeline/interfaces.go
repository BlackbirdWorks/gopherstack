package codepipeline

import (
	"context"
	"errors"
	"time"

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

// StackState is a CloudFormation stack as the CloudFormation deploy action observes it.
type StackState struct {
	Outputs map[string]string
	Status  string
	Reason  string
}

// ChangeSetState is a CloudFormation change set as the CloudFormation deploy action observes it.
type ChangeSetState struct {
	Status string
	Reason string
}

// StackRequest is what a CloudFormation deploy action hands CloudFormation to create or update a stack.
type StackRequest struct {
	Parameters   map[string]string
	Tags         map[string]string
	StackName    string
	TemplateBody string
	StackPolicy  string
	RoleARN      string
	Capabilities []string
}

// StackDeployer is the CloudFormation surface the Deploy/CloudFormation action needs, wired via SetStackDeployer.
// Absent stacks and change sets are reported as nil with no error.
type StackDeployer interface {
	DescribeStack(ctx context.Context, name string) (*StackState, error)
	CreateStack(ctx context.Context, req StackRequest) error
	// UpdateStack returns ErrNoStackUpdates when the template and parameters match the deployed stack.
	UpdateStack(ctx context.Context, req StackRequest) error
	DeleteStack(ctx context.Context, name string) error
	CreateChangeSet(ctx context.Context, req StackRequest, changeSetName string, forNewStack bool) error
	DescribeChangeSet(ctx context.Context, stackName, changeSetName string) (*ChangeSetState, error)
	DeleteChangeSet(ctx context.Context, stackName, changeSetName string) error
	ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) error
}

// ECSDeploymentState is one ECS service deployment as the Deploy/ECS action observes it.
type ECSDeploymentState struct {
	TaskDefinition     string
	Status             string
	RolloutState       string
	RolloutStateReason string
	DesiredCount       int
	RunningCount       int
	FailedTasks        int
}

// ECSServiceState is an ECS service as the Deploy/ECS action observes it.
type ECSServiceState struct {
	TaskDefinition string
	Deployments    []ECSDeploymentState
}

// ECSDeployer is the ECS surface the Deploy/ECS action needs, wired via SetECSDeployer.
type ECSDeployer interface {
	// DescribeService returns nil with no error when the service does not exist.
	DescribeService(ctx context.Context, cluster, service string) (*ECSServiceState, error)
	// RegisterRevisionWithImages registers a new revision of baseTaskDefinition with each named container's image
	// replaced, returning the new revision's ARN. It fails when a named container is not in the task definition.
	RegisterRevisionWithImages(ctx context.Context, baseTaskDefinition string, images map[string]string) (string, error)
	UpdateServiceTaskDefinition(ctx context.Context, cluster, service, taskDefinition string) error
}

// ImageInfo is an ECR image as the Source/ECR action observes it.
type ImageInfo struct {
	PushedAt       time.Time
	RegistryID     string
	RepositoryName string
	RepositoryURI  string
	Digest         string
	Tags           []string
	SizeInBytes    int64
}

// ImageSource is the ECR surface the Source/ECR action needs, wired via SetImageSource.
type ImageSource interface {
	// DescribeImage returns the image tagged tag, or an error wrapping awserr.ErrNotFound when it does not exist.
	DescribeImage(ctx context.Context, repository, tag string) (*ImageInfo, error)
}

// RepositorySnapshot is the head of a CodeCommit branch with its files.
type RepositorySnapshot struct {
	Files         map[string][]byte
	CommitID      string
	CommitMessage string
}

// RepositoryReader is the CodeCommit surface the Source/CodeCommit action needs, wired via SetRepositoryReader.
type RepositoryReader interface {
	// Snapshot returns the branch head, or an error wrapping awserr.ErrNotFound for a missing repository or branch.
	Snapshot(ctx context.Context, repository, branch string) (*RepositorySnapshot, error)
}

// ErrNoStackUpdates is returned by StackDeployer.UpdateStack when there is nothing to update.
var ErrNoStackUpdates = errors.New("no updates are to be performed")
