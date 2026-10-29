package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	codecommitbackend "github.com/blackbirdworks/gopherstack/services/codecommit"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

// wireCodePipelineActionBackends gives CodePipeline the ECS, ECR and CodeCommit backends its
// Deploy/ECS, Source/ECR and Source/CodeCommit actions drive.
func wireCodePipelineActionBackends(byName map[string]service.Registerable) {
	cpH, ok := byName["CodePipeline"].(*codepipelinebackend.Handler)
	if !ok {
		return
	}

	if h, hok := byName["ECS"].(*ecsbackend.Handler); hok {
		cpH.Backend.SetECSDeployer(&codepipelineECSDeployer{handler: h})
	}

	if h, hok := byName["ECR"].(*ecrbackend.Handler); hok {
		cpH.Backend.SetImageSource(&codepipelineImageSource{handler: h})
	}

	if h, hok := byName["CodeCommit"].(*codecommitbackend.Handler); hok {
		cpH.Backend.SetRepositoryReader(&codepipelineRepositoryReader{handler: h})
	}
}

// wireCodePipelineCloudFormation gives CodePipeline CloudFormation for Deploy/CloudFormation actions. It runs
// after CloudFormation is initialised, which is later than the other action backends.
func wireCodePipelineCloudFormation(codepipelineReg, cfnReg service.Registerable) {
	cpH, ok := codepipelineReg.(*codepipelinebackend.Handler)
	if !ok {
		return
	}

	if h, hok := cfnReg.(*cfnbackend.Handler); hok {
		cpH.Backend.SetStackDeployer(&codepipelineStackDeployer{handler: h})
	}
}

type codepipelineStackDeployer struct {
	handler *cfnbackend.Handler
}

func (d *codepipelineStackDeployer) backend(ctx context.Context) cfnbackend.StorageBackend {
	return d.handler.BackendFor(awsmeta.Region(ctx))
}

func cpStackParameters(m map[string]string) []cfnbackend.Parameter {
	out := make([]cfnbackend.Parameter, 0, len(m))
	for k, v := range m {
		out = append(out, cfnbackend.Parameter{ParameterKey: k, ParameterValue: v})
	}

	return out
}

func cpStackTags(m map[string]string) []cfnbackend.Tag {
	out := make([]cfnbackend.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, cfnbackend.Tag{Key: k, Value: v})
	}

	return out
}

func (d *codepipelineStackDeployer) DescribeStack(
	ctx context.Context, name string,
) (*codepipelinebackend.StackState, error) {
	bk := d.backend(ctx)

	stack, err := bk.DescribeStack(name)
	if errors.Is(err, cfnbackend.ErrStackNotFound) {
		return nil, nil //nolint:nilnil // absent stack is reported as nil by contract
	}

	if err != nil {
		return nil, err
	}

	st := &codepipelinebackend.StackState{
		Status:  stack.StackStatus,
		Reason:  stack.StackStatusReason,
		Outputs: make(map[string]string, len(stack.Outputs)),
	}

	for _, o := range stack.Outputs {
		st.Outputs[o.OutputKey] = o.OutputValue
	}

	if st.Reason == "" && (strings.Contains(st.Status, "FAILED") || strings.Contains(st.Status, "ROLLBACK")) {
		st.Reason = firstFailedStackEvent(bk, name)
	}

	return st, nil
}

func firstFailedStackEvent(bk cfnbackend.StorageBackend, name string) string {
	events, err := bk.DescribeStackEvents(name, "")
	if err != nil {
		return ""
	}

	for _, e := range events.Data {
		if strings.HasSuffix(e.ResourceStatus, "_FAILED") && e.ResourceStatusReason != "" {
			return fmt.Sprintf("%s (%s): %s", e.LogicalResourceID, e.ResourceType, e.ResourceStatusReason)
		}
	}

	return ""
}

func cpStackOptions(req codepipelinebackend.StackRequest) cfnbackend.StackOptions {
	return cfnbackend.StackOptions{
		Capabilities:    req.Capabilities,
		RoleARN:         req.RoleARN,
		Tags:            cpStackTags(req.Tags),
		StackPolicyBody: req.StackPolicy,
	}
}

func (d *codepipelineStackDeployer) CreateStack(ctx context.Context, req codepipelinebackend.StackRequest) error {
	_, err := d.backend(ctx).CreateStack(ctx, req.StackName, req.TemplateBody, cpStackParameters(req.Parameters),
		cpStackOptions(req))

	return err
}

func (d *codepipelineStackDeployer) UpdateStack(ctx context.Context, req codepipelinebackend.StackRequest) error {
	_, err := d.backend(ctx).UpdateStack(ctx, req.StackName, req.TemplateBody, cpStackParameters(req.Parameters),
		cpStackOptions(req))
	if errors.Is(err, cfnbackend.ErrNoUpdates) {
		return codepipelinebackend.ErrNoStackUpdates
	}

	return err
}

func (d *codepipelineStackDeployer) DeleteStack(ctx context.Context, name string) error {
	return d.backend(ctx).DeleteStack(ctx, name)
}

func (d *codepipelineStackDeployer) CreateChangeSet(
	ctx context.Context, req codepipelinebackend.StackRequest, changeSetName string, forNewStack bool,
) error {
	csType := "UPDATE"
	if forNewStack {
		csType = "CREATE"
	}

	_, err := d.backend(ctx).CreateChangeSet(ctx, req.StackName, changeSetName, req.TemplateBody, "",
		cpStackParameters(req.Parameters), req.Capabilities, cpStackTags(req.Tags),
		cfnbackend.CreateChangeSetOptions{ChangeSetType: csType, RoleARN: req.RoleARN})

	return err
}

func (d *codepipelineStackDeployer) DescribeChangeSet(
	ctx context.Context, stackName, changeSetName string,
) (*codepipelinebackend.ChangeSetState, error) {
	cs, err := d.backend(ctx).DescribeChangeSet(stackName, changeSetName)
	if errors.Is(err, cfnbackend.ErrChangeSetNotFound) || errors.Is(err, cfnbackend.ErrStackNotFound) {
		return nil, nil //nolint:nilnil // absent change set is reported as nil by contract
	}

	if err != nil {
		return nil, err
	}

	return &codepipelinebackend.ChangeSetState{Status: cs.Status, Reason: cs.StatusReason}, nil
}

func (d *codepipelineStackDeployer) DeleteChangeSet(ctx context.Context, stackName, changeSetName string) error {
	return d.backend(ctx).DeleteChangeSet(stackName, changeSetName)
}

func (d *codepipelineStackDeployer) ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) error {
	return d.backend(ctx).ExecuteChangeSet(ctx, stackName, changeSetName, false, false)
}

type codepipelineECSDeployer struct {
	handler *ecsbackend.Handler
}

func (d *codepipelineECSDeployer) backend(ctx context.Context) ecsbackend.Backend {
	return d.handler.BackendFor(awsmeta.Region(ctx))
}

func (d *codepipelineECSDeployer) DescribeService(
	ctx context.Context, cluster, service string,
) (*codepipelinebackend.ECSServiceState, error) {
	services, failures, err := d.backend(ctx).DescribeServices(cluster, []string{service})
	if err != nil {
		return nil, err
	}

	if len(services) == 0 || len(failures) > 0 || services[0].Status == "INACTIVE" {
		return nil, nil //nolint:nilnil // absent service is reported as nil by contract
	}

	st := &codepipelinebackend.ECSServiceState{TaskDefinition: services[0].TaskDefinition}

	for _, dep := range services[0].Deployments {
		st.Deployments = append(st.Deployments, codepipelinebackend.ECSDeploymentState{
			TaskDefinition:     dep.TaskDefinition,
			Status:             dep.Status,
			RolloutState:       dep.RolloutState,
			RolloutStateReason: dep.RolloutStateReason,
			DesiredCount:       dep.DesiredCount,
			RunningCount:       dep.RunningCount,
			FailedTasks:        dep.FailedTasks,
		})
	}

	return st, nil
}

func (d *codepipelineECSDeployer) RegisterRevisionWithImages(
	ctx context.Context, baseTaskDefinition string, images map[string]string,
) (string, error) {
	bk := d.backend(ctx)

	base, err := bk.DescribeTaskDefinition(baseTaskDefinition)
	if err != nil {
		return "", err
	}

	containers := make([]ecsbackend.ContainerDefinition, len(base.ContainerDefinitions))
	copy(containers, base.ContainerDefinitions)

	replaced := 0

	for i := range containers {
		if img, ok := images[containers[i].Name]; ok {
			containers[i].Image = img
			replaced++
		}
	}

	if replaced != len(images) {
		return "", fmt.Errorf("%w: image definitions name containers that are not in task definition %s",
			ecsbackend.ErrInvalidParameter, base.TaskDefinitionArn)
	}

	rev, err := bk.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
		Family:                  base.Family,
		TaskRoleArn:             base.TaskRoleArn,
		ExecutionRoleArn:        base.ExecutionRoleArn,
		NetworkMode:             base.NetworkMode,
		CPU:                     base.CPU,
		Memory:                  base.Memory,
		PlatformFamily:          base.PlatformFamily,
		IpcMode:                 base.IpcMode,
		PidMode:                 base.PidMode,
		ContainerDefinitions:    containers,
		Volumes:                 base.Volumes,
		PlacementConstraints:    base.PlacementConstraints,
		RequiresCompatibilities: base.RequiresCompatibilities,
		InferenceAccelerators:   base.InferenceAccelerators,
		ProxyConfiguration:      base.ProxyConfiguration,
		RuntimePlatform:         base.RuntimePlatform,
		EphemeralStorage:        base.EphemeralStorage,
		EnableFaultInjection:    base.EnableFaultInjection,
	})
	if err != nil {
		return "", err
	}

	return rev.TaskDefinitionArn, nil
}

func (d *codepipelineECSDeployer) UpdateServiceTaskDefinition(
	ctx context.Context, cluster, service, taskDefinition string,
) error {
	_, err := d.backend(ctx).UpdateService(ecsbackend.UpdateServiceInput{
		Cluster: cluster, Service: service, TaskDefinition: &taskDefinition,
	})

	return err
}

type codepipelineImageSource struct {
	handler *ecrbackend.Handler
}

func (s *codepipelineImageSource) DescribeImage(
	ctx context.Context, repository, tag string,
) (*codepipelinebackend.ImageInfo, error) {
	bk := s.handler.BackendFor(awsmeta.Region(ctx))

	repos, err := bk.DescribeRepositories(ctx, []string{repository})
	if err != nil {
		return nil, err
	}

	if len(repos) == 0 {
		return nil, fmt.Errorf("%w: %s", ecrbackend.ErrRepositoryNotFound, repository)
	}

	images, err := bk.DescribeImages(ctx, repository, []ecrbackend.ImageIdentifier{{ImageTag: tag}})
	if err != nil {
		return nil, err
	}

	if len(images) == 0 {
		return nil, fmt.Errorf("%w: %s:%s", ecrbackend.ErrImageNotFound, repository, tag)
	}

	img := images[0]

	return &codepipelinebackend.ImageInfo{
		PushedAt:       img.ImagePushedAt,
		RegistryID:     repos[0].RegistryID,
		RepositoryName: repos[0].RepositoryName,
		RepositoryURI:  repos[0].RepositoryURI,
		Digest:         img.ImageDigest,
		Tags:           img.Tags,
		SizeInBytes:    img.ImageSizeInBytes,
	}, nil
}

type codepipelineRepositoryReader struct {
	handler *codecommitbackend.Handler
}

func (r *codepipelineRepositoryReader) Snapshot(
	ctx context.Context, repository, branch string,
) (*codepipelinebackend.RepositorySnapshot, error) {
	bk := r.handler.RegionHandler(awsmeta.Region(ctx)).Backend

	br, err := bk.GetBranch(repository, branch)
	if err != nil {
		return nil, err
	}

	files, err := bk.GetFolderFiles(repository, br.CommitID, "")
	if err != nil {
		return nil, err
	}

	snap := &codepipelinebackend.RepositorySnapshot{
		CommitID: br.CommitID,
		Files:    make(map[string][]byte, len(files)),
	}

	for _, f := range files {
		full, ferr := bk.GetFile(repository, br.CommitID, f.FilePath)
		if ferr != nil {
			return nil, ferr
		}

		snap.Files[f.FilePath] = full.FileContent
	}

	if c, cerr := bk.GetCommit(repository, br.CommitID); cerr == nil {
		snap.CommitMessage = c.Message
	}

	return snap, nil
}
