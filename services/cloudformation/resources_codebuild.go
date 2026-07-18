package cloudformation

import (
	"fmt"

	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
)

// ---- CodeBuild ----

func (rc *ResourceCreator) createCodeBuildProject(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeBuild == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)
	serviceRole := strProp(props, "ServiceRole", params, physicalIDs)

	var source codebuildbackend.ProjectSource
	if s, ok := props["Source"].(map[string]any); ok {
		source.Type = resolve(s["Type"], params, physicalIDs)
		source.Location = resolve(s["Location"], params, physicalIDs)
	}
	if source.Type == "" {
		source.Type = "NO_SOURCE"
	}

	var artifacts codebuildbackend.ProjectArtifacts
	if a, ok := props["Artifacts"].(map[string]any); ok {
		artifacts.Type = resolve(a["Type"], params, physicalIDs)
	}
	if artifacts.Type == "" {
		artifacts.Type = "NO_ARTIFACTS"
	}

	var env codebuildbackend.ProjectEnvironment
	if e, ok := props["Environment"].(map[string]any); ok {
		env.Type = resolve(e["Type"], params, physicalIDs)
		env.Image = resolve(e["Image"], params, physicalIDs)
		env.ComputeType = resolve(e["ComputeType"], params, physicalIDs)
	}
	if env.Type == "" {
		env.Type = "LINUX_CONTAINER"
	}
	if env.ComputeType == "" {
		env.ComputeType = "BUILD_GENERAL1_SMALL"
	}

	project, err := rc.backends.CodeBuild.Backend.CreateProject(codebuildbackend.ProjectConfig{
		Name:        name,
		Description: description,
		Source:      &source,
		Artifacts:   &artifacts,
		Environment: &env,
		ServiceRole: serviceRole,
	})
	if err != nil {
		return "", fmt.Errorf("create CodeBuild project %s: %w", name, err)
	}

	return project.Arn, nil
}

func (rc *ResourceCreator) deleteCodeBuildProject(arn string) error {
	if rc.backends.CodeBuild == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.CodeBuild.Backend.DeleteProject(name)
}
