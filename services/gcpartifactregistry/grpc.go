package gcpartifactregistry

import (
	"context"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	artifactregistrypb.UnimplementedArtifactRegistryServer
	b *InMemoryBackend
}

func (s *server) CreateRepository(
	_ context.Context, in *artifactregistrypb.CreateRepositoryRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateRepository(in)
}

func (s *server) GetRepository(
	_ context.Context, in *artifactregistrypb.GetRepositoryRequest,
) (*artifactregistrypb.Repository, error) {
	return s.b.GetRepository(in.GetName())
}

func (s *server) ListRepositories(
	_ context.Context, in *artifactregistrypb.ListRepositoriesRequest,
) (*artifactregistrypb.ListRepositoriesResponse, error) {
	items, next, err := s.b.ListRepositories(in)
	if err != nil {
		return nil, err
	}

	return &artifactregistrypb.ListRepositoriesResponse{Repositories: items, NextPageToken: next}, nil
}

func (s *server) UpdateRepository(
	_ context.Context, in *artifactregistrypb.UpdateRepositoryRequest,
) (*artifactregistrypb.Repository, error) {
	return s.b.UpdateRepository(in)
}

func (s *server) DeleteRepository(
	ctx context.Context, in *artifactregistrypb.DeleteRepositoryRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteRepository(ctx, in.GetName())
}

func (s *server) ListDockerImages(
	ctx context.Context, in *artifactregistrypb.ListDockerImagesRequest,
) (*artifactregistrypb.ListDockerImagesResponse, error) {
	items, next, err := s.b.ListDockerImages(ctx, in)
	if err != nil {
		return nil, err
	}

	return &artifactregistrypb.ListDockerImagesResponse{DockerImages: items, NextPageToken: next}, nil
}

func (s *server) GetDockerImage(
	ctx context.Context, in *artifactregistrypb.GetDockerImageRequest,
) (*artifactregistrypb.DockerImage, error) {
	return s.b.GetDockerImage(ctx, in.GetName())
}

func (s *server) ListPackages(
	_ context.Context, in *artifactregistrypb.ListPackagesRequest,
) (*artifactregistrypb.ListPackagesResponse, error) {
	items, next, err := s.b.ListPackages(in)
	if err != nil {
		return nil, err
	}

	return &artifactregistrypb.ListPackagesResponse{Packages: items, NextPageToken: next}, nil
}

func (s *server) GetPackage(
	_ context.Context, in *artifactregistrypb.GetPackageRequest,
) (*artifactregistrypb.Package, error) {
	return s.b.GetPackage(in.GetName())
}

func (s *server) DeletePackage(
	ctx context.Context, in *artifactregistrypb.DeletePackageRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeletePackage(ctx, in.GetName())
}

func (s *server) ListVersions(
	ctx context.Context, in *artifactregistrypb.ListVersionsRequest,
) (*artifactregistrypb.ListVersionsResponse, error) {
	items, next, err := s.b.ListVersions(ctx, in)
	if err != nil {
		return nil, err
	}

	return &artifactregistrypb.ListVersionsResponse{Versions: items, NextPageToken: next}, nil
}

func (s *server) GetVersion(
	ctx context.Context, in *artifactregistrypb.GetVersionRequest,
) (*artifactregistrypb.Version, error) {
	return s.b.GetVersion(ctx, in)
}

func (s *server) DeleteVersion(
	ctx context.Context, in *artifactregistrypb.DeleteVersionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteVersion(ctx, in)
}

func (s *server) BatchDeleteVersions(
	ctx context.Context, in *artifactregistrypb.BatchDeleteVersionsRequest,
) (*longrunningpb.Operation, error) {
	return s.b.BatchDeleteVersions(ctx, in)
}

func (s *server) ListTags(
	_ context.Context, in *artifactregistrypb.ListTagsRequest,
) (*artifactregistrypb.ListTagsResponse, error) {
	items, next, err := s.b.ListTags(in)
	if err != nil {
		return nil, err
	}

	return &artifactregistrypb.ListTagsResponse{Tags: items, NextPageToken: next}, nil
}

func (s *server) GetTag(_ context.Context, in *artifactregistrypb.GetTagRequest) (*artifactregistrypb.Tag, error) {
	return s.b.GetTag(in.GetName())
}

func (s *server) CreateTag(
	ctx context.Context, in *artifactregistrypb.CreateTagRequest,
) (*artifactregistrypb.Tag, error) {
	return s.b.CreateTag(ctx, in)
}

func (s *server) UpdateTag(
	ctx context.Context, in *artifactregistrypb.UpdateTagRequest,
) (*artifactregistrypb.Tag, error) {
	return s.b.UpdateTag(ctx, in)
}

func (s *server) DeleteTag(ctx context.Context, in *artifactregistrypb.DeleteTagRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteTag(ctx, in.GetName())
}

func (s *server) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource())
}

func (s *server) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in.GetResource(), in.GetPolicy())
}

// TestIamPermissions reports every requested permission as granted: the emulator does not enforce IAM.
func (s *server) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	if _, err := s.b.GetIamPolicy(in.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: in.GetPermissions()}, nil
}

func (s *server) UpdatePackage(
	_ context.Context, in *artifactregistrypb.UpdatePackageRequest,
) (*artifactregistrypb.Package, error) {
	return s.b.UpdatePackage(in)
}

func (s *server) UpdateVersion(
	_ context.Context, in *artifactregistrypb.UpdateVersionRequest,
) (*artifactregistrypb.Version, error) {
	return s.b.UpdateVersion(in)
}

func (s *server) ListFiles(
	ctx context.Context, in *artifactregistrypb.ListFilesRequest,
) (*artifactregistrypb.ListFilesResponse, error) {
	items, next, err := s.b.ListFiles(ctx, in)
	if err != nil {
		return nil, err
	}

	return &artifactregistrypb.ListFilesResponse{Files: items, NextPageToken: next}, nil
}

func (s *server) GetFile(ctx context.Context, in *artifactregistrypb.GetFileRequest) (*artifactregistrypb.File, error) {
	return s.b.GetFile(ctx, in.GetName())
}

func (s *server) DeleteFile(
	ctx context.Context, in *artifactregistrypb.DeleteFileRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteFile(ctx, in.GetName())
}

func (s *server) UpdateFile(
	ctx context.Context, in *artifactregistrypb.UpdateFileRequest,
) (*artifactregistrypb.File, error) {
	return s.b.UpdateFile(ctx, in)
}

func (s *server) CreateRule(
	_ context.Context, in *artifactregistrypb.CreateRuleRequest,
) (*artifactregistrypb.Rule, error) {
	return s.b.CreateRule(in)
}

func (s *server) GetRule(_ context.Context, in *artifactregistrypb.GetRuleRequest) (*artifactregistrypb.Rule, error) {
	return s.b.GetRule(in.GetName())
}

func (s *server) ListRules(
	_ context.Context, in *artifactregistrypb.ListRulesRequest,
) (*artifactregistrypb.ListRulesResponse, error) {
	items, next, err := s.b.ListRules(in)
	if err != nil {
		return nil, err
	}

	return &artifactregistrypb.ListRulesResponse{Rules: items, NextPageToken: next}, nil
}

func (s *server) UpdateRule(
	_ context.Context, in *artifactregistrypb.UpdateRuleRequest,
) (*artifactregistrypb.Rule, error) {
	return s.b.UpdateRule(in)
}

func (s *server) DeleteRule(_ context.Context, in *artifactregistrypb.DeleteRuleRequest) (*emptypb.Empty, error) {
	return s.b.DeleteRule(in.GetName())
}

func (s *server) GetProjectSettings(
	_ context.Context, in *artifactregistrypb.GetProjectSettingsRequest,
) (*artifactregistrypb.ProjectSettings, error) {
	return s.b.GetProjectSettings(in.GetName())
}

func (s *server) UpdateProjectSettings(
	_ context.Context, in *artifactregistrypb.UpdateProjectSettingsRequest,
) (*artifactregistrypb.ProjectSettings, error) {
	return s.b.UpdateProjectSettings(in)
}

func (s *server) GetVPCSCConfig(
	_ context.Context, in *artifactregistrypb.GetVPCSCConfigRequest,
) (*artifactregistrypb.VPCSCConfig, error) {
	return s.b.GetVPCSCConfig(in.GetName())
}

func (s *server) UpdateVPCSCConfig(
	_ context.Context, in *artifactregistrypb.UpdateVPCSCConfigRequest,
) (*artifactregistrypb.VPCSCConfig, error) {
	return s.b.UpdateVPCSCConfig(in)
}
