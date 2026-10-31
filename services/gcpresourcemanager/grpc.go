package gcpresourcemanager

import (
	"context"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
)

type iamMethods struct{ b *InMemoryBackend }

// Unimplemented bases sit one level deeper than iamMethods so its IAM methods win the selector.
type unimplementedProjects struct{ resourcemanagerpb.UnimplementedProjectsServer }

type unimplementedFolders struct{ resourcemanagerpb.UnimplementedFoldersServer }

type unimplementedOrganizations struct{ resourcemanagerpb.UnimplementedOrganizationsServer }

type unimplementedTagKeys struct{ resourcemanagerpb.UnimplementedTagKeysServer }

type unimplementedTagValues struct{ resourcemanagerpb.UnimplementedTagValuesServer }

func (m iamMethods) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return m.b.GetIamPolicy(in)
}

func (m iamMethods) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return m.b.SetIamPolicy(in)
}

func (m iamMethods) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return m.b.TestIamPermissions(in)
}

type projectsServer struct {
	iamMethods
	unimplementedProjects
}

func (s projectsServer) GetProject(_ context.Context, in *resourcemanagerpb.GetProjectRequest) (
	*resourcemanagerpb.Project, error,
) {
	return s.b.GetProject(in.GetName())
}

func (s projectsServer) ListProjects(_ context.Context, in *resourcemanagerpb.ListProjectsRequest) (
	*resourcemanagerpb.ListProjectsResponse, error,
) {
	return s.b.ListProjects(in)
}

func (s projectsServer) SearchProjects(_ context.Context, in *resourcemanagerpb.SearchProjectsRequest) (
	*resourcemanagerpb.SearchProjectsResponse, error,
) {
	return s.b.SearchProjects(in)
}

func (s projectsServer) CreateProject(_ context.Context, in *resourcemanagerpb.CreateProjectRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.CreateProject(in)
}

func (s projectsServer) UpdateProject(_ context.Context, in *resourcemanagerpb.UpdateProjectRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.UpdateProject(in)
}

func (s projectsServer) MoveProject(_ context.Context, in *resourcemanagerpb.MoveProjectRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.MoveProject(in)
}

func (s projectsServer) DeleteProject(_ context.Context, in *resourcemanagerpb.DeleteProjectRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.DeleteProject(in)
}

func (s projectsServer) UndeleteProject(_ context.Context, in *resourcemanagerpb.UndeleteProjectRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.UndeleteProject(in)
}

type foldersServer struct {
	iamMethods
	unimplementedFolders
}

func (s foldersServer) GetFolder(_ context.Context, in *resourcemanagerpb.GetFolderRequest) (
	*resourcemanagerpb.Folder, error,
) {
	return s.b.GetFolder(in.GetName())
}

func (s foldersServer) ListFolders(_ context.Context, in *resourcemanagerpb.ListFoldersRequest) (
	*resourcemanagerpb.ListFoldersResponse, error,
) {
	return s.b.ListFolders(in)
}

func (s foldersServer) SearchFolders(_ context.Context, in *resourcemanagerpb.SearchFoldersRequest) (
	*resourcemanagerpb.SearchFoldersResponse, error,
) {
	return s.b.SearchFolders(in)
}

func (s foldersServer) CreateFolder(_ context.Context, in *resourcemanagerpb.CreateFolderRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.CreateFolder(in)
}

func (s foldersServer) UpdateFolder(_ context.Context, in *resourcemanagerpb.UpdateFolderRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.UpdateFolder(in)
}

func (s foldersServer) MoveFolder(_ context.Context, in *resourcemanagerpb.MoveFolderRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.MoveFolder(in)
}

func (s foldersServer) DeleteFolder(_ context.Context, in *resourcemanagerpb.DeleteFolderRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.DeleteFolder(in)
}

func (s foldersServer) UndeleteFolder(_ context.Context, in *resourcemanagerpb.UndeleteFolderRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.UndeleteFolder(in)
}

type organizationsServer struct {
	iamMethods
	unimplementedOrganizations
}

func (s organizationsServer) GetOrganization(_ context.Context, in *resourcemanagerpb.GetOrganizationRequest) (
	*resourcemanagerpb.Organization, error,
) {
	return s.b.GetOrganization(in.GetName())
}

func (s organizationsServer) SearchOrganizations(
	_ context.Context, in *resourcemanagerpb.SearchOrganizationsRequest,
) (*resourcemanagerpb.SearchOrganizationsResponse, error) {
	return s.b.SearchOrganizations(in)
}

type tagKeysServer struct {
	iamMethods
	unimplementedTagKeys
}

func (s tagKeysServer) ListTagKeys(_ context.Context, in *resourcemanagerpb.ListTagKeysRequest) (
	*resourcemanagerpb.ListTagKeysResponse, error,
) {
	return s.b.ListTagKeys(in)
}

func (s tagKeysServer) GetTagKey(_ context.Context, in *resourcemanagerpb.GetTagKeyRequest) (
	*resourcemanagerpb.TagKey, error,
) {
	return s.b.GetTagKey(in.GetName())
}

func (s tagKeysServer) GetNamespacedTagKey(_ context.Context, in *resourcemanagerpb.GetNamespacedTagKeyRequest) (
	*resourcemanagerpb.TagKey, error,
) {
	return s.b.GetNamespacedTagKey(in.GetName())
}

func (s tagKeysServer) CreateTagKey(_ context.Context, in *resourcemanagerpb.CreateTagKeyRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.CreateTagKey(in)
}

func (s tagKeysServer) UpdateTagKey(_ context.Context, in *resourcemanagerpb.UpdateTagKeyRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.UpdateTagKey(in)
}

func (s tagKeysServer) DeleteTagKey(_ context.Context, in *resourcemanagerpb.DeleteTagKeyRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.DeleteTagKey(in)
}

type tagValuesServer struct {
	iamMethods
	unimplementedTagValues
}

func (s tagValuesServer) ListTagValues(_ context.Context, in *resourcemanagerpb.ListTagValuesRequest) (
	*resourcemanagerpb.ListTagValuesResponse, error,
) {
	return s.b.ListTagValues(in)
}

func (s tagValuesServer) GetTagValue(_ context.Context, in *resourcemanagerpb.GetTagValueRequest) (
	*resourcemanagerpb.TagValue, error,
) {
	return s.b.GetTagValue(in.GetName())
}

func (s tagValuesServer) GetNamespacedTagValue(
	_ context.Context, in *resourcemanagerpb.GetNamespacedTagValueRequest,
) (*resourcemanagerpb.TagValue, error) {
	return s.b.GetNamespacedTagValue(in.GetName())
}

func (s tagValuesServer) CreateTagValue(_ context.Context, in *resourcemanagerpb.CreateTagValueRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.CreateTagValue(in)
}

func (s tagValuesServer) UpdateTagValue(_ context.Context, in *resourcemanagerpb.UpdateTagValueRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.UpdateTagValue(in)
}

func (s tagValuesServer) DeleteTagValue(_ context.Context, in *resourcemanagerpb.DeleteTagValueRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.DeleteTagValue(in)
}

type tagBindingsServer struct {
	resourcemanagerpb.UnimplementedTagBindingsServer
	b *InMemoryBackend
}

func (s tagBindingsServer) ListTagBindings(_ context.Context, in *resourcemanagerpb.ListTagBindingsRequest) (
	*resourcemanagerpb.ListTagBindingsResponse, error,
) {
	return s.b.ListTagBindings(in)
}

func (s tagBindingsServer) CreateTagBinding(_ context.Context, in *resourcemanagerpb.CreateTagBindingRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.CreateTagBinding(in)
}

func (s tagBindingsServer) DeleteTagBinding(_ context.Context, in *resourcemanagerpb.DeleteTagBindingRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.DeleteTagBinding(in)
}

type serviceUsageServer struct {
	serviceusagepb.UnimplementedServiceUsageServer
	b *InMemoryBackend
}

func (s serviceUsageServer) EnableService(_ context.Context, in *serviceusagepb.EnableServiceRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.EnableService(in)
}

func (s serviceUsageServer) DisableService(_ context.Context, in *serviceusagepb.DisableServiceRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.DisableService(in)
}

func (s serviceUsageServer) GetService(_ context.Context, in *serviceusagepb.GetServiceRequest) (
	*serviceusagepb.Service, error,
) {
	return s.b.GetService(in.GetName())
}

func (s serviceUsageServer) ListServices(_ context.Context, in *serviceusagepb.ListServicesRequest) (
	*serviceusagepb.ListServicesResponse, error,
) {
	return s.b.ListServices(in)
}

func (s serviceUsageServer) BatchEnableServices(_ context.Context, in *serviceusagepb.BatchEnableServicesRequest) (
	*longrunningpb.Operation, error,
) {
	return s.b.BatchEnableServices(in)
}

func (s serviceUsageServer) BatchGetServices(_ context.Context, in *serviceusagepb.BatchGetServicesRequest) (
	*serviceusagepb.BatchGetServicesResponse, error,
) {
	return s.b.BatchGetServices(in)
}
