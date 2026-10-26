package gcpiam

import (
	"context"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	adminpb.UnimplementedIAMServer
	b *InMemoryBackend
}

func (s *server) ListServiceAccounts(
	_ context.Context, in *adminpb.ListServiceAccountsRequest,
) (*adminpb.ListServiceAccountsResponse, error) {
	items, next, err := s.b.ListServiceAccounts(in)
	if err != nil {
		return nil, err
	}

	return &adminpb.ListServiceAccountsResponse{Accounts: items, NextPageToken: next}, nil
}

func (s *server) GetServiceAccount(
	_ context.Context,
	in *adminpb.GetServiceAccountRequest,
) (*adminpb.ServiceAccount, error) {
	return s.b.GetServiceAccount(in.GetName())
}

func (s *server) CreateServiceAccount(
	_ context.Context, in *adminpb.CreateServiceAccountRequest,
) (*adminpb.ServiceAccount, error) {
	return s.b.CreateServiceAccount(in)
}

func (s *server) UpdateServiceAccount(_ context.Context, in *adminpb.ServiceAccount) (*adminpb.ServiceAccount, error) {
	return s.b.UpdateServiceAccount(in)
}

func (s *server) PatchServiceAccount(
	_ context.Context, in *adminpb.PatchServiceAccountRequest,
) (*adminpb.ServiceAccount, error) {
	return s.b.PatchServiceAccount(in)
}

func (s *server) DeleteServiceAccount(
	_ context.Context,
	in *adminpb.DeleteServiceAccountRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteServiceAccount(in.GetName())
}

func (s *server) UndeleteServiceAccount(
	_ context.Context, in *adminpb.UndeleteServiceAccountRequest,
) (*adminpb.UndeleteServiceAccountResponse, error) {
	acct, err := s.b.UndeleteServiceAccount(in.GetName())
	if err != nil {
		return nil, err
	}

	return &adminpb.UndeleteServiceAccountResponse{RestoredAccount: acct}, nil
}

func (s *server) EnableServiceAccount(
	_ context.Context,
	in *adminpb.EnableServiceAccountRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.SetServiceAccountDisabled(in.GetName(), false)
}

func (s *server) DisableServiceAccount(
	_ context.Context,
	in *adminpb.DisableServiceAccountRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.SetServiceAccountDisabled(in.GetName(), true)
}

func (s *server) ListServiceAccountKeys(
	_ context.Context, in *adminpb.ListServiceAccountKeysRequest,
) (*adminpb.ListServiceAccountKeysResponse, error) {
	keys, err := s.b.ListServiceAccountKeys(in)
	if err != nil {
		return nil, err
	}

	return &adminpb.ListServiceAccountKeysResponse{Keys: keys}, nil
}

func (s *server) GetServiceAccountKey(
	_ context.Context, in *adminpb.GetServiceAccountKeyRequest,
) (*adminpb.ServiceAccountKey, error) {
	return s.b.GetServiceAccountKey(in)
}

func (s *server) CreateServiceAccountKey(
	_ context.Context, in *adminpb.CreateServiceAccountKeyRequest,
) (*adminpb.ServiceAccountKey, error) {
	return s.b.CreateServiceAccountKey(in)
}

func (s *server) UploadServiceAccountKey(
	_ context.Context, in *adminpb.UploadServiceAccountKeyRequest,
) (*adminpb.ServiceAccountKey, error) {
	return s.b.UploadServiceAccountKey(in)
}

func (s *server) DeleteServiceAccountKey(
	_ context.Context, in *adminpb.DeleteServiceAccountKeyRequest,
) (*emptypb.Empty, error) {
	return s.b.DeleteServiceAccountKey(in.GetName())
}

func (s *server) DisableServiceAccountKey(
	_ context.Context, in *adminpb.DisableServiceAccountKeyRequest,
) (*emptypb.Empty, error) {
	return s.b.SetServiceAccountKeyDisabled(in.GetName(), true)
}

func (s *server) EnableServiceAccountKey(
	_ context.Context, in *adminpb.EnableServiceAccountKeyRequest,
) (*emptypb.Empty, error) {
	return s.b.SetServiceAccountKeyDisabled(in.GetName(), false)
}

//nolint:staticcheck // SignBlob is deprecated upstream but part of the served API
func (s *server) SignBlob(_ context.Context, in *adminpb.SignBlobRequest) (*adminpb.SignBlobResponse, error) {
	keyID, sig, err := s.b.SignBlob(in.GetName(), in.GetBytesToSign())
	if err != nil {
		return nil, err
	}

	return &adminpb.SignBlobResponse{KeyId: keyID, Signature: sig}, nil
}

//nolint:staticcheck // SignJwt is deprecated upstream but part of the served API
func (s *server) SignJwt(_ context.Context, in *adminpb.SignJwtRequest) (*adminpb.SignJwtResponse, error) {
	keyID, jwt, err := s.b.SignJwt(in.GetName(), in.GetPayload())
	if err != nil {
		return nil, err
	}

	return &adminpb.SignJwtResponse{KeyId: keyID, SignedJwt: jwt}, nil
}

func (s *server) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource(), in.GetOptions().GetRequestedPolicyVersion())
}

func (s *server) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *server) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

func (s *server) QueryGrantableRoles(
	_ context.Context, in *adminpb.QueryGrantableRolesRequest,
) (*adminpb.QueryGrantableRolesResponse, error) {
	roles, next, err := s.b.QueryGrantableRoles(in)
	if err != nil {
		return nil, err
	}

	return &adminpb.QueryGrantableRolesResponse{Roles: roles, NextPageToken: next}, nil
}

func (s *server) ListRoles(_ context.Context, in *adminpb.ListRolesRequest) (*adminpb.ListRolesResponse, error) {
	roles, next, err := s.b.ListRoles(in)
	if err != nil {
		return nil, err
	}

	return &adminpb.ListRolesResponse{Roles: roles, NextPageToken: next}, nil
}

func (s *server) GetRole(_ context.Context, in *adminpb.GetRoleRequest) (*adminpb.Role, error) {
	return s.b.GetRole(in.GetName())
}

func (s *server) CreateRole(_ context.Context, in *adminpb.CreateRoleRequest) (*adminpb.Role, error) {
	return s.b.CreateRole(in)
}

func (s *server) UpdateRole(_ context.Context, in *adminpb.UpdateRoleRequest) (*adminpb.Role, error) {
	return s.b.UpdateRole(in)
}

func (s *server) DeleteRole(_ context.Context, in *adminpb.DeleteRoleRequest) (*adminpb.Role, error) {
	return s.b.DeleteRole(in)
}

func (s *server) UndeleteRole(_ context.Context, in *adminpb.UndeleteRoleRequest) (*adminpb.Role, error) {
	return s.b.UndeleteRole(in)
}

func (s *server) QueryTestablePermissions(
	_ context.Context, in *adminpb.QueryTestablePermissionsRequest,
) (*adminpb.QueryTestablePermissionsResponse, error) {
	perms, next, err := s.b.QueryTestablePermissions(in)
	if err != nil {
		return nil, err
	}

	return &adminpb.QueryTestablePermissionsResponse{Permissions: perms, NextPageToken: next}, nil
}

func (s *server) QueryAuditableServices(
	_ context.Context, in *adminpb.QueryAuditableServicesRequest,
) (*adminpb.QueryAuditableServicesResponse, error) {
	return s.b.QueryAuditableServices(in)
}

func (s *server) LintPolicy(_ context.Context, in *adminpb.LintPolicyRequest) (*adminpb.LintPolicyResponse, error) {
	return s.b.LintPolicy(in)
}
