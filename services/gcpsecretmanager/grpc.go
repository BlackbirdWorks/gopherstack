package gcpsecretmanager

import (
	"context"
	"math"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	secretmanagerpb.UnimplementedSecretManagerServiceServer
	b *InMemoryBackend
}

func (s *server) CreateSecret(
	_ context.Context,
	in *secretmanagerpb.CreateSecretRequest,
) (*secretmanagerpb.Secret, error) {
	return s.b.CreateSecret(in)
}

func (s *server) GetSecret(_ context.Context, in *secretmanagerpb.GetSecretRequest) (*secretmanagerpb.Secret, error) {
	return s.b.GetSecret(in.GetName())
}

func (s *server) UpdateSecret(
	_ context.Context,
	in *secretmanagerpb.UpdateSecretRequest,
) (*secretmanagerpb.Secret, error) {
	return s.b.UpdateSecret(in)
}

func (s *server) DeleteSecret(_ context.Context, in *secretmanagerpb.DeleteSecretRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteSecret(in)
}

func (s *server) ListSecrets(
	_ context.Context,
	in *secretmanagerpb.ListSecretsRequest,
) (*secretmanagerpb.ListSecretsResponse, error) {
	secrets, next, total, err := s.b.ListSecrets(in)
	if err != nil {
		return nil, err
	}

	return &secretmanagerpb.ListSecretsResponse{
		Secrets:       secrets,
		NextPageToken: next,
		TotalSize:     count32(total),
	}, nil
}

func (s *server) AddSecretVersion(
	_ context.Context,
	in *secretmanagerpb.AddSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return s.b.AddSecretVersion(in)
}

func (s *server) GetSecretVersion(
	_ context.Context,
	in *secretmanagerpb.GetSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return s.b.GetSecretVersion(in.GetName())
}

func (s *server) ListSecretVersions(
	_ context.Context,
	in *secretmanagerpb.ListSecretVersionsRequest,
) (*secretmanagerpb.ListSecretVersionsResponse, error) {
	versions, next, total, err := s.b.ListSecretVersions(in)
	if err != nil {
		return nil, err
	}

	return &secretmanagerpb.ListSecretVersionsResponse{
		Versions:      versions,
		NextPageToken: next,
		TotalSize:     count32(total),
	}, nil
}

func (s *server) AccessSecretVersion(
	_ context.Context,
	in *secretmanagerpb.AccessSecretVersionRequest,
) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	return s.b.AccessSecretVersion(in.GetName())
}

func (s *server) EnableSecretVersion(
	_ context.Context,
	in *secretmanagerpb.EnableSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return s.b.EnableSecretVersion(in)
}

func (s *server) DisableSecretVersion(
	_ context.Context,
	in *secretmanagerpb.DisableSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return s.b.DisableSecretVersion(in)
}

func (s *server) DestroySecretVersion(
	_ context.Context,
	in *secretmanagerpb.DestroySecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return s.b.DestroySecretVersion(in)
}

func (s *server) EnableManagedRotation(
	_ context.Context,
	in *secretmanagerpb.EnableManagedRotationRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return s.b.EnableManagedRotation(in)
}

func (s *server) RotateSecret(
	_ context.Context,
	in *secretmanagerpb.RotateSecretRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return s.b.RotateSecret(in)
}

func (s *server) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *server) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource())
}

func (s *server) TestIamPermissions(
	_ context.Context,
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

func count32(n int) int32 {
	return int32(min(n, math.MaxInt32)) //nolint:gosec // clamped to the int32 range
}
