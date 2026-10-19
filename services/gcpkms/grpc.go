package gcpkms

import (
	"context"
	"math"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

type server struct {
	kmspb.UnimplementedKeyManagementServiceServer
	b *InMemoryBackend
}

type iamServer struct {
	iampb.UnimplementedIAMPolicyServer
	b *InMemoryBackend
}

func (s *server) CreateKeyRing(_ context.Context, in *kmspb.CreateKeyRingRequest) (*kmspb.KeyRing, error) {
	return s.b.CreateKeyRing(in)
}

func (s *server) GetKeyRing(_ context.Context, in *kmspb.GetKeyRingRequest) (*kmspb.KeyRing, error) {
	return s.b.GetKeyRing(in.GetName())
}

func (s *server) ListKeyRings(_ context.Context, in *kmspb.ListKeyRingsRequest) (*kmspb.ListKeyRingsResponse, error) {
	items, next, total, err := s.b.ListKeyRings(in)
	if err != nil {
		return nil, err
	}

	return &kmspb.ListKeyRingsResponse{KeyRings: items, NextPageToken: next, TotalSize: count32(total)}, nil
}

func (s *server) CreateCryptoKey(_ context.Context, in *kmspb.CreateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	return s.b.CreateCryptoKey(in)
}

func (s *server) GetCryptoKey(_ context.Context, in *kmspb.GetCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	return s.b.GetCryptoKey(in.GetName())
}

func (s *server) ListCryptoKeys(
	_ context.Context, in *kmspb.ListCryptoKeysRequest,
) (*kmspb.ListCryptoKeysResponse, error) {
	items, next, total, err := s.b.ListCryptoKeys(in)
	if err != nil {
		return nil, err
	}

	return &kmspb.ListCryptoKeysResponse{CryptoKeys: items, NextPageToken: next, TotalSize: count32(total)}, nil
}

func (s *server) UpdateCryptoKey(_ context.Context, in *kmspb.UpdateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	return s.b.UpdateCryptoKey(in)
}

func (s *server) UpdateCryptoKeyPrimaryVersion(
	_ context.Context, in *kmspb.UpdateCryptoKeyPrimaryVersionRequest,
) (*kmspb.CryptoKey, error) {
	return s.b.UpdateCryptoKeyPrimaryVersion(in)
}

func (s *server) CreateCryptoKeyVersion(
	_ context.Context, in *kmspb.CreateCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	return s.b.CreateCryptoKeyVersion(in)
}

func (s *server) GetCryptoKeyVersion(
	_ context.Context, in *kmspb.GetCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	return s.b.GetCryptoKeyVersion(in.GetName())
}

func (s *server) ListCryptoKeyVersions(
	_ context.Context, in *kmspb.ListCryptoKeyVersionsRequest,
) (*kmspb.ListCryptoKeyVersionsResponse, error) {
	items, next, total, err := s.b.ListCryptoKeyVersions(in)
	if err != nil {
		return nil, err
	}

	return &kmspb.ListCryptoKeyVersionsResponse{
		CryptoKeyVersions: items, NextPageToken: next, TotalSize: count32(total),
	}, nil
}

func (s *server) UpdateCryptoKeyVersion(
	_ context.Context, in *kmspb.UpdateCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	return s.b.UpdateCryptoKeyVersion(in)
}

func (s *server) DestroyCryptoKeyVersion(
	_ context.Context, in *kmspb.DestroyCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	return s.b.DestroyCryptoKeyVersion(in)
}

func (s *server) RestoreCryptoKeyVersion(
	_ context.Context, in *kmspb.RestoreCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	return s.b.RestoreCryptoKeyVersion(in)
}

func (s *server) DeleteCryptoKey(
	_ context.Context, in *kmspb.DeleteCryptoKeyRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteCryptoKey(in)
}

func (s *server) DeleteCryptoKeyVersion(
	_ context.Context, in *kmspb.DeleteCryptoKeyVersionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteCryptoKeyVersion(in)
}

func (s *server) GetRetiredResource(
	_ context.Context, in *kmspb.GetRetiredResourceRequest,
) (*kmspb.RetiredResource, error) {
	return s.b.GetRetiredResource(in.GetName())
}

func (s *server) ListRetiredResources(
	_ context.Context, in *kmspb.ListRetiredResourcesRequest,
) (*kmspb.ListRetiredResourcesResponse, error) {
	items, next, total, err := s.b.ListRetiredResources(in)
	if err != nil {
		return nil, err
	}

	return &kmspb.ListRetiredResourcesResponse{
		RetiredResources: items, NextPageToken: next, TotalSize: int64(total),
	}, nil
}

func (s *server) CreateImportJob(_ context.Context, in *kmspb.CreateImportJobRequest) (*kmspb.ImportJob, error) {
	return s.b.CreateImportJob(in)
}

func (s *server) GetImportJob(_ context.Context, in *kmspb.GetImportJobRequest) (*kmspb.ImportJob, error) {
	return s.b.GetImportJob(in.GetName())
}

func (s *server) ListImportJobs(
	_ context.Context, in *kmspb.ListImportJobsRequest,
) (*kmspb.ListImportJobsResponse, error) {
	items, next, total, err := s.b.ListImportJobs(in)
	if err != nil {
		return nil, err
	}

	return &kmspb.ListImportJobsResponse{ImportJobs: items, NextPageToken: next, TotalSize: count32(total)}, nil
}

func (s *server) ImportCryptoKeyVersion(
	_ context.Context, in *kmspb.ImportCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	return s.b.ImportCryptoKeyVersion(in)
}

func (s *server) Encrypt(_ context.Context, in *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	return s.b.Encrypt(in)
}

func (s *server) Decrypt(_ context.Context, in *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	return s.b.Decrypt(in)
}

func (s *server) RawEncrypt(_ context.Context, in *kmspb.RawEncryptRequest) (*kmspb.RawEncryptResponse, error) {
	return s.b.RawEncrypt(in)
}

func (s *server) RawDecrypt(_ context.Context, in *kmspb.RawDecryptRequest) (*kmspb.RawDecryptResponse, error) {
	return s.b.RawDecrypt(in)
}

func (s *server) AsymmetricSign(
	_ context.Context, in *kmspb.AsymmetricSignRequest,
) (*kmspb.AsymmetricSignResponse, error) {
	return s.b.AsymmetricSign(in)
}

func (s *server) AsymmetricDecrypt(
	_ context.Context, in *kmspb.AsymmetricDecryptRequest,
) (*kmspb.AsymmetricDecryptResponse, error) {
	return s.b.AsymmetricDecrypt(in)
}

func (s *server) GetPublicKey(_ context.Context, in *kmspb.GetPublicKeyRequest) (*kmspb.PublicKey, error) {
	return s.b.GetPublicKey(in)
}

func (s *server) MacSign(_ context.Context, in *kmspb.MacSignRequest) (*kmspb.MacSignResponse, error) {
	return s.b.MacSign(in)
}

func (s *server) MacVerify(_ context.Context, in *kmspb.MacVerifyRequest) (*kmspb.MacVerifyResponse, error) {
	return s.b.MacVerify(in)
}

func (s *server) GenerateRandomBytes(
	_ context.Context, in *kmspb.GenerateRandomBytesRequest,
) (*kmspb.GenerateRandomBytesResponse, error) {
	return s.b.GenerateRandomBytes(in)
}

func (s *iamServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *iamServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource())
}

func (s *iamServer) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

func count32(n int) int32 {
	return int32(min(n, math.MaxInt32)) //nolint:gosec // clamped to the int32 range
}
