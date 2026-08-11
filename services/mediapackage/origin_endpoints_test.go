package mediapackage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// TestBackend_CreateOriginEndpoint_SetsCreatedAt verifies CreateOriginEndpoint
// populates CreatedAt, matching real MediaPackage's OriginEndpoint shape.
func TestBackend_CreateOriginEndpoint_SetsCreatedAt(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	ep, err := b.CreateOriginEndpoint("chan1", "ep1", "", "", 0, 0, "", nil, nil, mediapackage.PackagingConfig{})
	require.NoError(t, err)
	assert.NotEmpty(t, ep.CreatedAt)
}

// TestBackend_OriginEndpoint_PackagingConfigRoundTrip verifies that the
// opaque CDN-authorization and per-protocol packaging blocks
// (authorization/cmafPackage/dashPackage/hlsPackage/mssPackage) survive a
// Create then Describe -- these were previously accepted in
// CreateOriginEndpoint's request shape but silently discarded, so a
// Terraform/CDK OriginEndpoint configured with e.g. hlsPackage never
// round-tripped.
func TestBackend_OriginEndpoint_PackagingConfigRoundTrip(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	pkg := mediapackage.PackagingConfig{
		Authorization: &mediapackage.Authorization{
			CdnIdentifierSecret: "arn:aws:secretsmanager:1",
			SecretsRoleArn:      "arn:aws:iam:1",
		},
		HlsPackage:  map[string]any{"segmentDurationSeconds": float64(6)},
		DashPackage: map[string]any{"segmentDurationSeconds": float64(4)},
		CmafPackage: map[string]any{"segmentDurationSeconds": float64(2)},
		MssPackage:  &mediapackage.MssPackage{SegmentDurationSeconds: 10},
	}

	created, err := b.CreateOriginEndpoint("chan1", "ep1", "", "", 0, 0, "", nil, nil, pkg)
	require.NoError(t, err)
	assert.Equal(t, pkg.Authorization, created.Authorization)
	assert.Equal(t, pkg.HlsPackage, created.HlsPackage)
	assert.Equal(t, pkg.DashPackage, created.DashPackage)
	assert.Equal(t, pkg.CmafPackage, created.CmafPackage)
	assert.Equal(t, pkg.MssPackage, created.MssPackage)

	described, err := b.DescribeOriginEndpoint("ep1")
	require.NoError(t, err)
	assert.Equal(t, pkg.Authorization, described.Authorization)
	assert.Equal(t, pkg.HlsPackage, described.HlsPackage)
	assert.Equal(t, pkg.DashPackage, described.DashPackage)
	assert.Equal(t, pkg.CmafPackage, described.CmafPackage)
	assert.Equal(t, pkg.MssPackage, described.MssPackage)
}

// TestBackend_OriginEndpoint_PackagingConfig_UpdatePartial verifies
// UpdateOriginEndpoint only overwrites the packaging blocks explicitly
// provided (nil map means "not sent"), leaving the others as configured at
// create time.
func TestBackend_OriginEndpoint_PackagingConfig_UpdatePartial(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	initial := mediapackage.PackagingConfig{
		HlsPackage:  map[string]any{"segmentDurationSeconds": float64(6)},
		DashPackage: map[string]any{"segmentDurationSeconds": float64(4)},
	}
	_, err = b.CreateOriginEndpoint("chan1", "ep1", "", "", 0, 0, "", nil, nil, initial)
	require.NoError(t, err)

	update := mediapackage.PackagingConfig{
		HlsPackage: map[string]any{"segmentDurationSeconds": float64(8)},
	}
	updated, err := b.UpdateOriginEndpoint("ep1", "", "", -1, -1, "", nil, update)
	require.NoError(t, err)

	assert.Equal(t, update.HlsPackage, updated.HlsPackage, "hlsPackage should be replaced")
	assert.Equal(t, initial.DashPackage, updated.DashPackage, "dashPackage should be unchanged")
}

// TestBackend_Authorization_RequiredFields verifies that a present
// Authorization block requires both CdnIdentifierSecret and SecretsRoleArn,
// matching the SDK's required-together members (types.Authorization,
// aws-sdk-go-v2/service/mediapackage@v1.42.4, types/types.go:10-26).
func TestBackend_Authorization_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth    *mediapackage.Authorization
		name    string
		wantErr bool
	}{
		{
			name:    "nil authorization is valid (block optional)",
			auth:    nil,
			wantErr: false,
		},
		{
			name: "both fields present is valid",
			auth: &mediapackage.Authorization{
				CdnIdentifierSecret: "arn:aws:secretsmanager:1",
				SecretsRoleArn:      "arn:aws:iam:1",
			},
			wantErr: false,
		},
		{
			name:    "missing secretsRoleArn is rejected",
			auth:    &mediapackage.Authorization{CdnIdentifierSecret: "arn:aws:secretsmanager:1"},
			wantErr: true,
		},
		{
			name:    "missing cdnIdentifierSecret is rejected",
			auth:    &mediapackage.Authorization{SecretsRoleArn: "arn:aws:iam:1"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateChannel("chan1", "", nil)
			require.NoError(t, err)

			_, err = b.CreateOriginEndpoint(
				"chan1", "ep1", "", "", 0, 0, "", nil, nil,
				mediapackage.PackagingConfig{Authorization: tc.auth},
			)

			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, mediapackage.ErrInvalidParameter)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_MssPackage_SpekeValidation verifies the SPEKE required-field
// chain inside a present MssPackage.Encryption block (types.MssEncryption,
// types.SpekeKeyProvider, types.EncryptionContractConfiguration --
// aws-sdk-go-v2/service/mediapackage@v1.42.4, types/types.go:563-572,
// 681-721, 246-259).
func TestBackend_MssPackage_SpekeValidation(t *testing.T) {
	t.Parallel()

	validSpeke := &mediapackage.SpekeKeyProvider{
		ResourceID: "r1",
		RoleArn:    "arn:aws:iam:1",
		URL:        "https://speke.example.com",
		SystemIDs:  []string{"81376844-f976-481e-a695-0e6108b45a58"},
	}

	tests := []struct {
		mss     *mediapackage.MssPackage
		name    string
		wantErr bool
	}{
		{
			name:    "nil mssPackage is valid",
			mss:     nil,
			wantErr: false,
		},
		{
			name:    "encryption without spekeKeyProvider is rejected",
			mss:     &mediapackage.MssPackage{Encryption: &mediapackage.MssEncryption{}},
			wantErr: true,
		},
		{
			name: "valid spekeKeyProvider is accepted",
			mss: &mediapackage.MssPackage{
				Encryption: &mediapackage.MssEncryption{SpekeKeyProvider: validSpeke},
			},
			wantErr: false,
		},
		{
			name: "spekeKeyProvider missing resourceId is rejected",
			mss: &mediapackage.MssPackage{Encryption: &mediapackage.MssEncryption{
				SpekeKeyProvider: &mediapackage.SpekeKeyProvider{
					RoleArn:   "arn:aws:iam:1",
					URL:       "https://speke.example.com",
					SystemIDs: []string{"81376844-f976-481e-a695-0e6108b45a58"},
				},
			}},
			wantErr: true,
		},
		{
			name: "spekeKeyProvider with empty systemIds is rejected",
			mss: &mediapackage.MssPackage{Encryption: &mediapackage.MssEncryption{
				SpekeKeyProvider: &mediapackage.SpekeKeyProvider{
					ResourceID: "r1",
					RoleArn:    "arn:aws:iam:1",
					URL:        "https://speke.example.com",
				},
			}},
			wantErr: true,
		},
		{
			name: "encryptionContractConfiguration missing presetSpeke20Video is rejected",
			mss: &mediapackage.MssPackage{Encryption: &mediapackage.MssEncryption{
				SpekeKeyProvider: &mediapackage.SpekeKeyProvider{
					ResourceID: "r1",
					RoleArn:    "arn:aws:iam:1",
					URL:        "https://speke.example.com",
					SystemIDs:  []string{"81376844-f976-481e-a695-0e6108b45a58"},
					EncryptionContractConfiguration: &mediapackage.EncryptionContractConfiguration{
						PresetSpeke20Audio: "PRESET-AUDIO-1",
					},
				},
			}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateChannel("chan1", "", nil)
			require.NoError(t, err)

			_, err = b.CreateOriginEndpoint(
				"chan1", "ep1", "", "", 0, 0, "", nil, nil,
				mediapackage.PackagingConfig{MssPackage: tc.mss},
			)

			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, mediapackage.ErrInvalidParameter)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_MssPackage_FullDepthRoundTrip verifies the fully typed
// MssPackage (including the nested SPEKE/StreamSelection chain) survives
// Create->Describe unchanged, and that mutating the returned value does not
// corrupt the backend's stored copy.
func TestBackend_MssPackage_FullDepthRoundTrip(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	mss := &mediapackage.MssPackage{
		ManifestWindowSeconds:  60,
		SegmentDurationSeconds: 10,
		StreamSelection: &mediapackage.StreamSelection{
			StreamOrder:           "VIDEO_BITRATE_DESCENDING",
			MaxVideoBitsPerSecond: 5000000,
			MinVideoBitsPerSecond: 100000,
		},
		Encryption: &mediapackage.MssEncryption{
			SpekeKeyProvider: &mediapackage.SpekeKeyProvider{
				ResourceID:     "r1",
				RoleArn:        "arn:aws:iam:1",
				URL:            "https://speke.example.com",
				CertificateArn: "arn:aws:acm:1",
				SystemIDs:      []string{"81376844-f976-481e-a695-0e6108b45a58"},
				EncryptionContractConfiguration: &mediapackage.EncryptionContractConfiguration{
					PresetSpeke20Audio: "PRESET-AUDIO-1",
					PresetSpeke20Video: "PRESET-VIDEO-1",
				},
			},
		},
	}

	created, err := b.CreateOriginEndpoint(
		"chan1", "ep1", "", "", 0, 0, "", nil, nil, mediapackage.PackagingConfig{MssPackage: mss},
	)
	require.NoError(t, err)
	assert.Equal(t, mss, created.MssPackage)

	created.MssPackage.Encryption.SpekeKeyProvider.SystemIDs[0] = "corrupted"

	described, err := b.DescribeOriginEndpoint("ep1")
	require.NoError(t, err)
	assert.Equal(t, mss, described.MssPackage, "mutating a returned copy must not affect the stored value")
}
