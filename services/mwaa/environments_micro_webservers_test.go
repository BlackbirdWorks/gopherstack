package mwaa_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// TestCreateEnvironment_MicroWebserversDefault verifies mw1.micro defaults
// MaxWebservers/MinWebservers to 1, not the 2 every other class defaults to.
// AWS: "Defaults to 2 for all environment sizes except mw1.micro, which
// defaults to 1" (aws-sdk-go-v2/service/mwaa@v1.43.4/types/types.go's
// MaxWebservers/MinWebservers doc comments).
func TestCreateEnvironment_MicroWebserversDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		class         string
		wantMaxWebSrv int32
		wantMinWebSrv int32
	}{
		{name: "micro_defaults_to_one", class: "mw1.micro", wantMaxWebSrv: 1, wantMinWebSrv: 1},
		{name: "small_defaults_to_two", class: "mw1.small", wantMaxWebSrv: 2, wantMinWebSrv: 2},
		{name: "empty_class_defaults_to_two", class: "", wantMaxWebSrv: 2, wantMinWebSrv: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env, err := b.CreateEnvironment(
				context.Background(),
				"env-"+tt.name,
				&mwaa.ExportedCreateEnvironmentRequest{
					DagS3Path:            "dags/",
					ExecutionRoleArn:     "arn:aws:iam::123456789012:role/role",
					SourceBucketArn:      "arn:aws:s3:::bucket",
					NetworkConfiguration: testNetworkConfig(),
					EnvironmentClass:     tt.class,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantMaxWebSrv, env.MaxWebservers)
			assert.Equal(t, tt.wantMinWebSrv, env.MinWebservers)
		})
	}
}

// TestCreateEnvironment_MicroWebserversRange verifies mw1.micro rejects
// MaxWebservers/MinWebservers values outside 1: AWS's documented "2 to 5"
// range is explicitly scoped to "environments larger than mw1.micro"
// (same doc comment as above), so mw1.micro must not accept it.
func TestCreateEnvironment_MicroWebserversRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		class         string
		maxWebservers int32
		minWebservers int32
		wantErr       bool
	}{
		{name: "micro_explicit_one_ok", class: "mw1.micro", maxWebservers: 1, minWebservers: 1, wantErr: false},
		{name: "micro_two_rejected", class: "mw1.micro", maxWebservers: 2, minWebservers: 1, wantErr: true},
		{name: "micro_five_rejected", class: "mw1.micro", maxWebservers: 5, minWebservers: 1, wantErr: true},
		{name: "small_two_ok", class: "mw1.small", maxWebservers: 2, minWebservers: 2, wantErr: false},
		{name: "small_five_ok", class: "mw1.small", maxWebservers: 5, minWebservers: 2, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "env-"+tt.name, &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:            "dags/",
				ExecutionRoleArn:     "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:      "arn:aws:s3:::bucket",
				NetworkConfiguration: testNetworkConfig(),
				EnvironmentClass:     tt.class,
				MaxWebservers:        tt.maxWebservers,
				MinWebservers:        tt.minWebservers,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpdateEnvironment_MicroWebserversRange verifies the same mw1.micro
// range restriction applies to UpdateEnvironment, using the effective
// EnvironmentClass (the request's, if it changes class, else the persisted
// environment's).
func TestUpdateEnvironment_MicroWebserversRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		createClass     string
		updateClass     string
		updateMaxWebSrv int32
		updateMinWebSrv int32
		wantErr         bool
	}{
		{
			name: "existing_micro_rejects_two", createClass: "mw1.micro", updateClass: "",
			updateMaxWebSrv: 2, updateMinWebSrv: 1, wantErr: true,
		},
		{
			name: "existing_micro_accepts_one", createClass: "mw1.micro", updateClass: "",
			updateMaxWebSrv: 1, updateMinWebSrv: 1, wantErr: false,
		},
		{
			name: "switching_to_micro_rejects_two", createClass: "mw1.small", updateClass: "mw1.micro",
			updateMaxWebSrv: 2, updateMinWebSrv: 1, wantErr: true,
		},
		{
			name: "non_micro_accepts_two", createClass: "mw1.small", updateClass: "",
			updateMaxWebSrv: 2, updateMinWebSrv: 2, wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "env-"+tt.name, &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:            "dags/",
				ExecutionRoleArn:     "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:      "arn:aws:s3:::bucket",
				NetworkConfiguration: testNetworkConfig(),
				EnvironmentClass:     tt.createClass,
			})
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "env-"+tt.name) // promote CREATING -> AVAILABLE

			_, err = b.UpdateEnvironment(context.Background(), "env-"+tt.name, &mwaa.ExportedUpdateEnvironmentRequest{
				EnvironmentClass: tt.updateClass,
				MaxWebservers:    tt.updateMaxWebSrv,
				MinWebservers:    tt.updateMinWebSrv,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
