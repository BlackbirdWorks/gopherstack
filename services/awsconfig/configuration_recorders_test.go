package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recName    string
		roleARN    string
		wantName   string
		wantStatus string
		wantLen    int
	}{
		{
			name:       "success",
			recName:    "default",
			roleARN:    "arn:aws:iam::000000000000:role/config",
			wantLen:    1,
			wantName:   "default",
			wantStatus: "PENDING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.PutConfigurationRecorder(tt.recName, tt.roleARN, nil)
			require.NoError(t, err)

			recorders := b.DescribeConfigurationRecorders(nil)
			require.Len(t, recorders, tt.wantLen)
			assert.Equal(t, tt.wantName, recorders[0].Name)
			assert.Equal(t, tt.wantStatus, recorders[0].Status)
		})
	}
}

func TestAWSConfigBackend_StartConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recName    string
		setup      func(t *testing.T, b *awsconfig.InMemoryBackend)
		wantErr    error
		wantStatus string
	}{
		{
			name:    "success",
			recName: "default",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))
				require.NoError(t, b.PutDeliveryChannel("default", "my-bucket", "", "", nil))
			},
			wantStatus: "ACTIVE",
		},
		{
			name:    "not_found",
			recName: "nonexistent",
			wantErr: awsconfig.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.StartConfigurationRecorder(tt.recName)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			recorders := b.DescribeConfigurationRecorders(nil)
			require.Len(t, recorders, 1)
			assert.Equal(t, tt.wantStatus, recorders[0].Status)
		})
	}
}

func TestAWSConfigBackend_DescribeConfigurationRecorders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "one_recorder",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			recorders := b.DescribeConfigurationRecorders(nil)
			assert.Len(t, recorders, tt.wantCount)
		})
	}
}

func TestAWSConfigBackend_AssociateResourceTypes(t *testing.T) {
	t.Parallel()

	t.Run("known_recorder_by_name_mutates_recording_group", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))

		recorder, err := b.AssociateResourceTypes("default", []string{"AWS::EC2::Instance"})
		require.NoError(t, err)
		require.NotNil(t, recorder)
		assert.Equal(t, "default", recorder.Name)
		assert.Contains(t, recorder.Arn, "config-recorder/default")
		require.NotNil(t, recorder.RecordingGroup)
		assert.Equal(t, []string{"AWS::EC2::Instance"}, recorder.RecordingGroup.ResourceTypes)

		// The mutation is persisted on the backend, not just the returned copy.
		recs := b.DescribeConfigurationRecorders([]string{"default"})
		require.Len(t, recs, 1)
		require.NotNil(t, recs[0].RecordingGroup)
		assert.Equal(t, []string{"AWS::EC2::Instance"}, recs[0].RecordingGroup.ResourceTypes)
	})

	t.Run("known_recorder_by_full_arn", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackendWithMeta("000000000000", "us-east-1")
		require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))

		recorder, err := b.AssociateResourceTypes(
			"arn:aws:config:us-east-1:000000000000:config-recorder/default",
			[]string{"AWS::S3::Bucket"},
		)
		require.NoError(t, err)
		require.NotNil(t, recorder)
		assert.Equal(t, "default", recorder.Name)
	})

	t.Run("dedups_repeated_resource_types", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))

		_, err := b.AssociateResourceTypes("default", []string{"AWS::EC2::Instance"})
		require.NoError(t, err)
		recorder, err := b.AssociateResourceTypes("default", []string{"AWS::EC2::Instance", "AWS::S3::Bucket"})
		require.NoError(t, err)
		assert.Equal(t, []string{"AWS::EC2::Instance", "AWS::S3::Bucket"}, recorder.RecordingGroup.ResourceTypes)
	})

	t.Run("unknown_recorder_errors_not_found", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		_, err := b.AssociateResourceTypes(
			"arn:aws:config:us-east-1:000000000000:config-recorder/unknown",
			[]string{"AWS::S3::Bucket"},
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNotFound)
	})
}

func TestAWSConfigBackend_DisassociateResourceTypes(t *testing.T) {
	t.Parallel()

	t.Run("removes_resource_types", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))
		_, err := b.AssociateResourceTypes("default", []string{"AWS::EC2::Instance", "AWS::S3::Bucket"})
		require.NoError(t, err)

		require.NoError(t, b.DisassociateResourceTypes("default", []string{"AWS::EC2::Instance"}))

		recs := b.DescribeConfigurationRecorders([]string{"default"})
		require.Len(t, recs, 1)
		require.NotNil(t, recs[0].RecordingGroup)
		assert.Equal(t, []string{"AWS::S3::Bucket"}, recs[0].RecordingGroup.ResourceTypes)
	})

	t.Run("unknown_recorder_errors_not_found", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		err := b.DisassociateResourceTypes("unknown", []string{"AWS::EC2::Instance"})
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNotFound)
	})

	t.Run("empty_arn_is_validation_error", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		err := b.DisassociateResourceTypes("", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrValidation)
	})
}

func TestAWSConfigBackend_StopConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *awsconfig.InMemoryBackend)
		name       string
		recName    string
		wantErr    error
		wantStatus string
	}{
		{
			name: "stops_active_recorder",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::123:role/r", nil))
				require.NoError(t, b.PutDeliveryChannel("default", "my-bucket", "", "", nil))
				require.NoError(t, b.StartConfigurationRecorder("default"))
			},
			recName:    "default",
			wantStatus: "PENDING",
		},
		{
			name:    "not_found",
			recName: "nonexistent",
			wantErr: awsconfig.ErrNotFound,
		},
		{
			name:    "empty_name_returns_validation_error",
			recName: "",
			wantErr: awsconfig.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.StopConfigurationRecorder(tt.recName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			statuses := b.DescribeConfigurationRecorderStatus(nil)
			require.Len(t, statuses, 1)
			assert.False(t, statuses[0].Recording)
		})
	}
}

func TestAWSConfigBackend_PutConfigurationRecorder_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		recName string
		roleARN string
	}{
		{
			name:    "empty_name_fails",
			recName: "",
			roleARN: "arn:aws:iam::000000000000:role/r",
			wantErr: awsconfig.ErrValidation,
		},
		{
			name:    "empty_roleARN_fails",
			recName: "default",
			roleARN: "",
			wantErr: awsconfig.ErrValidation,
		},
		{
			name:    "update_preserves_status",
			recName: "default",
			roleARN: "arn:aws:iam::000000000000:role/new",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.name == "update_preserves_status" {
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/old", nil))
				require.NoError(t, b.PutDeliveryChannel("default", "bucket", "", "", nil))
				require.NoError(t, b.StartConfigurationRecorder("default"))
			}

			err := b.PutConfigurationRecorder(tt.recName, tt.roleARN, nil)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.name == "update_preserves_status" {
				recorders := b.DescribeConfigurationRecorders(nil)
				require.Len(t, recorders, 1)
				assert.Equal(t, "ACTIVE", recorders[0].Status)
				assert.Equal(t, tt.roleARN, recorders[0].RoleARN)
			}
		})
	}
}

func TestAWSConfigBackend_DescribeConfigurationRecorders_NameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    []string
		wantCount int
	}{
		{
			name:      "no_filter_returns_all_sorted",
			wantCount: 3,
		},
		{
			name:      "filter_one",
			filter:    []string{"rec-a"},
			wantCount: 1,
		},
		{
			name:      "filter_nonexistent",
			filter:    []string{"no-such"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			require.NoError(t, b.PutConfigurationRecorder("rec-c", "arn:aws:iam::123:role/r", nil))
			require.NoError(t, b.PutConfigurationRecorder("rec-a", "arn:aws:iam::123:role/r", nil))
			require.NoError(t, b.PutConfigurationRecorder("rec-b", "arn:aws:iam::123:role/r", nil))

			recs := b.DescribeConfigurationRecorders(tt.filter)
			assert.Len(t, recs, tt.wantCount)

			if tt.wantCount > 1 && len(tt.filter) == 0 {
				// verify sorted
				for i := 1; i < len(recs); i++ {
					assert.Less(t, recs[i-1].Name, recs[i].Name)
				}
			}
		})
	}
}

func TestAWSConfigBackend_AssociateResourceTypes_EmptyARN(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_, err := b.AssociateResourceTypes("", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awsconfig.ErrValidation)
}

func TestAWSConfigBackend_DeleteConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("rec1", "arn:aws:iam::000000000000:role/r", nil))
			},
			delName: "rec1",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteConfigurationRecorder(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
