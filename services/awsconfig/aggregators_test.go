package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_DeleteAggregationAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		accountID string
		region    string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutAggregationAuthorization("123456789012", "us-east-1"))
			},
			accountID: "123456789012",
			region:    "us-east-1",
		},
		{
			// Real AWS Config's DeleteAggregationAuthorization is idempotent (its
			// declared error model has no not-found exception), so deleting a
			// nonexistent authorization also succeeds.
			name:      "not_found_is_idempotent",
			accountID: "999999999999",
			region:    "eu-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			require.NoError(t, b.DeleteAggregationAuthorization(tt.accountID, tt.region))
		})
	}
}

func TestAWSConfigBackend_DeleteConfigurationAggregator(t *testing.T) {
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
				require.NoError(t, b.PutConfigurationAggregator("agg1", nil, nil))
			},
			delName: "agg1",
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

			err := b.DeleteConfigurationAggregator(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DescribeAggregationAuthorizations(t *testing.T) {
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
			name: "multiple_sorted",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutAggregationAuthorization("222222222222", "us-west-2"))
				require.NoError(t, b.PutAggregationAuthorization("111111111111", "us-east-1"))
				require.NoError(t, b.PutAggregationAuthorization("111111111111", "eu-west-1"))
			},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			auths := b.DescribeAggregationAuthorizations()
			assert.Len(t, auths, tt.wantCount)

			for i := 1; i < len(auths); i++ {
				prev := auths[i-1].AuthorizedAccountID + "#" + auths[i-1].AuthorizedAwsRegion
				curr := auths[i].AuthorizedAccountID + "#" + auths[i].AuthorizedAwsRegion
				assert.LessOrEqual(t, prev, curr)
			}
		})
	}
}

func TestAWSConfigBackend_DeleteAggregationAuthorization_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		accountID string
		region    string
	}{
		{
			name:      "empty_account_id_fails",
			accountID: "",
			region:    "us-east-1",
			wantErr:   awsconfig.ErrValidation,
		},
		{
			name:      "empty_region_fails",
			accountID: "123456789012",
			region:    "",
			wantErr:   awsconfig.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.DeleteAggregationAuthorization(tt.accountID, tt.region)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}
