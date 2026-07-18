package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_DeleteConformancePack(t *testing.T) {
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
				require.NoError(t, b.PutConformancePack("my-pack", "", ""))
			},
			delName: "my-pack",
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

			err := b.DeleteConformancePack(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDescribeConformancePackStatus(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutConformancePack("pack1", "", "")

	statuses := b.DescribeConformancePackStatus(nil)
	if len(statuses) != 1 || statuses[0].ConformancePackName != "pack1" {
		t.Fatalf("DescribeConformancePackStatus: %v", statuses)
	}

	if statuses[0].ConformancePackState != "COMPLETE" {
		t.Fatalf("expected COMPLETE state, got %q", statuses[0].ConformancePackState)
	}
}

func TestDescribeConformancePackCompliance(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	out := b.DescribeConformancePackCompliance("pack1")
	if len(out) != 0 {
		t.Fatalf("expected empty, got %v", out)
	}
}
