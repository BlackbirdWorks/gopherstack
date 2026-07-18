package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_DeleteOrganizationConfigRule(t *testing.T) {
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
				require.NoError(t, b.PutOrganizationConfigRule("org-rule"))
			},
			delName: "org-rule",
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

			err := b.DeleteOrganizationConfigRule(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteOrganizationConformancePack(t *testing.T) {
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
				require.NoError(t, b.PutOrganizationConformancePack("org-pack"))
			},
			delName: "org-pack",
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

			err := b.DeleteOrganizationConformancePack(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDescribeOrganizationConfigRuleStatuses(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutOrganizationConfigRule("org-rule1")

	statuses := b.DescribeOrganizationConfigRuleStatuses(nil)
	if len(statuses) != 1 || statuses[0].OrganizationConfigRuleName != "org-rule1" {
		t.Fatalf("DescribeOrganizationConfigRuleStatuses: %v", statuses)
	}

	if statuses[0].OrganizationRuleStatus != "CREATE_SUCCESSFUL" {
		t.Fatalf("expected CREATE_SUCCESSFUL, got %q", statuses[0].OrganizationRuleStatus)
	}
}

func TestDescribeOrganizationConformancePackStatuses(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutOrganizationConformancePack("org-pack1")

	statuses := b.DescribeOrganizationConformancePackStatuses(nil)
	if len(statuses) != 1 || statuses[0].OrganizationConformancePackName != "org-pack1" {
		t.Fatalf("DescribeOrganizationConformancePackStatuses: %v", statuses)
	}

	if statuses[0].Status != "CREATE_SUCCESSFUL" {
		t.Fatalf("expected CREATE_SUCCESSFUL, got %q", statuses[0].Status)
	}
}

func TestGetOrganizationCustomRulePolicy_DefaultEmpty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	policy := b.GetOrganizationCustomRulePolicy("org-rule")
	if policy != "" {
		t.Fatalf("expected empty policy, got %q", policy)
	}
}
