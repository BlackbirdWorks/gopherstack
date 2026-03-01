package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/swf"
)

func TestRegisterDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		preRegister []string
		domain      string
		description string
		wantErr     error
		wantCount   int
		wantName    string
		wantStatus  string
	}{
		{
			name:        "success",
			domain:      "my-domain",
			description: "test domain",
			wantCount:   1,
			wantName:    "my-domain",
			wantStatus:  "REGISTERED",
		},
		{
			name:        "AlreadyExists",
			preRegister: []string{"my-domain"},
			domain:      "my-domain",
			wantErr:     swf.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := swf.NewInMemoryBackend()
			for _, d := range tt.preRegister {
				require.NoError(t, b.RegisterDomain(d, ""))
			}

			err := b.RegisterDomain(tt.domain, tt.description)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			domains := b.ListDomains("REGISTERED")
			require.Len(t, domains, tt.wantCount)
			assert.Equal(t, tt.wantName, domains[0].Name)
			assert.Equal(t, tt.wantStatus, domains[0].Status)
		})
	}
}

func TestDeprecateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		preRegister bool
		domain      string
		wantErr     error
		wantCount   int
	}{
		{
			name:        "success",
			preRegister: true,
			domain:      "my-domain",
			wantCount:   1,
		},
		{
			name:    "NotFound",
			domain:  "nonexistent",
			wantErr: swf.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := swf.NewInMemoryBackend()
			if tt.preRegister {
				require.NoError(t, b.RegisterDomain(tt.domain, ""))
			}

			err := b.DeprecateDomain(tt.domain)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			domains := b.ListDomains("DEPRECATED")
			require.Len(t, domains, tt.wantCount)
		})
	}
}

func TestRegisterWorkflowType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("my-domain", ""))

	err := b.RegisterWorkflowType("my-domain", "my-workflow", "1.0")
	require.NoError(t, err)

	wts := b.ListWorkflowTypes("my-domain")
	require.Len(t, wts, 1)
	assert.Equal(t, "my-workflow", wts[0].Name)
}

func TestWorkflowExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		startFirst bool
		domain     string
		workflowID string
		runID      string
		wantErr    error
		wantStatus string
		wantRunID  string
	}{
		{
			name:       "StartAndDescribe",
			startFirst: true,
			domain:     "my-domain",
			workflowID: "wf-001",
			runID:      "run-001",
			wantStatus: "RUNNING",
			wantRunID:  "run-001",
		},
		{
			name:       "DescribeNotFound",
			domain:     "my-domain",
			workflowID: "nonexistent",
			wantErr:    swf.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := swf.NewInMemoryBackend()

			if tt.startFirst {
				exec, err := b.StartWorkflowExecution(tt.domain, tt.workflowID, tt.runID)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, exec.Status)
			}

			got, err := b.DescribeWorkflowExecution(tt.domain, tt.workflowID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantRunID, got.RunID)
		})
	}
}

func TestListDomains(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("d1", ""))
	require.NoError(t, b.RegisterDomain("d2", ""))

	all := b.ListDomains("")
	assert.Len(t, all, 2)
}
