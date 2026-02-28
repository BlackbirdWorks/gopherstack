package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/swf"
)

func TestSWFBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *swf.InMemoryBackend)
	}{
		{
			name: "RegisterDomain",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				err := b.RegisterDomain("my-domain", "test domain")
				require.NoError(t, err)

				domains := b.ListDomains("REGISTERED")
				require.Len(t, domains, 1)
				assert.Equal(t, "my-domain", domains[0].Name)
				assert.Equal(t, "REGISTERED", domains[0].Status)
			},
		},
		{
			name: "RegisterDomain/AlreadyExists",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				require.NoError(t, b.RegisterDomain("my-domain", ""))

				err := b.RegisterDomain("my-domain", "")
				require.Error(t, err)
				assert.ErrorIs(t, err, swf.ErrAlreadyExists)
			},
		},
		{
			name: "DeprecateDomain",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				require.NoError(t, b.RegisterDomain("my-domain", ""))

				err := b.DeprecateDomain("my-domain")
				require.NoError(t, err)

				domains := b.ListDomains("DEPRECATED")
				require.Len(t, domains, 1)
			},
		},
		{
			name: "DeprecateDomain/NotFound",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				err := b.DeprecateDomain("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, swf.ErrNotFound)
			},
		},
		{
			name: "RegisterWorkflowType",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				require.NoError(t, b.RegisterDomain("my-domain", ""))

				err := b.RegisterWorkflowType("my-domain", "my-workflow", "1.0")
				require.NoError(t, err)

				wts := b.ListWorkflowTypes("my-domain")
				require.Len(t, wts, 1)
				assert.Equal(t, "my-workflow", wts[0].Name)
			},
		},
		{
			name: "StartAndDescribeExecution",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				exec, err := b.StartWorkflowExecution("my-domain", "wf-001", "run-001")
				require.NoError(t, err)
				assert.Equal(t, "RUNNING", exec.Status)

				got, err := b.DescribeWorkflowExecution("my-domain", "wf-001")
				require.NoError(t, err)
				assert.Equal(t, "run-001", got.RunID)
			},
		},
		{
			name: "DescribeWorkflowExecution/NotFound",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				_, err := b.DescribeWorkflowExecution("my-domain", "nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, swf.ErrNotFound)
			},
		},
		{
			name: "ListDomains/AllStatuses",
			run: func(t *testing.T, b *swf.InMemoryBackend) {
				require.NoError(t, b.RegisterDomain("d1", ""))
				require.NoError(t, b.RegisterDomain("d2", ""))

				all := b.ListDomains("")
				assert.Len(t, all, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := swf.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}
