package mediastore_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

func newBackend() *mediastore.InMemoryBackend {
	return mediastore.NewInMemoryBackend()
}

func TestInMemoryBackend_CreateContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		container   string
		wantErr     bool
	}{
		{
			name:      "creates container successfully",
			container: "my-container",
		},
		{
			name:        "duplicate name returns already exists",
			container:   "dup-container",
			wantErr:     true,
			errSentinel: awserr.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if errors.Is(tt.errSentinel, awserr.ErrAlreadyExists) {
				_, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			c, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, tt.errSentinel == nil || errors.Is(err, tt.errSentinel))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.container, c.Name)
			assert.NotEmpty(t, c.ARN)
			assert.NotEmpty(t, c.Endpoint)
			assert.Equal(t, "ACTIVE", c.Status)
			assert.NotNil(t, c.CreationTime)
		})
	}
}

func TestInMemoryBackend_DeleteContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		container   string
		createFirst bool
		wantErr     bool
	}{
		{
			name:        "deletes existing container",
			container:   "to-delete",
			createFirst: true,
		},
		{
			name:        "not found returns error",
			container:   "missing",
			createFirst: false,
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.createFirst {
				_, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			err := b.DeleteContainer(tt.container)

			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, tt.errSentinel == nil || errors.Is(err, tt.errSentinel))

				return
			}

			require.NoError(t, err)

			_, err = b.DescribeContainer(tt.container)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_DescribeContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		container   string
		createFirst bool
		wantErr     bool
	}{
		{
			name:        "describes existing container",
			container:   "describe-me",
			createFirst: true,
		},
		{
			name:        "not found returns error",
			container:   "missing",
			createFirst: false,
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.createFirst {
				_, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			c, err := b.DescribeContainer(tt.container)

			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, tt.errSentinel == nil || errors.Is(err, tt.errSentinel))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.container, c.Name)
		})
	}
}

func TestInMemoryBackend_ListContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createN   int
		wantCount int
	}{
		{
			name:      "empty list",
			createN:   0,
			wantCount: 0,
		},
		{
			name:      "lists all containers",
			createN:   3,
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			for i := range tt.createN {
				_, err := b.CreateContainer(testRegion, testAccountID, fmt.Sprintf("container-%d", i), nil)
				require.NoError(t, err)
			}

			containers, err := b.ListContainers()
			require.NoError(t, err)
			assert.Len(t, containers, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_ContainerPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		container   string
		policy      string
		createFirst bool
		wantErr     bool
	}{
		{
			name:        "put and get policy",
			container:   "policy-container",
			policy:      `{"Version":"2012-10-17"}`,
			createFirst: true,
		},
		{
			name:        "get policy from missing container",
			container:   "missing",
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
		{
			name:        "get policy when none set",
			container:   "no-policy",
			createFirst: true,
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.createFirst {
				_, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			if tt.policy != "" {
				err := b.PutContainerPolicy(tt.container, tt.policy)
				require.NoError(t, err)
			}

			policy, err := b.GetContainerPolicy(tt.container)

			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, tt.errSentinel == nil || errors.Is(err, tt.errSentinel))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.policy, policy)
		})
	}
}

func TestInMemoryBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags        map[string]string
		wantTags    map[string]string
		name        string
		container   string
		removeKeys  []string
		createFirst bool
	}{
		{
			name:        "tag and untag resource",
			container:   "tag-me",
			tags:        map[string]string{"env": "test", "team": "backend"},
			removeKeys:  []string{"team"},
			wantTags:    map[string]string{"env": "test"},
			createFirst: true,
		},
		{
			name:        "list tags on empty resource",
			container:   "empty-tags",
			tags:        nil,
			wantTags:    map[string]string{},
			createFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)
			require.NoError(t, err)

			c, descErr := b.DescribeContainer(tt.container)
			require.NoError(t, descErr)

			if len(tt.tags) > 0 {
				err = b.TagResource(c.ARN, tt.tags)
				require.NoError(t, err)
			}

			if len(tt.removeKeys) > 0 {
				err = b.UntagResource(c.ARN, tt.removeKeys)
				require.NoError(t, err)
			}

			tags, err := b.ListTagsForResource(c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestInMemoryBackend_AccessLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		container   string
		start       bool
		wantEnabled bool
	}{
		{
			name:        "start access logging",
			container:   "log-me",
			start:       true,
			wantEnabled: true,
		},
		{
			name:        "stop access logging",
			container:   "no-log",
			start:       false,
			wantEnabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)
			require.NoError(t, err)

			if tt.start {
				require.NoError(t, b.StartAccessLogging(tt.container))
			} else {
				require.NoError(t, b.StartAccessLogging(tt.container))
				require.NoError(t, b.StopAccessLogging(tt.container))
			}

			c, err := b.DescribeContainer(tt.container)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, c.AccessLoggingEnabled)
		})
	}
}

func TestInMemoryBackend_DescribeContainer_ReturnsCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating returned container does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateContainer(testRegion, testAccountID, "copy-test", map[string]string{"k": "v"})
			require.NoError(t, err)

			c, err := b.DescribeContainer("copy-test")
			require.NoError(t, err)

			// Mutate the returned copy.
			c.Tags["injected"] = "evil"
			c.Status = "MUTATED"

			// Backend state must be unchanged.
			c2, err := b.DescribeContainer("copy-test")
			require.NoError(t, err)
			assert.Equal(t, "ACTIVE", c2.Status)
			_, hasInjected := c2.Tags["injected"]
			assert.False(t, hasInjected, "mutating returned container must not affect backend state")
		})
	}
}

func TestInMemoryBackend_ListContainers_ReturnsCopies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating listed containers does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateContainer(testRegion, testAccountID, "list-copy-a", map[string]string{"key": "val"})
			require.NoError(t, err)

			all, err := b.ListContainers()
			require.NoError(t, err)
			require.Len(t, all, 1)

			// Mutate returned copy.
			all[0].Tags["injected"] = "evil"

			// Backend state must be unchanged.
			all2, err := b.ListContainers()
			require.NoError(t, err)
			require.Len(t, all2, 1)
			_, hasInjected := all2[0].Tags["injected"]
			assert.False(t, hasInjected, "mutating listed container must not affect backend state")
		})
	}
}

func TestInMemoryBackend_MetricPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		setup       func(b *mediastore.InMemoryBackend)
		name        string
		container   string
		policy      mediastore.MetricPolicy
		wantErr     bool
	}{
		{
			name:      "put and get metric policy",
			container: "metric-container",
			policy: mediastore.MetricPolicy{
				ContainerLevelMetrics: "ENABLED",
			},
		},
		{
			name:        "get metric policy on missing container",
			container:   "missing",
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
		{
			name:      "get metric policy when none set",
			container: "no-metric",
			setup: func(b *mediastore.InMemoryBackend) {
				_, err := b.CreateContainer(testRegion, testAccountID, "no-metric", nil)
				if err != nil {
					panic(err)
				}
			},
			wantErr:     true,
			errSentinel: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			if tt.wantErr {
				_, err := b.GetMetricPolicy(tt.container)
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errSentinel)

				return
			}

			_, err := b.CreateContainer(testRegion, testAccountID, tt.container, nil)
			require.NoError(t, err)

			err = b.PutMetricPolicy(tt.container, tt.policy)
			require.NoError(t, err)

			got, err := b.GetMetricPolicy(tt.container)
			require.NoError(t, err)
			assert.Equal(t, tt.policy.ContainerLevelMetrics, got.ContainerLevelMetrics)

			err = b.DeleteMetricPolicy(tt.container)
			require.NoError(t, err)

			_, err = b.GetMetricPolicy(tt.container)
			require.Error(t, err)
		})
	}
}
