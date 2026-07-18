package mediastore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

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
				_, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			c, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)

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
				_, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			err := b.DeleteContainer(context.Background(), tt.container)

			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, tt.errSentinel == nil || errors.Is(err, tt.errSentinel))

				return
			}

			require.NoError(t, err)

			_, err = b.DescribeContainer(context.Background(), tt.container)
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
				_, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			c, err := b.DescribeContainer(context.Background(), tt.container)

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
				_, err := b.CreateContainer(context.Background(), testAccountID, fmt.Sprintf("container-%d", i), nil)
				require.NoError(t, err)
			}

			containers, _, err := b.ListContainers(context.Background(), "", 0)
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
				_, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)
				require.NoError(t, err)
			}

			if tt.policy != "" {
				err := b.PutContainerPolicy(context.Background(), tt.container, tt.policy)
				require.NoError(t, err)
			}

			policy, err := b.GetContainerPolicy(context.Background(), tt.container)

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

			_, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)
			require.NoError(t, err)

			if tt.start {
				require.NoError(t, b.StartAccessLogging(context.Background(), tt.container))
			} else {
				require.NoError(t, b.StartAccessLogging(context.Background(), tt.container))
				require.NoError(t, b.StopAccessLogging(context.Background(), tt.container))
			}

			c, err := b.DescribeContainer(context.Background(), tt.container)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, c.AccessLoggingEnabled)
		})
	}
}
