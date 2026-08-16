package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestSchemaRegistry_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	reg, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{
		RegistryName: "my-registry",
		Description:  "test registry",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-registry", reg.RegistryName)
	assert.NotEmpty(t, reg.RegistryArn)
	assert.Equal(t, "test registry", reg.Description)

	described, err := b.DescribeRegistry(context.Background(), "my-registry")
	require.NoError(t, err)
	assert.Equal(t, "my-registry", described.RegistryName)

	updated, err := b.UpdateRegistry(context.Background(), eventbridge.UpdateRegistryInput{
		RegistryName: "my-registry",
		Description:  "updated description",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", updated.Description)

	registries, _, err := b.ListRegistries(context.Background(), "my-", "")
	require.NoError(t, err)
	assert.Len(t, registries, 1)

	err = b.DeleteRegistry(context.Background(), "my-registry")
	require.NoError(t, err)

	_, err = b.DescribeRegistry(context.Background(), "my-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestSchemaRegistry_DuplicateCreateFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "dup-registry"})
	require.NoError(t, err)

	_, err = b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "dup-registry"})
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestSchemaRegistry_NotFoundErrors(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.DescribeRegistry(context.Background(), "no-such-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	err = b.DeleteRegistry(context.Background(), "no-such-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	_, err = b.UpdateRegistry(context.Background(), eventbridge.UpdateRegistryInput{RegistryName: "no-such-registry"})
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestDeleteBuiltinRegistry_Forbidden(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		registryName string
		wantForbid   bool
	}{
		{name: "aws.events cannot be deleted", registryName: "aws.events", wantForbid: true},
		{name: "discovered-schemas cannot be deleted", registryName: "discovered-schemas", wantForbid: true},
		{name: "user registry can be deleted", registryName: "my-custom-registry", wantForbid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if !tt.wantForbid {
				_, err := b.CreateRegistry(
					context.Background(),
					eventbridge.CreateRegistryInput{RegistryName: tt.registryName},
				)
				require.NoError(t, err)
			}

			err := b.DeleteRegistry(context.Background(), tt.registryName)

			if tt.wantForbid {
				require.Error(t, err)
				assert.ErrorIs(t, err, eventbridge.ErrForbiddenOperation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateBuiltinRegistry_Forbidden(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		registryName string
		wantForbid   bool
	}{
		{name: "aws.events cannot be created", registryName: "aws.events", wantForbid: true},
		{name: "discovered-schemas cannot be created", registryName: "discovered-schemas", wantForbid: true},
		{name: "user registry can be created", registryName: "my-valid-registry", wantForbid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateRegistry(
				context.Background(),
				eventbridge.CreateRegistryInput{RegistryName: tt.registryName},
			)

			if tt.wantForbid {
				require.Error(t, err)
				assert.ErrorIs(t, err, eventbridge.ErrForbiddenOperation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
