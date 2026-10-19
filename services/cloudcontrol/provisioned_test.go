package cloudcontrol_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

type fakeProvisioner struct {
	failNew error
	live    map[string]map[string]any
	nextID  int
	inPlace bool
}

func newFakeProvisioner() *fakeProvisioner {
	return &fakeProvisioner{live: map[string]map[string]any{}}
}

func (f *fakeProvisioner) Provision(_ context.Context, _ string, props map[string]any) (string, error) {
	if f.failNew != nil {
		return "", f.failNew
	}

	f.nextID++
	id := fmt.Sprintf("id-%d", f.nextID)
	f.live[id] = props

	return id, nil
}

func (f *fakeProvisioner) Update(_ context.Context, typeName, id string, _, desired map[string]any) error {
	if !f.inPlace {
		return fmt.Errorf("%w: %s is immutable", cloudcontrol.ErrNotUpdatable, typeName)
	}

	f.live[id] = desired

	return nil
}

func (f *fakeProvisioner) Deprovision(_ context.Context, _, id string, _ map[string]any) error {
	delete(f.live, id)

	return nil
}

func TestProvisionedType_Lifecycle(t *testing.T) {
	t.Parallel()

	const typeName = "AWS::Test::Widget"

	b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
	fp := newFakeProvisioner()
	fp.inPlace = true
	b.RegisterProvisionedTypes(fp, []string{typeName})

	ev, err := b.CreateResourceContext(t.Context(), typeName, `{"Name":"w"}`, "")
	require.NoError(t, err)
	id := ev.Identifier
	require.Contains(t, fp.live, id, "the provisioner must hold the created resource")

	got, err := b.GetResourceContext(t.Context(), typeName, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Name":"w"}`, got.Properties)

	_, err = b.UpdateResourceContext(t.Context(), typeName, id, `[{"op":"add","path":"/Size","value":3}]`, "")
	require.NoError(t, err)
	assert.EqualValues(t, 3, fp.live[id]["Size"])

	res, _, err := b.ListResourcesContext(t.Context(), typeName, 0, "", "")
	require.NoError(t, err)
	require.Len(t, res, 1)

	_, err = b.DeleteResourceContext(t.Context(), typeName, id, "")
	require.NoError(t, err)
	assert.NotContains(t, fp.live, id)

	_, err = b.GetResourceContext(t.Context(), typeName, id)
	require.ErrorIs(t, err, cloudcontrol.ErrNotFound)
}

func TestProvisionedType_Errors(t *testing.T) {
	t.Parallel()

	const typeName = "AWS::Test::Widget"

	tests := []struct {
		wantErr error
		run     func(b *cloudcontrol.InMemoryBackend, id string) error
		name    string
	}{
		{
			name: "update_without_in_place_support",
			run: func(b *cloudcontrol.InMemoryBackend, id string) error {
				_, err := b.UpdateResourceContext(t.Context(), typeName, id,
					`[{"op":"replace","path":"/Name","value":"x"}]`, "")

				return err
			},
			wantErr: cloudcontrol.ErrNotUpdatable,
		},
		{
			name: "get_missing",
			run: func(b *cloudcontrol.InMemoryBackend, _ string) error {
				_, err := b.GetResourceContext(t.Context(), typeName, "nope")

				return err
			},
			wantErr: cloudcontrol.ErrNotFound,
		},
		{
			name: "delete_missing",
			run: func(b *cloudcontrol.InMemoryBackend, _ string) error {
				_, err := b.DeleteResourceContext(t.Context(), typeName, "nope", "")

				return err
			},
			wantErr: cloudcontrol.ErrNotFound,
		},
		{
			name: "provision_failure_passes_through",
			run: func(b *cloudcontrol.InMemoryBackend, _ string) error {
				_, err := b.CreateResourceContext(t.Context(), "AWS::Test::Broken", `{}`, "")

				return err
			},
			wantErr: errBoom,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
			b.RegisterProvisionedTypes(newFakeProvisioner(), []string{typeName})

			broken := newFakeProvisioner()
			broken.failNew = errBoom
			b.RegisterProvisionedTypes(broken, []string{"AWS::Test::Broken"})

			ev, err := b.CreateResourceContext(t.Context(), typeName, `{"Name":"w"}`, "")
			require.NoError(t, err)

			require.ErrorIs(t, tc.run(b, ev.Identifier), tc.wantErr)
		})
	}
}

var errBoom = errors.New("backend refused")

func TestRejectUnprovisionedAWSTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		typeName string
		strict   bool
	}{
		{name: "lax_aws_type_uses_generic_store", typeName: "AWS::Some::Type", strict: false},
		{
			name:     "strict_aws_type_unsupported",
			typeName: "AWS::Some::Type",
			strict:   true,
			wantErr:  cloudcontrol.ErrUnsupportedAction,
		},
		{name: "strict_registered_type_ok", typeName: "AWS::Test::Widget", strict: true},
		{name: "strict_non_aws_type_uses_generic_store", typeName: "MyOrg::Svc::Res", strict: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1")
			b.RegisterProvisionedTypes(newFakeProvisioner(), []string{"AWS::Test::Widget"})

			if tc.strict {
				b.RejectUnprovisionedAWSTypes()
			}

			_, err := b.CreateResourceContext(t.Context(), tc.typeName, `{"Name":"a"}`, "")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
