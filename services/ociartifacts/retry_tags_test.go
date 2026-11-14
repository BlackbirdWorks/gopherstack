package ociartifacts_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env, name string) (string, error)
		remove func(t *testing.T, e *env, id string)
		name   string
	}{
		{
			name: "container repository",
			create: func(t *testing.T, e *env, name string) (string, error) {
				t.Helper()

				r, err := e.client.CreateContainerRepository(t.Context(), artifacts.CreateContainerRepositoryRequest{
					OpcRetryToken: new("tok"),
					CompartmentId: new(testCompartment), DisplayName: new(name),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteContainerRepository(t.Context(), artifacts.DeleteContainerRepositoryRequest{
					RepositoryId: &id,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "generic repository",
			create: func(t *testing.T, e *env, name string) (string, error) {
				t.Helper()

				r, err := e.client.CreateRepository(t.Context(), artifacts.CreateRepositoryRequest{
					OpcRetryToken: new("tok"),
					CreateRepositoryDetails: artifacts.CreateGenericRepositoryDetails{
						CompartmentId: new(testCompartment), IsImmutable: new(false), DisplayName: new(name),
					},
				})
				if err != nil {
					return "", err
				}

				return *r.Repository.(artifacts.GenericRepository).Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteRepository(t.Context(), artifacts.DeleteRepositoryRequest{RepositoryId: &id})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			id2, err := tt.create(t, e, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			id2, err := tt.create(t, fresh, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			id2, err := tt.create(t, e, "rt2")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			tt.remove(t, e, id)

			_, err = tt.create(t, e, "rt")
			require.Error(t, err)
			assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		free   map[string]string
		defd   map[string]map[string]any
		name   string
		code   string
		status int
	}{
		{
			name: "valid", free: map[string]string{"a": "b"},
			defd: map[string]map[string]any{"Ops": {"Cost": "1"}}, status: http.StatusOK,
		},
		{
			name: "long value", free: map[string]string{"a": strings.Repeat("x", 257)},
			status: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "unknown namespace", defd: map[string]map[string]any{"Nope": {"k": "v"}},
			status: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
				return ocitags.Def{}, strings.EqualFold(ns, "ops") && strings.EqualFold(key, "cost")
			})

			repo := e.createRepo(t, "base", nil)
			gen := e.createGenericRepo(t, false)

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.client.CreateContainerRepository(t.Context(), artifacts.CreateContainerRepositoryRequest{
				CompartmentId: new(testCompartment), DisplayName: new("tagged"),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.client.CreateRepository(t.Context(), artifacts.CreateRepositoryRequest{
				CreateRepositoryDetails: artifacts.CreateGenericRepositoryDetails{
					CompartmentId: new(testCompartment), IsImmutable: new(false), DisplayName: new("taggedgen"),
					FreeformTags: tt.free, DefinedTags: tt.defd,
				},
			})
			check(err)

			_, err = e.client.UpdateContainerRepository(t.Context(), artifacts.UpdateContainerRepositoryRequest{
				RepositoryId: repo.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.client.UpdateRepository(t.Context(), artifacts.UpdateRepositoryRequest{
				RepositoryId: gen.Id,
				UpdateRepositoryDetails: artifacts.UpdateGenericRepositoryDetails{
					FreeformTags: tt.free, DefinedTags: tt.defd,
				},
			})
			check(err)
		})
	}
}

func TestMutationRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *env, imageID *string, repoID, genericID *string) error
		name string
	}{
		{
			name: "remove version",
			run: func(t *testing.T, e *env, imageID, _, _ *string) error {
				t.Helper()

				_, err := e.client.RemoveContainerVersion(t.Context(), artifacts.RemoveContainerVersionRequest{
					ImageId: imageID, Version: new("extra"), OpcRetryToken: new("tok"),
				})

				return err
			},
		},
		{
			name: "restore image",
			run: func(t *testing.T, e *env, imageID, _, _ *string) error {
				t.Helper()

				_, err := e.client.RestoreContainerImage(t.Context(), artifacts.RestoreContainerImageRequest{
					ImageId: imageID, OpcRetryToken: new("tok"),
				})

				return err
			},
		},
		{
			name: "move container repository",
			run: func(t *testing.T, e *env, _, repoID, _ *string) error {
				t.Helper()

				_, err := e.client.ChangeContainerRepositoryCompartment(
					t.Context(), artifacts.ChangeContainerRepositoryCompartmentRequest{
						RepositoryId: repoID, CompartmentId: new(otherCompart), OpcRetryToken: new("tok"),
					})

				return err
			},
		},
		{
			name: "move generic repository",
			run: func(t *testing.T, e *env, _, _, genericID *string) error {
				t.Helper()

				_, err := e.client.ChangeRepositoryCompartment(
					t.Context(),
					artifacts.ChangeRepositoryCompartmentRequest{
						RepositoryId:  genericID,
						CompartmentId: new(otherCompart), OpcRetryToken: new("tok"),
					},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			repo := e.createRepo(t, "retry-mut", nil)
			gen := e.createGenericRepo(t, false)

			c := e.registry(t)
			name := testNamespace + "/retry-mut"
			c.PushImage(name, "v1")

			body := c.Do(http.MethodGet, "/v2/"+name+"/manifests/v1", nil, nil).Body
			c.Do(http.MethodPut, "/v2/"+name+"/manifests/extra", body,
				map[string]string{"Content-Type": ocitest.ManifestMediaType})

			imageID := e.listImages(t, nil)[0].Id

			if tt.name == "restore image" {
				_, err := e.client.DeleteContainerImage(
					t.Context(),
					artifacts.DeleteContainerImageRequest{ImageId: imageID},
				)
				require.NoError(t, err)
			}

			require.NoError(t, tt.run(t, e, imageID, repo.Id, gen.Id))
			require.NoError(t, tt.run(t, e, imageID, repo.Id, gen.Id), "the same token replays the first answer")
		})
	}
}
