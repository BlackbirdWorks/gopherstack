package ociidentity_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

type retryCase struct {
	create func(t *testing.T, e *env, token string) (string, error)
	remove func(t *testing.T, e *env, id string)
	name   string
}

func retryCases() []retryCase {
	return []retryCase{
		{
			name: "user",
			create: func(t *testing.T, e *env, token string) (string, error) {
				t.Helper()

				r, err := e.client.CreateUser(t.Context(), identity.CreateUserRequest{
					OpcRetryToken: new(token),
					CompartmentId: new(testTenancy), Name: new("u"), Description: new("d"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteUser(t.Context(), identity.DeleteUserRequest{UserId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "group",
			create: func(t *testing.T, e *env, token string) (string, error) {
				t.Helper()

				r, err := e.client.CreateGroup(t.Context(), identity.CreateGroupRequest{
					OpcRetryToken: new(token),
					CompartmentId: new(testTenancy), Name: new("g"), Description: new("d"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteGroup(t.Context(), identity.DeleteGroupRequest{GroupId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "policy",
			create: func(t *testing.T, e *env, token string) (string, error) {
				t.Helper()

				r, err := e.client.CreatePolicy(t.Context(), identity.CreatePolicyRequest{
					OpcRetryToken: new(token),
					CompartmentId: new(testTenancy), Name: new("p"), Description: new("d"),
					Statements: []string{"Allow group g to read buckets in tenancy"},
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeletePolicy(t.Context(), identity.DeletePolicyRequest{PolicyId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "compartment",
			create: func(t *testing.T, e *env, token string) (string, error) {
				t.Helper()

				r, err := e.client.CreateCompartment(t.Context(), identity.CreateCompartmentRequest{
					OpcRetryToken: new(token),
					CompartmentId: new(testTenancy), Name: new("c"), Description: new("d"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteCompartment(t.Context(), identity.DeleteCompartmentRequest{CompartmentId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "tag namespace",
			create: func(t *testing.T, e *env, token string) (string, error) {
				t.Helper()

				r, err := e.client.CreateTagNamespace(t.Context(), identity.CreateTagNamespaceRequest{
					OpcRetryToken: new(token),
					CompartmentId: new(testTenancy), Name: new("ns"), Description: new("d"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteTagNamespace(
					t.Context(),
					identity.DeleteTagNamespaceRequest{TagNamespaceId: &id},
				)
				require.NoError(t, err)
			},
		},
	}
}

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	for _, tc := range retryCases() {
		t.Run(tc.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			first, err := tc.create(t, e, "tok")
			require.NoError(t, err)

			second, err := tc.create(t, e, "tok")
			require.NoError(t, err)
			assert.Equal(t, first, second)
		})

		t.Run(tc.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			_, err := tc.create(t, e, "tok")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			_, err = tc.create(t, e, "tok")
			se := svcErr(t, err)
			assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode(), "an expired token is a new request")
			assert.NotEqual(t, "InvalidatedRetryToken", se.GetCode())
		})

		t.Run(tc.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			id, err := tc.create(t, e, "tok")
			require.NoError(t, err)

			tc.remove(t, e, id)

			_, err = tc.create(t, e, "tok")
			se := svcErr(t, err)
			assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidatedRetryToken", se.GetCode())
		})
	}
}

func TestRetryTokenSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	in := oci.CreateGroupIn{CompartmentID: testTenancy, Name: "g", Description: "d", RetryToken: "tok"}

	first, _, err := e.backend.CreateGroup(in)
	require.NoError(t, err)

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	again, _, err := restored.CreateGroup(in)
	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID)
}
