package ocinosql_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env) (string, error)
		remove func(t *testing.T, e *env)
		name   string
	}{
		{
			name: "table",
			create: func(t *testing.T, e *env) (string, error) {
				t.Helper()

				r, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
					OpcRetryToken: new("tok"), Name: new("rt"), CompartmentId: new(testCompartment),
					DdlStatement: new(usersDDL("rt")), TableLimits: provisioned(50, 50, 10),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env) {
				t.Helper()

				_, err := e.client.DeleteTable(t.Context(), nosql.DeleteTableRequest{
					TableNameOrId: new("rt"), CompartmentId: new(testCompartment),
				})
				require.NoError(t, err)
				e.settle()
			},
		},
		{
			name: "index",
			create: func(t *testing.T, e *env) (string, error) {
				t.Helper()

				if _, err := e.client.GetTable(t.Context(), nosql.GetTableRequest{
					TableNameOrId: new("base"), CompartmentId: new(testCompartment),
				}); err != nil {
					e.mkTable(t, "base", usersDDL("base"))
				}

				r, err := e.client.CreateIndex(t.Context(), nosql.CreateIndexRequest{
					OpcRetryToken: new("tok"), TableNameOrId: new("base"), CompartmentId: new(testCompartment),
					Name: new("ri"), Keys: []nosql.IndexKey{{ColumnName: new("age")}},
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env) {
				t.Helper()

				e.settle()

				_, err := e.client.DeleteIndex(t.Context(), nosql.DeleteIndexRequest{
					TableNameOrId: new("base"), IndexName: new("ri"), CompartmentId: new(testCompartment),
				})
				require.NoError(t, err)
				e.settle()
			},
		},
		{
			name: "change compartment",
			create: func(t *testing.T, e *env) (string, error) {
				t.Helper()

				if _, err := e.client.GetTable(t.Context(), nosql.GetTableRequest{
					TableNameOrId: new("mv"), CompartmentId: new(testCompartment),
				}); err != nil {
					e.mkTable(t, "mv", usersDDL("mv"))
				}

				r, err := e.client.ChangeTableCompartment(t.Context(), nosql.ChangeTableCompartmentRequest{
					OpcRetryToken: new("tok"), TableNameOrId: new("mv"),
					FromCompartmentId: new(testCompartment), ToCompartmentId: new(otherCompart),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env) {
				t.Helper()

				_, err := e.client.DeleteTable(t.Context(), nosql.DeleteTableRequest{
					TableNameOrId: new("mv"), CompartmentId: new(otherCompart),
				})
				require.NoError(t, err)
				e.settle()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			wr1, err := tt.create(t, e)
			require.NoError(t, err)

			wr2, err := tt.create(t, e)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			wr1, err := tt.create(t, e)
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			wr2, err := tt.create(t, fresh)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := tt.create(t, e)
			require.NoError(t, err)

			e.settle()
			tt.remove(t, e)

			_, err = tt.create(t, e)
			require.Error(t, err)
			assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestCreateRetryTokenExpires(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	create := func() error {
		_, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
			OpcRetryToken: new("tok"), Name: new("ex"), CompartmentId: new(testCompartment),
			DdlStatement: new(usersDDL("ex")), TableLimits: provisioned(50, 50, 10),
		})

		return err
	}

	require.NoError(t, create())
	e.clock.Advance(25 * time.Hour)

	err := create()
	require.Error(t, err)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())
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

			e.mkTable(t, "base", usersDDL("base"))

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.client.CreateTable(t.Context(), nosql.CreateTableRequest{
				Name: new("tg"), CompartmentId: new(testCompartment), DdlStatement: new(usersDDL("tg")),
				TableLimits: provisioned(50, 50, 10), FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.client.UpdateTable(t.Context(), nosql.UpdateTableRequest{
				TableNameOrId: new("base"),
				CompartmentId: new(testCompartment), FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
