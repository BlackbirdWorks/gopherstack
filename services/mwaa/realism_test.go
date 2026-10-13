package mwaa_test

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestTransitionDelay_EnvironmentLifecycle(t *testing.T) {
	t.Parallel()

	const delay = 10 * time.Second

	tests := []struct {
		run  func(t *testing.T, b *mwaa.InMemoryBackend)
		name string
	}{
		{
			name: "create settles",
			run: func(t *testing.T, b *mwaa.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateEnvironment(context.Background(), "e", newCreateReq())
				require.NoError(t, err)

				for range 3 {
					env, gErr := b.GetEnvironment(context.Background(), "e")
					require.NoError(t, gErr)
					assert.Equal(t, "CREATING", env.Status)
				}

				time.Sleep(delay)
				synctest.Wait()

				env, err := b.GetEnvironment(context.Background(), "e")
				require.NoError(t, err)
				assert.Equal(t, "AVAILABLE", env.Status)
			},
		},
		{
			name: "update settles",
			run: func(t *testing.T, b *mwaa.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateEnvironment(context.Background(), "e", newCreateReq())
				require.NoError(t, err)
				time.Sleep(delay)
				synctest.Wait()

				_, err = b.UpdateEnvironment(context.Background(), "e", &mwaa.ExportedUpdateEnvironmentRequest{
					EnvironmentClass: "mw1.medium",
				})
				require.NoError(t, err)

				env, err := b.GetEnvironment(context.Background(), "e")
				require.NoError(t, err)
				assert.Equal(t, "UPDATING", env.Status)
				assert.Equal(t, "PENDING", env.LastUpdate.Status)

				time.Sleep(delay)
				synctest.Wait()

				env, err = b.GetEnvironment(context.Background(), "e")
				require.NoError(t, err)
				assert.Equal(t, "AVAILABLE", env.Status)
				assert.Equal(t, "SUCCESS", env.LastUpdate.Status)
			},
		},
		{
			name: "delete goes through deleting",
			run: func(t *testing.T, b *mwaa.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateEnvironment(context.Background(), "e", newCreateReq())
				require.NoError(t, err)
				time.Sleep(delay)
				synctest.Wait()

				deleted, err := b.DeleteEnvironment(context.Background(), "e")
				require.NoError(t, err)
				assert.Equal(t, "DELETING", deleted.Status)

				env, err := b.GetEnvironment(context.Background(), "e")
				require.NoError(t, err)
				assert.Equal(t, "DELETING", env.Status)

				time.Sleep(delay)
				synctest.Wait()

				_, err = b.GetEnvironment(context.Background(), "e")
				require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newTestBackend()
				b.SetTransitionDelay(delay)
				tt.run(t, b)
			})
		})
	}
}

func TestCreateEnvironment_ARNAndTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(r *mwaa.ExportedCreateEnvironmentRequest)
		name   string
		valid  bool
	}{
		{name: "valid", mutate: func(*mwaa.ExportedCreateEnvironmentRequest) {}, valid: true},
		{
			name:   "bucket not arn",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) { r.SourceBucketArn = "my-bucket" },
		},
		{
			name:   "bucket is object arn",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) { r.SourceBucketArn = "arn:aws:s3:::b/key" },
		},
		{name: "role not arn", mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) { r.ExecutionRoleArn = "role" }},
		{
			name:   "role wrong service",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) { r.ExecutionRoleArn = "arn:aws:s3:::b" },
		},
		{
			name:   "reserved tag prefix",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) { r.Tags = map[string]string{"aws:x": "1"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := newCreateReq()
			tt.mutate(req)

			_, err := newTestBackend().CreateEnvironment(context.Background(), "e", req)
			if tt.valid {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, mwaa.ErrInvalidParameter)
		})
	}
}

func TestTagResource_ReservedPrefix(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	env, err := b.CreateEnvironment(context.Background(), "e", newCreateReq())
	require.NoError(t, err)

	err = b.TagResource(context.Background(), env.ARN, map[string]string{"AWS:team": "x"})
	require.ErrorIs(t, err, mwaa.ErrInvalidParameter)
	require.NoError(t, b.TagResource(context.Background(), env.ARN, map[string]string{"team": "x"}))
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	body := map[string]any{
		"DagS3Path":            "dags/",
		"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":      "arn:aws:s3:::b",
		"NetworkConfiguration": networkConfigBody(),
	}
	require.Equal(t, http.StatusOK, doMWAARequest(t, h, http.MethodPut, "/environments/e1", body).Code)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantMsg    string
		wantStatus int
	}{
		{
			name: "duplicate", method: http.MethodPut, path: "/environments/e1", body: body,
			wantStatus: http.StatusBadRequest, wantMsg: "Environment e1 already exists",
		},
		{
			name: "not found", method: http.MethodGet, path: "/environments/nope",
			wantStatus: http.StatusNotFound, wantMsg: "Environment nope not found",
		},
		{
			name: "bad list token", method: http.MethodGet, path: "/environments?NextToken=garbage",
			wantStatus: http.StatusBadRequest, wantMsg: "Invalid NextToken",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doMWAARequest(t, h, tt.method, tt.path, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)
			assert.JSONEq(t, `{"__type":"`+errType(tt.wantStatus)+`","message":"`+tt.wantMsg+`"}`, rec.Body.String())
		})
	}
}

func errType(status int) string {
	if status == http.StatusNotFound {
		return "ResourceNotFoundException"
	}

	return "ValidationException"
}
