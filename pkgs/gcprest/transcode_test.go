package gcprest_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

type fakeSecrets struct {
	secretmanagerpb.UnimplementedSecretManagerServiceServer
}

func (fakeSecrets) CreateSecret(
	_ context.Context, in *secretmanagerpb.CreateSecretRequest,
) (*secretmanagerpb.Secret, error) {
	if in.GetSecretId() == "dup" {
		return nil, status.Error(codes.AlreadyExists, "secret exists")
	}

	s := in.GetSecret()
	s.Name = in.GetParent() + "/secrets/" + in.GetSecretId()

	return s, nil
}

func (fakeSecrets) GetSecret(
	_ context.Context, in *secretmanagerpb.GetSecretRequest,
) (*secretmanagerpb.Secret, error) {
	if strings.HasSuffix(in.GetName(), "missing") {
		return nil, status.Error(codes.NotFound, "no such secret")
	}

	return &secretmanagerpb.Secret{Name: in.GetName()}, nil
}

func (fakeSecrets) ListSecrets(
	_ context.Context, in *secretmanagerpb.ListSecretsRequest,
) (*secretmanagerpb.ListSecretsResponse, error) {
	return &secretmanagerpb.ListSecretsResponse{
		Secrets:       []*secretmanagerpb.Secret{{Name: in.GetParent() + "/secrets/a", Etag: in.GetFilter()}},
		NextPageToken: "tok",
		TotalSize:     in.GetPageSize(),
	}, nil
}

func (fakeSecrets) UpdateSecret(
	_ context.Context, in *secretmanagerpb.UpdateSecretRequest,
) (*secretmanagerpb.Secret, error) {
	s := in.GetSecret()
	s.Etag = strings.Join(in.GetUpdateMask().GetPaths(), ",")

	return s, nil
}

func (fakeSecrets) AddSecretVersion(
	_ context.Context, in *secretmanagerpb.AddSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return &secretmanagerpb.SecretVersion{Name: in.GetParent() + "/versions/" + string(in.GetPayload().GetData())}, nil
}

func (fakeSecrets) AccessSecretVersion(
	_ context.Context, in *secretmanagerpb.AccessSecretVersionRequest,
) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	return &secretmanagerpb.AccessSecretVersionResponse{
		Name: in.GetName(), Payload: &secretmanagerpb.SecretPayload{Data: []byte("hello")},
	}, nil
}

func (fakeSecrets) DeleteSecret(context.Context, *secretmanagerpb.DeleteSecretRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

type fakeOps struct {
	longrunningpb.UnimplementedOperationsServer
}

func (fakeOps) GetOperation(
	_ context.Context,
	in *longrunningpb.GetOperationRequest,
) (*longrunningpb.Operation, error) {
	return &longrunningpb.Operation{
		Name: in.GetName(), Done: true,
		Result: &longrunningpb.Operation_Response{Response: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.cloud.secretmanager.v1.Secret", Value: nil,
		}},
	}, nil
}

func startServer(t *testing.T) (string, string) {
	t.Helper()

	srv := grpc.NewServer()
	secretmanagerpb.RegisterSecretManagerServiceServer(srv, fakeSecrets{})
	longrunningpb.RegisterOperationsServer(srv, fakeOps{})

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = gcprest.Serve(srv, l)
	}()

	t.Cleanup(func() {
		srv.Stop()
		<-done
	})

	return "http://" + l.Addr().String(), l.Addr().String()
}

func do(t *testing.T, method, url, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	client := httpclient.New(30 * time.Second)
	defer client.CloseIdleConnections()

	resp, err := client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(b)
}

func TestServeREST(t *testing.T) {
	t.Parallel()

	base, _ := startServer(t)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantJSON   string
		wantStatus int
	}{
		{
			name: "create with body field and query id", method: http.MethodPost,
			path: "/v1/projects/p/secrets?secretId=s1", body: `{"labels":{"a":"b"}}`,
			wantStatus: 200, wantJSON: `{"name":"projects/p/secrets/s1","labels":{"a":"b"}}`,
		},
		{
			name: "get path var", method: http.MethodGet, path: "/v1/projects/p/secrets/s",
			wantStatus: 200, wantJSON: `{"name":"projects/p/secrets/s"}`,
		},
		{
			name: "list query params", method: http.MethodGet,
			path:       "/v1/projects/p/secrets?pageSize=7&filter=x&key=ignored&alt=json",
			wantStatus: 200,
			wantJSON:   `{"secrets":[{"name":"projects/p/secrets/a","etag":"x"}],"nextPageToken":"tok","totalSize":7}`,
		},
		{
			name: "patch body field plus update mask", method: http.MethodPatch,
			path: "/v1/projects/p/secrets/s?updateMask=labels,annotations", body: `{"labels":{"k":"v"}}`,
			wantStatus: 200,
			wantJSON:   `{"name":"projects/p/secrets/s","labels":{"k":"v"},"etag":"labels,annotations"}`,
		},
		{
			name: "custom verb with bytes body", method: http.MethodPost,
			path: "/v1/projects/p/secrets/s:addVersion", body: `{"payload":{"data":"djE="}}`,
			wantStatus: 200, wantJSON: `{"name":"projects/p/secrets/s/versions/v1"}`,
		},
		{
			name: "custom verb get returns bytes base64", method: http.MethodGet,
			path:       "/v1/projects/p/secrets/s/versions/1:access",
			wantStatus: 200,
			wantJSON:   `{"name":"projects/p/secrets/s/versions/1","payload":{"data":"aGVsbG8="}}`,
		},
		{
			name:       "empty response",
			method:     http.MethodDelete,
			path:       "/v1/projects/p/secrets/s",
			wantStatus: 200,
			wantJSON:   `{}`,
		},
		{
			name: "lro passthrough", method: http.MethodGet, path: "/v1/operations/op-1",
			wantStatus: 200,
			wantJSON: `{"name":"operations/op-1","done":true,` +
				`"response":{"@type":"type.googleapis.com/google.cloud.secretmanager.v1.Secret"}}`,
		},
		{
			name: "lro resource-scoped name", method: http.MethodGet,
			path:       "/v1/projects/p/locations/l/operations/op-1",
			wantStatus: 200,
			wantJSON: `{"name":"projects/p/locations/l/operations/op-1","done":true,` +
				`"response":{"@type":"type.googleapis.com/google.cloud.secretmanager.v1.Secret"}}`,
		},
		{
			name: "not found maps to 404", method: http.MethodGet, path: "/v1/projects/p/secrets/missing",
			wantStatus: 404,
			wantJSON:   `{"error":{"message":"no such secret","status":"NOT_FOUND","details":[],"code":404}}`,
		},
		{
			name: "already exists maps to 409", method: http.MethodPost, path: "/v1/projects/p/secrets?secretId=dup",
			body: `{}`, wantStatus: 409,
			wantJSON: `{"error":{"message":"secret exists","status":"ALREADY_EXISTS","details":[],"code":409}}`,
		},
		{
			name: "bad json maps to 400", method: http.MethodPost, path: "/v1/projects/p/secrets?secretId=x",
			body: `{"labels":`, wantStatus: 400,
		},
		{
			name: "unknown route", method: http.MethodGet, path: "/v2/nothing",
			wantStatus: 404,
			wantJSON:   `{"error":{"message":"Method not found.","status":"NOT_FOUND","details":[],"code":404}}`,
		},
		{name: "bad query value", method: http.MethodGet, path: "/v1/projects/p/secrets?pageSize=abc", wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := do(t, tt.method, base+tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, code, body)

			if tt.wantJSON != "" {
				assert.JSONEq(t, tt.wantJSON, body)
			}
		})
	}
}

func TestServeSharesPortWithGRPC(t *testing.T) {
	t.Parallel()

	_, addr := startServer(t)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	defer conn.Close()

	got, err := secretmanagerpb.NewSecretManagerServiceClient(conn).GetSecret(
		t.Context(), &secretmanagerpb.GetSecretRequest{Name: "projects/p/secrets/g"},
	)
	require.NoError(t, err)
	assert.Equal(t, "projects/p/secrets/g", got.GetName())
}

func TestHTTPStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code codes.Code
		want int
	}{
		{codes.OK, 200}, {codes.InvalidArgument, 400}, {codes.NotFound, 404}, {codes.AlreadyExists, 409},
		{codes.PermissionDenied, 403}, {codes.Unauthenticated, 401}, {codes.FailedPrecondition, 400},
		{codes.Unimplemented, 501}, {codes.Unavailable, 503}, {codes.Code(99), 500},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, gcprest.HTTPStatus(tt.code))
		})
	}
}
