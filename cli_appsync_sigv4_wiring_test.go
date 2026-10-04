package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
)

func signAppSyncRequest(t *testing.T, secret string) *http.Request {
	t.Helper()

	body := `{"query":"query { hello }"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/apis/x/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	sum := sha256.Sum256([]byte(body))
	creds := aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: secret}
	err := v4.NewSigner().SignHTTP(
		context.Background(), creds, req, hex.EncodeToString(sum[:]), "appsync", "us-east-1", time.Now(),
	)
	require.NoError(t, err)

	return req
}

func TestInitializeServices_AppSyncSigV4SecretWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		signSecret string
		wantErr    bool
	}{
		{name: "configured_secret_accepted", signSecret: "custom-secret", wantErr: false},
		{name: "default_secret_rejected", signSecret: "test", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: "000000000000", Region: "us-east-1", SigV4Secret: "custom-secret"}
			portAlloc, err := portalloc.New(19200, 19300)
			require.NoError(t, err)

			appCtx := &service.AppContext{
				Logger:     slog.Default(),
				Config:     cli,
				JanitorCtx: t.Context(),
				PortAlloc:  portAlloc,
			}
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServices(appCtx)
			require.NoError(t, err)

			h, ok := serviceByName(services)["AppSync"].(*appsyncbackend.Handler)
			require.True(t, ok)

			bk, ok := h.Backend.(*appsyncbackend.InMemoryBackend)
			require.True(t, ok)

			api, err := bk.CreateGraphqlAPI("A", appsyncbackend.AuthTypeIAM, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = bk.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
			require.NoError(t, err)
			_, err = bk.CreateDataSource(api.APIID, &appsyncbackend.DataSource{
				Name: "NoneDS", Type: appsyncbackend.DataSourceTypeNone,
			})
			require.NoError(t, err)
			_, err = bk.CreateResolver(api.APIID, "Query", &appsyncbackend.Resolver{
				FieldName: "hello", DataSourceName: "NoneDS",
			})
			require.NoError(t, err)

			_, err = bk.ExecuteGraphQL(
				t.Context(), api.APIID, `query { hello }`, "", nil,
				appsyncbackend.GraphQLAuth{Request: signAppSyncRequest(t, tt.signSecret)},
			)
			if tt.wantErr {
				require.ErrorIs(t, err, appsyncbackend.ErrUnauthorized)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
