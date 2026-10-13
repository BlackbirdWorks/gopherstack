package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestExecuteGraphQLServiceInvoked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		auth    appsync.AuthenticationType
		invoked bool
		wantErr bool
	}{
		{name: "iam_service_invoked", auth: appsync.AuthTypeIAM, invoked: true},
		{name: "iam_unsigned", auth: appsync.AuthTypeIAM, wantErr: true},
		{name: "api_key_service_invoked", auth: appsync.AuthTypeAPIKey, invoked: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("API", tt.auth, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.StartSchemaCreation(api.APIID, `type Query { q: String }`)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "DS", Type: appsync.DataSourceTypeNone})
			require.NoError(t, err)
			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "q", DataSourceName: "DS"})
			require.NoError(t, err)

			_, err = b.ExecuteGraphQL(
				t.Context(), api.APIID, `query { q }`, "", nil, appsync.GraphQLAuth{ServiceInvoked: tt.invoked})
			if tt.wantErr {
				require.ErrorIs(t, err, appsync.ErrUnauthorized)

				return
			}

			require.NoError(t, err)
		})
	}
}
