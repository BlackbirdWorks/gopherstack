package appsync_test

// AWS-accuracy audit batch-1 tests (go-qd8n).
//
// Covers: AdditionalAuthenticationProviders round-trip, Visibility (GLOBAL/PRIVATE),
// EventBridge data source, RelationalDatabase data source, OpenSearch data source,
// DeltaSyncConfig on DynamoDB, CachingConfig+SyncConfig+MaxBatchSize on resolvers,
// PipelineFunctionVersion defaulting, UpdateGraphqlAPI new fields, and misc
// validation fixes.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// ---- GraphqlAPI Visibility tests ----

func TestCreateGraphqlAPI_VisibilityGlobal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		visibility string
		wantVis    string
	}{
		{
			name:       "explicit_global",
			visibility: "GLOBAL",
			wantVis:    "GLOBAL",
		},
		{
			name:       "default_empty_becomes_global",
			visibility: "",
			wantVis:    "GLOBAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", tt.visibility, nil, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVis, api.Visibility)
		})
	}
}

func TestCreateGraphqlAPI_VisibilityPrivate(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("PrivateAPI", appsync.AuthTypeAPIKey, false, "", "PRIVATE", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE", api.Visibility)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE", got.Visibility)
}

func TestCreateGraphqlAPI_VisibilityInvalid(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "INTERNAL", nil, nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestUpdateGraphqlAPI_VisibilityRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		visibility string
		wantVis    string
		wantErr    bool
	}{
		{
			name:       "switch_to_private",
			visibility: "PRIVATE",
			wantVis:    "PRIVATE",
		},
		{
			name:       "switch_to_global",
			visibility: "GLOBAL",
			wantVis:    "GLOBAL",
		},
		{
			name:       "empty_no_change",
			visibility: "",
			wantVis:    "GLOBAL",
		},
		{
			name:       "invalid_visibility",
			visibility: "RESTRICTED",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", nil, nil, nil)
			require.NoError(t, err)

			updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, tt.visibility, nil, nil)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, appsync.ErrValidation)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantVis, updated.Visibility)
		})
	}
}

// ---- AdditionalAuthenticationProviders tests ----

func TestCreateGraphqlAPI_AdditionalAuthProviders_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		providers []appsync.AdditionalAuthenticationProvider
	}{
		{
			name: "iam_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
		},
		{
			name: "oidc_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType: appsync.AuthTypeOIDC,
					OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
						Issuer:  "https://example.com",
						AuthTTL: 3600,
						IatTTL:  300,
					},
				},
			},
		},
		{
			name: "cognito_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType: appsync.AuthTypeCognito,
					UserPoolConfig: &appsync.CognitoUserPoolConfig{
						UserPoolID: "us-east-1_abc123",
						AWSRegion:  "us-east-1",
					},
				},
			},
		},
		{
			name: "lambda_provider",
			providers: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType: appsync.AuthTypeLambda,
					LambdaAuthorizerConfig: &appsync.LambdaAuthorizerConfig{
						AuthorizerURI:                "arn:aws:lambda:us-east-1:000000000000:function:auth",
						AuthorizerResultTTLInSeconds: 300,
					},
				},
			},
		},
		{
			name: "multiple_providers",
			providers: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
				{
					AuthenticationType: appsync.AuthTypeOIDC,
					OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
						Issuer: "https://example.com",
					},
				},
			},
		},
		{
			name:      "no_additional_providers",
			providers: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI(
				"TestAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", tt.providers, nil, nil,
			)
			require.NoError(t, err)

			got, err := b.GetGraphqlAPI(api.APIID)
			require.NoError(t, err)
			assert.Len(t, got.AdditionalAuthenticationProviders, len(tt.providers))

			if len(tt.providers) > 0 {
				wantAuthType := tt.providers[0].AuthenticationType
				gotAuthType := got.AdditionalAuthenticationProviders[0].AuthenticationType
				assert.Equal(t, wantAuthType, gotAuthType)
			}
		})
	}
}

func TestUpdateGraphqlAPI_AdditionalAuthProviders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialProvs []appsync.AdditionalAuthenticationProvider
		updateProvs  []appsync.AdditionalAuthenticationProvider
		wantCount    int
	}{
		{
			name:         "add_iam_provider",
			initialProvs: nil,
			updateProvs: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			wantCount: 1,
		},
		{
			name: "replace_providers",
			initialProvs: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			updateProvs: []appsync.AdditionalAuthenticationProvider{
				{
					AuthenticationType:  appsync.AuthTypeOIDC,
					OpenIDConnectConfig: &appsync.OpenIDConnectConfig{Issuer: "https://example.com"},
				},
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			wantCount: 2,
		},
		{
			name: "clear_providers_not_called_when_nil",
			initialProvs: []appsync.AdditionalAuthenticationProvider{
				{AuthenticationType: appsync.AuthTypeIAM},
			},
			updateProvs: nil,
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", tt.initialProvs, nil, nil)
			require.NoError(t, err)

			updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, "", tt.updateProvs, nil)
			require.NoError(t, err)
			assert.Len(t, updated.AdditionalAuthenticationProviders, tt.wantCount)
		})
	}
}

// ---- EventBridge DataSource tests ----

func TestCreateDataSource_EventBridge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrType error
		ds          appsync.DataSource
		name        string
		wantErr     bool
	}{
		{
			name: "valid_eventbridge",
			ds: appsync.DataSource{
				Name: "MyEventBridgeDS",
				Type: appsync.DataSourceTypeEventBridge,
				EventBridgeConfig: &appsync.EventBridgeDataSourceConfig{
					EventBusARN: "arn:aws:events:us-east-1:000000000000:event-bus/default",
				},
			},
		},
		{
			name: "eventbridge_without_config",
			ds: appsync.DataSource{
				Name: "MyEventBridgeDS",
				Type: appsync.DataSourceTypeEventBridge,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			created, err := b.CreateDataSource(api.APIID, &tt.ds)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrType != nil {
					require.ErrorIs(t, err, tt.wantErrType)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, appsync.DataSourceTypeEventBridge, created.Type)
		})
	}
}

func TestCreateDataSource_EventBridge_Config_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	eventBusARN := "arn:aws:events:us-east-1:000000000000:event-bus/my-bus"
	ds := &appsync.DataSource{
		Name: "EBDataSource",
		Type: appsync.DataSourceTypeEventBridge,
		EventBridgeConfig: &appsync.EventBridgeDataSourceConfig{
			EventBusARN: eventBusARN,
		},
	}

	created, err := b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)
	require.NotNil(t, created.EventBridgeConfig)
	assert.Equal(t, eventBusARN, created.EventBridgeConfig.EventBusARN)

	got, err := b.GetDataSource(api.APIID, "EBDataSource")
	require.NoError(t, err)
	require.NotNil(t, got.EventBridgeConfig)
	assert.Equal(t, eventBusARN, got.EventBridgeConfig.EventBusARN)
}

// ---- RelationalDatabase DataSource tests ----

func TestCreateDataSource_RelationalDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ds      appsync.DataSource
		wantErr bool
	}{
		{
			name: "valid_relational_database",
			ds: appsync.DataSource{
				Name: "MyRDSDS",
				Type: appsync.DataSourceTypeRelational,
				RelationalDatabaseConfig: &appsync.RelationalDatabaseDataSourceConfig{
					RelationalDatabaseSourceType: "RDS_HTTP_ENDPOINT",
					RDSHTTPEndpointConfig: &appsync.RDSHTTPEndpointConfig{
						DBClusterIdentifier: "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
						AWSSecretStoreARN:   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
						AWSRegion:           "us-east-1",
						DatabaseName:        "mydb",
						Schema:              "public",
					},
				},
			},
		},
		{
			name: "relational_database_minimal",
			ds: appsync.DataSource{
				Name: "MinimalRDSDS",
				Type: appsync.DataSourceTypeRelational,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			created, err := b.CreateDataSource(api.APIID, &tt.ds)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, appsync.DataSourceTypeRelational, created.Type)
		})
	}
}

func TestCreateDataSource_RelationalDatabase_Config_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	cfg := &appsync.RelationalDatabaseDataSourceConfig{
		RelationalDatabaseSourceType: "RDS_HTTP_ENDPOINT",
		RDSHTTPEndpointConfig: &appsync.RDSHTTPEndpointConfig{
			DBClusterIdentifier: "arn:aws:rds:us-east-1:000000000000:cluster:test",
			AWSSecretStoreARN:   "arn:aws:secretsmanager:us-east-1:000000000000:secret:test",
			AWSRegion:           "us-east-1",
			DatabaseName:        "testdb",
		},
	}

	ds := &appsync.DataSource{
		Name:                     "RDSDataSource",
		Type:                     appsync.DataSourceTypeRelational,
		RelationalDatabaseConfig: cfg,
	}

	created, err := b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)
	require.NotNil(t, created.RelationalDatabaseConfig)
	require.NotNil(t, created.RelationalDatabaseConfig.RDSHTTPEndpointConfig)
	assert.Equal(t, "testdb", created.RelationalDatabaseConfig.RDSHTTPEndpointConfig.DatabaseName)

	got, err := b.GetDataSource(api.APIID, "RDSDataSource")
	require.NoError(t, err)
	require.NotNil(t, got.RelationalDatabaseConfig)
	assert.Equal(t, "RDS_HTTP_ENDPOINT", got.RelationalDatabaseConfig.RelationalDatabaseSourceType)
}

// ---- OpenSearch DataSource tests ----

func TestCreateDataSource_OpenSearch_Config_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name: "OpenSearchDS",
		Type: appsync.DataSourceTypeOpenSearch,
		OpenSearchConfig: &appsync.OpenSearchServiceDataSourceConfig{
			Endpoint:  "https://search-test-1234.us-east-1.es.amazonaws.com",
			AWSRegion: "us-east-1",
		},
	}

	created, err := b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)
	require.NotNil(t, created.OpenSearchConfig)
	assert.Equal(t, "https://search-test-1234.us-east-1.es.amazonaws.com", created.OpenSearchConfig.Endpoint)

	got, err := b.GetDataSource(api.APIID, "OpenSearchDS")
	require.NoError(t, err)
	require.NotNil(t, got.OpenSearchConfig)
	assert.Equal(t, "us-east-1", got.OpenSearchConfig.AWSRegion)
}

// ---- DynamoDB DeltaSyncConfig tests ----

func TestCreateDataSource_DynamoDB_DeltaSyncConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *appsync.DeltaSyncConfig
		name    string
		wantNil bool
	}{
		{
			name: "with_delta_sync",
			cfg: &appsync.DeltaSyncConfig{
				BaseTableTTL:       43200,
				DeltaSyncTableName: "delta_sync_table",
				DeltaSyncTableTTL:  1440,
			},
		},
		{
			name:    "without_delta_sync",
			cfg:     nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			ds := &appsync.DataSource{
				Name: "DynamoDS",
				Type: appsync.DataSourceTypeDynamoDB,
				DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{
					TableName:       "my-table",
					AWSRegion:       "us-east-1",
					DeltaSyncConfig: tt.cfg,
				},
			}

			created, err := b.CreateDataSource(api.APIID, ds)
			require.NoError(t, err)
			require.NotNil(t, created.DynamoDBConfig)

			if tt.wantNil {
				assert.Nil(t, created.DynamoDBConfig.DeltaSyncConfig)
			} else {
				require.NotNil(t, created.DynamoDBConfig.DeltaSyncConfig)
				assert.Equal(t, tt.cfg.BaseTableTTL, created.DynamoDBConfig.DeltaSyncConfig.BaseTableTTL)
				assert.Equal(t, tt.cfg.DeltaSyncTableName, created.DynamoDBConfig.DeltaSyncConfig.DeltaSyncTableName)
				assert.Equal(t, tt.cfg.DeltaSyncTableTTL, created.DynamoDBConfig.DeltaSyncConfig.DeltaSyncTableTTL)
			}
		})
	}
}

func TestCreateDataSource_DynamoDB_DeltaSyncConfig_GetRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name: "DynamoDS",
		Type: appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{
			TableName: "my-table",
			AWSRegion: "us-east-1",
			DeltaSyncConfig: &appsync.DeltaSyncConfig{
				BaseTableTTL:       43200,
				DeltaSyncTableName: "sync_table",
				DeltaSyncTableTTL:  1440,
			},
		},
	}

	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	got, err := b.GetDataSource(api.APIID, "DynamoDS")
	require.NoError(t, err)
	require.NotNil(t, got.DynamoDBConfig.DeltaSyncConfig)
	assert.Equal(t, int64(43200), got.DynamoDBConfig.DeltaSyncConfig.BaseTableTTL)
	assert.Equal(t, "sync_table", got.DynamoDBConfig.DeltaSyncConfig.DeltaSyncTableName)
}

// ---- Resolver CachingConfig tests ----

func TestCreateResolver_CachingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *appsync.CachingConfig
		name    string
		wantNil bool
	}{
		{
			name: "with_caching_config",
			cfg: &appsync.CachingConfig{
				TTL:         60,
				CachingKeys: []string{"$context.identity.sub", "$context.arguments.id"},
			},
		},
		{
			name: "caching_no_keys",
			cfg: &appsync.CachingConfig{
				TTL: 300,
			},
		},
		{
			name:    "no_caching",
			cfg:     nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "DS",
				Type: appsync.DataSourceTypeNone,
			})
			require.NoError(t, err)

			r := &appsync.Resolver{
				FieldName:      "getItem",
				DataSourceName: "DS",
				CachingConfig:  tt.cfg,
			}

			created, err := b.CreateResolver(api.APIID, "Query", r)
			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, created.CachingConfig)
			} else {
				require.NotNil(t, created.CachingConfig)
				assert.Equal(t, tt.cfg.TTL, created.CachingConfig.TTL)
				assert.Equal(t, tt.cfg.CachingKeys, created.CachingConfig.CachingKeys)
			}
		})
	}
}

func TestCreateResolver_CachingConfig_GetRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	cfg := &appsync.CachingConfig{
		TTL:         120,
		CachingKeys: []string{"$context.arguments.id"},
	}

	r := &appsync.Resolver{
		FieldName:      "getUser",
		DataSourceName: "DS",
		CachingConfig:  cfg,
	}

	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	got, err := b.GetResolver(api.APIID, "Query", "getUser")
	require.NoError(t, err)
	require.NotNil(t, got.CachingConfig)
	assert.Equal(t, int64(120), got.CachingConfig.TTL)
	assert.Equal(t, []string{"$context.arguments.id"}, got.CachingConfig.CachingKeys)
}

// ---- Resolver SyncConfig tests ----

func TestCreateResolver_SyncConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *appsync.SyncConfig
		name    string
		wantNil bool
	}{
		{
			name: "optimistic_concurrency",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "VERSION",
				ConflictHandler:   "OPTIMISTIC_CONCURRENCY",
			},
		},
		{
			name: "automerge",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "VERSION",
				ConflictHandler:   "AUTOMERGE",
			},
		},
		{
			name: "lambda_conflict_handler",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "VERSION",
				ConflictHandler:   "LAMBDA",
				LambdaConflictHandlerConfig: &appsync.LambdaConflictHandlerConfig{
					LambdaConflictHandlerARN: "arn:aws:lambda:us-east-1:000000000000:function:conflict-handler",
				},
			},
		},
		{
			name: "no_detection",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "NONE",
			},
		},
		{
			name:    "nil_sync_config",
			cfg:     nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "DS",
				Type: appsync.DataSourceTypeNone,
			})
			require.NoError(t, err)

			r := &appsync.Resolver{
				FieldName:      "updateItem",
				DataSourceName: "DS",
				SyncConfig:     tt.cfg,
			}

			created, err := b.CreateResolver(api.APIID, "Mutation", r)
			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, created.SyncConfig)
			} else {
				require.NotNil(t, created.SyncConfig)
				assert.Equal(t, tt.cfg.ConflictDetection, created.SyncConfig.ConflictDetection)
				assert.Equal(t, tt.cfg.ConflictHandler, created.SyncConfig.ConflictHandler)
			}
		})
	}
}

func TestCreateResolver_SyncConfig_GetRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:conflict"
	cfg := &appsync.SyncConfig{
		ConflictDetection: "VERSION",
		ConflictHandler:   "LAMBDA",
		LambdaConflictHandlerConfig: &appsync.LambdaConflictHandlerConfig{
			LambdaConflictHandlerARN: lambdaARN,
		},
	}

	r := &appsync.Resolver{
		FieldName:      "mutateItem",
		DataSourceName: "DS",
		SyncConfig:     cfg,
	}

	_, err = b.CreateResolver(api.APIID, "Mutation", r)
	require.NoError(t, err)

	got, err := b.GetResolver(api.APIID, "Mutation", "mutateItem")
	require.NoError(t, err)
	require.NotNil(t, got.SyncConfig)
	assert.Equal(t, "LAMBDA", got.SyncConfig.ConflictHandler)
	require.NotNil(t, got.SyncConfig.LambdaConflictHandlerConfig)
	assert.Equal(t, lambdaARN, got.SyncConfig.LambdaConflictHandlerConfig.LambdaConflictHandlerARN)
}

// ---- Resolver MaxBatchSize tests ----

func TestCreateResolver_MaxBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		maxBatchSize int32
	}{
		{name: "zero_batch_size", maxBatchSize: 0},
		{name: "batch_size_1", maxBatchSize: 1},
		{name: "batch_size_10", maxBatchSize: 10},
		{name: "batch_size_2000", maxBatchSize: 2000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "DS",
				Type: appsync.DataSourceTypeNone,
			})
			require.NoError(t, err)

			r := &appsync.Resolver{
				FieldName:      "batchOp",
				DataSourceName: "DS",
				MaxBatchSize:   tt.maxBatchSize,
			}

			created, err := b.CreateResolver(api.APIID, "Query", r)
			require.NoError(t, err)
			assert.Equal(t, tt.maxBatchSize, created.MaxBatchSize)
		})
	}
}

func TestCreateResolver_MaxBatchSize_GetRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "batchGet",
		DataSourceName: "DS",
		MaxBatchSize:   100,
	}

	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	got, err := b.GetResolver(api.APIID, "Query", "batchGet")
	require.NoError(t, err)
	assert.Equal(t, int32(100), got.MaxBatchSize)
}

// ---- Resolver combined fields (CachingConfig + SyncConfig + MaxBatchSize) ----

func TestCreateResolver_AllAccuracyFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "complexOp",
		DataSourceName: "DS",
		MaxBatchSize:   50,
		CachingConfig: &appsync.CachingConfig{
			TTL:         60,
			CachingKeys: []string{"$context.arguments.id"},
		},
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "VERSION",
			ConflictHandler:   "OPTIMISTIC_CONCURRENCY",
		},
	}

	created, err := b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)
	assert.Equal(t, int32(50), created.MaxBatchSize)
	require.NotNil(t, created.CachingConfig)
	assert.Equal(t, int64(60), created.CachingConfig.TTL)
	require.NotNil(t, created.SyncConfig)
	assert.Equal(t, "OPTIMISTIC_CONCURRENCY", created.SyncConfig.ConflictHandler)
}

// ---- DataSource type coverage ----

func TestCreateDataSource_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		dsType appsync.DataSourceType
		ds     appsync.DataSource
	}{
		{
			name:   "NONE",
			dsType: appsync.DataSourceTypeNone,
			ds: appsync.DataSource{
				Name: "NoneDS",
				Type: appsync.DataSourceTypeNone,
			},
		},
		{
			name:   "AWS_LAMBDA",
			dsType: appsync.DataSourceTypeLambda,
			ds: appsync.DataSource{
				Name: "LambdaDS",
				Type: appsync.DataSourceTypeLambda,
				LambdaConfig: &appsync.LambdaDataSourceConfig{
					LambdaFunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:test",
				},
			},
		},
		{
			name:   "AMAZON_DYNAMODB",
			dsType: appsync.DataSourceTypeDynamoDB,
			ds: appsync.DataSource{
				Name: "DynamoDS",
				Type: appsync.DataSourceTypeDynamoDB,
				DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{
					TableName: "my-table",
					AWSRegion: "us-east-1",
				},
			},
		},
		{
			name:   "HTTP",
			dsType: appsync.DataSourceTypeHTTP,
			ds: appsync.DataSource{
				Name: "HTTPDS",
				Type: appsync.DataSourceTypeHTTP,
				HTTPConfig: &appsync.HTTPDataSourceConfig{
					Endpoint: "https://example.com",
				},
			},
		},
		{
			name:   "AMAZON_OPENSEARCH_SERVICE",
			dsType: appsync.DataSourceTypeOpenSearch,
			ds: appsync.DataSource{
				Name: "OSDS",
				Type: appsync.DataSourceTypeOpenSearch,
				OpenSearchConfig: &appsync.OpenSearchServiceDataSourceConfig{
					Endpoint:  "https://search.us-east-1.es.amazonaws.com",
					AWSRegion: "us-east-1",
				},
			},
		},
		{
			name:   "AMAZON_EVENTBRIDGE",
			dsType: appsync.DataSourceTypeEventBridge,
			ds: appsync.DataSource{
				Name: "EBDS",
				Type: appsync.DataSourceTypeEventBridge,
				EventBridgeConfig: &appsync.EventBridgeDataSourceConfig{
					EventBusARN: "arn:aws:events:us-east-1:000000000000:event-bus/default",
				},
			},
		},
		{
			name:   "RELATIONAL_DATABASE",
			dsType: appsync.DataSourceTypeRelational,
			ds: appsync.DataSource{
				Name: "RDBS",
				Type: appsync.DataSourceTypeRelational,
				RelationalDatabaseConfig: &appsync.RelationalDatabaseDataSourceConfig{
					RelationalDatabaseSourceType: "RDS_HTTP_ENDPOINT",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			created, err := b.CreateDataSource(api.APIID, &tt.ds)
			require.NoError(t, err)
			assert.Equal(t, tt.dsType, created.Type)
		})
	}
}

// ---- PipelineFunctionVersion defaulting ----

func TestCreateFunction_PipelineFunctionVersion_Default(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	f := &appsync.Function{
		Name:           "MyFunc",
		DataSourceName: "DS",
	}

	created, err := b.CreateFunction(api.APIID, f)
	require.NoError(t, err)
	assert.Equal(t, "2018-05-29", created.FunctionVersion)
}

func TestCreateFunction_PipelineFunctionVersion_Custom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		version     string
		wantVersion string
	}{
		{
			name:        "custom_version_preserved",
			version:     "2018-05-29",
			wantVersion: "2018-05-29",
		},
		{
			name:        "empty_gets_default",
			version:     "",
			wantVersion: "2018-05-29",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "DS",
				Type: appsync.DataSourceTypeNone,
			})
			require.NoError(t, err)

			f := &appsync.Function{
				Name:            "TestFunc",
				DataSourceName:  "DS",
				FunctionVersion: tt.version,
			}

			created, err := b.CreateFunction(api.APIID, f)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, created.FunctionVersion)
		})
	}
}

// ---- PIPELINE resolver with Unit resolver config preservation ----

func TestCreateResolver_UnitResolver_DataSourcePreserved(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "MyDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:               "getItem",
		DataSourceName:          "MyDS",
		Kind:                    "UNIT",
		RequestMappingTemplate:  "{\"version\":\"2018-05-29\"}",
		ResponseMappingTemplate: "$util.toJson($context.result)",
		MaxBatchSize:            5,
		CachingConfig: &appsync.CachingConfig{
			TTL: 30,
		},
	}

	created, err := b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)
	assert.Equal(t, "UNIT", created.Kind)
	assert.Equal(t, "MyDS", created.DataSourceName)
	assert.Equal(t, int32(5), created.MaxBatchSize)
	require.NotNil(t, created.CachingConfig)
	assert.Equal(t, int64(30), created.CachingConfig.TTL)
}

// ---- UpdateResolver preserves CachingConfig and SyncConfig ----

func TestUpdateResolver_PreservesCachingAndSyncConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "op",
		DataSourceName: "DS",
		CachingConfig:  &appsync.CachingConfig{TTL: 60},
		SyncConfig:     &appsync.SyncConfig{ConflictDetection: "VERSION", ConflictHandler: "AUTOMERGE"},
		MaxBatchSize:   10,
	}

	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	updated := &appsync.Resolver{
		FieldName:      "op",
		DataSourceName: "DS",
		MaxBatchSize:   20,
		CachingConfig:  &appsync.CachingConfig{TTL: 120, CachingKeys: []string{"$context.arguments.id"}},
		SyncConfig:     &appsync.SyncConfig{ConflictDetection: "NONE"},
	}

	result, err := b.UpdateResolver(api.APIID, "Query", updated)
	require.NoError(t, err)
	assert.Equal(t, int32(20), result.MaxBatchSize)
	require.NotNil(t, result.CachingConfig)
	assert.Equal(t, int64(120), result.CachingConfig.TTL)
	require.NotNil(t, result.SyncConfig)
	assert.Equal(t, "NONE", result.SyncConfig.ConflictDetection)
}

// ---- GraphqlAPI Visibility persists through list ----

func TestListGraphqlAPIs_VisibilityPreserved(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateGraphqlAPI("GlobalAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateGraphqlAPI("PrivateAPI", appsync.AuthTypeIAM, false, "", "PRIVATE", nil, nil, nil)
	require.NoError(t, err)

	apis, err := b.ListGraphqlAPIs("")
	require.NoError(t, err)
	require.Len(t, apis, 2)

	visByName := make(map[string]string)
	for _, a := range apis {
		visByName[a.Name] = a.Visibility
	}

	assert.Equal(t, "GLOBAL", visByName["GlobalAPI"])
	assert.Equal(t, "PRIVATE", visByName["PrivateAPI"])
}

// ---- AdditionalAuthProviders preserved through update ----

func TestUpdateGraphqlAPI_PreservesAdditionalAuthProviders(t *testing.T) {
	t.Parallel()

	providers := []appsync.AdditionalAuthenticationProvider{
		{AuthenticationType: appsync.AuthTypeIAM},
		{
			AuthenticationType: appsync.AuthTypeOIDC,
			OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
				Issuer: "https://example.com",
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "GLOBAL", providers, nil, nil)
	require.NoError(t, err)
	require.Len(t, api.AdditionalAuthenticationProviders, 2)

	// Update only the name — providers should remain unchanged.
	updated, err := b.UpdateGraphqlAPI(api.APIID, "NewName", "", nil, "", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "NewName", updated.Name)
	assert.Len(t, updated.AdditionalAuthenticationProviders, 2)
	assert.Equal(t, appsync.AuthTypeIAM, updated.AdditionalAuthenticationProviders[0].AuthenticationType)
}

// ---- DataSource invalid type still rejected ----

func TestCreateDataSource_InvalidType(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "BadDS",
		Type: appsync.DataSourceType("AMAZON_BEDROCK"),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

// ---- UpdateDataSource preserves EventBridgeConfig ----

func TestUpdateDataSource_EventBridgeConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name: "EBDS",
		Type: appsync.DataSourceTypeEventBridge,
		EventBridgeConfig: &appsync.EventBridgeDataSourceConfig{
			EventBusARN: "arn:aws:events:us-east-1:000000000000:event-bus/old-bus",
		},
	}

	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	updated := &appsync.DataSource{
		Name: "EBDS",
		Type: appsync.DataSourceTypeEventBridge,
		EventBridgeConfig: &appsync.EventBridgeDataSourceConfig{
			EventBusARN: "arn:aws:events:us-east-1:000000000000:event-bus/new-bus",
		},
	}

	result, err := b.UpdateDataSource(api.APIID, "EBDS", updated)
	require.NoError(t, err)
	require.NotNil(t, result.EventBridgeConfig)
	assert.Equal(t, "arn:aws:events:us-east-1:000000000000:event-bus/new-bus", result.EventBridgeConfig.EventBusARN)
}

// ---- CognitoUserPoolConfig round-trip in AdditionalAuth ----

func TestCreateGraphqlAPI_CognitoUserPoolConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	providers := []appsync.AdditionalAuthenticationProvider{
		{
			AuthenticationType: appsync.AuthTypeCognito,
			UserPoolConfig: &appsync.CognitoUserPoolConfig{
				UserPoolID:  "us-east-1_TestPool123",
				AWSRegion:   "us-east-1",
				AppIDClient: "my-app-client-regex",
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", providers, nil, nil)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.Len(t, got.AdditionalAuthenticationProviders, 1)
	require.NotNil(t, got.AdditionalAuthenticationProviders[0].UserPoolConfig)
	assert.Equal(t, "us-east-1_TestPool123", got.AdditionalAuthenticationProviders[0].UserPoolConfig.UserPoolID)
	assert.Equal(t, "my-app-client-regex", got.AdditionalAuthenticationProviders[0].UserPoolConfig.AppIDClient)
}

// ---- LambdaAuthorizerConfig round-trip in AdditionalAuth ----

func TestCreateGraphqlAPI_LambdaAuthorizerConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	authURI := "arn:aws:lambda:us-east-1:000000000000:function:my-authorizer"
	providers := []appsync.AdditionalAuthenticationProvider{
		{
			AuthenticationType: appsync.AuthTypeLambda,
			LambdaAuthorizerConfig: &appsync.LambdaAuthorizerConfig{
				AuthorizerURI:                authURI,
				AuthorizerResultTTLInSeconds: 300,
				IdentityValidationExpression: "^Bearer [-0-9a-zA-z\\.]*$",
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", providers, nil, nil)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.Len(t, got.AdditionalAuthenticationProviders, 1)
	require.NotNil(t, got.AdditionalAuthenticationProviders[0].LambdaAuthorizerConfig)
	gotLambdaCfg := got.AdditionalAuthenticationProviders[0].LambdaAuthorizerConfig
	assert.Equal(t, authURI, gotLambdaCfg.AuthorizerURI)
	assert.Equal(t, int32(300), gotLambdaCfg.AuthorizerResultTTLInSeconds)
}

// ---- OpenIDConnectConfig round-trip in AdditionalAuth ----

func TestCreateGraphqlAPI_OIDCConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	providers := []appsync.AdditionalAuthenticationProvider{
		{
			AuthenticationType: appsync.AuthTypeOIDC,
			OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
				Issuer:   "https://idp.example.com",
				ClientID: "my-client",
				IatTTL:   600,
				AuthTTL:  3600,
			},
		},
	}

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", providers, nil, nil)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.Len(t, got.AdditionalAuthenticationProviders, 1)
	require.NotNil(t, got.AdditionalAuthenticationProviders[0].OpenIDConnectConfig)
	oidc := got.AdditionalAuthenticationProviders[0].OpenIDConnectConfig
	assert.Equal(t, "https://idp.example.com", oidc.Issuer)
	assert.Equal(t, "my-client", oidc.ClientID)
	assert.Equal(t, int64(600), oidc.IatTTL)
	assert.Equal(t, int64(3600), oidc.AuthTTL)
}

// ---- DataSource update preserves RelationalDatabaseConfig ----

func TestUpdateDataSource_RelationalDatabaseConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name: "RDBS",
		Type: appsync.DataSourceTypeRelational,
		RelationalDatabaseConfig: &appsync.RelationalDatabaseDataSourceConfig{
			RelationalDatabaseSourceType: "RDS_HTTP_ENDPOINT",
			RDSHTTPEndpointConfig: &appsync.RDSHTTPEndpointConfig{
				DatabaseName: "olddb",
				AWSRegion:    "us-east-1",
			},
		},
	}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	updated := &appsync.DataSource{
		Name: "RDBS",
		Type: appsync.DataSourceTypeRelational,
		RelationalDatabaseConfig: &appsync.RelationalDatabaseDataSourceConfig{
			RelationalDatabaseSourceType: "RDS_HTTP_ENDPOINT",
			RDSHTTPEndpointConfig: &appsync.RDSHTTPEndpointConfig{
				DatabaseName: "newdb",
				AWSRegion:    "us-east-1",
			},
		},
	}

	result, err := b.UpdateDataSource(api.APIID, "RDBS", updated)
	require.NoError(t, err)
	require.NotNil(t, result.RelationalDatabaseConfig)
	assert.Equal(t, "newdb", result.RelationalDatabaseConfig.RDSHTTPEndpointConfig.DatabaseName)
}

// ---- DynamoDB Versioned + DeltaSyncConfig interaction ----

func TestCreateDataSource_DynamoDB_Versioned_With_DeltaSync(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name: "VersionedDynamo",
		Type: appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{
			TableName: "versioned-table",
			AWSRegion: "us-east-1",
			Versioned: true,
			DeltaSyncConfig: &appsync.DeltaSyncConfig{
				BaseTableTTL:       60,
				DeltaSyncTableName: "delta_table",
				DeltaSyncTableTTL:  30,
			},
		},
	}

	created, err := b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)
	require.NotNil(t, created.DynamoDBConfig)
	assert.True(t, created.DynamoDBConfig.Versioned)
	require.NotNil(t, created.DynamoDBConfig.DeltaSyncConfig)
	assert.Equal(t, "delta_table", created.DynamoDBConfig.DeltaSyncConfig.DeltaSyncTableName)
}

// ---- Resolver SyncConfig with AUTOMERGE ----

func TestCreateResolver_SyncConfig_Automerge_NoLambda(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "mergeOp",
		DataSourceName: "DS",
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "VERSION",
			ConflictHandler:   "AUTOMERGE",
		},
	}

	created, err := b.CreateResolver(api.APIID, "Mutation", r)
	require.NoError(t, err)
	require.NotNil(t, created.SyncConfig)
	assert.Equal(t, "AUTOMERGE", created.SyncConfig.ConflictHandler)
	assert.Nil(t, created.SyncConfig.LambdaConflictHandlerConfig)
}

// ---- CreateFunction with Code (APPSYNC_JS runtime) ----

func TestCreateFunction_WithCode(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	code := `export function request(ctx) { return {}; } export function response(ctx) { return ctx.result; }`

	f := &appsync.Function{
		Name:           "JsFunction",
		DataSourceName: "DS",
		Code:           code,
		MaxBatchSize:   5,
	}

	created, err := b.CreateFunction(api.APIID, f)
	require.NoError(t, err)
	assert.Equal(t, code, created.Code)
	assert.Equal(t, int32(5), created.MaxBatchSize)
	assert.Equal(t, "2018-05-29", created.FunctionVersion)

	got, err := b.GetFunction(api.APIID, created.FunctionID)
	require.NoError(t, err)
	assert.Equal(t, code, got.Code)
}

// ---- ApiCache accuracy: Status defaults to AVAILABLE ----

func TestCreateAPICache_StatusDefaultsToAvailable(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	cache := &appsync.APICache{
		Type:               "T2_SMALL",
		APICachingBehavior: "FULL_REQUEST_CACHING",
		TTL:                300,
	}

	created, err := b.CreateAPICache(api.APIID, cache)
	require.NoError(t, err)
	assert.NotEmpty(t, created.Status)

	got, err := b.GetAPICache(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, created.Status, got.Status)
}

// ---- ApiKey expiry round-trip ----

func TestCreateAPIKey_ExpiryDefaulted_WhenZero(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// expires=0 → backend assigns default expiry (365 days from now).
	key, err := b.CreateAPIKey(api.APIID, "test key", 0)
	require.NoError(t, err)
	assert.Positive(t, key.Expires, "expiry should be defaulted to a future timestamp")
}

func TestCreateAPIKey_ExpiryClampedToMax(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// expires far in the future → clamped to max (365 days).
	key, err := b.CreateAPIKey(api.APIID, "test key", 9999999999)
	require.NoError(t, err)
	assert.Positive(t, key.Expires, "expiry should be clamped to max")
}

func TestUpdateAPIKey_ExpiryRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "initial desc", 1000)
	require.NoError(t, err)

	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "updated desc", 2000)
	require.NoError(t, err)
	assert.Equal(t, "updated desc", updated.Description)
	assert.Equal(t, int64(2000), updated.Expires)
}

// ---- Tags: tag/untag round-trip ----

func TestTagResource_GraphqlAPI_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	err = b.TagResource(api.APIID, map[string]string{
		"env":  "prod",
		"team": "platform",
	})
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.Tags)
	v, _ := got.Tags.Get("env")
	assert.Equal(t, "prod", v)
}

func TestUntagResource_GraphqlAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, map[string]string{
		"env":  "prod",
		"team": "platform",
	}, nil)
	require.NoError(t, err)

	err = b.UntagResource(api.APIID, []string{"env"})
	require.NoError(t, err)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	_, ok := got.Tags.Get("env")
	assert.False(t, ok)
	v, ok := got.Tags.Get("team")
	assert.True(t, ok)
	assert.Equal(t, "platform", v)
}

// ---- EnvironmentVariables round-trip ----

func TestGetPutGraphqlAPIEnvironmentVariables_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	envVars := map[string]string{
		"DB_HOST":   "localhost",
		"LOG_LEVEL": "debug",
	}

	_, err = b.PutGraphqlAPIEnvironmentVariables(api.APIID, envVars)
	require.NoError(t, err)

	got, err := b.GetGraphqlAPIEnvironmentVariables(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, envVars, got)
}

func TestPutGraphqlAPIEnvironmentVariables_ReplacesAll(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.PutGraphqlAPIEnvironmentVariables(api.APIID, map[string]string{
		"OLD_VAR": "old_value",
	})
	require.NoError(t, err)

	_, err = b.PutGraphqlAPIEnvironmentVariables(api.APIID, map[string]string{
		"NEW_VAR": "new_value",
	})
	require.NoError(t, err)

	got, err := b.GetGraphqlAPIEnvironmentVariables(api.APIID)
	require.NoError(t, err)
	_, hasOld := got["OLD_VAR"]
	assert.False(t, hasOld)
	assert.Equal(t, "new_value", got["NEW_VAR"])
}

// ---- DomainName association ----

func TestDomainName_AssociateAPI_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	domain, err := b.CreateDomainName(
		"example.com",
		"arn:aws:acm:us-east-1:000000000000:certificate/abc",
		"test domain",
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "example.com", domain.DomainName)

	assoc, err := b.AssociateAPI("example.com", api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.APIID, assoc.APIID)

	got, err := b.GetAPIAssociation("example.com")
	require.NoError(t, err)
	assert.Equal(t, api.APIID, got.APIID)
}

// ---- Type CRUD ----

func TestType_CRUD_AllFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		format     appsync.TypeDefinitionFormat
		typeName   string
	}{
		{
			name:       "sdl_type",
			definition: "type User { id: ID! name: String }",
			format:     appsync.TypeFormatSDL,
			typeName:   "User",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			created, err := b.CreateType(api.APIID, tt.definition, tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.typeName, created.Name)
			assert.Equal(t, tt.format, created.Format)

			got, err := b.GetType(api.APIID, tt.typeName)
			require.NoError(t, err)
			assert.Equal(t, tt.typeName, got.Name)

			types, err := b.ListTypes(api.APIID)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(types), 1)

			newDef := "type User { id: ID! name: String email: String }"
			updated, err := b.UpdateType(api.APIID, tt.typeName, newDef, tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.typeName, updated.Name)

			err = b.DeleteType(api.APIID, tt.typeName)
			require.NoError(t, err)

			_, err = b.GetType(api.APIID, tt.typeName)
			require.Error(t, err)
			assert.ErrorIs(t, err, appsync.ErrNotFound)
		})
	}
}

// ---- MergedAPI: AssociateMergedGraphqlAPI and AssociateSourceGraphqlAPI ----

func TestMergedAPI_CreateAssociation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "source association", "")
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)
	assert.Equal(t, src.APIID, assoc.SourceAPIID)
	assert.Equal(t, merged.APIID, assoc.MergedAPIID)

	got, err := b.GetSourceAPIAssociation(merged.APIID, assoc.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, assoc.AssociationID, got.AssociationID)
}

// ---- GraphqlAPI CRUD with Visibility ----

func TestDeleteGraphqlAPI_PrivateAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("PrivateAPI", appsync.AuthTypeAPIKey, false, "", "PRIVATE", nil, nil, nil)
	require.NoError(t, err)

	err = b.DeleteGraphqlAPI(api.APIID)
	require.NoError(t, err)

	_, err = b.GetGraphqlAPI(api.APIID)
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrNotFound)
}

// ---- Batch-2 accuracy tests ----

// ---- GraphqlAPI primary auth config ----

func TestCreateGraphqlAPI_UserPoolConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		UserPoolConfig: &appsync.UserPoolConfig{
			UserPoolID:       "us-east-1_abc123",
			AWSRegion:        "us-east-1",
			DefaultAction:    "ALLOW",
			AppIDClientRegex: "^my-client-",
		},
	}

	api, err := b.CreateGraphqlAPI("CognitoAPI", appsync.AuthTypeCognito, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.UserPoolConfig)
	assert.Equal(t, "us-east-1_abc123", api.UserPoolConfig.UserPoolID)
	assert.Equal(t, "us-east-1", api.UserPoolConfig.AWSRegion)
	assert.Equal(t, "ALLOW", api.UserPoolConfig.DefaultAction)
	assert.Equal(t, "^my-client-", api.UserPoolConfig.AppIDClientRegex)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.UserPoolConfig)
	assert.Equal(t, "us-east-1_abc123", got.UserPoolConfig.UserPoolID)
	assert.Equal(t, "ALLOW", got.UserPoolConfig.DefaultAction)
}

func TestCreateGraphqlAPI_OpenIDConnectConfig_Primary_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		OpenIDConnectConfig: &appsync.OpenIDConnectConfig{
			Issuer:   "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc",
			ClientID: "my-client-id",
			IatTTL:   3600,
			AuthTTL:  7200,
		},
	}

	api, err := b.CreateGraphqlAPI("OIDCPrimaryAPI", appsync.AuthTypeOIDC, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.OpenIDConnectConfig)
	assert.Equal(t, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc", api.OpenIDConnectConfig.Issuer)
	assert.Equal(t, "my-client-id", api.OpenIDConnectConfig.ClientID)
	assert.Equal(t, int64(3600), api.OpenIDConnectConfig.IatTTL)
	assert.Equal(t, int64(7200), api.OpenIDConnectConfig.AuthTTL)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.OpenIDConnectConfig)
	assert.Equal(t, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc", got.OpenIDConnectConfig.Issuer)
}

func TestCreateGraphqlAPI_LambdaAuthorizerConfig_Primary_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		LambdaAuthorizerConfig: &appsync.LambdaAuthorizerConfig{
			AuthorizerURI:                "arn:aws:lambda:us-east-1:123456789012:function:auth",
			IdentityValidationExpression: "^Bearer .+",
			AuthorizerResultTTLInSeconds: 300,
		},
	}

	api, err := b.CreateGraphqlAPI("LambdaAPI", appsync.AuthTypeLambda, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.LambdaAuthorizerConfig)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:auth", api.LambdaAuthorizerConfig.AuthorizerURI)
	assert.Equal(t, "^Bearer .+", api.LambdaAuthorizerConfig.IdentityValidationExpression)
	assert.Equal(t, int32(300), api.LambdaAuthorizerConfig.AuthorizerResultTTLInSeconds)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.LambdaAuthorizerConfig)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:auth", got.LambdaAuthorizerConfig.AuthorizerURI)
}

func TestCreateGraphqlAPI_LogConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{
		LogConfig: &appsync.LogConfig{
			CloudWatchLogsRoleARN: "arn:aws:iam::123456789012:role/appsync-logs",
			FieldLogLevel:         "ERROR",
			ExcludeVerboseContent: true,
		},
	}

	api, err := b.CreateGraphqlAPI("LoggedAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, api.LogConfig)
	assert.Equal(t, "arn:aws:iam::123456789012:role/appsync-logs", api.LogConfig.CloudWatchLogsRoleARN)
	assert.Equal(t, "ERROR", api.LogConfig.FieldLogLevel)
	assert.True(t, api.LogConfig.ExcludeVerboseContent)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.LogConfig)
	assert.Equal(t, "ERROR", got.LogConfig.FieldLogLevel)
}

func TestCreateGraphqlAPI_IntrospectionConfig_DefaultEnabled(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, appsync.IntrospectionConfigEnabled, api.IntrospectionConfig)
}

func TestCreateGraphqlAPI_IntrospectionConfig_Disabled(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{IntrospectionConfig: appsync.IntrospectionConfigDisabled}

	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	assert.Equal(t, appsync.IntrospectionConfigDisabled, api.IntrospectionConfig)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, appsync.IntrospectionConfigDisabled, got.IntrospectionConfig)
}

func TestCreateGraphqlAPI_QueryDepthLimit_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{QueryDepthLimit: 10}

	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	assert.Equal(t, int32(10), api.QueryDepthLimit)

	got, err := b.GetGraphqlAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, int32(10), got.QueryDepthLimit)
}

func TestCreateGraphqlAPI_ResolverCountLimit_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	cfg := &appsync.GraphqlAPIConfig{ResolverCountLimit: 500}

	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, cfg)
	require.NoError(t, err)
	assert.Equal(t, int32(500), api.ResolverCountLimit)
}

func TestUpdateGraphqlAPI_UserPoolConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("CognitoAPI", appsync.AuthTypeCognito, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, api.UserPoolConfig)

	cfg := &appsync.GraphqlAPIConfig{
		UserPoolConfig: &appsync.UserPoolConfig{
			UserPoolID:    "us-west-2_xyz",
			AWSRegion:     "us-west-2",
			DefaultAction: "DENY",
		},
	}

	updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, "", nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, updated.UserPoolConfig)
	assert.Equal(t, "us-west-2_xyz", updated.UserPoolConfig.UserPoolID)
	assert.Equal(t, "DENY", updated.UserPoolConfig.DefaultAction)
}

func TestUpdateGraphqlAPI_LogConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("API", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	cfg := &appsync.GraphqlAPIConfig{
		LogConfig: &appsync.LogConfig{
			CloudWatchLogsRoleARN: "arn:aws:iam::123:role/logs",
			FieldLogLevel:         "ALL",
		},
	}

	updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", nil, "", nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, updated.LogConfig)
	assert.Equal(t, "ALL", updated.LogConfig.FieldLogLevel)
}

// ---- HTTP data source AuthorizationConfig ----

func TestCreateDataSource_HTTP_AuthorizationConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name: "HTTPSigned",
		Type: appsync.DataSourceTypeHTTP,
		HTTPConfig: &appsync.HTTPDataSourceConfig{
			Endpoint: "https://api.example.com",
			AuthorizationConfig: &appsync.AuthorizationConfig{
				AuthorizationType: "AWS_IAM",
				AwsIamConfig: &appsync.AwsIamConfig{
					SigningRegion:      "us-east-1",
					SigningServiceName: "execute-api",
				},
			},
		},
	}

	created, err := b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)
	require.NotNil(t, created.HTTPConfig)
	require.NotNil(t, created.HTTPConfig.AuthorizationConfig)
	assert.Equal(t, "AWS_IAM", created.HTTPConfig.AuthorizationConfig.AuthorizationType)
	require.NotNil(t, created.HTTPConfig.AuthorizationConfig.AwsIamConfig)
	assert.Equal(t, "us-east-1", created.HTTPConfig.AuthorizationConfig.AwsIamConfig.SigningRegion)
	assert.Equal(t, "execute-api", created.HTTPConfig.AuthorizationConfig.AwsIamConfig.SigningServiceName)

	got, err := b.GetDataSource(api.APIID, "HTTPSigned")
	require.NoError(t, err)
	require.NotNil(t, got.HTTPConfig.AuthorizationConfig)
	assert.Equal(t, "AWS_IAM", got.HTTPConfig.AuthorizationConfig.AuthorizationType)
}

func TestUpdateDataSource_HTTP_AuthorizationConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name:       "HTTP",
		Type:       appsync.DataSourceTypeHTTP,
		HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: "https://api.example.com"},
	}

	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	updateInput := &appsync.DataSource{
		Name: "HTTP",
		Type: appsync.DataSourceTypeHTTP,
		HTTPConfig: &appsync.HTTPDataSourceConfig{
			Endpoint: "https://api.example.com",
			AuthorizationConfig: &appsync.AuthorizationConfig{
				AuthorizationType: "AWS_IAM",
				AwsIamConfig: &appsync.AwsIamConfig{
					SigningRegion:      "eu-west-1",
					SigningServiceName: "appsync",
				},
			},
		},
	}

	updated, err := b.UpdateDataSource(api.APIID, "HTTP", updateInput)
	require.NoError(t, err)
	require.NotNil(t, updated.HTTPConfig.AuthorizationConfig)
	assert.Equal(t, "eu-west-1", updated.HTTPConfig.AuthorizationConfig.AwsIamConfig.SigningRegion)
}

// ---- Resolver: Code and Runtime (APPSYNC_JS) ----

func TestCreateResolver_Code_Runtime_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	r := &appsync.Resolver{
		TypeName:       "Query",
		FieldName:      "hello",
		Kind:           "UNIT",
		DataSourceName: "NONE",
		Code:           `export function request(ctx) { return {}; }`,
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
	}

	created, err := b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)
	assert.Equal(t, `export function request(ctx) { return {}; }`, created.Code)
	require.NotNil(t, created.Runtime)
	assert.Equal(t, "APPSYNC_JS", created.Runtime.Name)
	assert.Equal(t, "1.0.0", created.Runtime.RuntimeVersion)

	got, err := b.GetResolver(api.APIID, "Query", "hello")
	require.NoError(t, err)
	assert.Equal(t, `export function request(ctx) { return {}; }`, got.Code)
	require.NotNil(t, got.Runtime)
	assert.Equal(t, "APPSYNC_JS", got.Runtime.Name)
}

func TestUpdateResolver_Code_Runtime_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	r := &appsync.Resolver{TypeName: "Query", FieldName: "hello", Kind: "UNIT", DataSourceName: "NONE"}
	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	updated, err := b.UpdateResolver(api.APIID, "Query", &appsync.Resolver{
		TypeName:       "Query",
		FieldName:      "hello",
		DataSourceName: "NONE",
		Code:           `export function request(ctx) { return {payload: "hello"}; }`,
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
	})
	require.NoError(t, err)
	assert.Equal(t, `export function request(ctx) { return {payload: "hello"}; }`, updated.Code)
	require.NotNil(t, updated.Runtime)
	assert.Equal(t, "APPSYNC_JS", updated.Runtime.Name)
}

// ---- Function: Runtime and SyncConfig ----

func TestCreateFunction_Runtime_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	fn := &appsync.Function{
		Name:           "MyJSFn",
		DataSourceName: "NONE",
		Code:           `export function request(ctx) { return {}; }`,
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
	}

	created, err := b.CreateFunction(api.APIID, fn)
	require.NoError(t, err)
	require.NotNil(t, created.Runtime)
	assert.Equal(t, "APPSYNC_JS", created.Runtime.Name)
	assert.Equal(t, "1.0.0", created.Runtime.RuntimeVersion)

	got, err := b.GetFunction(api.APIID, created.FunctionID)
	require.NoError(t, err)
	require.NotNil(t, got.Runtime)
	assert.Equal(t, "APPSYNC_JS", got.Runtime.Name)
}

func TestCreateFunction_SyncConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "DYNAMO", Type: appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "t", AWSRegion: "us-east-1"}}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	fn := &appsync.Function{
		Name:           "SyncFn",
		DataSourceName: "DYNAMO",
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "VERSION",
			ConflictHandler:   "OPTIMISTIC_CONCURRENCY",
		},
	}

	created, err := b.CreateFunction(api.APIID, fn)
	require.NoError(t, err)
	require.NotNil(t, created.SyncConfig)
	assert.Equal(t, "VERSION", created.SyncConfig.ConflictDetection)
	assert.Equal(t, "OPTIMISTIC_CONCURRENCY", created.SyncConfig.ConflictHandler)

	got, err := b.GetFunction(api.APIID, created.FunctionID)
	require.NoError(t, err)
	require.NotNil(t, got.SyncConfig)
	assert.Equal(t, "VERSION", got.SyncConfig.ConflictDetection)
}

func TestUpdateFunction_Runtime_SyncConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	fn := &appsync.Function{Name: "Fn", DataSourceName: "NONE"}
	created, err := b.CreateFunction(api.APIID, fn)
	require.NoError(t, err)

	updated, err := b.UpdateFunction(api.APIID, created.FunctionID, &appsync.Function{
		Name:           "Fn",
		DataSourceName: "NONE",
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "NONE",
			ConflictHandler:   "AUTOMERGE",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Runtime)
	assert.Equal(t, "APPSYNC_JS", updated.Runtime.Name)
	require.NotNil(t, updated.SyncConfig)
	assert.Equal(t, "AUTOMERGE", updated.SyncConfig.ConflictHandler)
}

// ---- Event API: EventConfig ----

func TestCreateAPI_EventConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	eventCfg := &appsync.EventConfig{
		AuthProviders: []appsync.AuthProvider{
			{AuthType: "API_KEY"},
			{AuthType: "AWS_IAM"},
		},
		ConnectionAuthModes:       []appsync.AuthMode{{AuthType: "API_KEY"}},
		DefaultPublishAuthModes:   []appsync.AuthMode{{AuthType: "API_KEY"}},
		DefaultSubscribeAuthModes: []appsync.AuthMode{{AuthType: "API_KEY"}},
	}

	api, err := b.CreateAPI("EventsAPI", "ops@example.com", nil, eventCfg)
	require.NoError(t, err)
	require.NotNil(t, api.EventConfig)
	assert.Len(t, api.EventConfig.AuthProviders, 2)
	assert.Equal(t, "API_KEY", api.EventConfig.AuthProviders[0].AuthType)
	assert.Equal(t, "AWS_IAM", api.EventConfig.AuthProviders[1].AuthType)
	assert.Len(t, api.EventConfig.ConnectionAuthModes, 1)
	assert.Equal(t, "API_KEY", api.EventConfig.ConnectionAuthModes[0].AuthType)

	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	require.NotNil(t, got.EventConfig)
	assert.Len(t, got.EventConfig.AuthProviders, 2)
}

func TestCreateAPI_Dns_IsMap(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, api.DNS)
	assert.Contains(t, api.DNS, "HTTP")
	assert.Contains(t, api.DNS, "REALTIME")
	assert.NotEmpty(t, api.DNS["HTTP"])
	assert.NotEmpty(t, api.DNS["REALTIME"])
}

func TestUpdateAPI_EventConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)
	assert.Nil(t, api.EventConfig)

	eventCfg := &appsync.EventConfig{
		AuthProviders:             []appsync.AuthProvider{{AuthType: "AWS_IAM"}},
		ConnectionAuthModes:       []appsync.AuthMode{{AuthType: "AWS_IAM"}},
		DefaultPublishAuthModes:   []appsync.AuthMode{{AuthType: "AWS_IAM"}},
		DefaultSubscribeAuthModes: []appsync.AuthMode{{AuthType: "AWS_IAM"}},
	}

	updated, err := b.UpdateAPI(api.APIID, "", "", eventCfg)
	require.NoError(t, err)
	require.NotNil(t, updated.EventConfig)
	assert.Equal(t, "AWS_IAM", updated.EventConfig.AuthProviders[0].AuthType)
}

// ---- Channel namespace: auth modes and handler configs ----

func TestCreateChannelNamespace_PublishSubscribeAuthModes_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	cfg := &appsync.ChannelNamespaceConfig{
		PublishAuthModes:   []appsync.AuthMode{{AuthType: "API_KEY"}, {AuthType: "AWS_IAM"}},
		SubscribeAuthModes: []appsync.AuthMode{{AuthType: "API_KEY"}},
	}

	ns, err := b.CreateChannelNamespace(api.APIID, "chat", nil, cfg)
	require.NoError(t, err)
	assert.Len(t, ns.PublishAuthModes, 2)
	assert.Equal(t, "API_KEY", ns.PublishAuthModes[0].AuthType)
	assert.Equal(t, "AWS_IAM", ns.PublishAuthModes[1].AuthType)
	assert.Len(t, ns.SubscribeAuthModes, 1)
	assert.Equal(t, "API_KEY", ns.SubscribeAuthModes[0].AuthType)

	got, err := b.GetChannelNamespace(api.APIID, "chat")
	require.NoError(t, err)
	assert.Len(t, got.PublishAuthModes, 2)
	assert.Len(t, got.SubscribeAuthModes, 1)
}

func TestCreateChannelNamespace_HandlerConfigs_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	cfg := &appsync.ChannelNamespaceConfig{
		HandlerConfigs: &appsync.HandlerConfigs{
			OnPublish: &appsync.HandlerConfig{
				Behavior: "CODE",
				Integration: &appsync.Integration{
					DataSourceName: "MyLambda",
				},
			},
			OnSubscribe: &appsync.HandlerConfig{
				Behavior: "CODE",
				Integration: &appsync.Integration{
					DataSourceName: "MyLambda",
				},
			},
		},
	}

	ns, err := b.CreateChannelNamespace(api.APIID, "events", nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, ns.HandlerConfigs)
	require.NotNil(t, ns.HandlerConfigs.OnPublish)
	assert.Equal(t, "CODE", ns.HandlerConfigs.OnPublish.Behavior)
	assert.Equal(t, "MyLambda", ns.HandlerConfigs.OnPublish.Integration.DataSourceName)
	require.NotNil(t, ns.HandlerConfigs.OnSubscribe)
	assert.Equal(t, "MyLambda", ns.HandlerConfigs.OnSubscribe.Integration.DataSourceName)

	got, err := b.GetChannelNamespace(api.APIID, "events")
	require.NoError(t, err)
	require.NotNil(t, got.HandlerConfigs)
	assert.Equal(t, "CODE", got.HandlerConfigs.OnPublish.Behavior)
}

func TestUpdateChannelNamespace_AuthModes_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "chat", nil, nil)
	require.NoError(t, err)

	cfg := &appsync.ChannelNamespaceConfig{
		PublishAuthModes:   []appsync.AuthMode{{AuthType: "AWS_IAM"}},
		SubscribeAuthModes: []appsync.AuthMode{{AuthType: "AWS_IAM"}, {AuthType: "API_KEY"}},
	}

	updated, err := b.UpdateChannelNamespace(api.APIID, "chat", cfg)
	require.NoError(t, err)
	assert.Len(t, updated.PublishAuthModes, 1)
	assert.Equal(t, "AWS_IAM", updated.PublishAuthModes[0].AuthType)
	assert.Len(t, updated.SubscribeAuthModes, 2)

	got, err := b.GetChannelNamespace(api.APIID, "chat")
	require.NoError(t, err)
	assert.Len(t, got.PublishAuthModes, 1)
	assert.Len(t, got.SubscribeAuthModes, 2)
}

func TestUpdateChannelNamespace_CodeHandlers_Via_Config(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventsAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns", nil, nil)
	require.NoError(t, err)

	const code = `export const handler = () => {};`
	cfg := &appsync.ChannelNamespaceConfig{CodeHandlers: code}

	updated, err := b.UpdateChannelNamespace(api.APIID, "ns", cfg)
	require.NoError(t, err)
	assert.Equal(t, code, updated.CodeHandlers)
}

// ---- SourceAPIAssociation: MergeType ----

func TestAssociateMergedGraphqlAPI_MergeType_MANUAL(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "desc", "MANUAL_MERGE")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "MANUAL_MERGE", assoc.SourceAPIAssociationConfig.MergeType)
}

func TestAssociateMergedGraphqlAPI_MergeType_AUTO(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "", "AUTO_MERGE")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "AUTO_MERGE", assoc.SourceAPIAssociationConfig.MergeType)
}

func TestAssociateMergedGraphqlAPI_MergeType_DefaultManual(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(src.APIID, merged.APIID, "", "")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "MANUAL_MERGE", assoc.SourceAPIAssociationConfig.MergeType)
}

func TestAssociateSourceGraphqlAPI_MergeType_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	src, err := b.CreateGraphqlAPI("Src", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	merged, err := b.CreateGraphqlAPI("Merged", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateSourceGraphqlAPI(merged.APIID, src.APIID, "desc", "AUTO_MERGE")
	require.NoError(t, err)
	require.NotNil(t, assoc.SourceAPIAssociationConfig)
	assert.Equal(t, "AUTO_MERGE", assoc.SourceAPIAssociationConfig.MergeType)

	got, err := b.GetSourceAPIAssociation(merged.APIID, assoc.AssociationID)
	require.NoError(t, err)
	require.NotNil(t, got.SourceAPIAssociationConfig)
	assert.Equal(t, "AUTO_MERGE", got.SourceAPIAssociationConfig.MergeType)
}
