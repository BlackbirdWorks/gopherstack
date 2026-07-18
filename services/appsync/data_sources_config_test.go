package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

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

func TestInMemoryBackend_CreateDataSource_HTTPConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		ds        *appsync.DataSource
		name      string
		wantErr   bool
	}{
		{
			name: "http_with_endpoint",
			ds: &appsync.DataSource{
				Name:       "httpDs",
				Type:       "HTTP",
				HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: "https://example.com"},
			},
		},
		{
			name: "http_missing_endpoint",
			ds: &appsync.DataSource{
				Name: "httpDs",
				Type: "HTTP",
			},
			wantErr: true,
		},
		{
			name: "none_type_ok",
			ds: &appsync.DataSource{
				Name: "noneDs",
				Type: "NONE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, tt.ds)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_UpdateDataSource_HTTPConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name:       "httpDs",
		Type:       "HTTP",
		HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: "https://old.example.com"},
	})
	require.NoError(t, err)

	updated, err := b.UpdateDataSource(api.APIID, "httpDs", &appsync.DataSource{
		HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: "https://new.example.com"},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.HTTPConfig)
	assert.Equal(t, "https://new.example.com", updated.HTTPConfig.Endpoint)
}

// TestListDataSources_Pagination verifies maxResults/nextToken on ListDataSources.
func TestListDataSources_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "ds-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/datasources", apiID), map[string]any{
			"name": fmt.Sprintf("ds-%d", i),
			"type": "NONE",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/datasources", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/datasources?maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken   string           `json:"nextToken"`
				DataSources []map[string]any `json:"dataSources"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.DataSources, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
