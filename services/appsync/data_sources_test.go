package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

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

func TestInMemoryBackend_CreateDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ds      *appsync.DataSource
		name    string
		wantErr bool
	}{
		{
			name: "creates_lambda_datasource",
			ds: &appsync.DataSource{
				Name: "LambdaDS",
				Type: appsync.DataSourceTypeLambda,
				LambdaConfig: &appsync.LambdaDataSourceConfig{
					LambdaFunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:test",
				},
			},
		},
		{
			name: "creates_none_datasource",
			ds: &appsync.DataSource{
				Name: "NoneDS",
				Type: appsync.DataSourceTypeNone,
			},
		},
		{
			name: "error_on_duplicate_name",
			ds: &appsync.DataSource{
				Name: "DupDS",
				Type: appsync.DataSourceTypeNone,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			if tt.wantErr {
				// Create once to set up duplicate condition.
				_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
					Name: tt.ds.Name,
					Type: appsync.DataSourceTypeNone,
				})
			}

			ds, err := b.CreateDataSource(api.APIID, tt.ds)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrAlreadyExists)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ds.Name, ds.Name)
			assert.Equal(t, api.APIID, ds.APIID)
			assert.NotEmpty(t, ds.DataSourceARN)
		})
	}
}

func TestInMemoryBackend_GetDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsName  string
		wantErr bool
	}{
		{
			name:    "returns_existing_datasource",
			dsName:  "MyDS",
			wantErr: false,
		},
		{
			name:    "returns_not_found_for_missing_datasource",
			dsName:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "MyDS",
				Type: appsync.DataSourceTypeNone,
			})

			ds, err := b.GetDataSource(api.APIID, tt.dsName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.dsName, ds.Name)
		})
	}
}

func TestInMemoryBackend_ListDataSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*appsync.InMemoryBackend, string)
		name      string
		apiID     string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "empty_for_new_api",
			setup:     func(_ *appsync.InMemoryBackend, _ string) {},
			wantCount: 0,
		},
		{
			name: "returns_all_datasources",
			setup: func(b *appsync.InMemoryBackend, apiID string) {
				_, _ = b.CreateDataSource(apiID, &appsync.DataSource{Name: "DS1", Type: appsync.DataSourceTypeNone})
				_, _ = b.CreateDataSource(apiID, &appsync.DataSource{
					Name: "DS2",
					Type: appsync.DataSourceTypeLambda,
					LambdaConfig: &appsync.LambdaDataSourceConfig{
						LambdaFunctionARN: "arn:aws:lambda:us-east-1:000:function:fn",
					},
				})
			},
			wantCount: 2,
		},
		{
			name:    "returns_not_found_for_nonexistent_api",
			setup:   func(_ *appsync.InMemoryBackend, _ string) {},
			apiID:   "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			apiID := tt.apiID
			if apiID == "" {
				apiID = api.APIID
			}

			tt.setup(b, api.APIID)
			dss, err := b.ListDataSources(apiID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Len(t, dss, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DeleteDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsName  string
		wantErr bool
	}{
		{
			name:    "deletes_existing_datasource",
			dsName:  "MyDS",
			wantErr: false,
		},
		{
			name:    "error_for_missing_datasource",
			dsName:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "MyDS",
				Type: appsync.DataSourceTypeNone,
			})

			err := b.DeleteDataSource(api.APIID, tt.dsName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)

			// Verify it's gone.
			_, getErr := b.GetDataSource(api.APIID, "MyDS")
			require.Error(t, getErr)
		})
	}
}

func TestInMemoryBackend_CreateDataSource_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ds      appsync.DataSource
		wantErr bool
	}{
		{
			name: "valid",
			ds:   appsync.DataSource{Name: "MyDS", Type: appsync.DataSourceTypeNone},
		},
		{
			name:    "missing_name",
			ds:      appsync.DataSource{Type: appsync.DataSourceTypeNone},
			wantErr: true,
		},
		{
			name:    "missing_type",
			ds:      appsync.DataSource{Name: "MyDS"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, dsErr := b.CreateDataSource(api.APIID, &tt.ds)

			if tt.wantErr {
				require.Error(t, dsErr)

				return
			}

			require.NoError(t, dsErr)
		})
	}
}

func TestInMemoryBackend_UpdateDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dsName     string
		updateDesc string
		wantErr    bool
	}{
		{
			name:       "updates_description",
			dsName:     "myds",
			updateDesc: "updated",
		},
		{
			name:    "not_found_returns_error",
			dsName:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "myds", Type: "NONE"})
			require.NoError(t, err)

			updated, err := b.UpdateDataSource(api.APIID, tt.dsName, &appsync.DataSource{Description: tt.updateDesc})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.updateDesc, updated.Description)
		})
	}
}

func TestInMemoryBackend_UpdateDataSource_AllFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds1", Type: "NONE"})
	require.NoError(t, err)

	updated, err := b.UpdateDataSource(api.APIID, "ds1", &appsync.DataSource{
		Type:           "AWS_LAMBDA",
		ServiceRoleARN: "arn:aws:iam::000000000000:role/role",
		LambdaConfig:   &appsync.LambdaDataSourceConfig{LambdaFunctionARN: "arn:aws:lambda:us-east-1:000:function:fn"},
	})
	require.NoError(t, err)
	assert.Equal(t, "AWS_LAMBDA", string(updated.Type))
}

func TestInMemoryBackend_CreateDataSource_InvalidType(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "invalid_ds",
		Type: "INVALID_TYPE",
	})
	require.Error(t, err)
}

func TestInMemoryBackend_DeleteDataSource_BlockedByResolver(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "MyDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, "type Query { hello: String }")
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "MyDS",
		Kind:           "UNIT",
	})
	require.NoError(t, err)

	// Delete should be blocked.
	err = b.DeleteDataSource(api.APIID, "MyDS")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestInMemoryBackend_DeleteDataSource_BlockedByFunction(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "FuncDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	_, err = b.CreateFunction(api.APIID, &appsync.Function{
		Name:           "MyFunc",
		DataSourceName: "FuncDS",
	})
	require.NoError(t, err)

	// Delete should be blocked.
	err = b.DeleteDataSource(api.APIID, "FuncDS")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestInMemoryBackend_DeleteDataSource_SucceedsWhenUnreferenced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "UnusedDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	err = b.DeleteDataSource(api.APIID, "UnusedDS")
	require.NoError(t, err)
}

func TestInMemoryBackend_GetDataSource_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetDataSource("nonexistent-api", "myds")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrNotFound)
}

func TestBackend_StartDataSourceIntrospection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		dataSourceName string
		setupAPIID     bool
		setupDS        bool
		wantErr        bool
	}{
		{
			name:           "success",
			setupAPIID:     true,
			setupDS:        true,
			dataSourceName: "MyDS",
		},
		{
			name:       "api_not_found",
			setupAPIID: false,
			setupDS:    false,
			wantErr:    true,
		},
		{
			name:           "datasource_not_found",
			setupAPIID:     true,
			setupDS:        false,
			dataSourceName: "NoSuchDS",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := "nonexistent"

			if tt.setupAPIID {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				apiID = api.APIID
			}

			if tt.setupDS {
				_, err := b.CreateDataSource(apiID, &appsync.DataSource{
					Name: tt.dataSourceName,
					Type: appsync.DataSourceTypeNone,
				})
				require.NoError(t, err)
			}

			id, err := b.StartDataSourceIntrospection(apiID, tt.dataSourceName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, id)
		})
	}
}

func TestBackend_GetDataSourceIntrospection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		introspectionID string
		wantErr         bool
	}{
		{
			name:            "valid_id",
			introspectionID: "abc123",
		},
		{
			name:            "empty_id",
			introspectionID: "",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			result, err := b.GetDataSourceIntrospection(tt.introspectionID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.introspectionID, result.IntrospectionID)
			assert.Equal(t, "SUCCESS", result.Status)
		})
	}
}
