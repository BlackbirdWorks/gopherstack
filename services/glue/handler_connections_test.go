package glue_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestDescribeEntity_ValidConnection verifies that a valid connection returns Fields slice.
func TestDescribeEntity_ValidConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a connection so DescribeEntity can find it.
	createRec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "testconn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/mydb",
				"USERNAME":            "root",
				"PASSWORD":            "secret",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doGlueRequest(t, h, "DescribeEntity", map[string]any{
		"ConnectionName": "testconn",
		"EntityName":     "Customer",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Fields []any `json:"Fields"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out.Fields)
}

// TestGetEntityRecords_ValidConnection verifies a valid connection + known entity
// returns real, schema-shaped records (not an empty success), and that an unknown
// entity returns EntityNotFoundException.
func TestGetEntityRecords_ValidConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "sfconn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/sf",
				"USERNAME":            "user",
				"PASSWORD":            "pass",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
		"ConnectionName": "sfconn",
		"EntityName":     "Account",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string           `json:"NextToken"`
		Records   []map[string]any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Records)
	// Each record must conform to the Account schema.
	for _, r := range out.Records {
		assert.Contains(t, r, "Id")
		assert.Contains(t, r, "Name")
		assert.Contains(t, r, "AnnualRevenue")
	}

	// An unknown entity for a valid connection is EntityNotFoundException, not empty.
	missRec := doGlueRequest(t, h, "GetEntityRecords", map[string]any{
		"ConnectionName": "sfconn",
		"EntityName":     "DoesNotExist",
	})
	assert.Equal(t, http.StatusBadRequest, missRec.Code)
	assert.Contains(t, missRec.Body.String(), "EntityNotFoundException")
}

func TestConnection_CRUD_WithCredentials(t *testing.T) {
	t.Parallel()

	jdbcProps := map[string]any{
		"JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
		"USERNAME":            "admin",
		"PASSWORD":            "secret",
	}

	tests := []struct {
		body        map[string]any
		check       func(t *testing.T, body string)
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name: "create-jdbc",
			op:   "CreateConnection",
			body: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":                 "jdbc-conn",
					"ConnectionType":       "JDBC",
					"ConnectionProperties": jdbcProps,
				},
			},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name: "create-duplicate",
			op:   "CreateConnection",
			body: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "jdbc-conn",
					"ConnectionType": "JDBC",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetConnection",
			body:     map[string]any{"Name": "jdbc-conn"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "jdbc-conn")
				assert.Contains(t, body, "JDBC")
			},
		},
		{
			name:     "get-missing",
			op:       "GetConnection",
			body:     map[string]any{"Name": "no-conn"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-connections",
			op:       "GetConnections",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "update",
			op:   "UpdateConnection",
			body: map[string]any{
				"Name": "jdbc-conn",
				"ConnectionInput": map[string]any{
					"Name":           "jdbc-conn",
					"ConnectionType": "JDBC",
					"ConnectionProperties": map[string]any{
						"JDBC_CONNECTION_URL": "jdbc:mysql://newhost:3306/db",
						"USERNAME":            "admin2",
						"PASSWORD":            "newsecret",
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteConnection",
			body:     map[string]any{"ConnectionName": "jdbc-conn"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-missing",
			op:       "DeleteConnection",
			body:     map[string]any{"ConnectionName": "no-conn"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{
						"Name":                 "jdbc-conn",
						"ConnectionType":       "JDBC",
						"ConnectionProperties": jdbcProps,
					},
				})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestConnection_BatchDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"conn-a", "conn-b"} {
		doGlueRequest(t, h, "CreateConnection", map[string]any{
			"ConnectionInput": map[string]any{"Name": name, "ConnectionType": "JDBC"},
		})
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "batch-delete-existing",
			body:     map[string]any{"ConnectionNameList": []string{"conn-a", "conn-b"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "batch-delete-missing",
			body:     map[string]any{"ConnectionNameList": []string{"no-conn"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "batch-delete-mixed",
			body:     map[string]any{"ConnectionNameList": []string{"conn-a", "no-conn"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			for _, name := range []string{"conn-a", "conn-b"} {
				doGlueRequest(t, h2, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{"Name": name, "ConnectionType": "JDBC"},
				})
			}

			rec := doGlueRequest(t, h2, "BatchDeleteConnection", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestConnection_NetworkType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "network-conn",
			"ConnectionType": "NETWORK",
			"ConnectionProperties": map[string]any{
				"HOST":      "10.0.1.5",
				"PORT":      "5432",
				"USER_NAME": "postgres",
				"PASSWORD":  "pgpass",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "network-conn"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	conn := out["Connection"].(map[string]any)
	assert.Equal(t, "NETWORK", conn["ConnectionType"])
	props := conn["ConnectionProperties"].(map[string]any)
	assert.Equal(t, "10.0.1.5", props["HOST"])
}

func TestConnection_CredentialsPersistAfterUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "cred-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]any{
				"USERNAME": "user1",
				"PASSWORD": "pass1",
			},
		},
	})

	doGlueRequest(t, h, "UpdateConnection", map[string]any{
		"Name": "cred-conn",
		"ConnectionInput": map[string]any{
			"Name":           "cred-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]any{
				"USERNAME": "user2",
				"PASSWORD": "pass2",
			},
		},
	})

	getRec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "cred-conn"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	conn := out["Connection"].(map[string]any)
	props := conn["ConnectionProperties"].(map[string]any)
	assert.Equal(t, "user2", props["USERNAME"])
	assert.Equal(t, "pass2", props["PASSWORD"])
}

// TestConnectionCRUD tests Connection create/get/delete operations.
func TestConnectionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler)
		body     map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "create_jdbc_connection",
			action: "CreateConnection",
			body: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "jdbc-conn",
					"ConnectionType": "JDBC",
					"ConnectionProperties": map[string]string{
						"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/db",
					},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				assert.Equal(t, "jdbc-conn", out["Name"])
			},
		},
		{
			name:     "create_empty_name_fails",
			action:   "CreateConnection",
			body:     map[string]any{"ConnectionInput": map[string]any{"Name": ""}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_not_found",
			action:   "GetConnection",
			body:     map[string]any{"Name": "no-such"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteConnection",
			body:     map[string]any{"ConnectionName": "no-such"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "create_duplicate_fails",
			action: "CreateConnection",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{"Name": "dup"},
				})
			},
			body:     map[string]any{"ConnectionInput": map[string]any{"Name": "dup"}},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doGlueRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestConnectionGetFields tests that GetConnection returns expected fields.
func TestConnectionGetFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		connType       string
		wantConnType   string
		wantCreation   bool
		wantLastUpdate bool
	}{
		{
			name:           "jdbc_fields",
			connType:       "JDBC",
			wantConnType:   "JDBC",
			wantCreation:   true,
			wantLastUpdate: true,
		},
		{
			name:           "sftp_fields",
			connType:       "SFTP",
			wantConnType:   "SFTP",
			wantCreation:   true,
			wantLastUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateConnection", map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "conn-" + tt.name,
					"ConnectionType": tt.connType,
				},
			})

			rec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "conn-" + tt.name})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			conn := out["Connection"].(map[string]any)
			assert.Equal(t, tt.wantConnType, conn["ConnectionType"])
			if tt.wantCreation {
				assert.NotZero(t, conn["CreationTime"])
			}
			if tt.wantLastUpdate {
				assert.NotZero(t, conn["LastUpdatedTime"])
			}
		})
	}
}

// TestGetConnections tests listing connections.
func TestGetConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		connCount int
		wantLen   int
	}{
		{name: "empty", connCount: 0, wantLen: 0},
		{name: "one", connCount: 1, wantLen: 1},
		{name: "four_sorted", connCount: 4, wantLen: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range tt.connCount {
				doGlueRequest(t, h, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{
						"Name":           fmt.Sprintf("conn-%d", i),
						"ConnectionType": "JDBC",
					},
				})
			}

			rec := doGlueRequest(t, h, "GetConnections", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			list, _ := out["ConnectionList"].([]any)
			assert.Len(t, list, tt.wantLen)
		})
	}
}

// TestConnectionTagging tests tagging Connections via ARN.
func TestConnectionTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(*testing.T, map[string]any)
		name   string
		action string
	}{
		{
			name:   "tag_and_get_tags",
			action: "TagResource",
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				tags, _ := out["Tags"].(map[string]any)
				assert.Equal(t, "bar", tags["foo"])
			},
		},
		{
			name:   "untag_resource",
			action: "UntagResource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			b.AddConnectionInternal(&glue.Connection{Name: "tagged-conn", ConnectionType: "JDBC"})
			h := glue.NewHandler(b)

			connARN := "arn:aws:glue:us-east-1:000000000000:connection/tagged-conn"

			if tt.action == "TagResource" || tt.action == "UntagResource" {
				rec := doGlueRequest(t, h, "TagResource", map[string]any{
					"ResourceArn": connARN,
					"TagsToAdd":   map[string]string{"foo": "bar"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.action == "TagResource" {
				rec := doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": connARN})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}

			if tt.action == "UntagResource" {
				rec := doGlueRequest(t, h, "UntagResource", map[string]any{
					"ResourceArn":  connARN,
					"TagsToRemove": []string{"foo"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": connARN})
				require.Equal(t, http.StatusOK, rec2.Code)
				var out2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
				tags, _ := out2["Tags"].(map[string]any)
				_, hasFoo := tags["foo"]
				assert.False(t, hasFoo)
			}
		})
	}
}

// TestTestConnection_ValidatesConnection verifies TestConnection checks that
// the named connection actually exists rather than always succeeding.
func TestTestConnection_ValidatesConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "no_name_or_input_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
		{
			name:     "unknown_connection_returns_400",
			input:    map[string]any{"ConnectionName": "does-not-exist"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "known_connection_returns_200",
			input:    map[string]any{"ConnectionName": "my-conn"},
			wantCode: http.StatusOK,
		},
		{
			name: "both_name_and_adhoc_returns_400",
			input: map[string]any{
				"ConnectionName": "my-conn",
				"TestConnectionInput": map[string]any{
					"ConnectionType":       "JDBC",
					"ConnectionProperties": map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://h/db"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "adhoc_missing_properties_returns_400",
			input: map[string]any{
				"TestConnectionInput": map[string]any{"ConnectionType": "JDBC"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "adhoc_well_formed_returns_200",
			input: map[string]any{
				"TestConnectionInput": map[string]any{
					"ConnectionType":       "JDBC",
					"ConnectionProperties": map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://h/db"},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateConnection", map[string]any{
				"ConnectionInput": map[string]any{
					"Name":                 "my-conn",
					"ConnectionType":       "JDBC",
					"ConnectionProperties": map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://h/db"},
				},
			})

			rec := doGlueRequest(t, h, "TestConnection", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestUpdateConnection_Updates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "my-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "UpdateConnection", map[string]any{
		"Name": "my-conn",
		"ConnectionInput": map[string]any{
			"Name":           "my-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://newhost:3306/db",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetConnection", map[string]any{
		"Name": "my-conn",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Connection struct {
			ConnectionProperties map[string]string `json:"ConnectionProperties"`
		} `json:"Connection"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "jdbc:mysql://newhost:3306/db", out.Connection.ConnectionProperties["JDBC_CONNECTION_URL"])
}

func TestUpdateConnection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "UpdateConnection", map[string]any{
		"Name": "nonexistent",
		"ConnectionInput": map[string]any{
			"Name":           "nonexistent",
			"ConnectionType": "JDBC",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
