package redshiftdata_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	redshiftdatatypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

var errNoRelation = errors.New(`ERROR: relation "missing" does not exist`)

type fakeEngines struct {
	logins map[string]redshiftdata.EngineLogin
}

func (f fakeEngines) EngineLogin(region, id string) (redshiftdata.EngineLogin, bool) {
	l, ok := f.logins[region+"/"+id]

	return l, ok
}

type execCall struct {
	login  redshiftdata.EngineLogin
	sqls   []string
	params []redshiftdata.SQLParameter
	atomic bool
}

type fakeExecutor struct {
	outcomes []redshiftdata.SQLOutcome
	calls    []execCall
	mu       sync.Mutex
}

func (f *fakeExecutor) ExecuteSQL(
	_ context.Context, l redshiftdata.EngineLogin, sqls []string, params []redshiftdata.SQLParameter, atomic bool,
) []redshiftdata.SQLOutcome {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, execCall{login: l, sqls: sqls, params: params, atomic: atomic})

	return f.outcomes
}

func (f *fakeExecutor) lastCall(t *testing.T) execCall {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	require.NotEmpty(t, f.calls)

	return f.calls[len(f.calls)-1]
}

func str(s string) redshiftdata.Field { return redshiftdata.Field{StringValue: &s} }
func lng(v int64) redshiftdata.Field  { return redshiftdata.Field{LongValue: &v} }
func dbl(v float64) redshiftdata.Field {
	return redshiftdata.Field{DoubleValue: &v}
}
func boolean(v bool) redshiftdata.Field { return redshiftdata.Field{BooleanValue: &v} }
func null() redshiftdata.Field          { return redshiftdata.Field{IsNull: true} }

func sampleResult() *redshiftdata.ResultSet {
	return &redshiftdata.ResultSet{
		Columns: []redshiftdata.ColumnMetadata{
			{Name: "id", Label: "id", TypeName: "int4", Precision: 10, IsSigned: true, Nullable: 1},
			{Name: "name", Label: "name", TypeName: "varchar", Length: 256, IsCaseSensitive: true, Nullable: 1},
			{Name: "score", Label: "score", TypeName: "float8", Precision: 17, IsSigned: true, Nullable: 1},
			{Name: "ok", Label: "ok", TypeName: "bool", Nullable: 1},
			{Name: "at", Label: "at", TypeName: "timestamp", Nullable: 1},
		},
		Rows: [][]redshiftdata.Field{
			{lng(1), str("ada"), dbl(1.5), boolean(true), str("2024-01-02 03:04:05")},
			{lng(2), null(), null(), boolean(false), null()},
		},
	}
}

func engineHandler(
	t *testing.T, exec *fakeExecutor,
) (*redshiftdatasdk.Client, *redshiftdata.InMemoryBackend) {
	t.Helper()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	backend.SetClusterEngines(fakeEngines{logins: map[string]redshiftdata.EngineLogin{
		testRegion + "/live": {Addr: "127.0.0.1:15439", User: "admin", Password: "pw", DBName: "dev"},
	}})
	backend.SetSQLExecutor(exec)

	return newTestRedshiftDataSDKClient(t, redshiftdata.NewHandler(backend)), backend
}

func TestEngineExecuteStatementResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		outcome        redshiftdata.SQLOutcome
		name           string
		sql            string
		database       string
		wantStatus     redshiftdatatypes.StatusString
		wantErr        string
		wantDB         string
		wantRows       int64
		wantHasResults bool
	}{
		{
			name:           "select returns records",
			sql:            "SELECT * FROM t",
			outcome:        redshiftdata.SQLOutcome{Result: sampleResult()},
			wantStatus:     redshiftdatatypes.StatusStringFinished,
			wantHasResults: true,
			wantRows:       2,
			wantDB:         "dev",
		},
		{
			name: "database override", sql: "SELECT 1", database: "other",
			outcome:    redshiftdata.SQLOutcome{Result: sampleResult()},
			wantStatus: redshiftdatatypes.StatusStringFinished, wantHasResults: true, wantRows: 2, wantDB: "other",
		},
		{
			name: "ddl has no result set", sql: "CREATE TABLE t (id int)", outcome: redshiftdata.SQLOutcome{},
			wantStatus: redshiftdatatypes.StatusStringFinished, wantRows: -1, wantDB: "dev",
		},
		{
			name: "failure", sql: "SELECT * FROM missing", outcome: redshiftdata.SQLOutcome{Err: errNoRelation},
			wantStatus: redshiftdatatypes.StatusStringFailed, wantErr: errNoRelation.Error(), wantDB: "dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exec := &fakeExecutor{outcomes: []redshiftdata.SQLOutcome{tt.outcome}}
			client, _ := engineHandler(t, exec)

			out, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
				Sql: aws.String(tt.sql), ClusterIdentifier: aws.String("live"), Database: aws.String(tt.database),
			})
			require.NoError(t, err)
			assert.Equal(t, string(tt.wantStatus), string(out.Status))

			call := exec.lastCall(t)
			assert.Equal(t, []string{tt.sql}, call.sqls)
			assert.Equal(t, "admin", call.login.User)
			assert.Equal(t, tt.wantDB, call.login.DBName)
			assert.False(t, call.atomic)

			desc, err := client.DescribeStatement(t.Context(), &redshiftdatasdk.DescribeStatementInput{Id: out.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.Status)
			assert.Equal(t, tt.wantHasResults, aws.ToBool(desc.HasResultSet))
			assert.Equal(t, tt.wantErr, aws.ToString(desc.Error))

			if tt.wantStatus == redshiftdatatypes.StatusStringFinished {
				assert.Equal(t, tt.wantRows, desc.ResultRows)
			}

			res, err := client.GetStatementResult(t.Context(), &redshiftdatasdk.GetStatementResultInput{Id: out.Id})
			if !tt.wantHasResults {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantRows, res.TotalNumRows)
			require.Len(t, res.ColumnMetadata, 5)
			assert.Equal(t, "int4", aws.ToString(res.ColumnMetadata[0].TypeName))
			assert.Equal(t, "varchar", aws.ToString(res.ColumnMetadata[1].TypeName))
			assert.Equal(t, int32(256), res.ColumnMetadata[1].Length)
			require.Len(t, res.Records, 2)

			assert.Equal(t, int64(1), res.Records[0][0].(*redshiftdatatypes.FieldMemberLongValue).Value)
			assert.Equal(t, "ada", res.Records[0][1].(*redshiftdatatypes.FieldMemberStringValue).Value)
			assert.InDelta(t, 1.5, res.Records[0][2].(*redshiftdatatypes.FieldMemberDoubleValue).Value, 0)
			assert.True(t, res.Records[0][3].(*redshiftdatatypes.FieldMemberBooleanValue).Value)
			assert.Equal(t, "2024-01-02 03:04:05", res.Records[0][4].(*redshiftdatatypes.FieldMemberStringValue).Value)
			assert.True(t, res.Records[1][1].(*redshiftdatatypes.FieldMemberIsNull).Value)
			assert.True(t, res.Records[1][2].(*redshiftdatatypes.FieldMemberIsNull).Value)
			assert.False(t, res.Records[1][3].(*redshiftdatatypes.FieldMemberBooleanValue).Value)
		})
	}
}

func TestEngineExecuteStatementResultCSV(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{outcomes: []redshiftdata.SQLOutcome{{Result: sampleResult()}}}
	client, _ := engineHandler(t, exec)

	out, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
		Sql: aws.String("SELECT * FROM t"), ClusterIdentifier: aws.String("live"),
		ResultFormat: redshiftdatatypes.ResultFormatStringCsv,
	})
	require.NoError(t, err)

	res, err := client.GetStatementResultV2(t.Context(), &redshiftdatasdk.GetStatementResultV2Input{Id: out.Id})
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.TotalNumRows)
	require.Len(t, res.Records, 2)
	assert.Equal(
		t,
		"1,ada,1.5,true,2024-01-02 03:04:05",
		res.Records[0].(*redshiftdatatypes.QueryRecordsMemberCSVRecords).Value,
	)
	assert.Equal(t, "2,,,false,", res.Records[1].(*redshiftdatatypes.QueryRecordsMemberCSVRecords).Value)
}

func TestEngineParametersReachExecutor(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{outcomes: []redshiftdata.SQLOutcome{{}}}
	client, _ := engineHandler(t, exec)

	_, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
		Sql: aws.String("INSERT INTO t VALUES (:id)"), ClusterIdentifier: aws.String("live"),
		Parameters: []redshiftdatatypes.SqlParameter{{Name: aws.String("id"), Value: aws.String("7")}},
	})
	require.NoError(t, err)
	assert.Equal(t, []redshiftdata.SQLParameter{{Name: "id", Value: "7"}}, exec.lastCall(t).params)
}

func TestEngineNotUsedWithoutLiveCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input redshiftdatasdk.ExecuteStatementInput
	}{
		{name: "unknown cluster", input: redshiftdatasdk.ExecuteStatementInput{ClusterIdentifier: aws.String("stub")}},
		{name: "workgroup", input: redshiftdatasdk.ExecuteStatementInput{WorkgroupName: aws.String("live")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exec := &fakeExecutor{}
			client, _ := engineHandler(t, exec)
			tt.input.Sql = aws.String("SELECT 1")

			out, err := client.ExecuteStatement(t.Context(), &tt.input)
			require.NoError(t, err)

			res, err := client.GetStatementResult(t.Context(), &redshiftdatasdk.GetStatementResultInput{Id: out.Id})
			require.NoError(t, err)
			assert.Equal(t, "mock_value", res.Records[0][0].(*redshiftdatatypes.FieldMemberStringValue).Value)

			exec.mu.Lock()
			defer exec.mu.Unlock()
			assert.Empty(t, exec.calls)
		})
	}
}

func TestEngineBatchExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		executeMode  string
		outcomes     []redshiftdata.SQLOutcome
		wantStatus   redshiftdatatypes.StatusString
		wantSubs     []redshiftdatatypes.StatusString
		wantAtomic   bool
		wantErrorSet bool
	}{
		{
			name: "all succeed in a transaction", executeMode: "TRANSACTION", wantAtomic: true,
			outcomes:   []redshiftdata.SQLOutcome{{}, {Result: sampleResult()}, {}},
			wantStatus: redshiftdatatypes.StatusStringFinished,
			wantSubs: []redshiftdatatypes.StatusString{
				redshiftdatatypes.StatusStringFinished, redshiftdatatypes.StatusStringFinished,
				redshiftdatatypes.StatusStringFinished,
			},
		},
		{
			name: "second fails and rest abort", executeMode: "AUTO_COMMIT",
			outcomes:   []redshiftdata.SQLOutcome{{}, {Err: errNoRelation}},
			wantStatus: redshiftdatatypes.StatusStringFailed, wantErrorSet: true,
			wantSubs: []redshiftdatatypes.StatusString{
				redshiftdatatypes.StatusStringFinished, redshiftdatatypes.StatusStringFailed,
				redshiftdatatypes.StatusStringAborted,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exec := &fakeExecutor{outcomes: tt.outcomes}
			client, _ := engineHandler(t, exec)
			sqls := []string{"CREATE TABLE t (id int)", "SELECT * FROM t", "DROP TABLE t"}

			out, err := client.BatchExecuteStatement(t.Context(), &redshiftdatasdk.BatchExecuteStatementInput{
				Sqls:              sqls,
				ClusterIdentifier: aws.String("live"),
				ExecutionMode:     redshiftdatatypes.ExecutionMode(tt.executeMode),
			})
			require.NoError(t, err)

			call := exec.lastCall(t)
			assert.Equal(t, sqls, call.sqls)
			assert.Equal(t, tt.wantAtomic, call.atomic)

			desc, err := client.DescribeStatement(t.Context(), &redshiftdatasdk.DescribeStatementInput{Id: out.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.Status)
			assert.Equal(t, tt.wantErrorSet, aws.ToString(desc.Error) != "")
			require.Len(t, desc.SubStatements, len(tt.wantSubs))

			for i, want := range tt.wantSubs {
				assert.Equal(t, string(want), string(desc.SubStatements[i].Status), "sub %d", i)
			}

			if tt.wantStatus != redshiftdatatypes.StatusStringFinished {
				return
			}

			assert.True(t, aws.ToBool(desc.SubStatements[1].HasResultSet))
			assert.False(t, aws.ToBool(desc.SubStatements[0].HasResultSet))

			res, err := client.GetStatementResult(t.Context(), &redshiftdatasdk.GetStatementResultInput{
				Id: desc.SubStatements[1].Id,
			})
			require.NoError(t, err)
			assert.Len(t, res.Records, 2)
		})
	}
}

func TestEngineStatementsEvictCleanly(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{outcomes: []redshiftdata.SQLOutcome{{Result: sampleResult()}}}
	client, backend := engineHandler(t, exec)

	out, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
		Sql: aws.String("SELECT 1"), ClusterIdentifier: aws.String("live"),
	})
	require.NoError(t, err)

	assert.Equal(t, 1, backend.EvictExpiredStatements(time.Now().Add(time.Hour)))

	_, err = client.GetStatementResult(t.Context(), &redshiftdatasdk.GetStatementResultInput{Id: out.Id})
	require.Error(t, err)
}

func TestFieldFromValue(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)

	tests := []struct {
		value  any
		check  func(t *testing.T, f redshiftdata.Field)
		name   string
		dbType string
	}{
		{
			name:   "null",
			value:  nil,
			dbType: "INT4",
			check: func(t *testing.T, f redshiftdata.Field) {
				t.Helper()
				assert.True(t, f.IsNull)
			},
		},
		{name: "int8", value: int64(9007199254740993), dbType: "INT8", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, int64(9007199254740993), *f.LongValue)
		}},
		{name: "int4", value: int32(7), dbType: "INT4", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, int64(7), *f.LongValue)
		}},
		{name: "bool", value: true, dbType: "BOOL", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.True(t, *f.BooleanValue)
		}},
		{name: "float8", value: 2.25, dbType: "FLOAT8", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.InDelta(t, 2.25, *f.DoubleValue, 0)
		}},
		{name: "nan", value: math.NaN(), dbType: "FLOAT8", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, "NaN", *f.StringValue)
		}},
		{name: "varchar", value: "hello", dbType: "VARCHAR", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, "hello", *f.StringValue)
		}},
		{
			name:   "numeric keeps precision",
			value:  "12.50",
			dbType: "NUMERIC",
			check: func(t *testing.T, f redshiftdata.Field) {
				t.Helper()
				assert.Equal(t, "12.50", *f.StringValue)
			},
		},
		{name: "timestamp", value: ts, dbType: "TIMESTAMP", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, "2024-01-02 03:04:05.123456", *f.StringValue)
		}},
		{name: "timestamptz", value: ts, dbType: "TIMESTAMPTZ", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, "2024-01-02 03:04:05.123456+00", *f.StringValue)
		}},
		{name: "date", value: ts, dbType: "DATE", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, "2024-01-02", *f.StringValue)
		}},
		{name: "bytea", value: []byte{1, 2, 3}, dbType: "BYTEA", check: func(t *testing.T, f redshiftdata.Field) {
			t.Helper()
			assert.Equal(t, "AQID", *f.BlobValue)
		}},
		{
			name:   "json document",
			value:  map[string]any{"a": 1.0},
			dbType: "JSONB",
			check: func(t *testing.T, f redshiftdata.Field) {
				t.Helper()
				assert.JSONEq(t, `{"a":1}`, *f.StringValue)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.check(t, redshiftdata.FieldFromValue(tt.dbType, tt.value))
		})
	}
}

func TestBindNamedParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		params   []redshiftdata.SQLParameter
		wantSQL  string
		wantArgs []any
		wantErr  bool
	}{
		{
			name: "named placeholders", sql: "SELECT * FROM t WHERE id = :id AND name = :name",
			params:  []redshiftdata.SQLParameter{{Name: "id", Value: "7"}, {Name: "name", Value: "ada"}},
			wantSQL: "SELECT * FROM t WHERE id = $1 AND name = $2", wantArgs: []any{"7", "ada"},
		},
		{
			name: "repeated name reuses position", sql: "SELECT :a, :a",
			params:  []redshiftdata.SQLParameter{{Name: "a", Value: "x"}},
			wantSQL: "SELECT $1, $1", wantArgs: []any{"x"},
		},
		{
			name: "casts quotes and comments untouched", sql: "SELECT ':a'::text, \":a\", :a -- :a\n",
			params:  []redshiftdata.SQLParameter{{Name: "a", Value: "x"}},
			wantSQL: "SELECT ':a'::text, \":a\", $1 -- :a\n", wantArgs: []any{"x"},
		},
		{
			name: "missing parameter", sql: "SELECT :nope",
			params:  []redshiftdata.SQLParameter{{Name: "a", Value: "x"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotSQL, gotArgs, err := redshiftdata.BindNamedParameters(tt.sql, tt.params)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, gotSQL)
			assert.Equal(t, tt.wantArgs, gotArgs)
		})
	}
}
