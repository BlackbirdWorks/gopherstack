package app

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type athenaEngineFx struct {
	ath  *athena.Client
	s3c  *s3.Client
	glue *glue.Client
	out  string
}

func newAthenaEngineFx(t *testing.T) *athenaEngineFx {
	t.Helper()

	fx := newSFNFixture(t)
	f := &athenaEngineFx{
		ath:  athena.NewFromConfig(fx.cfg),
		s3c:  s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }),
		glue: glue.NewFromConfig(fx.cfg),
		out:  "athena-results",
	}

	_, err := f.s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(f.out)})
	require.NoError(t, err)

	_, err = f.s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("lake")})
	require.NoError(t, err)

	return f
}

func (f *athenaEngineFx) put(t *testing.T, key string, body []byte) {
	t.Helper()

	_, err := f.s3c.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("lake"), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	require.NoError(t, err)
}

func (f *athenaEngineFx) createDB(t *testing.T, name string) {
	t.Helper()

	_, err := f.glue.CreateDatabase(t.Context(), &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(name)},
	})
	require.NoError(t, err)
}

func (f *athenaEngineFx) run(t *testing.T, db, sql string) (string, *athenatypes.QueryExecution) {
	t.Helper()

	start, err := f.ath.StartQueryExecution(t.Context(), &athena.StartQueryExecutionInput{
		QueryString:           aws.String(sql),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{Database: aws.String(db)},
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String("s3://" + f.out + "/out/"),
		},
	})
	require.NoError(t, err)

	var qe *athenatypes.QueryExecution

	require.Eventually(t, func() bool {
		got, gerr := f.ath.GetQueryExecution(t.Context(), &athena.GetQueryExecutionInput{
			QueryExecutionId: start.QueryExecutionId,
		})
		if gerr != nil {
			return false
		}

		qe = got.QueryExecution

		return qe.Status.State != athenatypes.QueryExecutionStateQueued &&
			qe.Status.State != athenatypes.QueryExecutionStateRunning
	}, 10*time.Second, 5*time.Millisecond)

	return aws.ToString(start.QueryExecutionId), qe
}

func (f *athenaEngineFx) rows(t *testing.T, id string) [][]string {
	t.Helper()

	var out [][]string

	pager := athena.NewGetQueryResultsPaginator(f.ath, &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(id), MaxResults: aws.Int32(2),
	})

	for pager.HasMorePages() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, r := range page.ResultSet.Rows {
			row := make([]string, 0, len(r.Data))
			for _, d := range r.Data {
				row = append(row, aws.ToString(d.VarCharValue))
			}

			out = append(out, row)
		}
	}

	return out
}

func gz(t *testing.T, s string) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := gzip.NewWriter(&buf)
	_, err := w.Write([]byte(s))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

const salesCSV = "id,category,amount\n1,books,10\n2,books,20\n3,games,5\n4,games,50\n5,toys,7\n6,toys,3\n"

func TestAthenaEngine_SelectOverS3(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.createDB(t, "shop")
	f.put(t, "sales/part-0.csv", []byte(salesCSV))
	f.put(t, "events/e1.json", []byte(`{"user":"a","kind":"click","n":1}`+"\n"+`{"user":"b","kind":"view","n":2}`+"\n"))
	f.put(t, "events/e2.json", []byte(`{"user":"a","kind":"view","n":5}`+"\n"))
	f.put(t, "gzsales/s.csv.gz", gz(t, salesCSV))

	_, err := f.glue.CreateTable(t.Context(), &glue.CreateTableInput{
		DatabaseName: aws.String("shop"),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("sales"),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Location: aws.String("s3://lake/sales/"),
				Columns: []gluetypes.Column{
					{Name: aws.String("id"), Type: aws.String("int")},
					{Name: aws.String("category"), Type: aws.String("string")},
					{Name: aws.String("amount"), Type: aws.String("double")},
				},
				SerdeInfo: &gluetypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.serde2.OpenCSVSerde"),
				},
			},
			Parameters: map[string]string{"skip.header.line.count": "1"},
		},
	})
	require.NoError(t, err)

	cases := []struct {
		name  string
		setup string
		query string
		want  [][]string
	}{
		{
			name: "csv group order limit",
			query: "SELECT category, count(*) AS n, sum(amount) AS total FROM shop.sales WHERE amount > 4 " +
				"GROUP BY category ORDER BY total DESC LIMIT 2",
			want: [][]string{{"category", "n", "total"}, {"games", "2", "55.0"}, {"books", "2", "30.0"}},
		},
		{
			name:  "quoted identifiers",
			query: `SELECT "category", approx_distinct(id) AS d FROM "shop"."sales" GROUP BY 1 ORDER BY 1`,
			want:  [][]string{{"category", "d"}, {"books", "2"}, {"games", "2"}, {"toys", "2"}},
		},
		{
			name: "json ddl",
			setup: "CREATE EXTERNAL TABLE shop.events (user string, kind string, n int) " +
				"ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://lake/events/'",
			query: "SELECT user, sum(n) AS total FROM shop.events WHERE kind = 'view' GROUP BY user ORDER BY user",
			want:  [][]string{{"user", "total"}, {"a", "5"}, {"b", "2"}},
		},
		{
			name: "gzip csv ddl",
			setup: "CREATE EXTERNAL TABLE shop.gzsales (id int, category string, amount double) " +
				"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://lake/gzsales/' " +
				"TBLPROPERTIES ('skip.header.line.count'='1')",
			query: "SELECT count(*) AS n FROM shop.gzsales",
			want:  [][]string{{"n"}, {"6"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.setup != "" {
				_, qe := f.run(t, "shop", tc.setup)
				require.Equal(
					t,
					athenatypes.QueryExecutionStateSucceeded,
					qe.Status.State,
					aws.ToString(qe.Status.StateChangeReason),
				)
			}

			id, qe := f.run(t, "shop", tc.query)
			require.Equal(
				t,
				athenatypes.QueryExecutionStateSucceeded,
				qe.Status.State,
				aws.ToString(qe.Status.StateChangeReason),
			)
			assert.Equal(t, tc.want, f.rows(t, id))
			assert.Positive(t, aws.ToInt64(qe.Statistics.DataScannedInBytes))
			assert.Positive(t, aws.ToInt64(qe.Statistics.EngineExecutionTimeInMillis))

			obj, gerr := f.s3c.GetObject(t.Context(), &s3.GetObjectInput{
				Bucket: aws.String(f.out), Key: aws.String("out/" + id + ".csv"),
			})
			require.NoError(t, gerr)

			body, _ := io.ReadAll(obj.Body)
			lines := strings.Split(strings.TrimSpace(string(body)), "\n")
			assert.Len(t, lines, len(tc.want))
			assert.Contains(t, lines[0], `"`+tc.want[0][0]+`"`)
		})
	}
}

func TestAthenaEngine_PartitionsAndFailures(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.createDB(t, "logs")
	f.put(t, "logs/year=2024/month=01/a.csv", []byte("a,1\nb,2\n"))
	f.put(t, "logs/year=2024/month=02/b.csv", []byte("c,3\n"))
	f.put(t, "logs/year=2025/month=01/c.csv", []byte("d,4\n"))
	f.put(t, "pq/part.parquet", []byte("PAR1"))

	cases := []struct {
		name       string
		query      string
		wantState  athenatypes.QueryExecutionState
		wantReason string
		want       [][]string
	}{
		{
			name: "create partitioned",
			query: "CREATE EXTERNAL TABLE logs.hits (name string, v int) PARTITIONED BY (year int, month string) " +
				"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://lake/logs/'",
			wantState: athenatypes.QueryExecutionStateSucceeded,
		},
		{
			name:      "select before repair",
			query:     "SELECT count(*) AS n FROM logs.hits",
			wantState: athenatypes.QueryExecutionStateSucceeded,
			want:      [][]string{{"n"}, {"0"}},
		},
		{
			name:      "msck repair",
			query:     "MSCK REPAIR TABLE logs.hits",
			wantState: athenatypes.QueryExecutionStateSucceeded,
		},
		{
			name:      "partition columns",
			query:     "SELECT year, month, sum(v) AS s FROM logs.hits WHERE year = 2024 GROUP BY year, month ORDER BY month",
			wantState: athenatypes.QueryExecutionStateSucceeded,
			want:      [][]string{{"year", "month", "s"}, {"2024", "01", "3"}, {"2024", "02", "3"}},
		},
		{
			name:      "show partitions",
			query:     "SHOW PARTITIONS logs.hits",
			wantState: athenatypes.QueryExecutionStateSucceeded,
			want: [][]string{
				{"partition"}, {"year=2024/month=01"}, {"year=2024/month=02"}, {"year=2025/month=01"},
			},
		},
		{
			name:      "show tables",
			query:     "SHOW TABLES IN logs",
			wantState: athenatypes.QueryExecutionStateSucceeded,
			want:      [][]string{{"tab_name"}, {"hits"}},
		},
		{
			name:      "describe",
			query:     "DESCRIBE logs.hits",
			wantState: athenatypes.QueryExecutionStateSucceeded,
		},
		{
			name:       "parquet unsupported",
			query:      "CREATE EXTERNAL TABLE logs.pq (a string) STORED AS PARQUET LOCATION 's3://lake/pq/'",
			wantState:  athenatypes.QueryExecutionStateSucceeded,
			wantReason: "",
		},
		{
			name:       "parquet select fails",
			query:      "SELECT * FROM logs.pq",
			wantState:  athenatypes.QueryExecutionStateFailed,
			wantReason: "Parquet",
		},
		{
			name:       "syntax error",
			query:      "SELECT category FROM FROM logs.hits",
			wantState:  athenatypes.QueryExecutionStateFailed,
			wantReason: "SYNTAX_ERROR",
		},
		{
			name:       "missing column",
			query:      "SELECT nope FROM logs.hits",
			wantState:  athenatypes.QueryExecutionStateFailed,
			wantReason: "COLUMN_NOT_FOUND",
		},
		{
			name:       "missing table",
			query:      "SELECT * FROM logs.hits h JOIN logs.ghost g ON h.name = g.name",
			wantState:  athenatypes.QueryExecutionStateFailed,
			wantReason: "TABLE_NOT_FOUND",
		},
		{
			name:       "stacked statements",
			query:      "SELECT 1 FROM logs.hits; DROP TABLE logs.hits",
			wantState:  athenatypes.QueryExecutionStateFailed,
			wantReason: "single statement",
		},
		{
			name:      "drop table",
			query:     "DROP TABLE logs.hits",
			wantState: athenatypes.QueryExecutionStateSucceeded,
		},
		{
			name:       "select dropped",
			query:      "SELECT * FROM logs.hits",
			wantState:  athenatypes.QueryExecutionStateFailed,
			wantReason: "TABLE_NOT_FOUND",
		},
	}

	for _, tc := range cases {
		id, qe := f.run(t, "logs", tc.query)
		require.Equal(t, tc.wantState, qe.Status.State, tc.name+": "+aws.ToString(qe.Status.StateChangeReason))
		assert.Contains(t, aws.ToString(qe.Status.StateChangeReason), tc.wantReason, tc.name)

		if tc.want != nil {
			assert.Equal(t, tc.want, f.rows(t, id), tc.name)
		}
	}
}
