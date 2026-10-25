package glue_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *crawlEnv) jdbcConnection(name, url, user, password string) {
	e.t.Helper()

	_, err := e.client.CreateConnection(e.t.Context(), &gluesdk.CreateConnectionInput{
		ConnectionInput: &gluetypes.ConnectionInput{
			Name: aws.String(name), ConnectionType: gluetypes.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{"JDBC_CONNECTION_URL": url, "USERNAME": user, "PASSWORD": password},
		},
	})
	require.NoError(e.t, err)
}

func TestCrawler_JDBCConnectionFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "unreachable host", url: "jdbc:postgresql://127.0.0.1:1/appdb", want: "connect to JDBC data store"},
		{name: "unsupported engine", url: "jdbc:oracle:thin:@//127.0.0.1:1521/x", want: "unsupported JDBC engine"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.jdbcConnection("conn", tt.url, "u", "p")
			env.create(&gluetypes.CrawlerTargets{JdbcTargets: []gluetypes.JdbcTarget{{
				ConnectionName: aws.String("conn"), Path: aws.String("appdb/%"),
			}}}, nil)

			c := env.crawl()
			assert.Equal(t, gluetypes.LastCrawlStatusFailed, c.LastCrawl.Status)
			assert.Contains(t, aws.ToString(c.LastCrawl.ErrorMessage), tt.want)
			assert.Equal(t, gluetypes.CrawlerHistoryStateFailed, env.lastCrawl().State)
		})
	}
}

type jdbcEngine struct {
	name, image, path, wantTable, wantLoc, jdbcFmt, user string
	env                                                  []string
	port                                                 string
	ddl                                                  []string
	wantCols                                             []string
	excluded                                             string
	exclusions                                           []string
}

func startContainer(t *testing.T, eng jdbcEngine) string {
	t.Helper()

	if os.Getenv("GOPHERSTACK_TEST_DOCKER") != "1" {
		t.Skip("set GOPHERSTACK_TEST_DOCKER=1 to run docker-backed JDBC crawler tests")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	args := make([]string, 0, 7+2*len(eng.env))
	args = append(args, "run", "-d", "--rm", "-p", "127.0.0.1::"+eng.port, "--label", "gopherstack-glue-crawler-test=1")

	for _, e := range eng.env {
		args = append(args, "-e", e)
	}

	out, err := exec.CommandContext(t.Context(), "docker", append(args, eng.image)...).Output()
	require.NoError(t, err)

	id := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		_ = exec.CommandContext(context.WithoutCancel(t.Context()), "docker", "rm", "-f", id).Run()
	})

	portOut, err := exec.CommandContext(t.Context(), "docker", "port", id, eng.port+"/tcp").Output()
	require.NoError(t, err)

	return strings.TrimSpace(strings.Split(strings.TrimSpace(string(portOut)), "\n")[0])
}

func openDockerDB(t *testing.T, eng jdbcEngine, addr string) *sql.DB {
	t.Helper()

	var db *sql.DB

	var err error

	if eng.name == "mysql" {
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = "root", "pw", "tcp", addr, "appdb"

		connector, cerr := mysql.NewConnector(cfg)
		require.NoError(t, cerr)

		db = sql.OpenDB(connector)
	} else {
		db, err = sql.Open("pgx", "postgres://postgres:pw@"+addr+"/appdb?sslmode=disable")
		require.NoError(t, err)
	}

	t.Cleanup(func() { _ = db.Close() })

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		return db.PingContext(ctx) == nil
	}, 3*time.Minute, time.Second)

	return db
}

func TestCrawler_JDBCTarget_Docker(t *testing.T) {
	t.Parallel()

	engines := []jdbcEngine{
		{
			name:    "postgres",
			image:   "postgres:16",
			port:    "5432",
			user:    "postgres",
			env:     []string{"POSTGRES_PASSWORD=pw", "POSTGRES_DB=appdb"},
			jdbcFmt: "jdbc:postgresql://%s/appdb",
			path:    "appdb/public/%",
			ddl: []string{
				"CREATE TABLE public.orders (id int PRIMARY KEY, total numeric(10,2), note text, created timestamp, tags text[])",
				"CREATE TABLE public.tmp_scratch (id int)",
				"CREATE SCHEMA audit",
				"CREATE TABLE audit.log (id bigint)",
			},
			wantTable: "appdb_public_orders",
			wantLoc:   "appdb.public.orders",
			wantCols: []string{
				"id:int",
				"total:decimal(10,2)",
				"note:string",
				"created:timestamp",
				"tags:array<string>",
			},
			exclusions: []string{"tmp_%"},
			excluded:   "appdb_public_tmp_scratch",
		},
		{
			name: "mysql", image: "mysql:8.4", port: "3306", user: "root",
			env:     []string{"MYSQL_ROOT_PASSWORD=pw", "MYSQL_DATABASE=appdb"},
			jdbcFmt: "jdbc:mysql://%s/appdb", path: "appdb/%",
			ddl: []string{
				"CREATE TABLE orders (id int PRIMARY KEY, total decimal(10,2), note text, created datetime)",
				"CREATE TABLE tmp_scratch (id int)",
			},
			wantTable: "appdb_orders", wantLoc: "appdb.orders",
			wantCols:   []string{"id:int", "total:decimal(10,2)", "note:string", "created:timestamp"},
			exclusions: []string{"tmp_%"}, excluded: "appdb_tmp_scratch",
		},
	}

	for _, eng := range engines {
		t.Run(eng.name, func(t *testing.T) {
			t.Parallel()

			addr := startContainer(t, eng)
			db := openDockerDB(t, eng, addr)

			for _, stmt := range eng.ddl {
				_, err := db.ExecContext(t.Context(), stmt)
				require.NoError(t, err)
			}

			env := newCrawlEnv(t)
			env.jdbcConnection("conn", strings.Replace(eng.jdbcFmt, "%s", addr, 1), eng.user, "pw")
			env.create(&gluetypes.CrawlerTargets{JdbcTargets: []gluetypes.JdbcTarget{{
				ConnectionName: aws.String("conn"), Path: aws.String(eng.path), Exclusions: eng.exclusions,
			}}}, nil)

			c := env.crawl()
			require.Equal(
				t,
				gluetypes.LastCrawlStatusSucceeded,
				c.LastCrawl.Status,
				aws.ToString(c.LastCrawl.ErrorMessage),
			)

			tbl := env.table(eng.wantTable)
			assert.Equal(t, eng.wantCols, colSpec(tbl.StorageDescriptor.Columns))
			assert.Equal(t, eng.wantLoc, aws.ToString(tbl.StorageDescriptor.Location))
			assert.Equal(t, "conn", tbl.Parameters["connectionName"])
			assert.Equal(t, "table", tbl.Parameters["typeOfData"])
			assert.False(t, env.hasTable(eng.excluded), "excluded table must not be catalogued")
			assert.False(t, env.hasTable("appdb_audit_log"), "other schema outside the include path")
			assert.Equal(t, int32(1), env.metrics().TablesCreated)
		})
	}
}
