package gcpspanner_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const (
	rwSingers = "CREATE TABLE Singers (SingerId INT64 NOT NULL, FirstName STRING(1024), LastName STRING(1024), " +
		"SingerInfo BYTES(MAX), BirthDate DATE, Id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE Seq)), " +
		"FullName STRING(2048) AS (ARRAY_TO_STRING([FirstName, LastName], \" \")) STORED, " +
		"LastUpdated TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), " +
		"CONSTRAINT chk CHECK (SingerId > 0)) PRIMARY KEY (SingerId)"
	rwAlbums = "CREATE TABLE Albums (SingerId INT64 NOT NULL, AlbumId INT64 NOT NULL, AlbumTitle STRING(MAX), " +
		"MarketingBudget INT64, CONSTRAINT FK_x FOREIGN KEY (SingerId) REFERENCES Singers (SingerId)) " +
		"PRIMARY KEY (SingerId, AlbumId), INTERLEAVE IN PARENT Singers ON DELETE CASCADE"
)

func realWorldGoogleSQL() []string {
	return []string{
		"CREATE SCHEMA sales",
		"CREATE SEQUENCE Seq OPTIONS (sequence_kind='bit_reversed_positive')",
		rwSingers,
		rwAlbums,
		"CREATE TABLE IF NOT EXISTS Logs (Id INT64 NOT NULL, ts TIMESTAMP) PRIMARY KEY (Id), " +
			"ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 30 DAY))",
		"CREATE TABLE Events (Id INT64 NOT NULL, Ref INT64) PRIMARY KEY (Id), INTERLEAVE IN Singers",
		"CREATE TABLE sales.Orders (Id INT64 NOT NULL) PRIMARY KEY (Id)",
		"CREATE UNIQUE NULL_FILTERED INDEX AlbumsByTitle ON Albums (AlbumTitle DESC) STORING (MarketingBudget), " +
			"INTERLEAVE IN Singers",
		"CREATE INDEX IF NOT EXISTS i2 ON Singers (LastName, FirstName ASC)",
		"CREATE INDEX salesIdx ON sales.Orders (Id)",
		"CREATE SEARCH INDEX AlbumsIdx ON Albums (AlbumTitle_Tokens) STORING (MarketingBudget) " +
			"PARTITION BY SingerId ORDER BY AlbumId OPTIONS (sort_order_sharding=true)",
		"CREATE VECTOR INDEX Vec ON Albums (Emb) WHERE Emb IS NOT NULL OPTIONS (distance_type='COSINE', tree_depth=2)",
		"CREATE VIEW v SQL SECURITY INVOKER AS SELECT * FROM Singers",
		"CREATE OR REPLACE VIEW v SQL SECURITY INVOKER AS SELECT 'a,b)' AS x FROM Singers",
		"CREATE CHANGE STREAM Cs FOR Singers(FirstName), Albums OPTIONS (retention_period = '7d')",
		"CREATE CHANGE STREAM Cs2 FOR ALL",
		"CREATE ROLE r",
		"CREATE ROLE r2",
		"GRANT SELECT ON TABLE Singers TO ROLE r",
		"GRANT ROLE r TO ROLE r2",
		"REVOKE SELECT ON TABLE Singers FROM ROLE r",
		"CREATE MODEL M INPUT (a INT64) OUTPUT (b INT64) REMOTE OPTIONS (endpoint='//aiplatform.googleapis.com/e')",
		"CREATE PROPERTY GRAPH G NODE TABLES (Singers KEY (SingerId)) EDGE TABLES (Albums KEY (AlbumId) " +
			"SOURCE KEY (SingerId) REFERENCES Singers (SingerId) " +
			"DESTINATION KEY (SingerId) REFERENCES Singers (SingerId))",
		"CREATE PROTO BUNDLE (examples.shipping.Order)",
		"ALTER PROTO BUNDLE INSERT (examples.shipping.Item)",
		"CREATE LOCALITY GROUP spill OPTIONS (storage='ssd')",
		"ALTER DATABASE db SET OPTIONS (optimizer_version=2)",
		"ALTER TABLE Singers ADD COLUMN Nick STRING(10)",
		"ALTER TABLE Singers ADD Nick2 STRING(10)",
		"ALTER TABLE Singers ALTER COLUMN Nick SET OPTIONS (allow_commit_timestamp=null)",
		"ALTER TABLE Singers ALTER COLUMN Nick2 DROP DEFAULT",
		"ALTER TABLE Singers ALTER COLUMN Nick2 SET DEFAULT ('x')",
		"ALTER TABLE Singers ADD SYNONYM S",
		"ALTER TABLE Albums ADD FOREIGN KEY (SingerId) REFERENCES Singers (SingerId)",
		"ALTER TABLE Albums ADD CHECK (AlbumId > 0)",
		"ALTER INDEX i2 ADD STORED COLUMN Nick",
		"ALTER INDEX i2 DROP STORED COLUMN Nick",
		"ALTER CHANGE STREAM Cs SET FOR ALL",
		"ALTER SEQUENCE Seq SET OPTIONS (skip_range_min=1, skip_range_max=2)",
		"ALTER STATISTICS pkg SET OPTIONS (allow_gc=false)",
		"ALTER TABLE Albums SET INTERLEAVE IN Singers",
		"ANALYZE",
		"RENAME TABLE Events TO Events2",
		"DROP PROTO BUNDLE",
		"DROP LOCALITY GROUP spill",
		"DROP PROPERTY GRAPH G",
		"DROP MODEL M",
		"DROP VECTOR INDEX Vec",
		"DROP SEARCH INDEX AlbumsIdx",
		"DROP VIEW v",
		"DROP CHANGE STREAM Cs",
		"DROP CHANGE STREAM Cs2",
		"DROP ROLE r2",
		"DROP INDEX salesIdx",
		"DROP TABLE sales.Orders",
		"DROP SCHEMA sales",
		"DROP TABLE IF EXISTS Logs",
		"DROP INDEX IF EXISTS gone",
		"CREATE TABLE `order` (`select` INT64 NOT NULL, `a\\`b` STRING(MAX) OPTIONS (x=\"a,b\")) PRIMARY KEY (`select`)",
		"CREATE TABLE /* c */ Quoted ( -- c\n  Id INT64 NOT NULL, # c\n" +
			"  S STRING(MAX) DEFAULT ('it''s', \"\"\"x\"\"\")\n) PRIMARY KEY (Id)",
		"CREATE VIEW v3 SQL SECURITY INVOKER AS SELECT r'\\d+' AS p, b'\\x00' AS q FROM Quoted",
	}
}

func realWorldPostgres() []string {
	return []string{
		"CREATE SCHEMA sales",
		"CREATE TABLE singers (id bigint NOT NULL, name varchar(100), PRIMARY KEY (id))",
		"CREATE TABLE albums (singer_id bigint NOT NULL, album_id bigint NOT NULL, title varchar, " +
			"PRIMARY KEY (singer_id, album_id)) INTERLEAVE IN PARENT singers ON DELETE CASCADE",
		"CREATE TABLE public.items (id bigint PRIMARY KEY, v text)",
		"CREATE TABLE sales.orders (id bigint PRIMARY KEY)",
		"CREATE INDEX albums_by_title ON albums (title DESC) INCLUDE (album_id) WHERE title IS NOT NULL",
		"CREATE UNIQUE INDEX IF NOT EXISTS u ON public.singers (name)",
		"CREATE INDEX i ON public.items (v)",
		"CREATE SEQUENCE s BIT_REVERSED_POSITIVE",
		"CREATE VIEW v SQL SECURITY INVOKER AS SELECT * FROM singers",
		"CREATE VIEW v2 SQL SECURITY INVOKER AS SELECT 'C:\\' AS p, \"id\" FROM singers",
		"CREATE CHANGE STREAM cs FOR ALL",
		"CREATE TABLE \"Order\" (\"Id\" bigint PRIMARY KEY, \"a\"\"b\" text)",
		"ALTER TABLE singers ADD COLUMN nick varchar(10)",
		"ALTER TABLE singers ALTER COLUMN nick TYPE varchar(20)",
		"ALTER TABLE singers ALTER COLUMN nick SET NOT NULL",
		"ALTER TABLE singers ALTER COLUMN nick DROP NOT NULL",
		"ALTER TABLE albums ADD CONSTRAINT fk FOREIGN KEY (singer_id) REFERENCES singers (id)",
		"ALTER TABLE singers ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL '1 day'))",
		"ALTER TABLE singers DROP COLUMN nick",
		"DROP INDEX albums_by_title",
		"DROP VIEW v",
		"DROP CHANGE STREAM cs",
		"DROP SEQUENCE s",
		"DROP INDEX i",
		"DROP TABLE public.items",
		"DROP TABLE sales.orders",
		"DROP SCHEMA sales",
	}
}

func TestDDL_RealWorldStatementsAreAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stmts    []string
		postgres bool
	}{
		{"googlesql", realWorldGoogleSQL(), false},
		{"postgresql", realWorldPostgres(), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var cur []string

			for _, st := range tt.stmts {
				next, err := gcpspanner.FoldDDL(cur, []string{st}, tt.postgres)
				require.NoError(t, err, st)

				cur = next
			}
		})
	}
}

func TestDDL_UnmodelledAlterIsKeptVerbatim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		alter    string
		postgres bool
	}{
		{"identity", "ALTER TABLE t ALTER COLUMN id ALTER IDENTITY RESTART COUNTER WITH 5", false},
		{"pg row deletion policy", "ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL '1 day'))", true},
		{"pg rename with synonym", "ALTER TABLE t RENAME WITH SYNONYM TO t2", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			create := "CREATE TABLE t (id INT64, a STRING(10), ts TIMESTAMP) PRIMARY KEY (id)"
			if tt.postgres {
				create = "CREATE TABLE t (id bigint PRIMARY KEY, a varchar(10), ts timestamptz)"
			}

			got, err := gcpspanner.FoldDDL([]string{create}, []string{tt.alter}, tt.postgres)
			require.NoError(t, err)
			require.NotEmpty(t, got)
			assert.Equal(t, create, got[0], "an unmodelled action must not rewrite the table")
			assert.Contains(t, got, tt.alter)
		})
	}
}

func TestDDL_DropRemovesVerbatimAlters(t *testing.T) {
	t.Parallel()

	cur, err := gcpspanner.FoldDDL(nil, []string{
		"CREATE TABLE t (id INT64) PRIMARY KEY (id)",
		"ALTER TABLE t ADD SYNONYM s",
		"DROP TABLE t",
	}, false)
	require.NoError(t, err)
	assert.Empty(t, cur, strings.Join(cur, "; "))
}
