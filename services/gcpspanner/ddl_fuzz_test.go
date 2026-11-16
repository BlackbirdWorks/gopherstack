package gcpspanner_test

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const fuzzSeedTable = "CREATE TABLE t (id INT64, a STRING(10)) PRIMARY KEY (id), INTERLEAVE IN PARENT p ON DELETE"

func fuzzCorpus() []string {
	out := append(realWorldGoogleSQL(), realWorldPostgres()...)

	return append(out,
		"ALTER TABLE t SET ON DELETE CASCADE",
		"ALTER TABLE t SET ON DELETE",
		"ALTER TABLE t DROP COLUMN",
		"ALTER TABLE t DROP COLUMN IF EXISTS",
		"ALTER TABLE t ALTER COLUMN",
		"ALTER TABLE t ADD",
		"ALTER TABLE t ADD COLUMN",
		"ALTER TABLE IF EXISTS t ADD CONSTRAINT",
		"CREATE TABLE t (id INT64) PRIMARY KEY (id ]",
		"CREATE TABLE t (a ARRAY[1) PRIMARY KEY (a)",
		"RENAME TABLE a TO",
		"RENAME TABLE",
		"DROP",
		"DROP TABLE",
		"CREATE",
		"CREATE TABLE",
		"CREATE INDEX i ON",
		"CREATE INDEX i ON t (",
		"CREATE PROTO BUNDLE",
	)
}

func TestDDL_FuzzNeverPanicsOrStalls(t *testing.T) {
	t.Parallel()

	corpus := fuzzCorpus()
	rng := rand.New(rand.NewPCG(7, 11))
	punct := []string{"(", ")", "[", "]", "'", "\"", "`", "\\", "--", "/*", "*/", "#", ",", ".", "\x00", "\xff", "'''"}

	var cases []string

	for _, st := range corpus {
		for i := 0; i <= len(st); i += max(len(st)/16, 1) {
			cases = append(cases, st[:i], st[i:])
		}

		for range 6 {
			b := []byte(st)
			at := rng.IntN(len(b) + 1)
			b = append(b[:at], append([]byte(punct[rng.IntN(len(punct))]), b[at:]...)...)
			cases = append(cases, string(b))

			if len(st) > 1 {
				del := rng.IntN(len(st))
				cases = append(cases, st[:del]+st[del+1:])
			}
		}
	}

	cases = append(cases,
		strings.Repeat("(", 100_000), strings.Repeat("'", 100_001), strings.Repeat("`a", 50_000),
		"CREATE TABLE t ("+strings.Repeat("a INT64,", 20_000)+"z INT64) PRIMARY KEY (z)",
		"CREATE VIEW v AS SELECT "+strings.Repeat("/*", 50_000),
	)

	start := time.Now()

	for _, postgres := range []bool{false, true} {
		for _, c := range cases {
			assert.NotPanics(t, func() {
				_, _ = gcpspanner.FoldDDL([]string{fuzzSeedTable, "CREATE TABLE p (id INT64) PRIMARY KEY (id)"},
					[]string{c}, postgres)
				_, _ = gcpspanner.FoldDDL(nil, []string{c}, postgres)
			}, "%q", c)
		}
	}

	assert.Less(t, time.Since(start), 30*time.Second)
}

func TestOpFilter_FuzzNeverPanicsOrStalls(t *testing.T) {
	t.Parallel()

	corpus := []string{
		"done=true", "NOT done=true", "metadata.@type:CreateDatabaseMetadata AND done=true",
		"(done=true OR done=false) AND name:\"a b\"", "metadata.progress.start_time>=\"2024-01-01T00:00:00Z\"",
		"- done = true", "error.code != 0", "a:*", "((((a=b))))", "a=b OR", "AND", "()", "\"", "a <= ", "x.y.z:1",
	}
	rng := rand.New(rand.NewPCG(3, 5))
	punct := []string{"(", ")", "\"", " AND ", " OR ", "NOT ", "-", "=", ":", "<=", "\x00", "\xff", ".."}
	start := time.Now()

	for _, st := range corpus {
		for i := 0; i <= len(st); i++ {
			assert.NotPanics(t, func() { _ = gcpspanner.ParseOpFilter(st[:i]) }, "%q", st[:i])
		}

		for range 40 {
			b := []byte(st)
			at := rng.IntN(len(b) + 1)
			b = append(b[:at], append([]byte(punct[rng.IntN(len(punct))]), b[at:]...)...)
			assert.NotPanics(t, func() { _ = gcpspanner.ParseOpFilter(string(b)) }, "%q", b)
		}
	}

	for _, big := range []string{
		strings.Repeat("(", 10_000), strings.Repeat("NOT ", 5_000) + "a=b", strings.Repeat("a=b AND ", 2_000),
		strings.Repeat("\"", 5_000),
	} {
		assert.NotPanics(t, func() { _ = gcpspanner.ParseOpFilter(big) })
	}

	assert.Less(t, time.Since(start), 10*time.Second)
}
