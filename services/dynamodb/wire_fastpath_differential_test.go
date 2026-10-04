package dynamodb_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// levelHandler discards records but reports Enabled for levels at or above level.
type levelHandler struct{ level slog.Level }

func (h *levelHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }
func (*levelHandler) Handle(context.Context, slog.Record) error      { return nil }
func (h *levelHandler) WithAttrs([]slog.Attr) slog.Handler           { return h }
func (h *levelHandler) WithGroup(string) slog.Handler                { return h }

type wireResult struct {
	body string
	crc  string
	code int
}

// doWire posts one request; legacy forces the reflection decoder by enabling debug logging.
func doWire(h *dynamodb.DynamoDBHandler, legacy bool, action, body string) wireResult {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)

	lg := slog.New(&levelHandler{level: slog.LevelInfo})
	if legacy {
		lg = slog.New(&levelHandler{level: slog.LevelDebug})
	}

	req = req.WithContext(logger.Save(context.Background(), lg))

	w := httptest.NewRecorder()
	c := echo.New().NewContext(req, w)
	_ = h.Handler()(c)

	return wireResult{code: w.Code, body: w.Body.String(), crc: w.Header().Get("X-Amz-Crc32")}
}

func newDiffHandler(t *testing.T) *dynamodb.DynamoDBHandler {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")
	h := dynamodb.NewHandler(db)

	setup := []string{
		`{"TableName":"Tbl","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],` +
			`"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"},` +
			`{"AttributeName":"g","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST",` +
			`"GlobalSecondaryIndexes":[{"IndexName":"gsi","KeySchema":[{"AttributeName":"g","KeyType":"HASH"}],` +
			`"Projection":{"ProjectionType":"ALL"}}],"StreamSpecification":{"StreamEnabled":true,` +
			`"StreamViewType":"NEW_AND_OLD_IMAGES"}}`,
		`{"TableName":"Hsh","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],` +
			`"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"N"}],"BillingMode":"PAY_PER_REQUEST"}`,
	}

	for _, body := range setup {
		res := doWire(h, false, "CreateTable", body)
		require.Equal(t, http.StatusOK, res.code, res.body)
	}

	return h
}

type wireStep struct{ action, body string }

// diffCorpus loads tab-separated "Action<TAB>body" requests replayed in order against both paths.
func diffCorpus(t *testing.T) []wireStep {
	t.Helper()

	raw, err := os.ReadFile("testdata/fastpath_corpus.tsv")
	require.NoError(t, err)

	var steps []wireStep

	for line := range strings.Lines(string(raw)) {
		action, body, found := strings.Cut(strings.TrimSuffix(line, "\n"), "\t")
		require.True(t, found, line)

		steps = append(steps, wireStep{action: action, body: body})
	}

	return steps
}

func TestFastPath_DifferentialAgainstLegacy(t *testing.T) {
	t.Parallel()

	fast := newDiffHandler(t)
	legacy := newDiffHandler(t)

	for i, step := range diffCorpus(t) {
		got := doWire(fast, false, step.action, step.body)
		want := doWire(legacy, true, step.action, step.body)

		assert.Equal(t, want, got, "step %d %s %s", i, step.action, step.body)
	}
}

func TestFastPath_LargeItemRoundTrip(t *testing.T) {
	t.Parallel()

	blob := strings.Repeat("xé<", 60_000)
	put := `{"TableName":"Hsh","Item":{"id":{"N":"1"},"blob":{"S":"` + blob + `"}}}`

	fast := newDiffHandler(t)
	legacy := newDiffHandler(t)

	for _, step := range []wireStep{
		{"PutItem", put},
		{"GetItem", `{"TableName":"Hsh","Key":{"id":{"N":"1"}}}`},
		{"Scan", `{"TableName":"Hsh"}`},
	} {
		assert.Equal(t, doWire(legacy, true, step.action, step.body), doWire(fast, false, step.action, step.body))
	}
}

func TestFastPath_ConcurrentMixedOps(t *testing.T) {
	t.Parallel()

	h := newDiffHandler(t)

	const workers = 8

	var failures atomic.Int64

	var wg sync.WaitGroup

	for w := range workers {
		wg.Go(func() {
			id := strconv.Itoa(w)

			for i := range 150 {
				n := strconv.Itoa(i % 10)
				key := `{"pk":{"S":"w` + id + `"},"sk":{"S":"` + n + `"}}`
				bwKey := `{"pk":{"S":"bw` + id + `"},"sk":{"S":"` + n + `"}}`
				steps := []wireStep{
					{"PutItem", `{"TableName":"Tbl","Item":{"pk":{"S":"w` + id + `"},"sk":{"S":"` + n +
						`"},"g":{"S":"g"},"v":{"N":"` + n + `"}}}`},
					{"GetItem", `{"TableName":"Tbl","Key":` + key + `}`},
					{"UpdateItem", `{"TableName":"Tbl","Key":` + key +
						`,"UpdateExpression":"ADD v :one","ExpressionAttributeValues":{":one":{"N":"1"}}}`},
					{"Query", `{"TableName":"Tbl","KeyConditionExpression":"pk = :p",` +
						`"ExpressionAttributeValues":{":p":{"S":"w` + id + `"}}}`},
					{"Scan", `{"TableName":"Tbl","Limit":20}`},
					{"BatchWriteItem", `{"RequestItems":{"Tbl":[{"PutRequest":{"Item":` + bwKey + `}}]}}`},
					{"BatchGetItem", `{"RequestItems":{"Tbl":{"Keys":[` + key + `]}}}`},
					{"DeleteItem", `{"TableName":"Tbl","Key":` + bwKey + `}`},
				}

				for _, s := range steps {
					if res := doWire(h, false, s.action, s.body); res.code != http.StatusOK {
						failures.Add(1)
					}
				}
			}
		})
	}

	wg.Wait()
	assert.Zero(t, failures.Load())

	res := doWire(h, false, "Scan", `{"TableName":"Tbl","Select":"COUNT"}`)
	assert.Equal(t, http.StatusOK, res.code)
	assert.Contains(t, res.body, `"Count"`)
}
