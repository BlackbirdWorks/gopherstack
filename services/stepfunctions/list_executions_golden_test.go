package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// normalizeListExecutionsGolden blanks startDate/stopDate, the only fields
// in a ListExecutions response that carry a real wall-clock value.
func normalizeListExecutionsGolden(b []byte) []byte {
	re := regexp.MustCompile(`"(start|stop)Date":[0-9.]+`)

	return re.ReplaceAll(b, []byte(`"${1}Date":0`))
}

// TestListExecutions_PageGolden seeds a state machine with 12 completed
// executions and reads them back a page (maxResults=5) at a time. The
// concatenated pages pin down both content and order, so a byte comparison
// against the checked-in golden (captured from the pre-optimization
// value-copy-then-sort code) catches any ordering or pagination change from
// switching ListExecutions to sort-pointers-then-copy-only-the-page.
func TestListExecutions_PageGolden(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		bk := stepfunctions.NewInMemoryBackend()
		h := stepfunctions.NewHandler(bk)
		ctx := context.Background()

		sm, err := bk.CreateStateMachine(
			ctx, "golden-list-executions",
			`{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`,
			"arn:role", "STANDARD",
		)
		require.NoError(t, err)

		const total = 12
		for i := range total {
			exec, startErr := bk.StartExecution(sm.StateMachineArn, "exec-"+strconv.Itoa(i), `{}`)
			require.NoError(t, startErr)

			// Pin a distinct, strictly increasing StartDate per execution so
			// ListExecutions' descending sort has no ties -- without this,
			// executions started within the same wall-clock second tie on
			// StartDate and their relative order is unspecified (it falls back
			// to the index's iteration order, which is not stable).
			bk.SetExecutionStartDateForTest(exec.ExecutionArn, float64(1700000000+i))
		}

		synctest.Wait()

		execs, _, listErr := bk.ListExecutions(sm.StateMachineArn, "", "", total+1)
		require.NoError(t, listErr)
		require.Len(t, execs, total)

		for _, exec := range execs {
			require.NotEqual(t, "RUNNING", exec.Status)
		}

		e := echo.New()

		var got strings.Builder

		token := ""
		for page := range 3 {
			body := `{"stateMachineArn":"` + sm.StateMachineArn + `","maxResults":5`
			if token != "" {
				body += `,"nextToken":"` + token + `"`
			}
			body += "}"

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("X-Amz-Target", "AmazonStates.ListExecutions")

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			got.WriteString("--- page ")
			got.WriteString(strconv.Itoa(page))
			got.WriteString(" ---\n")
			got.Write(normalizeListExecutionsGolden(rec.Body.Bytes()))
			got.WriteString("\n")

			var resp struct {
				NextToken string `json:"nextToken"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if resp.NextToken == "" {
				break
			}

			token = resp.NextToken
		}

		want, err := os.ReadFile("testdata/list_executions_golden.txt")
		require.NoError(t, err)

		require.Equal(t, string(want), got.String())
	})
}
