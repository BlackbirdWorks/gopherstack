package stepfunctions_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// seedListExecutionsBenchBackend seeds one state machine with n completed
// executions -- representative of a long-lived state machine that has
// accumulated history well past the default 100-result page ListExecutions
// returns on every call.
func seedListExecutionsBenchBackend(b *testing.B, n int) (*stepfunctions.Handler, *echo.Echo, string) {
	b.Helper()

	bk := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(bk)
	ctx := context.Background()

	sm, err := bk.CreateStateMachine(
		ctx, "bench-list-executions",
		`{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`,
		"arn:role", "STANDARD",
	)
	require.NoError(b, err)

	for i := range n {
		_, startErr := bk.StartExecution(sm.StateMachineArn, "exec-"+strconv.Itoa(i), `{}`)
		require.NoError(b, startErr)
	}

	require.Eventually(b, func() bool {
		execs, _, listErr := bk.ListExecutions(sm.StateMachineArn, "", "", n+1)
		if listErr != nil || len(execs) != n {
			return false
		}

		for _, exec := range execs {
			if exec.Status == "RUNNING" {
				return false
			}
		}

		return true
	}, 30*time.Second, 20*time.Millisecond)

	return h, echo.New(), sm.StateMachineArn
}

func benchmarkListExecutions(b *testing.B, n int) {
	b.Helper()

	h, e, arn := seedListExecutionsBenchBackend(b, n)
	body := `{"stateMachineArn":"` + arn + `"}`

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("X-Amz-Target", "AmazonStates.ListExecutions")

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		if err := h.Handler()(c); err != nil {
			b.Fatal(err)
		}

		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d", rec.Code)
		}
	}
}

func BenchmarkListExecutions_1000(b *testing.B) { benchmarkListExecutions(b, 1000) }
func BenchmarkListExecutions_5000(b *testing.B) { benchmarkListExecutions(b, 5000) }
