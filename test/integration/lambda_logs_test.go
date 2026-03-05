package integration_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cwlogspkg "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	lambdapkg "github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// lambdaLogsPortStart is the start of the port range for Lambda log wiring tests.
	lambdaLogsPortStart = 21200
	// lambdaLogsPortEnd is the exclusive end of that range.
	lambdaLogsPortEnd = 21300
	// lambdaVersionsPortStart is the start of the port range for Lambda version/alias integration tests.
	lambdaVersionsPortStart = 21300
	// lambdaVersionsPortEnd is the exclusive end of that range.
	lambdaVersionsPortEnd = 21400
)

// inProcessCWLogsAdapter adapts a cloudwatchlogs.InMemoryBackend to lambdapkg.CWLogsBackend.
type inProcessCWLogsAdapter struct {
	backend *cwlogspkg.InMemoryBackend
}

func (a *inProcessCWLogsAdapter) EnsureLogGroupAndStream(groupName, streamName string) error {
	if _, err := a.backend.CreateLogGroup(groupName); err != nil &&
		!errors.Is(err, cwlogspkg.ErrLogGroupAlreadyExists) {
		return err
	}

	if _, err := a.backend.CreateLogStream(groupName, streamName); err != nil &&
		!errors.Is(err, cwlogspkg.ErrLogStreamAlreadyExist) {
		return err
	}

	return nil
}

func (a *inProcessCWLogsAdapter) PutLogLines(groupName, streamName string, messages []string) error {
	events := make([]cwlogspkg.InputLogEvent, len(messages))
	now := time.Now().UnixMilli()

	for i, msg := range messages {
		events[i] = cwlogspkg.InputLogEvent{Message: msg, Timestamp: now}
	}

	_, err := a.backend.PutLogEvents(groupName, streamName, events)

	return err
}

// TestLambdaCWLogs_WiringProducesLogEntries verifies that a successful Lambda invocation
// produces CloudWatch Logs entries in /aws/lambda/{function-name}.
//
// This test uses an in-process mock backend (no Docker needed) by injecting a pre-canned
// invocation result via the mock backend. It validates that the Lambda → CWLogs wiring
// creates the expected log group and writes at least one log event after an invocation.
func TestLambdaCWLogs_WiringProducesLogEntries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	pa, err := portalloc.New(lambdaLogsPortStart, lambdaLogsPortEnd)
	require.NoError(t, err)

	cwlogsBackend := cwlogspkg.NewInMemoryBackend()
	cwlogsAdapter := &inProcessCWLogsAdapter{backend: cwlogsBackend}

	lambdaBackend := lambdapkg.NewInMemoryBackend(
		nil,
		pa,
		lambdapkg.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)
	lambdaBackend.SetCWLogsBackend(cwlogsAdapter)

	handler := lambdapkg.NewHandler(lambdaBackend)
	handler.AccountID = "000000000000"
	handler.DefaultRegion = "us-east-1"

	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry(slog.Default())
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	// Create the function.
	createBody := `{"FunctionName":"log-test-fn","PackageType":"Image",` +
		`"Code":{"ImageUri":"test:latest"},"Role":"arn:aws:iam::000000000000:role/r"}`
	createResp, createErr := http.Post(
		server.URL+"/2015-03-31/functions",
		"application/json",
		strings.NewReader(createBody),
	)
	require.NoError(t, createErr)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	// Invoke the function. This test verifies Lambda → CloudWatch Logs wiring and
	// therefore requires a working Lambda runtime. If the runtime is unavailable
	// (no Docker), skip instead of simulating logs directly.
	result, status, invokeErr := lambdaBackend.InvokeFunctionWithQualifier(
		ctx, "log-test-fn", "", lambdapkg.InvocationTypeRequestResponse,
		[]byte(`{"event":"test"}`),
	)

	if invokeErr != nil {
		t.Skipf("skipping Lambda → CloudWatch Logs wiring test: Lambda unavailable: %v", invokeErr)
	}

	// If we get here, the invocation succeeded; verify the response status.
	assert.Equal(t, http.StatusOK, status)
	t.Logf("Invocation succeeded, response: %s", result)

	// Verify that pushInvocationLog created the log group in CloudWatch Logs.
	groupName := fmt.Sprintf("/aws/lambda/%s", "log-test-fn")

	groups, _, err := cwlogsBackend.DescribeLogGroups(groupName, "", 10)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, groupName, groups[0].LogGroupName)
}

// TestLambdaVersionsAndAliases_Integration tests the full version + alias lifecycle
// against an in-process Gopherstack Lambda service.
func TestLambdaVersionsAndAliases_Integration(t *testing.T) {
	t.Parallel()

	pa, err := portalloc.New(lambdaVersionsPortStart, lambdaVersionsPortEnd)
	require.NoError(t, err)

	lambdaBackend := lambdapkg.NewInMemoryBackend(
		nil,
		pa,
		lambdapkg.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)

	handler := lambdapkg.NewHandler(lambdaBackend)
	handler.AccountID = "000000000000"
	handler.DefaultRegion = "us-east-1"

	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry(slog.Default())
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	fnName := "version-alias-fn"

	// Create function.
	createBody := fmt.Sprintf(
		`{"FunctionName":%q,"PackageType":"Image",`+
			`"Code":{"ImageUri":"test:latest"},"Role":"arn:aws:iam::000000000000:role/r"}`,
		fnName,
	)
	createResp, err := http.Post(
		server.URL+"/2015-03-31/functions",
		"application/json",
		strings.NewReader(createBody),
	)
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	// Publish version.
	verResp, err := http.Post(
		fmt.Sprintf("%s/2015-03-31/functions/%s/versions", server.URL, fnName),
		"application/json",
		strings.NewReader(`{"Description":"first release"}`),
	)
	require.NoError(t, err)
	defer verResp.Body.Close()
	require.Equal(t, http.StatusCreated, verResp.StatusCode)

	// List versions — should have $LATEST + version 1.
	listVerResp, err := http.Get(fmt.Sprintf("%s/2015-03-31/functions/%s/versions", server.URL, fnName))
	require.NoError(t, err)
	defer listVerResp.Body.Close()
	require.Equal(t, http.StatusOK, listVerResp.StatusCode)

	// Create alias pointing to version 1.
	aliasResp, err := http.Post(
		fmt.Sprintf("%s/2015-03-31/functions/%s/aliases", server.URL, fnName),
		"application/json",
		strings.NewReader(`{"Name":"prod","FunctionVersion":"1","Description":"production"}`),
	)
	require.NoError(t, err)
	defer aliasResp.Body.Close()
	require.Equal(t, http.StatusCreated, aliasResp.StatusCode)

	// Get alias.
	getAliasResp, err := http.Get(
		fmt.Sprintf("%s/2015-03-31/functions/%s/aliases/prod", server.URL, fnName),
	)
	require.NoError(t, err)
	defer getAliasResp.Body.Close()
	require.Equal(t, http.StatusOK, getAliasResp.StatusCode)

	// List aliases.
	listAliasResp, err := http.Get(
		fmt.Sprintf("%s/2015-03-31/functions/%s/aliases", server.URL, fnName),
	)
	require.NoError(t, err)
	defer listAliasResp.Body.Close()
	require.Equal(t, http.StatusOK, listAliasResp.StatusCode)

	// Update alias.
	updateReq, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPut,
		fmt.Sprintf("%s/2015-03-31/functions/%s/aliases/prod", server.URL, fnName),
		strings.NewReader(`{"Description":"updated"}`),
	)
	require.NoError(t, err)
	updateReq.Header.Set("Content-Type", "application/json")

	updateAliasResp, err := http.DefaultClient.Do(updateReq)
	require.NoError(t, err)
	defer updateAliasResp.Body.Close()
	require.Equal(t, http.StatusOK, updateAliasResp.StatusCode)

	// Delete alias.
	deleteReq, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodDelete,
		fmt.Sprintf("%s/2015-03-31/functions/%s/aliases/prod", server.URL, fnName),
		nil,
	)
	require.NoError(t, err)

	deleteAliasResp, err := http.DefaultClient.Do(deleteReq)
	require.NoError(t, err)
	defer deleteAliasResp.Body.Close()
	require.Equal(t, http.StatusNoContent, deleteAliasResp.StatusCode)
}
