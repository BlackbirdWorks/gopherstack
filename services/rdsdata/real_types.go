package rdsdata

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// Real engine kinds, matching the RDS engine families.
const (
	kindPostgres = "postgres"
	kindMySQL    = "mysql"
)

// RealTarget is a running database engine behind an Aurora cluster with the HTTP endpoint enabled.
type RealTarget struct {
	Kind   string
	Addr   string
	DBName string
}

// ClusterResolver finds the real engine behind a cluster ARN; ErrNotRealCluster keeps the SQLite path.
type ClusterResolver interface {
	DataAPITarget(resourceARN string) (RealTarget, error)
}

// SecretReader reads a Secrets Manager secret string by name or ARN.
type SecretReader interface {
	SecretString(ctx context.Context, secretID string) (string, error)
}

// ErrNotRealCluster means the cluster has no real engine and the SQLite engine serves it.
var ErrNotRealCluster = errors.New("cluster has no real database engine")

// apiError is a modeled Data API exception with its documented HTTP status.
type apiError struct {
	code   string
	msg    string
	status int
}

func (e *apiError) Error() string { return e.msg }

func newAPIError(code string, status int, msg string) *apiError {
	return &apiError{code: code, status: status, msg: msg}
}

// Modeled exceptions (rdsdata@v1.35.4 types/errors.go; statuses per the API reference).
var (
	// ErrHTTPEndpointNotEnabled: the cluster's HTTP endpoint is disabled.
	ErrHTTPEndpointNotEnabled = newAPIError("HttpEndpointNotEnabledException", http.StatusBadRequest,
		"HttpEndpoint is not enabled for this cluster")
	// ErrClusterNotReady: the cluster's engine is not available yet.
	ErrClusterNotReady = newAPIError("InvalidResourceStateException", http.StatusBadRequest,
		"the DB cluster is not in a state that accepts Data API calls")
	errDatabaseUnavailable = newAPIError("DatabaseUnavailableException", http.StatusGatewayTimeout,
		"the writer instance in the DB cluster is not available")
	errStatementTimeout = newAPIError("StatementTimeoutException", http.StatusBadRequest,
		"the execution of the SQL statement timed out")
	errTooManyTransactions = fmt.Errorf("%w: too many open transactions", ErrValidation)
)

type requestTargetKey struct{}

// requestTarget carries the request's secretArn and database to the backend.
type requestTarget struct {
	SecretARN string
	Database  string
}

func withRequestTarget(ctx context.Context, secretARN, database string) context.Context {
	return context.WithValue(ctx, requestTargetKey{}, requestTarget{SecretARN: secretARN, Database: database})
}

func getRequestTarget(ctx context.Context) requestTarget {
	t, _ := ctx.Value(requestTargetKey{}).(requestTarget)

	return t
}

func errSecretsError(msg string) error {
	return newAPIError("SecretsErrorException", http.StatusBadRequest, msg)
}

func errInvalidSecret(msg string) error {
	return newAPIError("InvalidSecretException", http.StatusBadRequest, msg)
}

func errDatabaseError(msg string) error {
	return newAPIError("DatabaseErrorException", http.StatusBadRequest, msg)
}

func errUnsupportedResult(msg string) error {
	return newAPIError("UnsupportedResultException", http.StatusBadRequest, msg)
}
