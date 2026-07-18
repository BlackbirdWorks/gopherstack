package firehose

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_rddata "github.com/aws/aws-sdk-go-v2/service/redshiftdata"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// redshiftRetryDuration is the default retry window for Redshift delivery.
const redshiftRetryDuration = 7200 * time.Second

// redshiftHostParts is the SplitN limit for extracting cluster ID from JDBC host.
const redshiftHostParts = 2

const (
	redshiftBackoffInitial = 2 * time.Second
	redshiftBackoffMax     = 60 * time.Second
)

// buildRedshiftInsertSQL constructs a batch INSERT SQL statement for Redshift delivery.
// Returns the SQL string and true, or ("", false) when records is empty.
func buildRedshiftInsertSQL(tableName, columns string, records [][]byte) (string, bool) {
	if columns == "" {
		columns = "data"
	}

	sqlParts := make([]string, 0, len(records))
	for _, rec := range records {
		encoded := base64.StdEncoding.EncodeToString(rec)
		escaped := strings.ReplaceAll(encoded, "'", "''")
		sqlParts = append(sqlParts, fmt.Sprintf("('%s')", escaped))
	}

	if len(sqlParts) == 0 {
		return "", false
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columns, strings.Join(sqlParts, ",")), true
}

// deliverToRedshift inserts records into a Redshift table via the Redshift Data API
// (ExecuteStatement). Each record is inserted as a single-column row with the raw
// record bytes stored as a base64-encoded string in the configured DataTableName.
//
// The ClusterJDBCURL is parsed to extract the cluster endpoint and database name.
// Format: jdbc:redshift://<host>:<port>/<database>
// parseRedshiftJDBCURL extracts the cluster identifier and database name from a
// Redshift JDBC connection string of the form
// jdbc:redshift://<cluster>.<suffix>.redshift.amazonaws.com:<port>/<database>.
// Returns an error when the URL cannot be parsed or is missing a cluster or database.
func parseRedshiftJDBCURL(clusterJDBCURL string) (string, string, error) {
	jdbcURL := strings.TrimPrefix(clusterJDBCURL, "jdbc:redshift://")
	parsed, parseErr := url.Parse("https://" + jdbcURL)
	if parseErr != nil {
		return "", "", parseErr
	}

	host := parsed.Hostname()
	database := strings.TrimPrefix(parsed.Path, "/")

	// Extract cluster identifier from the host: <cluster>.<suffix>.redshift.amazonaws.com
	clusterID := strings.SplitN(host, ".", redshiftHostParts)[0]

	if clusterID == "" || database == "" {
		return "", "", fmt.Errorf("%w: JDBC URL missing cluster or database", ErrValidation)
	}

	return clusterID, database, nil
}

// executeRedshiftInsertWithRetry runs insertSQL via the Redshift Data API, retrying
// with exponential back-off until maxRetry elapses or ctx is cancelled.
func (b *InMemoryBackend) executeRedshiftInsertWithRetry(
	ctx context.Context,
	clusterID, database, dbUser, insertSQL, streamARN string,
	maxRetry time.Duration,
) {
	rdClient := sdk_rddata.NewFromConfig(aws.Config{Region: b.region})

	deadline := time.Now().Add(maxRetry)
	backoff := redshiftBackoffInitial

	for {
		_, execErr := rdClient.ExecuteStatement(ctx, &sdk_rddata.ExecuteStatementInput{
			ClusterIdentifier: aws.String(clusterID),
			Database:          aws.String(database),
			DbUser:            aws.String(dbUser),
			Sql:               aws.String(insertSQL),
		})
		if execErr == nil {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: Redshift delivery failed after retries",
				"cluster", clusterID, "database", database, "stream", streamARN, "error", execErr)

			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > redshiftBackoffMax {
				backoff = redshiftBackoffMax
			}
		}
	}
}

func (b *InMemoryBackend) deliverToRedshift(
	ctx context.Context,
	records [][]byte,
	dest *RedshiftDestinationDescription,
	streamARN string,
) {
	if dest.ClusterJDBCURL == "" || dest.CopyCommand == nil || dest.CopyCommand.DataTableName == "" {
		return
	}

	clusterID, database, parseErr := parseRedshiftJDBCURL(dest.ClusterJDBCURL)
	if parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: cannot parse Redshift JDBC URL",
			"url", dest.ClusterJDBCURL, "stream", streamARN, "error", parseErr)

		return
	}

	insertSQL, ok := buildRedshiftInsertSQL(dest.CopyCommand.DataTableName, dest.CopyCommand.DataTableColumns, records)
	if !ok {
		return
	}

	maxRetry := redshiftRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	b.executeRedshiftInsertWithRetry(ctx, clusterID, database, dest.Username, insertSQL, streamARN, maxRetry)
}
