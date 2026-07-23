package redshiftdata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// handleExecuteStatement handles ExecuteStatement.
//
// ClientToken and SessionKeepAliveSeconds are accepted on the wire for
// request-shape parity (the real ExecuteStatementInput carries both) but are
// not used to change backend behavior: ClientToken idempotency and session
// keep-alive/expiry both require modeling request retries and time-bounded
// session lifetimes this in-memory mock does not have, and there is no clean
// way to verify the exact undocumented AWS semantics without a live cluster
// (same reasoning as rdsdata's typeHint gap).
func (h *Handler) handleExecuteStatement(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		StatementName           string         `json:"StatementName"`
		ClusterIdentifier       string         `json:"ClusterIdentifier"`
		WorkgroupName           string         `json:"WorkgroupName"`
		Database                string         `json:"Database"`
		DBUser                  string         `json:"DbUser"`
		SecretArn               string         `json:"SecretArn"`
		SQL                     string         `json:"Sql"`
		ResultFormat            string         `json:"ResultFormat"`
		SessionID               string         `json:"SessionId"`
		ClientToken             string         `json:"ClientToken"`
		Parameters              []SQLParameter `json:"Parameters"`
		SessionKeepAliveSeconds int32          `json:"SessionKeepAliveSeconds"`
		WithEvent               bool           `json:"WithEvent"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stmt, err := h.Backend.ExecuteStatement(
		ctx,
		req.SQL, req.ClusterIdentifier, req.WorkgroupName,
		req.Database, req.DBUser, req.SecretArn, req.StatementName,
		req.WithEvent, req.ResultFormat, req.Parameters,
		req.SessionID,
	)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"Id":                stmt.ID,
		"ClusterIdentifier": stmt.ClusterIdentifier,
		"WorkgroupName":     stmt.WorkgroupName,
		"Database":          stmt.Database,
		"DbUser":            stmt.DBUser,
		"SecretArn":         stmt.SecretARN,
		keyCreatedAt:        epochSeconds(stmt.CreatedAt),
	}

	if stmt.SessionID != "" {
		resp["SessionId"] = stmt.SessionID
	}

	return json.Marshal(resp)
}

// handleBatchExecuteStatement handles BatchExecuteStatement. See
// handleExecuteStatement for why ClientToken/SessionKeepAliveSeconds are
// accepted on the wire but not behaviorally significant.
func (h *Handler) handleBatchExecuteStatement(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResultFormat            string         `json:"ResultFormat"`
		WorkgroupName           string         `json:"WorkgroupName"`
		Database                string         `json:"Database"`
		DBUser                  string         `json:"DbUser"`
		SecretArn               string         `json:"SecretArn"`
		StatementName           string         `json:"StatementName"`
		ClusterIdentifier       string         `json:"ClusterIdentifier"`
		SessionID               string         `json:"SessionId"`
		ClientToken             string         `json:"ClientToken"`
		Sqls                    []string       `json:"Sqls"`
		Parameters              []SQLParameter `json:"Parameters"`
		SessionKeepAliveSeconds int32          `json:"SessionKeepAliveSeconds"`
		WithEvent               bool           `json:"WithEvent"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stmt, err := h.Backend.BatchExecuteStatement(
		ctx,
		req.Sqls, req.ClusterIdentifier, req.WorkgroupName,
		req.Database, req.DBUser, req.SecretArn, req.StatementName,
		req.WithEvent, req.ResultFormat, req.Parameters,
		req.SessionID,
	)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"Id":                stmt.ID,
		"ClusterIdentifier": stmt.ClusterIdentifier,
		"WorkgroupName":     stmt.WorkgroupName,
		"Database":          stmt.Database,
		"DbUser":            stmt.DBUser,
		"SecretArn":         stmt.SecretARN,
		keyCreatedAt:        epochSeconds(stmt.CreatedAt),
	}

	if stmt.SessionID != "" {
		resp["SessionId"] = stmt.SessionID
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDescribeStatement(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID string `json:"Id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	stmt, err := h.Backend.DescribeStatement(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(statementToDescribeResponse(stmt))
}

func (h *Handler) handleGetStatementResult(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID string `json:"Id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	stmt, err := h.Backend.DescribeStatement(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if !stmt.HasResultSet {
		return nil, fmt.Errorf(
			"%w: statement %s does not have a result set",
			ErrNoResultSet,
			req.ID,
		)
	}

	if stmt.ResultFormat != resultFormatJSON && stmt.ResultFormat != "" {
		return nil, fmt.Errorf("%w: statement %s result format is not JSON", ErrValidation, req.ID)
	}

	// Return a single demo row so the UI can render a non-empty result table.
	// NextToken is empty because the demo result set fits on one page.
	return json.Marshal(map[string]any{
		"Records": [][]map[string]any{
			{{"stringValue": "mock_value"}},
		},
		"ColumnMetadata": []map[string]any{
			{
				keyName:     valColumn1,
				"label":     valColumn1,
				keyTypeName: typeVarchar,
				keyLength:   mockColumnSize,
				keyNullable: mockColumnNullable,
			},
		},
		"TotalNumRows": int64(1),
		keyNextToken:   "",
	})
}

func (h *Handler) handleGetStatementResultV2(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID        string `json:"Id"`
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	stmt, err := h.Backend.DescribeStatement(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if !stmt.HasResultSet {
		return nil, fmt.Errorf(
			"%w: statement %s does not have a result set",
			ErrNoResultSet,
			req.ID,
		)
	}

	if stmt.ResultFormat != resultFormatCSV {
		return nil, fmt.Errorf("%w: statement %s result format is not CSV", ErrValidation, req.ID)
	}

	// Return a single demo CSV record matching the V2 format.
	// NextToken is empty because the demo result set fits on one page.
	//
	// Records is []types.QueryRecords, a union whose only member is
	// CSVRecords (a comma-joined string per row) — each element must be an
	// object of the form {"CSVRecords": "..."}, not a bare string, or the
	// SDK's union deserializer treats it as an unknown member and drops it.
	return json.Marshal(map[string]any{
		"Records": []map[string]any{
			{keyCSVRecords: "mock_value"},
		},
		"ColumnMetadata": []map[string]any{
			{
				keyName:     valColumn1,
				"label":     valColumn1,
				keyTypeName: typeVarchar,
				keyLength:   mockColumnSize,
				keyNullable: mockColumnNullable,
			},
		},
		"TotalNumRows":  int64(1),
		keyResultFormat: resultFormatCSV,
		keyNextToken:    "",
	})
}

func (h *Handler) handleListStatements(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterIdentifier string `json:"ClusterIdentifier"`
		WorkgroupName     string `json:"WorkgroupName"`
		Database          string `json:"Database"`
		StatementName     string `json:"StatementName"`
		Status            string `json:"Status"`
		RoleLevel         *bool  `json:"RoleLevel"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MaxResults > maxListStatementsResults {
		return nil, fmt.Errorf(
			"%w: MaxResults must be ≤ %d",
			ErrValidation,
			maxListStatementsResults,
		)
	}

	if err := ValidateListStatementsStatus(req.Status); err != nil {
		return nil, err
	}

	stmts, nextToken, err := h.Backend.ListStatements(ctx, ListStatementsFilter{
		ClusterIdentifier: req.ClusterIdentifier,
		WorkgroupName:     req.WorkgroupName,
		Database:          req.Database,
		StatementName:     req.StatementName,
		Status:            req.Status,
		NextToken:         req.NextToken,
		MaxResults:        req.MaxResults,
	})
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(stmts))

	for _, stmt := range stmts {
		items = append(items, statementToListItem(stmt))
	}

	resp := map[string]any{
		"Statements": items,
	}

	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleCancelStatement(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID string `json:"Id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	if err := h.Backend.CancelStatement(ctx, req.ID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyStatusField: true,
	})
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS JSON 1.1 protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// durationNanos converts a duration tracked internally in milliseconds
// (Statement.DurationMs / SubStatementData.DurationMs) to the nanoseconds
// unit the wire protocol's "Duration" field requires (see
// DescribeStatementOutput.Duration and SubStatementData.Duration in
// aws-sdk-go-v2/service/redshiftdata: "The amount of time in nanoseconds
// that the statement ran"). Sending raw milliseconds under that field
// understates real durations by a factor of 1e6.
func durationNanos(ms int64) int64 {
	return ms * int64(time.Millisecond/time.Nanosecond)
}

// statementToListItem converts a statement to the summary map used in ListStatements.
func statementToListItem(stmt *Statement) map[string]any {
	item := map[string]any{
		"Id":               stmt.ID,
		keyStatusField:     stmt.Status,
		keyQueryString:     stmt.QueryString,
		"IsBatchStatement": stmt.IsBatchStatement,
		keyHasResultSet:    stmt.HasResultSet,
		keyCreatedAt:       epochSeconds(stmt.CreatedAt),
		keyUpdatedAt:       epochSeconds(stmt.UpdatedAt),
		keyDuration:        durationNanos(stmt.DurationMs),
		keyResultFormat:    statementResultFormat(stmt),
	}

	if stmt.StatementName != "" {
		item["StatementName"] = stmt.StatementName
	}

	if stmt.SecretARN != "" {
		item["SecretArn"] = stmt.SecretARN
	}

	if stmt.Database != "" {
		item["Database"] = stmt.Database
	}

	if stmt.ClusterIdentifier != "" {
		item["ClusterIdentifier"] = stmt.ClusterIdentifier
	}

	if stmt.WorkgroupName != "" {
		item["WorkgroupName"] = stmt.WorkgroupName
	}

	if stmt.DBUser != "" {
		item["DbUser"] = stmt.DBUser
	}

	if stmt.SessionID != "" {
		item["SessionId"] = stmt.SessionID
	}

	if len(stmt.QueryStrings) > 0 {
		item["QueryStrings"] = stmt.QueryStrings
	}

	if len(stmt.Parameters) > 0 {
		item["QueryParameters"] = stmt.Parameters
	}

	return item
}

// statementToDescribeResponse converts a statement to a DescribeStatement response map.
func statementToDescribeResponse(stmt *Statement) map[string]any {
	resp := map[string]any{
		"Id":               stmt.ID,
		keyStatusField:     stmt.Status,
		keyQueryString:     stmt.QueryString,
		keyHasResultSet:    stmt.HasResultSet,
		"IsBatchStatement": stmt.IsBatchStatement,
		keyCreatedAt:       epochSeconds(stmt.CreatedAt),
		keyUpdatedAt:       epochSeconds(stmt.UpdatedAt),
		keyDuration:        durationNanos(stmt.DurationMs),
		"ResultRows":       stmt.ResultRows,
		"ResultSize":       stmt.ResultSize,
		"WithEvent":        stmt.WithEvent,
		keyResultFormat:    statementResultFormat(stmt),
		// RedshiftQueryId is a synthetic numeric query identifier. AWS
		// includes this in DescribeStatement for provisioned clusters;
		// we return 0 since we have no real cluster backing.
		"RedshiftQueryId": int64(0),
	}

	if stmt.ClusterIdentifier != "" {
		resp["ClusterIdentifier"] = stmt.ClusterIdentifier
	}

	if stmt.WorkgroupName != "" {
		resp["WorkgroupName"] = stmt.WorkgroupName
	}

	if stmt.Database != "" {
		resp["Database"] = stmt.Database
	}

	if stmt.DBUser != "" {
		resp["DbUser"] = stmt.DBUser
	}

	if stmt.SecretARN != "" {
		resp["SecretArn"] = stmt.SecretARN
	}

	if stmt.SessionID != "" {
		resp["SessionId"] = stmt.SessionID
	}

	if stmt.StatementName != "" {
		resp["StatementName"] = stmt.StatementName
	}

	if stmt.Error != "" {
		resp["Error"] = stmt.Error
	}

	if len(stmt.QueryStrings) > 0 {
		resp["QueryStrings"] = stmt.QueryStrings
	}

	if len(stmt.Parameters) > 0 {
		resp["QueryParameters"] = stmt.Parameters
	}

	if len(stmt.SubStatements) > 0 {
		subs := make([]map[string]any, len(stmt.SubStatements))
		for i, sub := range stmt.SubStatements {
			subs[i] = map[string]any{
				"Id":            sub.ID,
				keyCreatedAt:    epochSeconds(sub.CreatedAt),
				keyUpdatedAt:    epochSeconds(sub.UpdatedAt),
				keyQueryString:  sub.QueryString,
				keyStatusField:  sub.Status,
				keyHasResultSet: sub.HasResultSet,
				keyDuration:     durationNanos(sub.DurationMs),
				"ResultRows":    sub.ResultRows,
				"ResultSize":    sub.ResultSize,
				// RedshiftQueryId mirrors the top-level statement's synthetic
				// identifier (see statementToDescribeResponse): AWS includes
				// this per sub-statement for provisioned clusters, we return
				// 0 since there is no real cluster backing.
				"RedshiftQueryId": int64(0),
			}

			if sub.Error != "" {
				subs[i]["Error"] = sub.Error
			}
		}

		resp["SubStatements"] = subs
	}

	return resp
}
