package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// ErrInvalidStatement is returned when a PartiQL statement cannot be parsed.
var ErrInvalidStatement = errors.New("invalid PartiQL statement")

// partiqlValidationExceptionCode is the error code used in BatchExecuteStatement
// error responses for parameter-conversion and statement-parse failures.
const partiqlValidationExceptionCode = "ValidationException"

// errScanFallback is an internal sentinel returned by tryQueryOptimization to
// signal that the caller should fall back to a full Scan instead of Query.
var errScanFallback = errors.New("scan fallback required")

// errNoKeyCondition is returned by partiqlExtractKeyFromWhere when the WHERE
// clause contains no equality condition on a key attribute. SELECT callers
// treat this as a signal to fall back to Scan; UPDATE/DELETE treat it as an error.
var errNoKeyCondition = errors.New("no key condition in WHERE clause")

// fromClauseRegex extracts the table name from a SELECT/DELETE ... FROM "tableName" statement.
// Supports DynamoDB table names: alphanumeric, hyphen, dot, and underscore.
var fromClauseRegex = regexp.MustCompile(`(?i)FROM\s+"([\w.\-]+)"`)

// partiqlInsertTableRe extracts the table name from INSERT INTO "tableName" statements.
var partiqlInsertTableRe = regexp.MustCompile(`(?i)INTO\s+"([\w.\-]+)"`)

// partiqlUpdateTableRe extracts the table name from UPDATE "tableName" statements.
var partiqlUpdateTableRe = regexp.MustCompile(`(?i)^\s*UPDATE\s+"([\w.\-]+)"`)

// Statement type detection regexes.
var (
	partiqlSelectRe = regexp.MustCompile(`(?i)^\s*SELECT\s+`)
	partiqlInsertRe = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+`)
	partiqlUpdateRe = regexp.MustCompile(`(?i)^\s*UPDATE\s+`)
	partiqlDeleteRe = regexp.MustCompile(`(?i)^\s*DELETE\s+FROM\s+`)
)

// Clause extraction regexes.
var (
	// partiqlWhereRe extracts the WHERE clause body (stops before ORDER BY / LIMIT).
	partiqlWhereRe = regexp.MustCompile(
		`(?i)\bWHERE\b\s+(.+?)(?:\s+ORDER\s+BY\b|\s+LIMIT\s+\d|\s*$)`,
	)
	// partiqlLimitRe extracts the LIMIT integer value.
	partiqlLimitRe = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	// partiqlSetRe extracts the SET clause body in an UPDATE statement.
	// Stops before REMOVE, WHERE, or end of string so that a following REMOVE clause is not consumed.
	partiqlSetRe = regexp.MustCompile(`(?i)\bSET\s+(.+?)(?:\s+REMOVE\b|\s+WHERE\b|\s*$)`)
	// partiqlRemoveRe extracts the REMOVE clause body in an UPDATE statement.
	partiqlRemoveRe = regexp.MustCompile(`(?i)\bREMOVE\s+(.+?)(?:\s+WHERE\b|\s*$)`)
	// partiqlSelectColsRe extracts the column list between SELECT and FROM.
	partiqlSelectColsRe = regexp.MustCompile(`(?i)^\s*SELECT\s+(.+?)\s+FROM\s+"`)
	// partiqlValueRe extracts the VALUE tuple body in an INSERT statement.
	partiqlValueRe = regexp.MustCompile(`(?is)\bVALUE\s+(\{.+\})\s*$`)
	// partiqlStringLiteralRe matches single-quoted string literals, including SQL-style '' escapes.
	partiqlStringLiteralRe = regexp.MustCompile(`'((?:''|[^'])*)'`)
	// partiqlANDSplitRe splits on AND (case-insensitive) with surrounding whitespace.
	partiqlANDSplitRe = regexp.MustCompile(`(?i)\s+AND\s+`)
	// partiqlOrderByRe captures the optional ASC/DESC direction from an ORDER BY clause.
	partiqlOrderByRe = regexp.MustCompile(`(?i)\bORDER\s+BY\s+\S+(?:\s+(ASC|DESC))?`)
)

// minRegexMatch is the minimum number of submatches expected from a regex with one capture group.
const minRegexMatch = 2

// executeStatementRequest is the wire format for ExecuteStatement.
//
// Limit is the SDK's structured page-size field (dynamodb.ExecuteStatementInput.Limit,
// serialized as a top-level "Limit" JSON integer -- see serializers.go's
// awsAwsjson10_serializeOpDocumentExecuteStatementInput), distinct from a
// "LIMIT n" clause embedded in the Statement text itself.
type executeStatementRequest struct {
	Limit          *int32           `json:"Limit,omitempty"`
	Statement      string           `json:"Statement"`
	NextToken      string           `json:"NextToken,omitempty"`
	Parameters     []map[string]any `json:"Parameters,omitempty"`
	ConsistentRead bool             `json:"ConsistentRead,omitempty"`
}

// executeStatementResponse is the wire response for ExecuteStatement.
// Items and LastEvaluatedKey use the DynamoDB wire format (map[string]any
// with {"S":…}, {"N":…} etc.) so that the AWS SDK can deserialise them
// correctly. LastEvaluatedKey is a distinct field from NextToken on the real
// ExecuteStatementOutput (deserializers.go's
// awsAwsjson10_deserializeOpDocumentExecuteStatementOutput switches on both
// "LastEvaluatedKey" and "NextToken" as separate top-level keys) -- dropping
// it left any client reading output.LastEvaluatedKey (the Query/Scan-style
// pagination field) always empty even when more pages existed.
type executeStatementResponse struct {
	TableName        string           `json:"-"` // internal: table name for ConsumedCapacity tracking
	NextToken        string           `json:"NextToken,omitempty"`
	LastEvaluatedKey map[string]any   `json:"LastEvaluatedKey,omitempty"`
	Items            []map[string]any `json:"Items"`
}

// batchStatementRequest is one statement entry inside BatchExecuteStatement.
//
// ConsistentRead mirrors types.BatchStatementRequest.ConsistentRead (see
// serializers.go's awsAwsjson10_serializeDocumentBatchStatementRequest). The
// backend's BatchExecuteStatement already forwards it correctly once it
// arrives; without this field it never left the wire request.
type batchStatementRequest struct {
	Statement      string           `json:"Statement"`
	Parameters     []map[string]any `json:"Parameters,omitempty"`
	ConsistentRead bool             `json:"ConsistentRead,omitempty"`
}

// batchExecuteStatementRequest is the wire format for BatchExecuteStatement.
type batchExecuteStatementRequest struct {
	Statements []batchStatementRequest `json:"Statements"`
}

// batchStatementResponse is one result entry inside BatchExecuteStatement response.
// TableName is populated only when Error is set, matching the real
// BatchStatementResponse ("the table name associated with a failed PartiQL
// batch statement" -- types.go's doc comment on BatchStatementResponse.TableName).
type batchStatementResponse struct {
	Item      map[string]any       `json:"Item,omitempty"`
	Error     *batchStatementError `json:"Error,omitempty"`
	TableName string               `json:"TableName,omitempty"`
}

type batchStatementError struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// batchExecuteStatementResponse is the wire response for BatchExecuteStatement.
type batchExecuteStatementResponse struct {
	Responses []batchStatementResponse `json:"Responses"`
}

// partiQLRunner executes individual PartiQL statements against any StorageBackend.
// Using a runner instead of handler methods allows InMemoryDB.BatchExecuteStatement
// to satisfy the StorageBackend interface without circular dependencies.
type partiQLRunner struct {
	backend StorageBackend
}

// lookupKeySchema returns the key schema for the named table.
// When the backend is an *InMemoryDB the lookup is served from the expression
// cache (TTL: 10 minutes), avoiding repeated global-lock acquisitions on hot
// SELECT/UPDATE/DELETE paths. For other backends it falls back to DescribeTable.
func (r *partiQLRunner) lookupKeySchema(
	ctx context.Context,
	tableName string,
) ([]models.KeySchemaElement, error) {
	if db, ok := r.backend.(*InMemoryDB); ok {
		return db.getKeySchemaForPartiQL(ctx, tableName)
	}

	descOut, err := r.backend.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	return models.FromSDKKeySchema(descOut.Table.KeySchema), nil
}

// executeStatement dispatches a single PartiQL statement to the appropriate handler.
func (r *partiQLRunner) executeStatement(
	ctx context.Context,
	req executeStatementRequest,
) (*executeStatementResponse, error) {
	stmt := strings.TrimSpace(req.Statement)

	switch {
	case partiqlSelectRe.MatchString(stmt):
		return r.executePartiQLSelect(ctx, req)
	case partiqlInsertRe.MatchString(stmt):
		return r.executePartiQLInsert(ctx, req)
	case partiqlUpdateRe.MatchString(stmt):
		return r.executePartiQLUpdate(ctx, req)
	case partiqlDeleteRe.MatchString(stmt):
		return r.executePartiQLDelete(ctx, req)
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidStatement, stmt)
	}
}

// handleExecuteStatement routes to specific DML/DQL handlers based on the statement type.
func (h *DynamoDBHandler) handleExecuteStatement(ctx context.Context, body []byte) (any, error) {
	var req executeStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	runner := &partiQLRunner{backend: h.Backend}
	out, err := runner.executeStatement(ctx, req)
	if err != nil {
		// ErrInvalidStatement maps to AWS ValidationException, not 500.
		if errors.Is(err, ErrInvalidStatement) {
			return nil, NewValidationException(err.Error())
		}

		return nil, err
	}

	return out, nil
}

// handleBatchExecuteStatement delegates to the StorageBackend.BatchExecuteStatement interface
// method, translating between wire format and SDK v2 types.
func (h *DynamoDBHandler) handleBatchExecuteStatement(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req batchExecuteStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	// Pre-allocate responses and sdkStmts; track which original indices have pending
	// SDK responses so the final slice can be assembled in original order.
	responses := make([]batchStatementResponse, len(req.Statements))
	sdkStmts := make([]types.BatchStatementRequest, 0, len(req.Statements))
	// originalIdx maps sdkStmts position → req.Statements position.
	originalIdx := make([]int, 0, len(req.Statements))

	for i, s := range req.Statements {
		sdkParams := make([]types.AttributeValue, 0, len(s.Parameters))

		var convFailed bool

		for _, p := range s.Parameters {
			av, convErr := models.ToSDKAttributeValue(p)
			if convErr != nil {
				convFailed = true

				break
			}

			sdkParams = append(sdkParams, av)
		}

		if convFailed {
			responses[i] = batchStatementResponse{
				Error: &batchStatementError{
					Code:    partiqlValidationExceptionCode,
					Message: "failed to convert one or more statement parameters",
				},
			}

			continue
		}

		sdkStmts = append(sdkStmts, types.BatchStatementRequest{
			Statement:      aws.String(s.Statement),
			Parameters:     sdkParams,
			ConsistentRead: aws.Bool(s.ConsistentRead),
		})
		originalIdx = append(originalIdx, i)
	}

	if len(sdkStmts) > 0 {
		out, err := h.Backend.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{
			Statements: sdkStmts,
		})
		if err != nil {
			return nil, err
		}

		for j, resp := range out.Responses {
			idx := originalIdx[j]
			if resp.Error != nil {
				responses[idx] = batchStatementResponse{
					Error: &batchStatementError{
						Code:    string(resp.Error.Code),
						Message: aws.ToString(resp.Error.Message),
					},
					TableName: aws.ToString(resp.TableName),
				}

				continue
			}

			wireResp := batchStatementResponse{}
			if resp.Item != nil {
				wireResp.Item = models.FromSDKItem(resp.Item)
			}

			responses[idx] = wireResp
		}
	}

	return &batchExecuteStatementResponse{Responses: responses}, nil
}

// partiqlExtractScanIndexForward returns false when an ORDER BY … DESC clause is
// present in the statement, and true otherwise (ascending is the DynamoDB default).
func partiqlExtractScanIndexForward(stmt string) bool {
	m := partiqlOrderByRe.FindStringSubmatch(stmt)
	if len(m) < minRegexMatch {
		return true
	}

	return !strings.EqualFold(strings.TrimSpace(m[1]), "DESC")
}

// executePartiQLSelect handles SELECT statements, supporting WHERE, LIMIT and column projection.
func (r *partiQLRunner) executePartiQLSelect(
	ctx context.Context,
	req executeStatementRequest,
) (*executeStatementResponse, error) {
	// Substitute all ? placeholders in the full statement first.
	substituted, eav, err := partiqlSubstituteParams(req.Statement, req.Parameters)
	if err != nil {
		return nil, err
	}

	tableName, err := extractTableNameFromStatement(substituted)
	if err != nil {
		return nil, err
	}

	whereClause := partiqlExtractWhere(substituted)
	filterExpr, eav := partiqlSubstituteLiterals(whereClause, eav)
	limit := partiqlExtractLimit(substituted)
	// The structured Limit field (set via the SDK request, not statement text)
	// takes precedence when present, matching real ExecuteStatementInput.Limit.
	if req.Limit != nil && *req.Limit > 0 {
		limit = int(*req.Limit)
	}
	colList := partiqlExtractColumns(substituted)
	scanIndexForward := partiqlExtractScanIndexForward(substituted)

	// Try to use Query if the partition key is present in the WHERE clause.
	out, queryErr := r.tryQueryOptimization(
		ctx,
		req,
		tableName,
		whereClause,
		filterExpr,
		eav,
		colList,
		limit,
		scanIndexForward,
	)
	if queryErr != nil && !errors.Is(queryErr, errScanFallback) {
		return nil, queryErr
	}
	if out != nil {
		return out, nil
	}

	// Log at debug when falling back to Scan so callers can detect performance issues.
	logger.Load(ctx).DebugContext(
		ctx, "PartiQL SELECT falling back to Scan",
		slog.String("table", tableName),
		slog.String("where", whereClause),
	)

	return r.executeScanSelect(ctx, req, tableName, filterExpr, eav, colList, limit)
}

// tryQueryOptimization attempts to convert the PartiQL SELECT into a Query operation
// when the partition key is present. Returns (nil, nil) when scan should be used instead,
// or (result, nil) on success, or (nil, err) when a definitive error occurred.
//
// Key schema lookups are performed via getKeySchemaForPartiQL, which caches results
// in the expression cache to avoid repeated global-lock acquisitions on hot paths.
func (r *partiQLRunner) tryQueryOptimization(
	ctx context.Context,
	req executeStatementRequest,
	tableName, whereClause, filterExpr string,
	eav map[string]any,
	colList string,
	limit int,
	scanIndexForward bool,
) (*executeStatementResponse, error) {
	var keySchema []models.KeySchemaElement

	if db, ok := r.backend.(*InMemoryDB); ok {
		ks, err := db.getKeySchemaForPartiQL(ctx, tableName)
		if err != nil {
			return nil, errScanFallback
		}

		keySchema = ks
	} else {
		// Fallback for alternative backends that don't implement the cache.
		descOut, descErr := r.backend.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if descErr != nil {
			return nil, errScanFallback
		}

		keySchema = models.FromSDKKeySchema(descOut.Table.KeySchema)
	}

	keyAttrs := make(map[string]bool, len(keySchema))
	for _, k := range keySchema {
		keyAttrs[k.AttributeName] = true
	}

	wireKey, err := partiqlExtractKeyFromWhere(whereClause, eav, keyAttrs)
	if err != nil {
		if !errors.Is(err, errNoKeyCondition) {
			// A real validation error (e.g., missing placeholder): propagate it.
			return nil, err
		}
		// No PK equality condition found in WHERE; fall back to full scan.
		return nil, errScanFallback
	}

	pkName, _ := getPKAndSK(keySchema)

	if wireKey[pkName.AttributeName] == nil {
		return nil, errScanFallback // PK value not present
	}

	queryInput, err := r.buildQueryInput(
		req,
		tableName,
		whereClause,
		filterExpr,
		eav,
		pkName.AttributeName,
		colList,
		limit,
		scanIndexForward,
	)
	if err != nil {
		return nil, err
	}

	if startKey := decodePartiQLNextToken(req.NextToken); startKey != nil {
		queryInput.ExclusiveStartKey = startKey
	}

	out, queryErr := r.backend.Query(ctx, queryInput)
	if queryErr != nil {
		return nil, queryErr
	}

	return &executeStatementResponse{
		Items:            itemsToWire(out.Items),
		NextToken:        encodePartiQLNextToken(out.LastEvaluatedKey),
		LastEvaluatedKey: lastEvaluatedKeyToWire(out.LastEvaluatedKey),
	}, nil
}

// buildQueryInput constructs a QueryInput from the parsed PartiQL components.
// ConsistentRead from the original statement request is forwarded.
func (r *partiQLRunner) buildQueryInput(
	req executeStatementRequest,
	tableName, whereClause, filterExpr string,
	eav map[string]any,
	pkAttr, colList string,
	limit int,
	scanIndexForward bool,
) (*dynamodb.QueryInput, error) {
	sdkEAV, err := partiqlBuildSDKEAV(eav)
	if err != nil {
		return nil, fmt.Errorf("building QueryInput EAV: %w", err)
	}

	keyCond := fmt.Sprintf("%s = %s", pkAttr, findPlaceholderForKey(whereClause, pkAttr))
	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName),
		ExpressionAttributeValues: sdkEAV,
		KeyConditionExpression:    aws.String(keyCond),
	}

	if req.ConsistentRead {
		queryInput.ConsistentRead = aws.Bool(true)
	}

	if filterExpr != "" {
		queryInput.FilterExpression = aws.String(filterExpr)
	}
	if limit > 0 {
		// #nosec G115 -- limit is parsed from a non-negative decimal digit sequence; fits in int32
		queryInput.Limit = aws.Int32(int32(limit))
	}
	if colList != "" && colList != "*" {
		queryInput.ProjectionExpression = aws.String(colList)
	}

	// ORDER BY DESC maps to ScanIndexForward=false; ASC (default) keeps true.
	if !scanIndexForward {
		queryInput.ScanIndexForward = aws.Bool(false)
	}

	return queryInput, nil
}

// executeScanSelect runs a full Scan for a PartiQL SELECT that couldn't be optimized.
// ConsistentRead and NextToken from the original statement request are forwarded.
func (r *partiQLRunner) executeScanSelect(
	ctx context.Context,
	req executeStatementRequest,
	tableName, filterExpr string,
	eav map[string]any,
	colList string,
	limit int,
) (*executeStatementResponse, error) {
	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	if req.ConsistentRead {
		scanInput.ConsistentRead = aws.Bool(true)
	}

	if filterExpr != "" {
		sdkEAV, eavErr := partiqlBuildSDKEAV(eav)
		if eavErr != nil {
			return nil, eavErr
		}

		scanInput.FilterExpression = aws.String(filterExpr)
		scanInput.ExpressionAttributeValues = sdkEAV
	}

	if limit > 0 {
		// #nosec G115 -- limit is parsed by regex from a non-negative decimal digit sequence;
		// realistic LIMIT values are well within int32 range.
		scanInput.Limit = aws.Int32(int32(limit))
	}

	if colList != "" && colList != "*" {
		scanInput.ProjectionExpression = aws.String(colList)
	}

	if startKey := decodePartiQLNextToken(req.NextToken); startKey != nil {
		scanInput.ExclusiveStartKey = startKey
	}

	out, err := r.backend.Scan(ctx, scanInput)
	if err != nil {
		return nil, err
	}

	return &executeStatementResponse{
		Items:            itemsToWire(out.Items),
		NextToken:        encodePartiQLNextToken(out.LastEvaluatedKey),
		LastEvaluatedKey: lastEvaluatedKeyToWire(out.LastEvaluatedKey),
	}, nil
}

// encodePartiQLNextToken encodes a LastEvaluatedKey map as a base64-JSON NextToken.
// Returns "" when lastKey is empty (no more pages).
func encodePartiQLNextToken(lastKey map[string]types.AttributeValue) string {
	if len(lastKey) == 0 {
		return ""
	}

	wire := models.FromSDKItem(lastKey)

	b, err := json.Marshal(wire)
	if err != nil {
		return ""
	}

	return base64.StdEncoding.EncodeToString(b)
}

// decodePartiQLNextToken decodes a NextToken into an ExclusiveStartKey.
// Returns nil when token is empty or malformed (treat as first page).
func decodePartiQLNextToken(token string) map[string]types.AttributeValue {
	if token == "" {
		return nil
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil
	}

	var wire map[string]any
	if unmarshalErr := json.Unmarshal(b, &wire); unmarshalErr != nil {
		return nil
	}

	sdkItem, err := models.ToSDKItem(wire)
	if err != nil {
		return nil
	}

	return sdkItem
}

// lastEvaluatedKeyToWire converts an SDK LastEvaluatedKey to its wire form,
// returning nil (so the omitempty tag drops it) rather than an empty map
// when there is no more data to page through.
func lastEvaluatedKeyToWire(key map[string]types.AttributeValue) map[string]any {
	if len(key) == 0 {
		return nil
	}

	return models.FromSDKItem(key)
}

func itemsToWire(items []map[string]types.AttributeValue) []map[string]any {
	wireItems := make([]map[string]any, 0, len(items))
	for _, item := range items {
		wireItems = append(wireItems, models.FromSDKItem(item))
	}

	return wireItems
}

func findPlaceholderForKey(whereExpr, keyName string) string {
	conditions := partiqlSplitANDConditions(whereExpr)
	for _, cond := range conditions {
		attrName, placeholder, found := strings.Cut(cond, "=")
		if found && strings.TrimSpace(attrName) == keyName {
			return strings.TrimSpace(placeholder)
		}
	}

	return ""
}

// executePartiQLInsert handles INSERT INTO "table" VALUE {...} statements.
func (r *partiQLRunner) executePartiQLInsert(
	ctx context.Context,
	req executeStatementRequest,
) (*executeStatementResponse, error) {
	matches := partiqlInsertTableRe.FindStringSubmatch(req.Statement)
	if len(matches) < minRegexMatch {
		return nil, fmt.Errorf("%w: cannot extract table name from INSERT", ErrInvalidStatement)
	}

	tableName := matches[1]

	valueMatches := partiqlValueRe.FindStringSubmatch(req.Statement)
	if len(valueMatches) < minRegexMatch {
		return nil, fmt.Errorf("%w: no VALUE clause in INSERT statement", ErrInvalidStatement)
	}

	paramIdx := 0
	wireItem, err := partiqlParseValueClause(valueMatches[1], req.Parameters, &paramIdx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStatement, err)
	}

	sdkItem, err := models.ToSDKItem(wireItem)
	if err != nil {
		return nil, err
	}

	// Build a ConditionExpression that rejects duplicate primary keys.
	// AWS DynamoDB PartiQL INSERT raises DuplicateItemException when an item
	// with the same key already exists; PutItem silently overwrites.
	keySchema, ksErr := r.lookupKeySchema(ctx, tableName)
	if ksErr != nil || len(keySchema) == 0 {
		if _, putErr := r.backend.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      sdkItem,
		}); putErr != nil {
			return nil, putErr
		}

		return &executeStatementResponse{Items: []map[string]any{}}, nil
	}

	pkDef, _ := getPKAndSK(keySchema)
	condExpr := "attribute_not_exists(#__pk)"
	sdkEANs := map[string]string{"#__pk": pkDef.AttributeName}
	_, putErr := r.backend.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                aws.String(tableName),
		Item:                     sdkItem,
		ConditionExpression:      aws.String(condExpr),
		ExpressionAttributeNames: sdkEANs,
	})
	if putErr != nil {
		var ddbErr *Error
		if errors.As(putErr, &ddbErr) &&
			strings.Contains(ddbErr.Type, "ConditionalCheckFailedException") {
			return nil, NewDuplicateItemException(
				"The conditional request failed: item with this key already exists",
			)
		}

		return nil, putErr
	}

	return &executeStatementResponse{Items: []map[string]any{}}, nil
}

// partiqlUpdateParsed holds parsed clauses from a PartiQL UPDATE statement.
type partiqlUpdateParsed struct {
	eav          map[string]any
	tableName    string
	setClause    string
	removeClause string
	whereClause  string
}

// parsePartiQLUpdateClauses extracts table name, SET/REMOVE/WHERE clauses, and substitutes params.
func parsePartiQLUpdateClauses(req executeStatementRequest) (*partiqlUpdateParsed, error) {
	matches := partiqlUpdateTableRe.FindStringSubmatch(req.Statement)
	if len(matches) < minRegexMatch {
		return nil, fmt.Errorf("%w: cannot extract table name from UPDATE", ErrInvalidStatement)
	}

	tableName := matches[1]

	substituted, eav, err := partiqlSubstituteParams(req.Statement, req.Parameters)
	if err != nil {
		return nil, err
	}

	var setClause string
	if setMatches := partiqlSetRe.FindStringSubmatch(substituted); len(setMatches) >= minRegexMatch {
		setClause = strings.TrimSpace(setMatches[1])
	}

	var removeClause string
	if removeMatches := partiqlRemoveRe.FindStringSubmatch(substituted); len(removeMatches) >= minRegexMatch {
		removeClause = strings.TrimSpace(removeMatches[1])
	}

	if setClause == "" && removeClause == "" {
		return nil, fmt.Errorf("%w: UPDATE requires a SET or REMOVE clause", ErrInvalidStatement)
	}

	whereClause := partiqlExtractWhere(substituted)
	if whereClause == "" {
		return nil, fmt.Errorf("%w: UPDATE requires a WHERE clause", ErrInvalidStatement)
	}

	if setClause != "" {
		setClause, eav = partiqlSubstituteLiterals(setClause, eav)
	}
	whereClause, eav = partiqlSubstituteLiterals(whereClause, eav)

	return &partiqlUpdateParsed{
		tableName:    tableName,
		setClause:    setClause,
		removeClause: removeClause,
		whereClause:  whereClause,
		eav:          eav,
	}, nil
}

// executePartiQLUpdate handles UPDATE "table" SET/REMOVE ... WHERE ... statements.
func (r *partiQLRunner) executePartiQLUpdate(
	ctx context.Context,
	req executeStatementRequest,
) (*executeStatementResponse, error) {
	parsed, err := parsePartiQLUpdateClauses(req)
	if err != nil {
		return nil, err
	}

	keySchema, err := r.lookupKeySchema(ctx, parsed.tableName)
	if err != nil {
		return nil, err
	}

	keyAttrs := make(map[string]bool, len(keySchema))
	for _, k := range keySchema {
		keyAttrs[k.AttributeName] = true
	}

	wireKey, err := partiqlExtractKeyFromWhere(parsed.whereClause, parsed.eav, keyAttrs)
	if err != nil {
		return nil, err
	}

	sdkKey, err := models.ToSDKItem(wireKey)
	if err != nil {
		return nil, err
	}

	// Build combined UpdateExpression: "SET ... REMOVE ..." (each part only when present).
	var exprParts []string
	if parsed.setClause != "" {
		exprParts = append(exprParts, "SET "+parsed.setClause)
	}
	if parsed.removeClause != "" {
		exprParts = append(exprParts, "REMOVE "+parsed.removeClause)
	}
	updateExpr := strings.Join(exprParts, " ")

	// Filter EAV to only values referenced in the SET clause.
	// WHERE params were consumed by key extraction; REMOVE has no values.
	setEAV := filterEAVByExpression(parsed.eav, parsed.setClause)

	sdkEAV, err := partiqlBuildSDKEAV(setEAV)
	if err != nil {
		return nil, err
	}

	if _, updateErr := r.backend.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(parsed.tableName),
		Key:                       sdkKey,
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeValues: sdkEAV,
	}); updateErr != nil {
		return nil, updateErr
	}

	return &executeStatementResponse{Items: []map[string]any{}}, nil
}

// executePartiQLDelete handles DELETE FROM "table" WHERE ... statements.
func (r *partiQLRunner) executePartiQLDelete(
	ctx context.Context,
	req executeStatementRequest,
) (*executeStatementResponse, error) {
	// Substitute all ? at once.
	substituted, eav, err := partiqlSubstituteParams(req.Statement, req.Parameters)
	if err != nil {
		return nil, err
	}

	tableName, err := extractTableNameFromStatement(substituted)
	if err != nil {
		return nil, err
	}

	whereClause := partiqlExtractWhere(substituted)
	if whereClause == "" {
		return nil, fmt.Errorf("%w: DELETE requires a WHERE clause", ErrInvalidStatement)
	}

	whereClause, eav = partiqlSubstituteLiterals(whereClause, eav)

	keySchema, err := r.lookupKeySchema(ctx, tableName)
	if err != nil {
		return nil, err
	}

	keyAttrs := make(map[string]bool, len(keySchema))
	for _, k := range keySchema {
		keyAttrs[k.AttributeName] = true
	}

	wireKey, err := partiqlExtractKeyFromWhere(whereClause, eav, keyAttrs)
	if err != nil {
		return nil, err
	}

	sdkKey, err := models.ToSDKItem(wireKey)
	if err != nil {
		return nil, err
	}

	if _, delErr := r.backend.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key:       sdkKey,
	}); delErr != nil {
		return nil, delErr
	}

	return &executeStatementResponse{Items: []map[string]any{}}, nil
}

// extractTableNameFromStatement extracts the table name from a SELECT/DELETE PartiQL statement.
func extractTableNameFromStatement(statement string) (string, error) {
	const minMatchLen = 2 // full match + first capture group

	matches := fromClauseRegex.FindStringSubmatch(statement)
	if len(matches) < minMatchLen {
		return "", fmt.Errorf("%w: %q", ErrInvalidStatement, statement)
	}

	return matches[1], nil
}

// extractPartiQLTableName returns the table name from any PartiQL DML statement.
// Returns empty string when the statement type or table name cannot be determined.
func extractPartiQLTableName(stmt string) string {
	if m := fromClauseRegex.FindStringSubmatch(stmt); len(m) >= minRegexMatch {
		return m[1]
	}
	if m := partiqlInsertTableRe.FindStringSubmatch(stmt); len(m) >= minRegexMatch {
		return m[1]
	}
	if m := partiqlUpdateTableRe.FindStringSubmatch(stmt); len(m) >= minRegexMatch {
		return m[1]
	}

	return ""
}

// advancePastStringLiteral advances index i (which must point to an opening single-quote)
// past the matching closing single-quote, handling SQL-style ” escaped quotes.
// Returns the index of the character immediately after the closing quote.
func advancePastStringLiteral(s string, i int) int {
	i++ // skip opening quote

	for i < len(s) {
		c := s[i]
		i++

		if c == '\'' {
			// SQL-style escaped quote: '' — skip the second ' and continue inside the literal.
			if i < len(s) && s[i] == '\'' {
				i++
			} else {
				break // end of string literal
			}
		}
	}

	return i
}

// partiqlSubstituteParams replaces every '?' placeholder in stmt with ':p0', ':p1', …
// skipping '?' characters that appear inside single-quoted string literals.
// It returns the modified statement and the ExpressionAttributeValues map.
func partiqlSubstituteParams(stmt string, params []map[string]any) (string, map[string]any, error) {
	eav := make(map[string]any)
	paramIdx := 0

	var result strings.Builder

	i := 0
	for i < len(stmt) {
		ch := stmt[i]

		// Pass string literals through verbatim so '?' inside them is not consumed as a parameter.
		if ch == '\'' {
			end := advancePastStringLiteral(stmt, i)
			result.WriteString(stmt[i:end])
			i = end

			continue
		}

		if ch == '?' {
			if paramIdx >= len(params) {
				return "", nil, fmt.Errorf(
					"%w: not enough parameters — need index %d but only %d provided",
					ErrInvalidStatement, paramIdx, len(params),
				)
			}

			key := fmt.Sprintf(":p%d", paramIdx)
			eav[key] = params[paramIdx]
			result.WriteString(key)
			paramIdx++
		} else {
			result.WriteByte(ch)
		}

		i++
	}

	return result.String(), eav, nil
}

// partiqlSubstituteLiterals replaces single-quoted string literals ('…') in expr with
// named :_lN placeholders and adds them to eav as DynamoDB S-type wire values.
// SQL-style escaped quotes (”) inside literals are unescaped to a single quote.
func partiqlSubstituteLiterals(expr string, eav map[string]any) (string, map[string]any) {
	if expr == "" {
		return "", eav
	}

	if eav == nil {
		eav = make(map[string]any)
	}

	litIdx := len(eav) // start after any existing entries to avoid collisions

	result := partiqlStringLiteralRe.ReplaceAllStringFunc(expr, func(match string) string {
		// Strip surrounding single quotes and unescape SQL-style '' to '
		inner := strings.ReplaceAll(match[1:len(match)-1], "''", "'")
		key := fmt.Sprintf(":_l%d", litIdx)
		litIdx++
		eav[key] = map[string]any{"S": inner}

		return key
	})

	return result, eav
}

// partiqlExtractWhere returns the trimmed body of the WHERE clause from a
// (possibly already ?-substituted) PartiQL statement, or "" if absent.
func partiqlExtractWhere(stmt string) string {
	m := partiqlWhereRe.FindStringSubmatch(stmt)
	if len(m) < minRegexMatch {
		return ""
	}

	return strings.TrimSpace(m[1])
}

// partiqlExtractLimit returns the LIMIT value from a PartiQL statement, or 0 if absent.
// It parses the value as a 32-bit integer so the result safely fits into int32.
func partiqlExtractLimit(stmt string) int {
	m := partiqlLimitRe.FindStringSubmatch(stmt)
	if len(m) < minRegexMatch {
		return 0
	}

	// Parse as a 32-bit integer to ensure the result safely fits into int32 when used in ScanInput.Limit.
	parsed, err := strconv.ParseInt(m[1], 10, 32)
	if err != nil {
		return 0
	}

	return int(parsed)
}

// partiqlExtractColumns returns the projection column list (e.g. "col1, col2") from a
// SELECT statement, or "" if not found.  A result of "*" means all columns.
func partiqlExtractColumns(stmt string) string {
	m := partiqlSelectColsRe.FindStringSubmatch(stmt)
	if len(m) < minRegexMatch {
		return ""
	}

	return strings.TrimSpace(m[1])
}

// partiqlSplitANDConditions splits a WHERE expression on "AND" (case-insensitive)
// while preserving BETWEEN … AND … clauses as a single condition.
func partiqlSplitANDConditions(expr string) []string {
	rawParts := partiqlANDSplitRe.Split(expr, -1)
	conditions := make([]string, 0, len(rawParts))

	for i := 0; i < len(rawParts); i++ {
		part := rawParts[i]
		// Re-join BETWEEN ... AND ... pairs that were incorrectly split.
		if strings.Contains(strings.ToUpper(part), " BETWEEN ") && i+1 < len(rawParts) {
			part = part + " AND " + rawParts[i+1]
			i++
		}

		conditions = append(conditions, part)
	}

	return conditions
}

// partiqlExtractKeyFromWhere parses a WHERE expression (with positional parameters already
// substituted as :pN) and extracts equality conditions on key attributes, returning a
// wire-format item containing only the key attributes.
// AND is matched case-insensitively.
func partiqlExtractKeyFromWhere(
	whereExpr string,
	eav map[string]any,
	keyAttrs map[string]bool,
) (map[string]any, error) {
	conditions := partiqlSplitANDConditions(whereExpr)
	key := make(map[string]any, len(keyAttrs))

	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)

		// We only handle simple equality: attr = :placeholder
		attrName, placeholder, found := strings.Cut(cond, "=")
		if !found {
			continue
		}

		attrName = strings.TrimSpace(attrName)
		placeholder = strings.TrimSpace(placeholder)

		if !keyAttrs[attrName] {
			continue
		}

		// Only process named parameters (:name) or positional parameters (:pN).
		// Literal values (e.g. 'hello', 42) are not suitable for Query optimization;
		// skip them here and let the caller fall back to Scan.
		if !strings.HasPrefix(placeholder, ":") {
			continue
		}

		val, ok := eav[placeholder]
		if !ok {
			return nil, fmt.Errorf(
				"%w: placeholder %q not found in parameters",
				ErrInvalidStatement,
				placeholder,
			)
		}

		wireVal, ok := val.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(
				"%w: unexpected value type for placeholder %q",
				ErrInvalidStatement,
				placeholder,
			)
		}

		key[attrName] = wireVal
	}

	if len(key) == 0 {
		return nil, errNoKeyCondition
	}

	return key, nil
}

// partiqlParseValueClause parses a PartiQL INSERT VALUE tuple body such as
// {'pk': ?, 'attr': 'hello', 'n': 42} into a DynamoDB wire-format item.
// paramIdx is incremented for each ? consumed.
func partiqlParseValueClause(
	valueBody string,
	params []map[string]any,
	paramIdx *int,
) (map[string]any, error) {
	// Strip outer { }
	body := strings.TrimSpace(valueBody)
	if len(body) < minRegexMatch || body[0] != '{' || body[len(body)-1] != '}' {
		return nil, fmt.Errorf("%w: VALUE clause must be wrapped in {…}", ErrInvalidStatement)
	}

	body = body[1 : len(body)-1]
	item := make(map[string]any)

	// Split on commas that are not inside nested structures or string literals.
	pairs := splitTopLevelCommas(body)

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		rawKey, rawVal, found := strings.Cut(pair, ":")
		if !found {
			return nil, fmt.Errorf(
				"%w: invalid key:value pair in VALUE clause: %q",
				ErrInvalidStatement,
				pair,
			)
		}

		// Strip optional quotes from attribute name.
		attrName := strings.Trim(strings.TrimSpace(rawKey), `'"`)
		rawVal = strings.TrimSpace(rawVal)

		wireVal, err := partiqlParseScalar(rawVal, params, paramIdx)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", attrName, err)
		}

		item[attrName] = wireVal
	}

	return item, nil
}

// partiqlParseScalar converts a single PartiQL scalar token to DynamoDB wire format.
// Supported forms: ? (parameter), 'string' (with ” escape), bare integer/decimal, TRUE/FALSE, NULL.
func partiqlParseScalar(
	token string,
	params []map[string]any,
	paramIdx *int,
) (map[string]any, error) {
	token = strings.TrimSpace(token)

	// ? — positional parameter
	if token == "?" {
		if *paramIdx >= len(params) {
			return nil, fmt.Errorf(
				"%w: not enough parameters for ? at position %d",
				ErrInvalidStatement, *paramIdx,
			)
		}

		v := params[*paramIdx]
		(*paramIdx)++

		return v, nil
	}

	// 'string literal' — supports SQL-style '' escaped quotes
	if len(token) >= minRegexMatch && token[0] == '\'' && token[len(token)-1] == '\'' {
		inner := strings.ReplaceAll(token[1:len(token)-1], "''", "'")

		return map[string]any{"S": inner}, nil
	}

	// TRUE / FALSE
	upper := strings.ToUpper(token)
	if upper == "TRUE" {
		return map[string]any{typeBOOL: true}, nil
	}

	if upper == "FALSE" {
		return map[string]any{typeBOOL: false}, nil
	}

	// NULL
	if upper == "NULL" {
		return map[string]any{typeNULL: true}, nil
	}

	// Numeric literal (integer or decimal)
	if _, err := strconv.ParseFloat(token, 64); err == nil {
		return map[string]any{"N": token}, nil
	}

	return nil, fmt.Errorf(
		"%w: unsupported value token %q in VALUE clause",
		ErrInvalidStatement,
		token,
	)
}

// filterEAVByExpression returns a subset of eav containing only the keys that
// are referenced in expr. This is used by the PartiQL UPDATE path to avoid
// passing WHERE-clause parameters to UpdateItem's ExpressionAttributeValues,
// which would trigger an unused-EAV validation error.
func filterEAVByExpression(eav map[string]any, expr string) map[string]any {
	if len(eav) == 0 {
		return eav
	}

	out := make(map[string]any, len(eav))

	for k, v := range eav {
		if strings.Contains(expr, k) {
			out[k] = v
		}
	}

	return out
}

// partiqlBuildSDKEAV converts a wire-format EAV map to the SDK AttributeValue map.
// It returns an error if any value fails conversion, surfacing issues rather than silently dropping entries.
func partiqlBuildSDKEAV(eav map[string]any) (map[string]types.AttributeValue, error) {
	out := make(map[string]types.AttributeValue, len(eav))

	for k, v := range eav {
		av, err := models.ToSDKAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("invalid expression attribute value for %q: %w", k, err)
		}

		out[k] = av
	}

	return out, nil
}

// splitTopLevelCommas splits s on commas that are not inside {} or [] nesting,
// or inside single-quoted string literals (including ” escaped quotes).
func splitTopLevelCommas(s string) []string {
	var result []string

	depth := 0
	start := 0

	i := 0
	for i < len(s) {
		ch := s[i]

		// Skip string literal contents so embedded commas are not treated as separators.
		if ch == '\'' {
			i = advancePastStringLiteral(s, i)

			continue
		}

		switch ch {
		case '{', '[':
			depth++
		case '}', ']':
			depth--
		case ',':
			if depth == 0 {
				result = append(result, s[start:i])
				start = i + 1
			}
		}

		i++
	}

	result = append(result, s[start:])

	return result
}
