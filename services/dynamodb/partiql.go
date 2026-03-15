package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// ErrInvalidStatement is returned when a PartiQL statement cannot be parsed.
var ErrInvalidStatement = errors.New("invalid PartiQL statement")

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
	partiqlWhereRe = regexp.MustCompile(`(?i)\bWHERE\b\s+(.+?)(?:\s+ORDER\s+BY\b|\s+LIMIT\s+\d|\s*$)`)
	// partiqlLimitRe extracts the LIMIT integer value.
	partiqlLimitRe = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	// partiqlSetRe extracts the SET clause body in an UPDATE statement.
	partiqlSetRe = regexp.MustCompile(`(?i)\bSET\s+(.+?)(?:\s+WHERE\b|\s*$)`)
	// partiqlSelectColsRe extracts the column list between SELECT and FROM.
	partiqlSelectColsRe = regexp.MustCompile(`(?i)^\s*SELECT\s+(.+?)\s+FROM\s+"`)
	// partiqlValueRe extracts the VALUE tuple body in an INSERT statement.
	partiqlValueRe = regexp.MustCompile(`(?is)\bVALUE\s+(\{.+\})\s*$`)
	// partiqlStringLiteralRe matches single-quoted string literals, including SQL-style '' escapes.
	partiqlStringLiteralRe = regexp.MustCompile(`'((?:''|[^'])*)'`)
	// partiqlANDSplitRe splits on AND (case-insensitive) with surrounding whitespace.
	partiqlANDSplitRe = regexp.MustCompile(`(?i)\s+AND\s+`)
)

// minRegexMatch is the minimum number of submatches expected from a regex with one capture group.
const minRegexMatch = 2

// executeStatementRequest is the wire format for ExecuteStatement.
type executeStatementRequest struct {
	Statement  string           `json:"Statement"`
	NextToken  string           `json:"NextToken,omitempty"`
	Parameters []map[string]any `json:"Parameters,omitempty"`
}

// executeStatementResponse is the wire response for ExecuteStatement.
// Items uses the DynamoDB wire format (map[string]any with {"S":…}, {"N":…} etc.)
// so that the AWS SDK can deserialise it correctly.
type executeStatementResponse struct {
	NextToken string           `json:"NextToken,omitempty"`
	Items     []map[string]any `json:"Items"`
}

// batchStatementRequest is one statement entry inside BatchExecuteStatement.
type batchStatementRequest struct {
	Statement  string           `json:"Statement"`
	Parameters []map[string]any `json:"Parameters,omitempty"`
}

// batchExecuteStatementRequest is the wire format for BatchExecuteStatement.
type batchExecuteStatementRequest struct {
	Statements []batchStatementRequest `json:"Statements"`
}

// batchStatementResponse is one result entry inside BatchExecuteStatement response.
type batchStatementResponse struct {
	Item  map[string]any       `json:"Item,omitempty"`
	Error *batchStatementError `json:"Error,omitempty"`
}

type batchStatementError struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// batchExecuteStatementResponse is the wire response for BatchExecuteStatement.
type batchExecuteStatementResponse struct {
	Responses []batchStatementResponse `json:"Responses"`
}

// handleExecuteStatement routes to specific DML/DQL handlers based on the statement type.
func (h *DynamoDBHandler) handleExecuteStatement(ctx context.Context, body []byte) (any, error) {
	var req executeStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	stmt := strings.TrimSpace(req.Statement)

	switch {
	case partiqlSelectRe.MatchString(stmt):
		return h.executePartiQLSelect(ctx, req)
	case partiqlInsertRe.MatchString(stmt):
		return h.executePartiQLInsert(ctx, req)
	case partiqlUpdateRe.MatchString(stmt):
		return h.executePartiQLUpdate(ctx, req)
	case partiqlDeleteRe.MatchString(stmt):
		return h.executePartiQLDelete(ctx, req)
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidStatement, stmt)
	}
}

// handleBatchExecuteStatement handles multiple PartiQL statements, dispatching each one.
func (h *DynamoDBHandler) handleBatchExecuteStatement(ctx context.Context, body []byte) (any, error) {
	var req batchExecuteStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	responses := make([]batchStatementResponse, 0, len(req.Statements))

	for _, stmt := range req.Statements {
		stmtBody, _ := json.Marshal(executeStatementRequest{
			Statement:  stmt.Statement,
			Parameters: stmt.Parameters,
		})

		result, err := h.handleExecuteStatement(ctx, stmtBody)
		if err != nil {
			responses = append(responses, batchStatementResponse{
				Error: &batchStatementError{
					Code:    "StatementError",
					Message: err.Error(),
				},
			})

			continue
		}

		execResp, ok := result.(*executeStatementResponse)
		if !ok || len(execResp.Items) == 0 {
			responses = append(responses, batchStatementResponse{})

			continue
		}

		// BatchExecuteStatement returns one Item per statement (key-based lookup).
		// Return the first matched item; callers should use key-based WHERE clauses.
		responses = append(responses, batchStatementResponse{Item: execResp.Items[0]})
	}

	return &batchExecuteStatementResponse{Responses: responses}, nil
}

// executePartiQLSelect handles SELECT statements, supporting WHERE, LIMIT and column projection.
func (h *DynamoDBHandler) executePartiQLSelect(
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
	colList := partiqlExtractColumns(substituted)

	// Try to optimize for Query if PK is present
	descOut, err := h.Backend.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		keyAttrs := make(map[string]bool, len(descOut.Table.KeySchema))
		for _, k := range descOut.Table.KeySchema {
			keyAttrs[aws.ToString(k.AttributeName)] = true
		}

		wireKey, _ := partiqlExtractKeyFromWhere(whereClause, eav, keyAttrs)
		pkName, _ := getPKAndSK(models.FromSDKKeySchema(descOut.Table.KeySchema))

		if wireKey != nil && wireKey[pkName.AttributeName] != nil {
			// Partition key found, use Query
			queryInput := &dynamodb.QueryInput{
				TableName: aws.String(tableName),
			}

			sdkEAV, _ := partiqlBuildSDKEAV(eav)
			queryInput.ExpressionAttributeValues = sdkEAV
			
			// Build KeyConditionExpression
			keyCond := fmt.Sprintf("%s = %s", pkName.AttributeName, findPlaceholderForKey(whereClause, pkName.AttributeName))
			queryInput.KeyConditionExpression = aws.String(keyCond)

			if filterExpr != "" {
				queryInput.FilterExpression = aws.String(filterExpr)
			}

			if limit > 0 {
				queryInput.Limit = aws.Int32(int32(limit))
			}

			if colList != "" && colList != "*" {
				queryInput.ProjectionExpression = aws.String(colList)
			}

			out, queryErr := h.Backend.Query(ctx, queryInput)
			if queryErr == nil {
				return &executeStatementResponse{Items: itemsToWire(out.Items)}, nil
			}
		}
	}

	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
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

	out, err := h.Backend.Scan(ctx, scanInput)
	if err != nil {
		return nil, err
	}

	return &executeStatementResponse{Items: itemsToWire(out.Items)}, nil
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
func (h *DynamoDBHandler) executePartiQLInsert(
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

	if _, putErr := h.Backend.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      sdkItem,
	}); putErr != nil {
		return nil, putErr
	}

	return &executeStatementResponse{Items: []map[string]any{}}, nil
}

// executePartiQLUpdate handles UPDATE "table" SET ... WHERE ... statements.
func (h *DynamoDBHandler) executePartiQLUpdate(
	ctx context.Context,
	req executeStatementRequest,
) (*executeStatementResponse, error) {
	matches := partiqlUpdateTableRe.FindStringSubmatch(req.Statement)
	if len(matches) < minRegexMatch {
		return nil, fmt.Errorf("%w: cannot extract table name from UPDATE", ErrInvalidStatement)
	}

	tableName := matches[1]

	// Substitute all ? at once so clause positions are preserved.
	substituted, eav, err := partiqlSubstituteParams(req.Statement, req.Parameters)
	if err != nil {
		return nil, err
	}

	setMatches := partiqlSetRe.FindStringSubmatch(substituted)
	if len(setMatches) < minRegexMatch {
		return nil, fmt.Errorf("%w: no SET clause in UPDATE statement", ErrInvalidStatement)
	}

	setClause := strings.TrimSpace(setMatches[1])

	whereClause := partiqlExtractWhere(substituted)
	if whereClause == "" {
		return nil, fmt.Errorf("%w: UPDATE requires a WHERE clause", ErrInvalidStatement)
	}

	// Substitute any remaining string literals in both clauses.
	setClause, eav = partiqlSubstituteLiterals(setClause, eav)
	whereClause, eav = partiqlSubstituteLiterals(whereClause, eav)

	// Get key schema to identify which WHERE conditions are key conditions.
	descOut, err := h.Backend.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	keyAttrs := make(map[string]bool, len(descOut.Table.KeySchema))
	for _, k := range descOut.Table.KeySchema {
		keyAttrs[aws.ToString(k.AttributeName)] = true
	}

	wireKey, err := partiqlExtractKeyFromWhere(whereClause, eav, keyAttrs)
	if err != nil {
		return nil, err
	}

	sdkKey, err := models.ToSDKItem(wireKey)
	if err != nil {
		return nil, err
	}

	sdkEAV, err := partiqlBuildSDKEAV(eav)
	if err != nil {
		return nil, err
	}

	if _, updateErr := h.Backend.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName),
		Key:                       sdkKey,
		UpdateExpression:          aws.String("SET " + setClause),
		ExpressionAttributeValues: sdkEAV,
	}); updateErr != nil {
		return nil, updateErr
	}

	return &executeStatementResponse{Items: []map[string]any{}}, nil
}

// executePartiQLDelete handles DELETE FROM "table" WHERE ... statements.
func (h *DynamoDBHandler) executePartiQLDelete(
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

	descOut, err := h.Backend.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	keyAttrs := make(map[string]bool, len(descOut.Table.KeySchema))
	for _, k := range descOut.Table.KeySchema {
		keyAttrs[aws.ToString(k.AttributeName)] = true
	}

	wireKey, err := partiqlExtractKeyFromWhere(whereClause, eav, keyAttrs)
	if err != nil {
		return nil, err
	}

	sdkKey, err := models.ToSDKItem(wireKey)
	if err != nil {
		return nil, err
	}

	if _, delErr := h.Backend.DeleteItem(ctx, &dynamodb.DeleteItemInput{
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

		val, ok := eav[placeholder]
		if !ok {
			return nil, fmt.Errorf("%w: placeholder %q not found in parameters", ErrInvalidStatement, placeholder)
		}

		wireVal, ok := val.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected value type for placeholder %q", ErrInvalidStatement, placeholder)
		}

		key[attrName] = wireVal
	}

	if len(key) == 0 {
		return nil, fmt.Errorf("%w: no key conditions found in WHERE clause", ErrInvalidStatement)
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
			return nil, fmt.Errorf("%w: invalid key:value pair in VALUE clause: %q", ErrInvalidStatement, pair)
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
func partiqlParseScalar(token string, params []map[string]any, paramIdx *int) (map[string]any, error) {
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
		return map[string]any{"BOOL": true}, nil
	}

	if upper == "FALSE" {
		return map[string]any{"BOOL": false}, nil
	}

	// NULL
	if upper == "NULL" {
		return map[string]any{"NULL": true}, nil
	}

	// Numeric literal (integer or decimal)
	if _, err := strconv.ParseFloat(token, 64); err == nil {
		return map[string]any{"N": token}, nil
	}

	return nil, fmt.Errorf("%w: unsupported value token %q in VALUE clause", ErrInvalidStatement, token)
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
