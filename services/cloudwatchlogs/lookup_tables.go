package cloudwatchlogs

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	// lookupTableMaxNameLen matches CreateLookupTableInput's doc comment:
	// "The name can contain only alphanumeric characters and underscores,
	// and can be up to 256 characters long.".
	lookupTableMaxNameLen = 256
	// lookupTableMaxBodyBytes matches CreateLookupTableInput's doc comment:
	// "The content must use UTF-8 encoding and not exceed 10 MB.".
	lookupTableMaxBodyBytes = 10 * 1024 * 1024
	// defaultLookupTableLimit/maxLookupTableLimit match DescribeLookupTablesInput's
	// doc comment: "The default value is 50 and the maximum value is 100.".
	defaultLookupTableLimit = 50
	maxLookupTableLimit     = 100
)

// validLookupTableName reports whether name matches CreateLookupTableInput's
// documented character set (alphanumeric + underscore) and length limit.
func validLookupTableName(name string) bool {
	if name == "" || len(name) > lookupTableMaxNameLen {
		return false
	}

	for _, r := range name {
		isAlnum := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if !isAlnum && r != '_' {
			return false
		}
	}

	return true
}

func (b *InMemoryBackend) lookupTableARN(name string) string {
	return arn.Build("logs", b.region, b.accountID, "lookup-table:"+name)
}

// parseLookupTableCSV parses a lookup table's CSV content into its header
// row (table fields) and a count of data rows, per CreateLookupTableInput's
// doc comment: "The first row must be a header row with column names.".
func parseLookupTableCSV(body string) ([]string, int64, error) {
	r := csv.NewReader(strings.NewReader(body))
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		//nolint:errorlint // wrapped for message only.
		return nil, 0, fmt.Errorf("%w: unable to parse CSV header row: %v", ErrValidation, err)
	}

	var rows int64

	for {
		_, rerr := r.Read()
		if rerr == io.EOF {
			break
		}

		if rerr != nil {
			//nolint:errorlint // wrapped for message only.
			return nil, 0, fmt.Errorf("%w: malformed CSV content: %v", ErrValidation, rerr)
		}

		rows++
	}

	return header, rows, nil
}

// resolveLookupTableBody enforces CreateLookupTableInput/UpdateLookupTableInput's
// documented "You must specify either tableBody or queryId, but not both"
// (api_op_CreateLookupTable.go, api_op_UpdateLookupTable.go) and, when queryID is
// given, synthesizes CSV content from that query's completed results via
// lookupTableBodyFromQuery.
func (b *InMemoryBackend) resolveLookupTableBody(tableBody, queryID string) (string, error) {
	switch {
	case tableBody != "" && queryID != "":
		return "", fmt.Errorf("%w: must specify either tableBody or queryId, but not both", ErrValidation)
	case tableBody != "":
		if len(tableBody) > lookupTableMaxBodyBytes {
			return "", fmt.Errorf("%w: tableBody exceeds the 10 MB limit", ErrValidation)
		}

		return tableBody, nil
	case queryID != "":
		return b.lookupTableBodyFromQuery(queryID)
	default:
		return "", fmt.Errorf("%w: must specify either tableBody or queryId", ErrValidation)
	}
}

// lookupTableBodyFromQuery renders a completed Logs Insights query's results
// ([][]ResultField) as CSV, matching CreateLookupTableInput's doc comment ("The
// ID of a completed CloudWatch Logs query whose results populate the lookup
// table."). Column order follows the first result row's field order.
func (b *InMemoryBackend) lookupTableBodyFromQuery(queryID string) (string, error) {
	results, _, status, err := b.GetQueryResults(queryID)
	if err != nil {
		return "", err
	}

	if status != QueryStatusComplete {
		return "", fmt.Errorf("%w: query %s has not completed (status %s)", ErrValidation, queryID, status)
	}

	if len(results) == 0 {
		return "", fmt.Errorf("%w: query %s returned no results to build a lookup table from", ErrValidation, queryID)
	}

	header := make([]string, 0, len(results[0]))
	seen := make(map[string]bool, len(results[0]))

	for _, f := range results[0] {
		if !seen[f.Field] {
			seen[f.Field] = true
			header = append(header, f.Field)
		}
	}

	var sb strings.Builder

	w := csv.NewWriter(&sb)
	if writeErr := w.Write(header); writeErr != nil {
		//nolint:errorlint // wrapped for message only.
		return "", fmt.Errorf("%w: %v", ErrValidation, writeErr)
	}

	for _, row := range results {
		values := make(map[string]string, len(row))
		for _, f := range row {
			values[f.Field] = f.Value
		}

		record := make([]string, len(header))
		for i, name := range header {
			record[i] = values[name]
		}

		if writeErr := w.Write(record); writeErr != nil {
			//nolint:errorlint // wrapped for message only.
			return "", fmt.Errorf("%w: %v", ErrValidation, writeErr)
		}
	}

	w.Flush()

	if flushErr := w.Error(); flushErr != nil {
		//nolint:errorlint // wrapped for message only.
		return "", fmt.Errorf("%w: %v", ErrValidation, flushErr)
	}

	return sb.String(), nil
}

// CreateLookupTable creates a new lookup table from real CSV content, or from a
// completed query's results when queryID is given (see the LookupTable doc
// comment in models.go for why this backend stores/parses TableBody directly
// rather than referencing S3).
func (b *InMemoryBackend) CreateLookupTable(
	name, tableBody, description, kmsKeyID, queryID string,
) (*LookupTable, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: lookupTableName is required", ErrValidation)
	}

	if !validLookupTableName(name) {
		return nil, fmt.Errorf(
			"%w: lookupTableName must contain only alphanumeric characters and underscores, up to %d characters",
			ErrValidation, lookupTableMaxNameLen,
		)
	}

	body, err := b.resolveLookupTableBody(tableBody, queryID)
	if err != nil {
		return nil, err
	}

	fields, records, err := parseLookupTableCSV(body)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateLookupTable")
	defer b.mu.Unlock()

	tableArn := b.lookupTableARN(name)
	if b.lookupTables.Has(tableArn) {
		return nil, fmt.Errorf("%w: lookup table %s already exists", ErrLookupTableAlreadyExists, name)
	}

	now := time.Now().UnixMilli()
	t := &LookupTable{
		LookupTableArn:  tableArn,
		LookupTableName: name,
		Description:     description,
		KmsKeyID:        kmsKeyID,
		TableBody:       body,
		TableFields:     fields,
		RecordsCount:    records,
		SizeBytes:       int64(len(body)),
		CreatedAt:       now,
		LastUpdatedTime: now,
	}
	b.lookupTables.Put(t)

	cp := *t

	return &cp, nil
}

// GetLookupTable returns the full content (including TableBody) of a lookup
// table by ARN.
func (b *InMemoryBackend) GetLookupTable(lookupTableArn string) (*LookupTable, error) {
	if lookupTableArn == "" {
		return nil, fmt.Errorf("%w: lookupTableArn is required", ErrValidation)
	}

	b.mu.RLock("GetLookupTable")
	defer b.mu.RUnlock()

	t, ok := b.lookupTables.Get(lookupTableArn)
	if !ok {
		return nil, fmt.Errorf("%w: lookup table %s not found", ErrLookupTableNotFound, lookupTableArn)
	}

	cp := *t

	return &cp, nil
}

// UpdateLookupTable replaces a lookup table's CSV content in full ("This is
// a full replacement operation."), optionally updating description/kmsKeyId
// when the caller supplies them (nil means leave unchanged, matching the
// real Input's *string/optional-pointer fields).
func (b *InMemoryBackend) UpdateLookupTable(
	lookupTableArn, tableBody string, description, kmsKeyID *string, queryID string,
) (*LookupTable, error) {
	if lookupTableArn == "" {
		return nil, fmt.Errorf("%w: lookupTableArn is required", ErrValidation)
	}

	body, err := b.resolveLookupTableBody(tableBody, queryID)
	if err != nil {
		return nil, err
	}

	fields, records, err := parseLookupTableCSV(body)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateLookupTable")
	defer b.mu.Unlock()

	t, ok := b.lookupTables.Get(lookupTableArn)
	if !ok {
		return nil, fmt.Errorf("%w: lookup table %s not found", ErrLookupTableNotFound, lookupTableArn)
	}

	t.TableBody = body
	t.TableFields = fields
	t.RecordsCount = records
	t.SizeBytes = int64(len(body))

	if description != nil {
		t.Description = *description
	}

	if kmsKeyID != nil {
		t.KmsKeyID = *kmsKeyID
	}

	t.LastUpdatedTime = time.Now().UnixMilli()

	cp := *t

	return &cp, nil
}

// DeleteLookupTable permanently deletes a lookup table by ARN.
func (b *InMemoryBackend) DeleteLookupTable(lookupTableArn string) error {
	if lookupTableArn == "" {
		return fmt.Errorf("%w: lookupTableArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteLookupTable")
	defer b.mu.Unlock()

	if !b.lookupTables.Delete(lookupTableArn) {
		return fmt.Errorf("%w: lookup table %s not found", ErrLookupTableNotFound, lookupTableArn)
	}

	return nil
}

// DescribeLookupTables returns lookup table metadata (no TableBody -- see
// the real DescribeLookupTablesOutput's LookupTables shape, types.LookupTable,
// which has no such field), optionally filtered by name prefix, with
// pagination.
func (b *InMemoryBackend) DescribeLookupTables(
	namePrefix, nextToken string, limit int,
) ([]LookupTable, string) {
	b.mu.RLock("DescribeLookupTables")
	defer b.mu.RUnlock()

	all := make([]LookupTable, 0, b.lookupTables.Len())

	for _, t := range b.lookupTables.All() {
		if namePrefix == "" || strings.HasPrefix(t.LookupTableName, namePrefix) {
			all = append(all, *t)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].LookupTableName < all[j].LookupTableName })

	if limit <= 0 || limit > maxLookupTableLimit {
		limit = defaultLookupTableLimit
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LookupTable{}, ""
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}
