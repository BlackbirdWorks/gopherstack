package dynamodb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// exportIDSuffixLen is the number of characters taken from the UUID to form the
// second component of an export ID suffix. 16 characters is chosen to keep ARNs
// short while still providing enough randomness to avoid collisions.
const exportIDSuffixLen = 16

// exportARNRegionIdx is the zero-based position of the region field in a colon-split ARN.
const exportARNRegionIdx = 3

// exportARNAccountIdx is the zero-based position of the account-ID field in a colon-split ARN.
const exportARNAccountIdx = 4

// exportARNPartCount is the expected number of parts when splitting a full DynamoDB ARN on ":".
const exportARNPartCount = 6

// exportARNPathParts is the expected number of parts when splitting the resource portion of an ARN on "/".
const exportARNPathParts = 2

// generateExportID creates a short unique suffix for export ARNs.
// Format matches the AWS convention: a zero-padded Unix millisecond timestamp
// followed by a UUID-derived hex suffix.
func generateExportID() string {
	return fmt.Sprintf(
		"%016x-%s",
		time.Now().UnixMilli(),
		strings.ReplaceAll(uuid.New().String(), "-", "")[:exportIDSuffixLen],
	)
}

// exportRegionAccount extracts region and accountID from a DynamoDB table ARN.
func exportRegionAccount(tableARN string) (string, string) {
	region, accountID := config.DefaultRegion, config.DefaultAccountID
	if tableARN == "" {
		return region, accountID
	}
	parts := strings.SplitN(tableARN, ":", exportARNPartCount)
	if len(parts) >= exportARNRegionIdx+1 && parts[exportARNRegionIdx] != "" {
		region = parts[exportARNRegionIdx]
	}
	if len(parts) >= exportARNAccountIdx+1 && parts[exportARNAccountIdx] != "" {
		accountID = parts[exportARNAccountIdx]
	}

	return region, accountID
}

// buildExportARN constructs a unique export ARN from the table ARN.
func buildExportARN(tableARN, region, accountID string) string {
	tableSlug := "unknown"
	if tableARN != "" {
		if parts := strings.SplitN(tableARN, "/", exportARNPathParts); len(
			parts,
		) == exportARNPathParts {
			tableSlug = parts[1]
		}
	}
	exportID := fmt.Sprintf("%s/%s", tableSlug, generateExportID())

	return arn.Build("dynamodb", region, accountID, "table/"+exportID)
}

// completeExportSync performs the S3 write (if a bucket is configured) and
// updates the stored export record to its terminal state (COMPLETED or FAILED).
func (db *InMemoryDB) completeExportSync(
	ctx context.Context,
	exportARN string,
	req *exportTableToPointInTimeInput,
) {
	var (
		manifestKey string
		itemCount   int64
		billedBytes int64
		failCode    string
		failMsg     string
		finalStatus = "COMPLETED"
	)
	if req.S3Bucket != "" {
		manifestKey, itemCount, billedBytes, failCode, failMsg, finalStatus =
			db.exportToS3Bucket(ctx, req)
	} else {
		if n, err := db.countTableItems(ctx, req.TableArn); err == nil {
			itemCount = int64(n)
			billedBytes = itemCount * avgExportItemBytes
		}
	}
	db.updateExport(exportARN, finalStatus, manifestKey, failCode, failMsg, itemCount, billedBytes)
}

// exportToS3Bucket writes export data to S3 and returns completion metadata.
func (db *InMemoryDB) exportToS3Bucket(
	ctx context.Context,
	req *exportTableToPointInTimeInput,
) (string, int64, int64, string, string, string) {
	base := strings.TrimSuffix(req.S3Prefix, "/")
	if base != "" {
		base += "/"
	}
	objBase := fmt.Sprintf("%sAWSDynamoDB/%s", base, generateExportID())
	dataKey := objBase + "/data/00000.json.gz"
	manifestKey := objBase + "/manifest-summary.json"
	n, err := db.exportTableToS3(ctx, req.TableArn, req.S3Bucket, dataKey, manifestKey)
	if err != nil {
		return manifestKey, 0, 0, "ExportError", err.Error(), "FAILED"
	}
	itemCount := n
	billedBytes := itemCount * avgExportItemBytes

	return manifestKey, itemCount, billedBytes, "", "", "COMPLETED"
}

// maxImportObjectBytes caps how many bytes are read from a single source object,
// bounding memory use and guarding against decompression bombs.
const maxImportObjectBytes = 256 * 1024 * 1024

// importScannerBufferBytes is the initial bufio.Scanner buffer size for parsing
// newline-delimited import records.
const importScannerBufferBytes = 64 * 1024

// errUnsupportedImportFormat is returned when an InputFormat we cannot parse
// (currently ION) is requested.
var errUnsupportedImportFormat = errors.New("unsupported import format")

// S3Accessor is the subset of S3 operations DynamoDB needs to read ImportTable
// source objects and write ExportTableToPointInTime output. It is satisfied by
// the in-process S3 backend, wired in cli.go alongside the Firehose→S3 wiring.
type S3Accessor interface {
	GetObject(ctx context.Context, in *s3sdk.GetObjectInput) (*s3sdk.GetObjectOutput, error)
	ListObjectsV2(
		ctx context.Context,
		in *s3sdk.ListObjectsV2Input,
	) (*s3sdk.ListObjectsV2Output, error)
	PutObject(ctx context.Context, in *s3sdk.PutObjectInput) (*s3sdk.PutObjectOutput, error)
}

// SetS3Backend wires the S3 backend used for ImportTable / ExportTableToPointInTime.
func (db *InMemoryDB) SetS3Backend(s3 S3Accessor) {
	db.mu.Lock("SetS3Backend")
	defer db.mu.Unlock()

	db.s3 = s3
}

// s3Backend returns the wired S3 accessor, or nil when none is configured.
func (db *InMemoryDB) s3Backend() S3Accessor {
	db.mu.RLock("s3Backend")
	defer db.mu.RUnlock()

	return db.s3
}

// importResult accumulates per-import counters.
type importResult struct {
	imported  int64
	processed int64
	bytes     int64
	errors    int64
}

// importFromS3 reads every object under the source bucket/prefix, parses each
// according to inputFormat, and PutItems the parsed items into tableName. It
// returns the accumulated counters. A nil S3 accessor yields an empty result
// (the table is still created — matching the load-bearing behavior callers need).
func (db *InMemoryDB) importFromS3(
	ctx context.Context,
	tableName string,
	src *types.S3BucketSource,
	inputFormat types.InputFormat,
	compression types.InputCompressionType,
	opts *types.InputFormatOptions,
) (importResult, error) {
	var res importResult

	s3 := db.s3Backend()
	if s3 == nil || src == nil {
		return res, nil
	}

	bucket := aws.ToString(src.S3Bucket)
	prefix := aws.ToString(src.S3KeyPrefix)

	keys, err := listSourceKeys(ctx, s3, bucket, prefix)
	if err != nil {
		return res, err
	}

	for _, key := range keys {
		data, getErr := readSourceObject(ctx, s3, bucket, key, compression)
		if getErr != nil {
			return res, getErr
		}

		res.bytes += int64(len(data))

		items, parseErr := parseImportItems(data, inputFormat, opts)
		if parseErr != nil {
			return res, parseErr
		}

		for _, item := range items {
			res.processed++
			if putErr := db.putImportedItem(ctx, tableName, item); putErr != nil {
				res.errors++

				continue
			}
			res.imported++
		}
	}

	return res, nil
}

// listSourceKeys returns all object keys under bucket/prefix, following pagination.
func listSourceKeys(
	ctx context.Context,
	s3 S3Accessor,
	bucket, prefix string,
) ([]string, error) {
	var (
		keys  []string
		token *string
	)

	for {
		out, err := s3.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list import source objects: %w", err)
		}

		for i := range out.Contents {
			keys = append(keys, aws.ToString(out.Contents[i].Key))
		}

		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}

	return keys, nil
}

// readSourceObject fetches a single object and decompresses it when needed. GZIP is
// inferred from the requested compression type or a ".gz" suffix.
func readSourceObject(
	ctx context.Context,
	s3 S3Accessor,
	bucket, key string,
	compression types.InputCompressionType,
) ([]byte, error) {
	out, err := s3.GetObject(ctx, &s3sdk.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("read import source object %q: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(out.Body, maxImportObjectBytes))
	if err != nil {
		return nil, fmt.Errorf("read import source object %q: %w", key, err)
	}

	gzipped := compression == types.InputCompressionTypeGzip || strings.HasSuffix(key, ".gz")
	if !gzipped {
		return raw, nil
	}

	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("gunzip import source object %q: %w", key, err)
	}
	defer func() { _ = gz.Close() }()

	decoded, err := io.ReadAll(io.LimitReader(gz, maxImportObjectBytes))
	if err != nil {
		return nil, fmt.Errorf("gunzip import source object %q: %w", key, err)
	}

	return decoded, nil
}

// parseImportItems parses object bytes into DynamoDB wire items based on format.
func parseImportItems(
	data []byte,
	inputFormat types.InputFormat,
	opts *types.InputFormatOptions,
) ([]map[string]any, error) {
	switch inputFormat {
	case types.InputFormatDynamodbJson, types.InputFormat(""):
		return parseDynamoDBJSONLines(data)
	case types.InputFormatCsv:
		return parseCSVItems(data, opts)
	default: // ION and any future formats
		return nil, fmt.Errorf("%w: %s", errUnsupportedImportFormat, inputFormat)
	}
}

// parseDynamoDBJSONLines parses newline-delimited {"Item": {...}} records, the
// format produced by ExportTableToPointInTime and accepted by ImportTable.
func parseDynamoDBJSONLines(data []byte) ([]map[string]any, error) {
	var items []map[string]any

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, importScannerBufferBytes), maxImportObjectBytes)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var rec struct {
			Item map[string]any `json:"Item"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("parse DynamoDB JSON line: %w", err)
		}

		if rec.Item != nil {
			items = append(items, rec.Item)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan DynamoDB JSON: %w", err)
	}

	return items, nil
}

// parseCSVItems parses CSV rows into items, mapping every column to a String (S)
// attribute (matching AWS CSV import semantics). Headers come from
// InputFormatOptions.Csv.HeaderList when supplied, otherwise the first row.
func parseCSVItems(data []byte, opts *types.InputFormatOptions) ([]map[string]any, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1

	var headers []string
	if opts != nil && opts.Csv != nil {
		if d := aws.ToString(opts.Csv.Delimiter); d != "" {
			reader.Comma = rune(d[0])
		}
		headers = opts.Csv.HeaderList
	}

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse CSV: %w", err)
	}

	if len(headers) == 0 {
		if len(rows) == 0 {
			return nil, nil
		}
		headers = rows[0]
		rows = rows[1:]
	}

	items := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := make(map[string]any, len(headers))
		for i, h := range headers {
			if i >= len(row) || row[i] == "" {
				continue
			}
			item[h] = map[string]any{"S": row[i]}
		}
		if len(item) > 0 {
			items = append(items, item)
		}
	}

	return items, nil
}

// exportTableToS3 serialises a table's items as gzip-compressed, newline-delimited
// DynamoDB-JSON ({"Item": {...}} per line) and writes them to the given S3
// bucket/key, plus a small manifest object alongside. It returns the number of
// items exported. A nil S3 accessor is a no-op (returns 0, nil), preserving the
// prior "export recorded, no data written" behaviour. The output is directly
// re-importable via ImportTable (InputFormat=DYNAMODB_JSON, GZIP).
func (db *InMemoryDB) exportTableToS3(
	ctx context.Context,
	tableARN, bucket, dataKey, manifestKey string,
) (int64, error) {
	s3 := db.s3Backend()
	if s3 == nil {
		return 0, nil
	}

	items := db.snapshotItemsByTableARN(tableARN)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)

	var count int64
	for i := range items {
		if err := enc.Encode(struct {
			Item map[string]any `json:"Item"`
		}{Item: items[i]}); err != nil {
			_ = gz.Close()

			return 0, fmt.Errorf("encode export item: %w", err)
		}
		count++
	}

	if err := gz.Close(); err != nil {
		return 0, fmt.Errorf("finalise export gzip: %w", err)
	}

	data := buf.Bytes()
	if _, err := s3.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: &bucket,
		Key:    &dataKey,
		Body:   bytes.NewReader(data),
	}); err != nil {
		return 0, fmt.Errorf("write export data object: %w", err)
	}

	manifest, _ := json.Marshal(map[string]any{
		"version":       1,
		"tableArn":      tableARN,
		"itemCount":     count,
		"dataFileS3Key": dataKey,
	})
	if _, err := s3.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: &bucket,
		Key:    &manifestKey,
		Body:   bytes.NewReader(manifest),
	}); err != nil {
		return 0, fmt.Errorf("write export manifest object: %w", err)
	}

	return count, nil
}

// snapshotItemsByTableARN returns deep copies of all items in the table whose
// TableArn matches, or nil when no such table exists.
func (db *InMemoryDB) snapshotItemsByTableARN(tableARN string) []map[string]any {
	db.mu.RLock("snapshotItemsByTableARN")
	defer db.mu.RUnlock()

	for _, t := range db.tables.All() {
		if t.TableArn != tableARN {
			continue
		}

		return snapshotItemsRLocked(t)
	}

	return nil
}

// snapshotItemsRLocked returns deep copies of every item in t under a
// defer-protected table.mu.RLock.
func snapshotItemsRLocked(t *Table) []map[string]any {
	t.mu.RLock("snapshotItemsByTableARN")
	defer t.mu.RUnlock()

	items := make([]map[string]any, 0, len(t.Items))
	for i := range t.Items {
		items = append(items, deepCopyItem(t.Items[i]))
	}

	return items
}

// avgExportItemBytes is a rough average item size used to estimate BilledSizeBytes
// when exact byte measurements aren't available (matches AWS's minimum ~100B billing unit).
const avgExportItemBytes = 100

// countTableItems returns the number of items in the table identified by tableARN.
func (db *InMemoryDB) countTableItems(_ context.Context, tableARN string) (int, error) {
	db.mu.RLock("countTableItems")
	defer db.mu.RUnlock()

	for _, t := range db.tables.All() {
		if t.TableArn != tableARN {
			continue
		}

		return itemCountRLocked(t), nil
	}

	return 0, nil
}

// itemCountRLocked returns len(t.Items) under a defer-protected table.mu.RLock.
func itemCountRLocked(t *Table) int {
	t.mu.RLock("countTableItems")
	defer t.mu.RUnlock()

	return len(t.Items)
}

// putImportedItem writes a single wire item into the target table via PutItem so
// that indexes, streams, and validation are all applied consistently.
func (db *InMemoryDB) putImportedItem(
	ctx context.Context,
	tableName string,
	item map[string]any,
) error {
	sdkItem, err := models.ToSDKItem(item)
	if err != nil {
		return err
	}

	_, err = db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item:      sdkItem,
	})

	return err
}

// --- DescribeImport ---

// DescribeImport returns the import description for a given import ARN.
// If the import was started via ImportTable, the stored record is returned.
// Otherwise, a synthetic COMPLETED response is returned for backwards compatibility.
func (db *InMemoryDB) DescribeImport(
	ctx context.Context,
	input *dynamodb.DescribeImportInput,
) (*dynamodb.DescribeImportOutput, error) {
	if input.ImportArn == nil || *input.ImportArn == "" {
		return nil, NewValidationException("ImportArn is required")
	}

	importARN := *input.ImportArn

	requestRegion := getRegionFromContext(ctx, db)
	if db.regionFromARN(importARN) != requestRegion {
		return nil, NewImportNotFoundException("Import not found: " + importARN)
	}

	imp, ok := db.lookupImport(importARN)
	if !ok {
		// AWS returns ImportNotFoundException for an unknown ARN, not a fake COMPLETED.
		return nil, NewImportNotFoundException("Import not found: " + importARN)
	}

	return &dynamodb.DescribeImportOutput{
		ImportTableDescription: importDescriptionFromRecord(imp),
	}, nil
}

// --- ImportTable ---

// ImportTable creates the target table from TableCreationParameters and, when an
// S3 backend is wired, populates it from the source objects (DYNAMODB_JSON or CSV,
// optionally gzip-compressed). It records accurate counts so DescribeImport and
// ListImports report real progress. ION input is reported as a FAILED import.
func (db *InMemoryDB) ImportTable(
	ctx context.Context,
	input *dynamodb.ImportTableInput,
) (*dynamodb.ImportTableOutput, error) {
	if input.TableCreationParameters == nil {
		return nil, NewValidationException("TableCreationParameters is required")
	}

	if input.S3BucketSource == nil || input.S3BucketSource.S3Bucket == nil {
		return nil, NewValidationException("S3BucketSource.S3Bucket is required")
	}

	tcp := input.TableCreationParameters
	if aws.ToString(tcp.TableName) == "" {
		return nil, NewValidationException("TableCreationParameters.TableName is required")
	}

	tableName := aws.ToString(tcp.TableName)
	region := getRegionFromContext(ctx, db)
	account := accountFromContext(ctx, db)
	importID := uuid.New().String()
	importARN := arn.Build("dynamodb", region, account, "table/import/"+importID)
	tableARN := arn.Build("dynamodb", region, account, "table/"+tableName)
	// CloudWatchLogGroupArn is AWS-generated (not caller-supplied) and this
	// emulator writes no real CloudWatch logs; synthesize a plausible ARN
	// under the same import ID so the field round-trips non-empty rather
	// than silently dropping, matching how TableArn/ImportArn are built.
	logGroupArn := arn.Build("logs", region, account, "log-group:/aws/dynamodb/imports/"+importID)
	start := time.Now()

	// Create the target table; surface CreateTable errors (e.g. ResourceInUse).
	createOut, err := db.CreateTable(ctx, createInputFromImportParams(tcp))
	if err != nil {
		return nil, err
	}

	var tableID string
	if createOut.TableDescription != nil {
		tableID = aws.ToString(createOut.TableDescription.TableId)
	}

	rec := storedImport{
		ImportArn:             importARN,
		TableArn:              tableARN,
		TableID:               tableID,
		ClientToken:           aws.ToString(input.ClientToken),
		CloudWatchLogGroupArn: logGroupArn,
		S3Bucket:              aws.ToString(input.S3BucketSource.S3Bucket),
		S3Prefix:              aws.ToString(input.S3BucketSource.S3KeyPrefix),
		S3BucketOwner:         aws.ToString(input.S3BucketSource.S3BucketOwner),
		InputFormat:           string(input.InputFormat),
		InputCompression:      string(input.InputCompressionType),
		StartTime:             start,
		CreatedAt:             start,
		ImportStatus:          string(types.ImportStatusInProgress),
	}

	if input.InputFormatOptions != nil && input.InputFormatOptions.Csv != nil {
		rec.CsvDelimiter = aws.ToString(input.InputFormatOptions.Csv.Delimiter)
		rec.CsvHeaderList = input.InputFormatOptions.Csv.HeaderList
	}

	db.storeImport(rec)

	go func(r storedImport, tName string, in *dynamodb.ImportTableInput) {
		res, importErr := db.importFromS3(
			context.WithoutCancel(ctx), tName, in.S3BucketSource,
			in.InputFormat, in.InputCompressionType, in.InputFormatOptions,
		)
		r.EndTime = time.Now()
		r.ImportedItemCount = res.imported
		r.ProcessedItemCount = res.processed
		r.ProcessedSizeBytes = res.bytes
		r.ErrorCount = res.errors

		if importErr != nil {
			r.ImportStatus = string(types.ImportStatusFailed)
			r.FailureCode = "InputFormatError"
			r.FailureMessage = importErr.Error()
		} else {
			r.ImportStatus = string(types.ImportStatusCompleted)
		}

		db.storeImport(r)
	}(rec, tableName, input)

	return &dynamodb.ImportTableOutput{
		ImportTableDescription: importDescriptionFromRecord(rec),
	}, nil
}

// createInputFromImportParams maps TableCreationParameters to a CreateTableInput.
func createInputFromImportParams(tcp *types.TableCreationParameters) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName:              tcp.TableName,
		KeySchema:              tcp.KeySchema,
		AttributeDefinitions:   tcp.AttributeDefinitions,
		BillingMode:            tcp.BillingMode,
		GlobalSecondaryIndexes: tcp.GlobalSecondaryIndexes,
		ProvisionedThroughput:  tcp.ProvisionedThroughput,
		OnDemandThroughput:     tcp.OnDemandThroughput,
		SSESpecification:       tcp.SSESpecification,
	}
}

// importSummaryFromRecord builds the SDK ImportSummary from a stored import.
// ImportSummary is a narrower type than ImportTableDescription -- it carries
// no TableId, ClientToken, item counts, or failure fields, so those are
// deliberately left off here even though importDescriptionFromRecord below
// sets them for Describe/ImportTable's fuller shape.
func importSummaryFromRecord(rec storedImport) types.ImportSummary {
	status := rec.ImportStatus
	if status == "" {
		status = string(types.ImportStatusCompleted)
	}

	s := types.ImportSummary{
		ImportArn:    aws.String(rec.ImportArn),
		ImportStatus: types.ImportStatus(status),
		TableArn:     aws.String(rec.TableArn),
		InputFormat:  types.InputFormat(rec.InputFormat),
		S3BucketSource: &types.S3BucketSource{
			S3Bucket:      aws.String(rec.S3Bucket),
			S3KeyPrefix:   aws.String(rec.S3Prefix),
			S3BucketOwner: ptrconv.NilIfEmpty(rec.S3BucketOwner),
		},
	}
	if !rec.StartTime.IsZero() {
		s.StartTime = aws.Time(rec.StartTime)
	}
	if !rec.EndTime.IsZero() {
		s.EndTime = aws.Time(rec.EndTime)
	}
	if rec.CloudWatchLogGroupArn != "" {
		s.CloudWatchLogGroupArn = aws.String(rec.CloudWatchLogGroupArn)
	}

	return s
}

// importDescriptionFromRecord builds the SDK description from a stored import.
func importDescriptionFromRecord(rec storedImport) *types.ImportTableDescription {
	desc := &types.ImportTableDescription{
		ImportArn:            aws.String(rec.ImportArn),
		ImportStatus:         types.ImportStatus(rec.ImportStatus),
		TableArn:             aws.String(rec.TableArn),
		InputFormat:          types.InputFormat(rec.InputFormat),
		InputCompressionType: types.InputCompressionType(rec.InputCompression),
		ImportedItemCount:    rec.ImportedItemCount,
		ProcessedItemCount:   rec.ProcessedItemCount,
		ProcessedSizeBytes:   aws.Int64(rec.ProcessedSizeBytes),
		ErrorCount:           rec.ErrorCount,
		S3BucketSource: &types.S3BucketSource{
			S3Bucket:      aws.String(rec.S3Bucket),
			S3KeyPrefix:   aws.String(rec.S3Prefix),
			S3BucketOwner: ptrconv.NilIfEmpty(rec.S3BucketOwner),
		},
	}
	if !rec.StartTime.IsZero() {
		desc.StartTime = aws.Time(rec.StartTime)
	}
	if !rec.EndTime.IsZero() {
		desc.EndTime = aws.Time(rec.EndTime)
	}
	if rec.FailureCode != "" {
		desc.FailureCode = aws.String(rec.FailureCode)
		desc.FailureMessage = aws.String(rec.FailureMessage)
	}
	if rec.TableID != "" {
		desc.TableId = aws.String(rec.TableID)
	}
	if rec.ClientToken != "" {
		desc.ClientToken = aws.String(rec.ClientToken)
	}
	if rec.CloudWatchLogGroupArn != "" {
		desc.CloudWatchLogGroupArn = aws.String(rec.CloudWatchLogGroupArn)
	}
	if rec.CsvDelimiter != "" || len(rec.CsvHeaderList) > 0 {
		desc.InputFormatOptions = &types.InputFormatOptions{
			Csv: &types.CsvOptions{
				Delimiter:  ptrconv.NilIfEmpty(rec.CsvDelimiter),
				HeaderList: rec.CsvHeaderList,
			},
		}
	}

	return desc
}

// --- ListImports ---

// ListImports returns stored import records for the request region.
// Supports NextToken-based pagination and PageSize per the real AWS API.
func (db *InMemoryDB) ListImports(
	ctx context.Context,
	input *dynamodb.ListImportsInput,
) (*dynamodb.ListImportsOutput, error) {
	const defaultListImportsLimit = 25

	region := getRegionFromContext(ctx, db)
	stored := db.listImportsStored()

	// NextToken is the ImportArn of the last record returned previously.
	nextToken := aws.ToString(input.NextToken)
	pageSize := defaultListImportsLimit
	if input.PageSize != nil && *input.PageSize > 0 {
		pageSize = int(*input.PageSize)
	}

	tableArnFilter := aws.ToString(input.TableArn)

	// Filter by region and apply cursor.
	summaries := make([]types.ImportSummary, 0, len(stored))
	started := nextToken == ""

	for _, imp := range stored {
		if db.regionFromARN(imp.ImportArn) != region {
			continue
		}
		if tableArnFilter != "" && imp.TableArn != tableArnFilter {
			continue
		}
		if !started {
			if imp.ImportArn == nextToken {
				started = true
			}

			continue
		}

		summaries = append(summaries, importSummaryFromRecord(imp))
	}

	var outNextToken *string
	if len(summaries) > pageSize {
		tok := *summaries[pageSize-1].ImportArn
		outNextToken = &tok
		summaries = summaries[:pageSize]
	}

	return &dynamodb.ListImportsOutput{
		ImportSummaryList: summaries,
		NextToken:         outNextToken,
	}, nil
}

// --- ExportTableToPointInTime / DescribeExport / ListExports ---

// exportDescFieldsToSDK converts the internally-tracked export record to the
// SDK ExportDescription type.
func exportDescFieldsToSDK(d exportDescriptionFields) *types.ExportDescription {
	ed := &types.ExportDescription{
		ExportArn:      aws.String(d.ExportArn),
		ExportStatus:   types.ExportStatus(d.ExportStatus),
		TableArn:       ptrconv.NilIfEmpty(d.TableArn),
		S3Bucket:       ptrconv.NilIfEmpty(d.S3Bucket),
		S3Prefix:       ptrconv.NilIfEmpty(d.S3Prefix),
		ExportFormat:   types.ExportFormat(d.ExportFormat),
		ExportType:     types.ExportType(d.ExportType),
		ExportManifest: ptrconv.NilIfEmpty(d.ExportManifest),
		FailureCode:    ptrconv.NilIfEmpty(d.FailureCode),
		FailureMessage: ptrconv.NilIfEmpty(d.FailureMessage),
	}

	if d.ExportTime != 0 {
		t := time.Unix(int64(d.ExportTime), 0).UTC()
		ed.ExportTime = &t
	}
	if d.StartTime != 0 {
		t := time.Unix(int64(d.StartTime), 0).UTC()
		ed.StartTime = &t
	}
	if d.EndTime != 0 {
		t := time.Unix(int64(d.EndTime), 0).UTC()
		ed.EndTime = &t
	}
	if d.BilledSizeBytes != 0 {
		ed.BilledSizeBytes = aws.Int64(d.BilledSizeBytes)
	}
	if d.ItemCount != 0 {
		ed.ItemCount = aws.Int64(d.ItemCount)
	}

	return ed
}

// ExportTableToPointInTime starts an (immediately-completing) export of a
// table's items to the configured S3 destination.
// It satisfies the StorageBackend interface using official AWS SDK v2 types.
func (db *InMemoryDB) ExportTableToPointInTime(
	ctx context.Context,
	input *dynamodb.ExportTableToPointInTimeInput,
) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	tableARN := aws.ToString(input.TableArn)
	region, accountID := exportRegionAccount(tableARN)
	exportARN := buildExportARN(tableARN, region, accountID)

	exportFmt := string(input.ExportFormat)
	if exportFmt == "" {
		exportFmt = "DYNAMODB_JSON"
	}

	var exportTime float64
	if input.ExportTime != nil {
		exportTime = float64(input.ExportTime.Unix())
	}

	s3Bucket := aws.ToString(input.S3Bucket)
	s3Prefix := aws.ToString(input.S3Prefix)

	// Persist as IN_PROGRESS (AWS initial response), then complete asynchronously.
	// Real AWS takes minutes; the emulator finishes in microseconds.
	desc := exportDescriptionFields{
		ExportArn:    exportARN,
		ExportStatus: "IN_PROGRESS",
		TableArn:     tableARN,
		S3Bucket:     s3Bucket,
		S3Prefix:     s3Prefix,
		ExportFormat: exportFmt,
		ExportType:   "FULL_EXPORT",
		StartTime:    float64(time.Now().Unix()),
		ExportTime:   exportTime,
	}
	db.storeExport(desc)

	exportReq := &exportTableToPointInTimeInput{
		TableArn:     tableARN,
		S3Bucket:     s3Bucket,
		S3Prefix:     s3Prefix,
		ExportFormat: exportFmt,
		ExportTime:   exportTime,
	}
	go db.completeExportSync(context.WithoutCancel(ctx), exportARN, exportReq)

	return &dynamodb.ExportTableToPointInTimeOutput{
		ExportDescription: exportDescFieldsToSDK(desc),
	}, nil
}

// DescribeExport returns the stored description of a table export, restricted
// to the request region. It satisfies the StorageBackend interface using
// official AWS SDK v2 types.
func (db *InMemoryDB) DescribeExport(
	ctx context.Context,
	input *dynamodb.DescribeExportInput,
) (*dynamodb.DescribeExportOutput, error) {
	exportArn := aws.ToString(input.ExportArn)
	if exportArn == "" {
		return nil, NewValidationException("ExportArn is required")
	}

	requestRegion := getRegionFromContext(ctx, db)
	if db.regionFromARN(exportArn) != requestRegion {
		// AWS returns ExportNotFoundException for an unknown ARN, not a fake COMPLETED.
		return nil, NewExportNotFoundException("Export not found: " + exportArn)
	}

	desc, found := db.lookupExport(exportArn)
	if !found {
		return nil, NewExportNotFoundException("Export not found: " + exportArn)
	}

	return &dynamodb.DescribeExportOutput{
		ExportDescription: exportDescFieldsToSDK(desc),
	}, nil
}

// ListExports returns export summaries filtered by request region and,
// optionally, TableArn. It satisfies the StorageBackend interface using
// official AWS SDK v2 types -- ExportSummary only carries ExportArn,
// ExportStatus and ExportType, and (*DynamoDBHandler).listExports emits
// exactly that; it no longer widens this with per-ARN DescribeExport calls.
func (db *InMemoryDB) ListExports(
	ctx context.Context,
	input *dynamodb.ListExportsInput,
) (*dynamodb.ListExportsOutput, error) {
	requestRegion := getRegionFromContext(ctx, db)
	tableArn := aws.ToString(input.TableArn)

	summaries := db.collectExportSummariesRLocked(tableArn, requestRegion)

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].ExportArn < summaries[j].ExportArn
	})

	start := 0
	nextToken := aws.ToString(input.NextToken)
	if nextToken != "" {
		for i, s := range summaries {
			if s.ExportArn == nextToken {
				start = i + 1

				break
			}
		}
	}
	summaries = summaries[start:]

	const defaultMaxResults = 25

	pageSize := defaultMaxResults
	if input.MaxResults != nil && *input.MaxResults > 0 {
		pageSize = int(*input.MaxResults)
	}

	var outNextToken string
	if len(summaries) > pageSize {
		outNextToken = summaries[pageSize-1].ExportArn
		summaries = summaries[:pageSize]
	}

	out := make([]types.ExportSummary, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, types.ExportSummary{
			ExportArn:    aws.String(s.ExportArn),
			ExportStatus: types.ExportStatus(s.ExportStatus),
			ExportType:   types.ExportType(s.ExportType),
		})
	}

	return &dynamodb.ListExportsOutput{
		ExportSummaries: out,
		NextToken:       ptrconv.NilIfEmpty(outNextToken),
	}, nil
}
