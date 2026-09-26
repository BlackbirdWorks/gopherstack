package asl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ResultWriterDetails points to a Distributed Map's exported result
// manifest.
type ResultWriterDetails struct {
	Bucket string `json:"Bucket"`
	Key    string `json:"Key"`
}

// mapExportOutput is a Distributed Map state's own output when ResultWriter
// exports to S3: the Map Run ARN plus the manifest location, replacing the
// inline results array (AWS docs: input-output-resultwriter.html, example
// under "Exporting to Amazon S3").
type mapExportOutput struct {
	MapRunArn           string              `json:"MapRunArn,omitempty"`
	ResultWriterDetails ResultWriterDetails `json:"ResultWriterDetails"`
}

type resultManifest struct {
	MapRunArn         string              `json:"MapRunArn,omitempty"`
	DestinationBucket string              `json:"DestinationBucket"`
	ResultFiles       resultManifestFiles `json:"ResultFiles"`
}

// resultManifestFiles mirrors AWS's manifest.json ResultFiles section: one
// array per status, populated only for statuses that produced a file.
// gopherstack runs every resolved item synchronously, so PENDING (items
// never scheduled because the Map Run aborted) is always empty here.
type resultManifestFiles struct {
	Succeeded []resultManifestFile `json:"SUCCEEDED"`
	Failed    []resultManifestFile `json:"FAILED"`
	Pending   []resultManifestFile `json:"PENDING"`
}

type resultManifestFile struct {
	Key  string `json:"Key"`
	Size int    `json:"Size"`
}

const (
	transformationNone    = "NONE"
	transformationCompact = "COMPACT"
	transformationFlatten = "FLATTEN"

	outputTypeJSON  = "JSON"
	outputTypeJSONL = "JSONL"

	redriveStatusRedrivable    = "REDRIVABLE"
	redriveStatusNotRedrivable = "NOT_REDRIVABLE"
	redriveReasonSucceeded     = "Execution is SUCCEEDED and cannot be redriven."

	statusSucceeded = "SUCCEEDED"
	statusFailed    = "FAILED"
)

// detailsIncluded mirrors CloudWatchEventsExecutionDataDetails's one real
// member (sfn@v1.49.0 types.go): whether the data was included, never
// truncated in this emulator.
type detailsIncluded struct {
	Included bool `json:"Included"`
}

// mapItemRecord is one entry in a Distributed Map ResultWriter's
// Transformation: NONE output -- the "workflow metadata" AWS docs
// (input-output-resultwriter.html) describe: the full per-child-execution
// record, Input/Output as JSON-encoded strings (matching a real
// DescribeExecution's Input/Output shape). ExecutionArn/Name/StartDate/
// StopDate are populated only for DISTRIBUTED Map items, which run as real
// child Executions (see DistributedMapItemResult); an INLINE Map's
// in-process iterations have no such resource to report and leave them
// empty, honestly, rather than fabricating them.
type mapItemRecord struct {
	OutputDetails       *detailsIncluded `json:"OutputDetails,omitempty"`
	Error               string           `json:"Error,omitempty"`
	RedriveStatus       string           `json:"RedriveStatus,omitempty"`
	Name                string           `json:"Name,omitempty"`
	Output              string           `json:"Output,omitempty"`
	Input               string           `json:"Input"`
	ExecutionArn        string           `json:"ExecutionArn,omitempty"`
	Cause               string           `json:"Cause,omitempty"`
	Status              string           `json:"Status"`
	RedriveStatusReason string           `json:"RedriveStatusReason,omitempty"`
	StateMachineArn     string           `json:"StateMachineArn,omitempty"`
	RedriveCount        int              `json:"RedriveCount"`
	StartDate           float64          `json:"StartDate,omitempty"`
	StopDate            float64          `json:"StopDate,omitempty"`
	InputDetails        detailsIncluded  `json:"InputDetails"`
}

const resultWriterFileIndex = 0

// exportMapResults applies a Distributed Map's ResultWriter.WriterConfig
// (Transformation/OutputType) and, when Resource+Parameters name an S3
// destination, writes the per-item results plus a manifest to the wired
// S3Writer -- returning ResultWriterDetails in place of inline results (AWS
// docs: input-output-resultwriter.html). It also returns the number of
// successful results actually written, for MapRunItemCounts.ResultsWritten.
//
// AWS documents three valid ResultWriter shapes (required field
// combinations): WriterConfig alone previews the formatted output without
// exporting; Resource+Parameters alone exports with NONE/JSON defaults; all
// three format AND export. When no S3Writer is wired, or Parameters.Bucket
// is unset despite Resource being set, results still degrade to the
// formatted inline output instead of failing the Map state -- the
// computation already succeeded, only its export is unavailable -- logging
// a warning naming the state so the degradation is diagnosable.
func (e *Executor) exportMapResults(
	ctx context.Context, state *State, stateName, mapRunARN string,
	items, results []any, errs []error, meta []DistributedMapItemResult, stateMachineArn string,
) (any, int, error) {
	rw := state.ResultWriter
	bucket, _ := rw.Parameters["Bucket"].(string)
	exporting := rw.Resource != "" || bucket != ""

	transformation := resolveTransformation(rw.WriterConfig, exporting)
	outputType := resolveOutputType(rw.WriterConfig)

	records := buildMapItemRecords(items, results, errs, meta, stateMachineArn)

	if !exporting {
		preview, err := previewValue(records, results, transformation, outputType)
		if err != nil {
			return nil, 0, fmt.Errorf("ResultWriter: %w", err)
		}

		return preview, 0, nil
	}

	if e.s3w == nil {
		logger.Load(ctx).WarnContext(ctx,
			"stepfunctions: ResultWriter configured but export unavailable, returning inline results",
			"state", stateName, "bucket", bucket)

		preview, err := previewValue(records, results, transformation, outputType)
		if err != nil {
			return nil, 0, fmt.Errorf("ResultWriter: %w", err)
		}

		return preview, 0, nil
	}

	succeededCount := countStatus(records, statusSucceeded)

	prefix, _ := rw.Parameters["Prefix"].(string)
	folder := resultFolderKey(prefix, mapRunARN)

	files, err := e.writeMapResultFiles(ctx, bucket, folder, records, results, transformation, outputType)
	if err != nil {
		return nil, 0, fmt.Errorf("ResultWriter: %w", err)
	}

	manifestKey := folder + "manifest.json"

	manifestBytes, err := json.Marshal(resultManifest{
		DestinationBucket: bucket,
		MapRunArn:         mapRunARN,
		ResultFiles:       files,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("ResultWriter: marshal manifest: %w", err)
	}

	if putErr := e.s3w.PutObjectBytes(ctx, bucket, manifestKey, manifestBytes); putErr != nil {
		return nil, 0, fmt.Errorf("ResultWriter: write manifest: %w", putErr)
	}

	out := mapExportOutput{
		MapRunArn:           mapRunARN,
		ResultWriterDetails: ResultWriterDetails{Bucket: bucket, Key: manifestKey},
	}

	return out, succeededCount, nil
}

// resolveTransformation applies AWS's documented WriterConfig.Transformation
// defaults: NONE when exporting to S3 and unspecified, COMPACT otherwise
// (input-output-resultwriter.html, "Contents of the ResultWriter field").
func resolveTransformation(wc *ResultWriterConfig, exporting bool) string {
	if wc != nil && wc.Transformation != "" {
		return strings.ToUpper(wc.Transformation)
	}

	if exporting {
		return transformationNone
	}

	return transformationCompact
}

// resolveOutputType applies WriterConfig.OutputType's documented default: JSON.
func resolveOutputType(wc *ResultWriterConfig) string {
	if wc != nil && strings.ToUpper(wc.OutputType) == outputTypeJSONL {
		return outputTypeJSONL
	}

	return outputTypeJSON
}

// buildMapItemRecords builds one mapItemRecord per Map item, in original
// order, regardless of Transformation -- callers pick which fields of each
// record to surface.
func buildMapItemRecords(
	items, results []any, errs []error, meta []DistributedMapItemResult, stateMachineArn string,
) []mapItemRecord {
	records := make([]mapItemRecord, len(items))

	for i, item := range items {
		var m DistributedMapItemResult
		if i < len(meta) {
			m = meta[i]
		}

		records[i] = buildMapItemRecord(item, results[i], errs[i], m, stateMachineArn)
	}

	return records
}

func buildMapItemRecord(
	item, result any,
	err error,
	meta DistributedMapItemResult,
	stateMachineArn string,
) mapItemRecord {
	rec := mapItemRecord{
		Input:           stringifyJSON(item),
		InputDetails:    detailsIncluded{Included: true},
		ExecutionArn:    meta.ExecutionArn,
		Name:            meta.Name,
		StartDate:       meta.StartDate,
		StopDate:        meta.StopDate,
		StateMachineArn: stateMachineArn,
	}

	if err != nil {
		rec.Status = statusFailed
		rec.Error, rec.Cause = mapResultErrorCodeAndCause(err)
		rec.RedriveStatus = redriveStatusRedrivable

		return rec
	}

	rec.Status = statusSucceeded
	rec.Output = stringifyJSON(result)
	rec.OutputDetails = &detailsIncluded{Included: true}
	rec.RedriveStatus = redriveStatusNotRedrivable
	rec.RedriveStatusReason = redriveReasonSucceeded

	return rec
}

func stringifyJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return aslNullLiteral
	}

	return string(b)
}

func countStatus(records []mapItemRecord, status string) int {
	n := 0

	for _, r := range records {
		if r.Status == status {
			n++
		}
	}

	return n
}

// previewValue renders the WriterConfig-formatted result the Map state
// returns inline: entries in original item order, mixing SUCCEEDED (per
// Transformation) and FAILED (always the full NONE-shaped record -- AWS
// docs: "If a child workflow execution fails, Step Functions returns its
// execution result unchanged"). OutputType JSON returns the native array;
// JSONL returns a newline-delimited string, matching what a JSONL S3 object
// would contain.
func previewValue(records []mapItemRecord, results []any, transformation, outputType string) (any, error) {
	entries := make([]any, len(records))

	for i, rec := range records {
		if rec.Status == statusFailed || transformation == transformationNone {
			entries[i] = rec

			continue
		}

		entries[i] = results[i]
	}

	if transformation == transformationFlatten {
		entries = flattenValues(entries)
	}

	return encodeEntriesInline(entries, outputType)
}

// flattenValues implements Transformation: FLATTEN -- when an entry is
// itself a JSON array, its elements are spliced into the result in place of
// the array (AWS docs: "If a child workflow execution returns an array,
// this option flattens the array"). Non-array entries (including FAILED
// mapItemRecord entries) pass through unchanged.
func flattenValues(vals []any) []any {
	out := make([]any, 0, len(vals))

	for _, v := range vals {
		if arr, ok := v.([]any); ok {
			out = append(out, arr...)

			continue
		}

		out = append(out, v)
	}

	return out
}

func encodeEntriesInline(entries []any, outputType string) (any, error) {
	if outputType != outputTypeJSONL {
		return entries, nil
	}

	data, err := encodeJSONLines(entries)
	if err != nil {
		return nil, err
	}

	return string(data), nil
}

// encodeJSONLines renders entries as JSON Lines: one JSON value per line,
// no enclosing array (WriterConfig.OutputType: JSONL).
func encodeJSONLines(entries []any) ([]byte, error) {
	var buf bytes.Buffer

	for i, e := range entries {
		if i > 0 {
			buf.WriteByte('\n')
		}

		b, err := json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("marshal JSONL entry: %w", err)
		}

		buf.Write(b)
	}

	return buf.Bytes(), nil
}

// mapResultErrorCodeAndCause splits a Map item's error into AWS's separate
// Error/Cause fields; a *FailError already carries them apart, anything
// else has no distinct Cause.
func mapResultErrorCodeAndCause(err error) (string, string) {
	if failErr, ok := errors.AsType[*FailError](err); ok {
		return failErr.ErrCode, failErr.Cause
	}

	return errCodeStatesTaskFailed, err.Error()
}

// resultFolderKey builds the slash-terminated S3 key prefix holding one Map
// Run's exported result files. Real AWS uses Prefix/<map-run-uuid>/, the
// UUID taken from the "Map:<uuid>" suffix of the MapRunArn. gopherstack's
// MapRunArn instead ends in "/<execName>/<stateName>" (see
// map_runs.go:mapRunARNFor) with no UUID component, so that suffix is used
// as the folder segment instead -- still unique per Map Run and traceable
// back to the same MapRunArn DescribeMapRun returns.
func resultFolderKey(prefix, mapRunARN string) string {
	const marker = ":mapRun:"

	id := "unknown"
	if idx := strings.LastIndex(mapRunARN, marker); idx >= 0 {
		id = mapRunARN[idx+len(marker):]
	}

	if prefix == "" {
		return id + "/"
	}

	return strings.TrimSuffix(prefix, "/") + "/" + id + "/"
}

// writeMapResultFiles writes SUCCEEDED_0.json/FAILED_0.json, each only when
// non-empty -- AWS's own manifest only references files that were actually
// created. Filenames keep the .json extension regardless of OutputType
// (AWS's manifest.json documentation names them unconditionally); only the
// bytes written differ between a JSON array and JSON Lines.
func (e *Executor) writeMapResultFiles(
	ctx context.Context,
	bucket, folder string,
	records []mapItemRecord,
	results []any,
	transformation, outputType string,
) (resultManifestFiles, error) {
	var files resultManifestFiles

	succeeded, succeededResults, failed := partitionRecords(records, results)

	if len(succeeded) > 0 {
		entries := succeededEntries(succeeded, succeededResults, transformation)

		entry, err := e.writeMapResultFile(ctx, bucket, folder, "SUCCEEDED", entries, outputType)
		if err != nil {
			return files, err
		}

		files.Succeeded = []resultManifestFile{entry}
	}

	if len(failed) > 0 {
		entries := make([]any, len(failed))
		for i, r := range failed {
			entries[i] = r
		}

		entry, err := e.writeMapResultFile(ctx, bucket, folder, "FAILED", entries, outputType)
		if err != nil {
			return files, err
		}

		files.Failed = []resultManifestFile{entry}
	}

	return files, nil
}

// partitionRecords splits records (and their parallel raw results) into
// SUCCEEDED and FAILED groups, preserving relative order within each group.
func partitionRecords(
	records []mapItemRecord,
	results []any,
) ([]mapItemRecord, []any, []mapItemRecord) {
	var succ, failed []mapItemRecord

	var succResults []any

	for i, rec := range records {
		if rec.Status == statusFailed {
			failed = append(failed, rec)

			continue
		}

		succ = append(succ, rec)
		succResults = append(succResults, results[i])
	}

	return succ, succResults, failed
}

// succeededEntries formats only-successful items per Transformation: NONE
// keeps the full record, COMPACT returns each child's raw output, FLATTEN
// additionally splices any array output into the result.
func succeededEntries(records []mapItemRecord, results []any, transformation string) []any {
	if transformation == transformationNone {
		entries := make([]any, len(records))
		for i, r := range records {
			entries[i] = r
		}

		return entries
	}

	if transformation == transformationFlatten {
		return flattenValues(results)
	}

	return results
}

func (e *Executor) writeMapResultFile(
	ctx context.Context, bucket, folder, status string, entries []any, outputType string,
) (resultManifestFile, error) {
	var (
		data []byte
		err  error
	)

	if outputType == outputTypeJSONL {
		data, err = encodeJSONLines(entries)
	} else {
		data, err = json.Marshal(entries)
	}

	if err != nil {
		return resultManifestFile{}, fmt.Errorf("marshal %s results: %w", status, err)
	}

	key := fmt.Sprintf("%s%s_%d.json", folder, status, resultWriterFileIndex)
	if putErr := e.s3w.PutObjectBytes(ctx, bucket, key, data); putErr != nil {
		return resultManifestFile{}, fmt.Errorf("write %s results: %w", status, putErr)
	}

	return resultManifestFile{Key: key, Size: len(data)}, nil
}
