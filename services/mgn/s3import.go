package mgn

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Backs StartImport's real SourceServer creation path (exportimport.go's
// StartImport/scheduleImportLocked call into it). The AWS SDK module itself
// publishes no CSV schema (StartImportInput carries only an S3BucketSource --
// no format-options field), but AWS's MGN User Guide ("Importing your data
// inventory", import-main.html) DOES document the parameter set: every column
// is a "mgn:<resource>:<field>" namespaced key. This file implements the
// SourceServer-scoped subset of that documented schema -- the identification
// hints, tag, and user-provided-id parameters -- confirmed by direct read of
// the published parameter table. It does NOT implement the mgn:app:*/
// mgn:wave:*/mgn:launch:* parameters (implicit Application/Wave creation and
// per-row LaunchConfiguration overrides): those are real, doc-confirmed
// parameters too, but acting on them would mean creating/associating
// Applications and Waves and persisting a dozen LaunchConfiguration
// sub-fields (instance profile, per-NIC subnet/security-group/private-IP,
// placement, licensing, volume type) this backend's LaunchConfiguration type
// has no fields for at all -- a materially larger feature than "fix the
// SourceServer schema", left a documented, explicit gap rather than a
// half-finished multi-resource importer (PARITY.md).
//
// mgn:server:hostname/mgn:server:fqdn/mgn:server:aws-instance-id/
// mgn:server:vmware-uuid/mgn:server:vmpath are NOT in AWS's own published
// parameter table -- that table has no explicit IP/FQDN identification
// column at all, despite its own prose stating "Server entries must include
// either the server IP address, or the FQDN." This is a real, if
// incompletely documented, requirement: these five column names are this
// package's best-effort extension of the confirmed "mgn:server:*" naming
// convention onto the SDK's own real IdentificationHints fields (AwsInstanceID/
// Fqdn/Hostname/VmPath/VmWareUuid, types.go), each backed by a real wire
// field, not fabricated. IP-address identification specifically is not
// modeled: IdentificationHints has no IP field on the real SDK type, so
// there is nowhere honest to put one.

// maxImportObjectBytes caps how many bytes StartImport reads from the
// caller's S3 object, matching services/dynamodb's identical import-source
// safety cap (bounds memory use, guards against decompression-less but
// still enormous objects).
const maxImportObjectBytes = 64 * 1024 * 1024

// S3Accessor is the subset of S3 operations StartImport needs to read its
// source object. Satisfied by the in-process S3 backend (services/s3),
// wired in cli.go alongside the DynamoDB/Firehose->S3 wiring -- see
// SetS3Backend.
type S3Accessor interface {
	GetObject(ctx context.Context, in *s3sdk.GetObjectInput) (*s3sdk.GetObjectOutput, error)
}

// SetS3Backend wires the S3 backend StartImport reads its source object
// from. Until this is called, StartImport always fails the ImportTask (no
// backend to honestly read from) rather than fabricating created records --
// see readImportSourceServers.
func (b *InMemoryBackend) SetS3Backend(s3 S3Accessor) {
	b.mu.Lock("SetS3Backend")
	defer b.mu.Unlock()

	b.s3 = s3
}

// s3Backend returns the wired S3 accessor, or nil when none is configured.
func (b *InMemoryBackend) s3Backend() S3Accessor {
	b.mu.RLock("s3Backend")
	defer b.mu.RUnlock()

	return b.s3
}

// errImportSourceUnreadable wraps every reason StartImport's S3 object
// could not be read AT ALL (no backend wired, missing bucket/key, a real
// GetObject failure, or an empty/header-less body) -- these fail the whole
// ImportTask (see exportimport.go's finishImportLocked), distinct from a
// single malformed CSV row, which never fails the task, only that one row
// (see parseSourceServerCSV).
var errImportSourceUnreadable = errors.New("mgn: import source object could not be read")

// errImportCSVEmpty/errImportRowNoIdentification are this file's static
// sentinel errors (err113: no ad hoc errors.New at the call site) backing
// parseSourceServerCSV/parseSourceServerRow's own whole-parse and per-row
// failure messages.
var (
	errImportCSVEmpty            = errors.New("parse CSV: source object is empty (no header row)")
	errImportRowNoIdentification = errors.New(
		"row has no identification hint (one of " +
			csvColFqdnForActionFramework + ", " + csvColHostname + ", " + csvColFqdn + ", " +
			csvColAwsInstanceID + ", " + csvColVMWareUUID + ", " + csvColVMPath + " is required)",
	)
)

// importedSourceServer is one successfully-parsed CSV row, ready to become
// (or update, via UserProvidedID dedup) a SourceServer through
// sourceServerSeed.
type importedSourceServer struct {
	sourceProperties       *SourceProperties
	importTags             map[string]string
	userProvidedID         string
	fqdnForActionFramework string
}

// importCSVResult accumulates every row parseSourceServerCSV actually
// produced -- real parsed servers plus real per-row errors, never a
// fabricated count of either.
type importCSVResult struct {
	servers []importedSourceServer
	errors  []*ImportTaskError
}

// readImportSourceServers fetches source's S3 object and parses it as this
// package's documented CSV schema (see this file's doc comment). A nil
// accessor, a missing bucket/key, or a GetObject/read failure all return an
// error wrapping errImportSourceUnreadable, letting the caller distinguish
// "could not even read the object" (whole-task FAILED) from "read fine,
// some rows were malformed" (importCSVResult.errors, task still SUCCEEDED).
func (b *InMemoryBackend) readImportSourceServers(
	ctx context.Context,
	source *S3BucketSource,
) (*importCSVResult, error) {
	s3 := b.s3Backend()
	if s3 == nil {
		return nil, fmt.Errorf("%w: no S3 backend configured", errImportSourceUnreadable)
	}

	bucket, key := source.S3Bucket, source.S3Key

	out, err := s3.GetObject(ctx, &s3sdk.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", errImportSourceUnreadable, bucket, key, err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxImportObjectBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", errImportSourceUnreadable, bucket, key, err)
	}

	result, err := parseSourceServerCSV(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", errImportSourceUnreadable, bucket, key, err)
	}

	return result, nil
}

// importColumn names -- matched case-insensitively with surrounding space
// trimmed against the object's first (header) row. See this file's doc
// comment for which of these are confirmed against AWS's own published
// parameter table versus this package's own extension of its naming
// convention. csvColTagPrefix is matched as a prefix; everything after it in
// the header names the tag key (case preserved).
const (
	csvColUserProvidedID         = "mgn:server:user-provided-id"
	csvColFqdnForActionFramework = "mgn:server:fqdn-for-action-framework"
	csvColHostname               = "mgn:server:hostname"
	csvColFqdn                   = "mgn:server:fqdn"
	csvColAwsInstanceID          = "mgn:server:aws-instance-id"
	csvColVMWareUUID             = "mgn:server:vmware-uuid"
	csvColVMPath                 = "mgn:server:vmpath"
	csvColTagPrefix              = "mgn:server:tag:"
)

// parseSourceServerCSV parses data as this package's documented CSV schema (see
// this file's doc comment and the csvCol* constants above). The first row is
// always the header (StartImport's S3BucketSource input carries no format-options
// field to say otherwise). An empty body fails the whole parse (ImportTask
// FAILED, via errImportSourceUnreadable); past that, a malformed row becomes a
// real ImportTaskError (never silently dropped) while every other row still
// creates or updates its SourceServer.
func parseSourceServerCSV(data []byte) (*importCSVResult, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse CSV: %w", err)
	}

	if len(rows) == 0 {
		return nil, errImportCSVEmpty
	}

	cols, tagCols := indexHeader(rows[0])

	res := &importCSVResult{}

	for i, row := range rows[1:] {
		// 1-indexed data row, header excluded -- this package's own
		// convention for ImportErrorData.RowNumber (undocumented by AWS).
		rowNumber := int64(i + 1)

		server, rowErr := parseSourceServerRow(row, cols, tagCols)
		if rowErr != nil {
			res.errors = append(res.errors, &ImportTaskError{
				ErrorType:     ImportErrorTypeValidation,
				ErrorDateTime: nowRFC3339(),
				ErrorData:     &ImportErrorData{RowNumber: rowNumber, RawError: rowErr.Error()},
			})

			continue
		}

		res.servers = append(res.servers, *server)
	}

	return res, nil
}

// indexHeader maps each header row's fixed mgn:server:* column to its index
// (case-insensitive) and each mgn:server:tag:* column to its tag key (case
// preserved after the prefix).
func indexHeader(header []string) (map[string]int, map[string]int) {
	cols := make(map[string]int, len(header))
	tagCols := make(map[string]int)

	for i, h := range header {
		trimmed := strings.TrimSpace(h)
		lower := strings.ToLower(trimmed)

		if strings.HasPrefix(lower, csvColTagPrefix) {
			tagCols[trimmed[len(csvColTagPrefix):]] = i

			continue
		}

		cols[lower] = i
	}

	return cols, tagCols
}

// colValue returns row's trimmed value for the named column, or "" if the
// column is absent from the header or this row is short that many fields.
func colValue(row []string, cols map[string]int, name string) string {
	idx, ok := cols[name]
	if !ok || idx >= len(row) {
		return ""
	}

	return strings.TrimSpace(row[idx])
}

// parseSourceServerRow builds one row's SourceProperties/UserProvidedID/tags.
// A row identifies its server via at least one IdentificationHints field
// (real SDK fields -- see this file's doc comment); a row with none of them
// fails (errImportRowNoIdentification).
func parseSourceServerRow(row []string, cols, tagCols map[string]int) (*importedSourceServer, error) {
	hints := &IdentificationHints{
		Hostname:      colValue(row, cols, csvColHostname),
		Fqdn:          colValue(row, cols, csvColFqdn),
		AwsInstanceID: colValue(row, cols, csvColAwsInstanceID),
		VMWareUUID:    colValue(row, cols, csvColVMWareUUID),
		VMPath:        colValue(row, cols, csvColVMPath),
	}
	fqdnForActionFramework := colValue(row, cols, csvColFqdnForActionFramework)

	if hints.Hostname == "" && hints.Fqdn == "" && hints.AwsInstanceID == "" &&
		hints.VMWareUUID == "" && hints.VMPath == "" && fqdnForActionFramework == "" {
		return nil, errImportRowNoIdentification
	}

	rowTags := make(map[string]string, len(tagCols))

	for key, idx := range tagCols {
		if idx < len(row) {
			if v := strings.TrimSpace(row[idx]); v != "" {
				rowTags[key] = v
			}
		}
	}

	return &importedSourceServer{
		userProvidedID:         colValue(row, cols, csvColUserProvidedID),
		fqdnForActionFramework: fqdnForActionFramework,
		sourceProperties:       &SourceProperties{IdentificationHints: hints},
		importTags:             rowTags,
	}, nil
}
