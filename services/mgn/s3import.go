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

// Backs StartImport's real SourceServer/Application/Wave creation path
// (exportimport.go's StartImport/scheduleImportLocked call into it). The AWS
// SDK module itself publishes no CSV schema (StartImportInput carries only
// an S3BucketSource -- no format-options field), but AWS's MGN User Guide
// ("Inventory Import parameters", import-parameters.html, fetched directly
// for this pass) DOES document the real parameter table, plus the
// resource-matching rules under "Additional considerations": a resource's
// explicit *:id column looks it up (import fails if not found); otherwise
// its alternative identification (mgn:app:name / mgn:wave:name /
// mgn:server:user-provided-id) is looked up, updating an existing match or
// creating a new resource when none exists.
//
// This pass adds the mgn:app:*/mgn:wave:* columns (implicit Application/Wave
// creation and association) plus mgn:server:id (explicit update-by-id),
// none of which existed before. Every one of these is a real, doc-confirmed
// parameter (import-parameters.html's table), landing on real fields this
// backend's Application/Wave/SourceServer models already have.
//
// mgn:server:hostname/mgn:server:fqdn/mgn:server:aws-instance-id/
// mgn:server:vmware-uuid/mgn:server:vmpath predate this pass and are NOT in
// AWS's published parameter table (confirmed by this pass's own fetch: the
// table has no server identification column besides
// mgn:server:fqdn-for-action-framework, mgn:server:id and
// mgn:server:user-provided-id, despite prose elsewhere stating "Server
// entries must include either the server IP address, or the FQDN"). They
// are left unchanged -- this package's own best-effort extension of the
// mgn:server:* naming convention onto the SDK's real IdentificationHints
// fields, already disclosed as such, and load-bearing across ~20 existing
// test call sites (seedSourceServerViaImport and its callers) that this
// pass's scope (mgn:app:*/mgn:wave:*) does not touch.
//
// mgn:launch:*/mgn:replication:*/mgn:account-id/mgn:region remain
// unimplemented: mgn:launch:*/mgn:replication:* would require adding a
// dozen fields this backend's LaunchConfiguration/ReplicationConfiguration
// types don't have at all (a materially larger feature than this pass's
// scope); mgn:account-id targets a delegated member account import (no
// cross-account import path exists here); mgn:region is a single-region
// backend concept with nothing to select between. mgn:server:platform IS a
// real column but has no corresponding field anywhere on
// types.SourceServer/types.SourceProperties in this SDK version (confirmed
// by direct read) -- accepted (recognized, not treated as "unknown") but
// its value has nowhere honest to land, so it is parsed and discarded, the
// same "accepted input with no wire field" precedent
// StartImportFileEnrichment's Tags already sets in this package.

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
// (see parseImportCSV).
var errImportSourceUnreadable = errors.New("mgn: import source object could not be read")

// errImportCSVEmpty/errImportRowNoIdentification/errImportResourceIDNotFound
// are this file's static sentinel errors (err113: no ad hoc errors.New at
// the call site) backing parseImportCSV/parseImportRow/
// processImportRowLocked's own whole-parse and per-row failure messages.
var (
	errImportCSVEmpty            = errors.New("parse CSV: source object is empty (no header row)")
	errImportRowNoIdentification = errors.New(
		"row identifies no resource (need one of " +
			csvColHostname + ", " + csvColFqdn + ", " + csvColAwsInstanceID + ", " +
			csvColVMWareUUID + ", " + csvColVMPath + ", " + csvColFqdnForActionFramework + ", " +
			csvColServerID + " for a server; " + csvColAppID + " or " + csvColAppName + " for an" +
			" application; " + csvColWaveID + " or " + csvColWaveName + " for a wave)",
	)
	// errImportResourceIDNotFound backs AWS's documented rule ("If a
	// resource's ID is provided, the service will look for this resource
	// in order to update it. If this resource is not found, the import
	// will fail" -- import-parameters.html, Additional considerations
	// #3). Interpreted here as failing that row, not the whole ImportTask,
	// matching every other row-level failure this backend already reports
	// via ListImportErrors rather than aborting the task outright.
	errImportResourceIDNotFound = errors.New("no resource exists with the given id")
)

// importedRow is one successfully-parsed CSV row: up to three optional
// resource references (server, application, wave -- AWS's own docs allow a
// single row to carry properties for any subset of them, see this file's
// doc comment). hasServer/hasApp/hasWave distinguish a row that never
// mentioned a resource from one that mentioned it with an empty value.
type importedRow struct {
	appTags                map[string]string
	sourceProperties       *SourceProperties
	serverTags             map[string]string
	waveTags               map[string]string
	userProvidedID         string
	fqdnForActionFramework string
	appID                  string
	appName                string
	appDescription         string
	serverID               string
	waveID                 string
	waveName               string
	waveDescription        string
	rowNumber              int64
	hasServer              bool
	hasApp                 bool
	hasWave                bool
}

// importCSVResult accumulates every row parseImportCSV actually produced --
// real parsed rows plus real per-row errors, never a fabricated count of
// either.
type importCSVResult struct {
	rows   []importedRow
	errors []*ImportTaskError
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

	result, err := parseImportCSV(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", errImportSourceUnreadable, bucket, key, err)
	}

	return result, nil
}

// importColumn names -- matched case-insensitively with surrounding space
// trimmed against the object's first (header) row. See this file's doc
// comment for which of these are confirmed against AWS's own published
// parameter table versus this package's own extension of its naming
// convention. Every *TagPrefix constant is matched as a prefix; everything
// after it in the header names the tag key (case preserved).
const (
	csvColUserProvidedID         = "mgn:server:user-provided-id"
	csvColServerID               = "mgn:server:id"
	csvColFqdnForActionFramework = "mgn:server:fqdn-for-action-framework"
	csvColPlatform               = "mgn:server:platform"
	csvColHostname               = "mgn:server:hostname"
	csvColFqdn                   = "mgn:server:fqdn"
	csvColAwsInstanceID          = "mgn:server:aws-instance-id"
	csvColVMWareUUID             = "mgn:server:vmware-uuid"
	csvColVMPath                 = "mgn:server:vmpath"
	csvColServerTagPrefix        = "mgn:server:tag:"

	csvColAppID          = "mgn:app:id"
	csvColAppName        = "mgn:app:name"
	csvColAppDescription = "mgn:app:description"
	csvColAppTagPrefix   = "mgn:app:tag:"

	csvColWaveID          = "mgn:wave:id"
	csvColWaveName        = "mgn:wave:name"
	csvColWaveDescription = "mgn:wave:description"
	csvColWaveTagPrefix   = "mgn:wave:tag:"
)

// parseImportCSV parses data as this package's documented CSV schema (see
// this file's doc comment). The first row is always the header
// (StartImport's S3BucketSource input carries no format-options field to
// say otherwise). An empty body fails the whole parse (ImportTask FAILED,
// via errImportSourceUnreadable); past that, a malformed row becomes a real
// ImportTaskError (never silently dropped) while every other row still
// resolves/creates its resources.
func parseImportCSV(data []byte) (*importCSVResult, error) {
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

	idx := indexImportHeader(rows[0])

	res := &importCSVResult{}

	for i, row := range rows[1:] {
		// 1-indexed data row, header excluded -- this package's own
		// convention for ImportErrorData.RowNumber (undocumented by AWS).
		rowNumber := int64(i + 1)

		parsed, rowErr := parseImportRow(row, idx)
		if rowErr != nil {
			res.errors = append(res.errors, &ImportTaskError{
				ErrorType:     ImportErrorTypeValidation,
				ErrorDateTime: nowRFC3339(),
				ErrorData:     &ImportErrorData{RowNumber: rowNumber, RawError: rowErr.Error()},
			})

			continue
		}

		parsed.rowNumber = rowNumber
		res.rows = append(res.rows, parsed)
	}

	return res, nil
}

// importHeaderIndex maps each recognized column to its position in a data
// row, split by which resource kind's tag-prefixed columns it carries.
type importHeaderIndex struct {
	cols          map[string]int
	serverTagCols map[string]int
	appTagCols    map[string]int
	waveTagCols   map[string]int
}

// indexImportHeader maps each header row's fixed columns to their index
// (case-insensitive) and each *:tag:* column to its tag key (case
// preserved after the prefix), split per resource kind.
func indexImportHeader(header []string) importHeaderIndex {
	idx := importHeaderIndex{
		cols:          make(map[string]int, len(header)),
		serverTagCols: make(map[string]int),
		appTagCols:    make(map[string]int),
		waveTagCols:   make(map[string]int),
	}

	for i, h := range header {
		trimmed := strings.TrimSpace(h)
		lower := strings.ToLower(trimmed)

		switch {
		case strings.HasPrefix(lower, csvColServerTagPrefix):
			idx.serverTagCols[trimmed[len(csvColServerTagPrefix):]] = i
		case strings.HasPrefix(lower, csvColAppTagPrefix):
			idx.appTagCols[trimmed[len(csvColAppTagPrefix):]] = i
		case strings.HasPrefix(lower, csvColWaveTagPrefix):
			idx.waveTagCols[trimmed[len(csvColWaveTagPrefix):]] = i
		default:
			idx.cols[lower] = i
		}
	}

	return idx
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

// rowTagMap returns row's non-empty values for tagCols, keyed by tag name.
func rowTagMap(row []string, tagCols map[string]int) map[string]string {
	out := make(map[string]string, len(tagCols))

	for key, idx := range tagCols {
		if idx < len(row) {
			if v := strings.TrimSpace(row[idx]); v != "" {
				out[key] = v
			}
		}
	}

	return out
}

// parseImportRow builds one row's server/application/wave references. A row
// identifies a server via at least one IdentificationHints field, an
// explicit mgn:server:id, or mgn:server:fqdn-for-action-framework (real SDK
// fields -- see this file's doc comment); an application via mgn:app:id or
// mgn:app:name; a wave via mgn:wave:id or mgn:wave:name. AWS's own docs say
// a row "should" also identify any resource whose property it sets
// (import-parameters.html, Additional considerations #2) but do not
// document this as a hard requirement the way row identification itself is
// -- so a property given without identifying its resource is silently
// dropped (parseImportRowApplication/parseImportRowWave never set hasApp/
// hasWave from a property field alone), the same "recognized but nowhere to
// land" precedent this file's own doc comment already applies to
// mgn:server:platform, rather than invalidating a row's otherwise-valid
// server/application/wave references over one orphaned property. A row
// identifying no resource at all still fails (errImportRowNoIdentification).
func parseImportRow(row []string, idx importHeaderIndex) (importedRow, error) {
	r := parseImportRowServer(row, idx)
	parseImportRowApplication(row, idx, &r)
	parseImportRowWave(row, idx, &r)

	if !r.hasServer && !r.hasApp && !r.hasWave {
		return importedRow{}, errImportRowNoIdentification
	}

	return r, nil
}

// parseImportRowServer builds row's server identification/properties.
func parseImportRowServer(row []string, idx importHeaderIndex) importedRow {
	hints := &IdentificationHints{
		Hostname:      colValue(row, idx.cols, csvColHostname),
		Fqdn:          colValue(row, idx.cols, csvColFqdn),
		AwsInstanceID: colValue(row, idx.cols, csvColAwsInstanceID),
		VMWareUUID:    colValue(row, idx.cols, csvColVMWareUUID),
		VMPath:        colValue(row, idx.cols, csvColVMPath),
	}

	r := importedRow{
		serverID:               colValue(row, idx.cols, csvColServerID),
		userProvidedID:         colValue(row, idx.cols, csvColUserProvidedID),
		fqdnForActionFramework: colValue(row, idx.cols, csvColFqdnForActionFramework),
		serverTags:             rowTagMap(row, idx.serverTagCols),
	}

	hasHints := hints.Hostname != "" || hints.Fqdn != "" || hints.AwsInstanceID != "" ||
		hints.VMWareUUID != "" || hints.VMPath != ""
	if hasHints {
		r.sourceProperties = &SourceProperties{IdentificationHints: hints}
	}

	r.hasServer = hasHints || r.serverID != "" || r.fqdnForActionFramework != ""

	return r
}

// parseImportRowApplication fills r's application fields from row.
func parseImportRowApplication(row []string, idx importHeaderIndex, r *importedRow) {
	r.appID = colValue(row, idx.cols, csvColAppID)
	r.appName = colValue(row, idx.cols, csvColAppName)
	r.appDescription = colValue(row, idx.cols, csvColAppDescription)
	r.appTags = rowTagMap(row, idx.appTagCols)
	r.hasApp = r.appID != "" || r.appName != ""
}

// parseImportRowWave fills r's wave fields from row.
func parseImportRowWave(row []string, idx importHeaderIndex, r *importedRow) {
	r.waveID = colValue(row, idx.cols, csvColWaveID)
	r.waveName = colValue(row, idx.cols, csvColWaveName)
	r.waveDescription = colValue(row, idx.cols, csvColWaveDescription)
	r.waveTags = rowTagMap(row, idx.waveTagCols)
	r.hasWave = r.waveID != "" || r.waveName != ""
}

// bumpImportCount returns a countPair reflecting a single created or
// modified resource.
func bumpImportCount(created bool) countPair {
	if created {
		return countPair{CreatedCount: 1}
	}

	return countPair{ModifiedCount: 1}
}

// newImportRowError builds a VALIDATION_ERROR ImportTaskError for row's
// rowNumber, tagging data with whichever resource ID the failing row
// referenced (even when that ID was not found -- surfacing the value that
// caused the problem, the same convention notFoundError uses for every
// other resource in this package).
func newImportRowError(rowNumber int64, err error, data *ImportErrorData) *ImportTaskError {
	data.RowNumber = rowNumber
	data.RawError = err.Error()

	return &ImportTaskError{
		ErrorType:     ImportErrorTypeValidation,
		ErrorDateTime: nowRFC3339(),
		ErrorData:     data,
	}
}

// processImportRowLocked resolves/creates/updates every resource row
// references -- Wave, then Application (attached to that Wave), then
// SourceServer (attached to that Application) -- matching the Wave ->
// Application -> SourceServer grouping hierarchy applications.go documents.
// Each resource's outcome is independent: a wave lookup failure does not
// stop the row's application or server from being processed, only that
// resource's own attachment link is skipped. Callers must hold b.mu.
func (b *InMemoryBackend) processImportRowLocked(
	row importedRow,
) ([]*ImportTaskError, countPair, countPair, countPair) {
	var errs []*ImportTaskError

	var servers, apps, waves countPair

	waveID, waveCreated, waveErr := b.resolveOrCreateWaveLocked(row)

	switch {
	case waveErr != nil:
		errs = append(errs, newImportRowError(row.rowNumber, waveErr, &ImportErrorData{WaveID: row.waveID}))
	case row.hasWave:
		waves = bumpImportCount(waveCreated)
	}

	appID, appCreated, appErr := b.resolveOrCreateApplicationLocked(row, waveID)

	switch {
	case appErr != nil:
		errs = append(errs, newImportRowError(row.rowNumber, appErr, &ImportErrorData{ApplicationID: row.appID}))
	case row.hasApp:
		apps = bumpImportCount(appCreated)
	}

	if row.hasServer {
		serverCreated, serverErr := b.resolveOrCreateServerLocked(row, appID)
		if serverErr != nil {
			errs = append(
				errs,
				newImportRowError(row.rowNumber, serverErr, &ImportErrorData{SourceServerID: row.serverID}),
			)
		} else {
			servers = bumpImportCount(serverCreated)
		}
	}

	return errs, servers, apps, waves
}

// resolveOrCreateWaveLocked resolves row's Wave reference (mgn:wave:id or
// mgn:wave:name -- AWS MGN User Guide, Inventory Import parameters,
// Additional considerations #3-6), creating one when identified only by
// name and no match exists. Returns "" with no error when row carries no
// wave reference at all. Callers must hold b.mu.
func (b *InMemoryBackend) resolveOrCreateWaveLocked(row importedRow) (string, bool, error) {
	if !row.hasWave {
		return "", false, nil
	}

	w, ok, err := b.resolveImportWaveLocked(row)
	if err != nil {
		return "", false, err
	}

	if !ok {
		w = b.createWaveLocked(row.waveName, row.waveDescription, row.waveTags)

		return w.WaveID, true, nil
	}

	applyImportWaveRowLocked(w, row.waveDescription, row.waveTags)

	return w.WaveID, false, nil
}

func (b *InMemoryBackend) resolveImportWaveLocked(row importedRow) (*Wave, bool, error) {
	if row.waveID != "" {
		w, ok := b.resolveWaveLocked(row.waveID)
		if !ok {
			return nil, false, fmt.Errorf("%w: %s", errImportResourceIDNotFound, row.waveID)
		}

		return w, true, nil
	}

	w, ok := b.resolveWaveByNameLocked(row.waveName)

	return w, ok, nil
}

// applyImportWaveRowLocked merges a re-imported row's description/tags onto
// an existing Wave -- the "update" half of considerations #4-5. A blank
// description leaves the existing one untouched (colValue's own "absent ==
// unset" convention, applied consistently here).
func applyImportWaveRowLocked(w *Wave, description string, waveTags map[string]string) {
	if description != "" {
		w.Description = description
	}

	if w.Tags != nil {
		w.Tags.Merge(waveTags)
	}

	w.LastModifiedDateTime = nowRFC3339()
}

// resolveOrCreateApplicationLocked resolves row's Application reference
// (mgn:app:id or mgn:app:name), creating one when identified only by name
// and no match exists, and attaches waveID (from this row's own Wave
// resolution) when non-empty. Returns "" with no error when row carries no
// application reference at all. Callers must hold b.mu.
func (b *InMemoryBackend) resolveOrCreateApplicationLocked(row importedRow, waveID string) (string, bool, error) {
	if !row.hasApp {
		return "", false, nil
	}

	app, ok, err := b.resolveImportApplicationLocked(row)
	if err != nil {
		return "", false, err
	}

	created := false

	if !ok {
		app = b.createApplicationLocked(row.appName, row.appDescription, row.appTags)
		created = true
	} else {
		applyImportApplicationRowLocked(app, row.appDescription, row.appTags)
	}

	if waveID != "" {
		app.WaveID = waveID
	}

	return app.ApplicationID, created, nil
}

func (b *InMemoryBackend) resolveImportApplicationLocked(row importedRow) (*Application, bool, error) {
	if row.appID != "" {
		app, ok := b.resolveApplicationLocked(row.appID)
		if !ok {
			return nil, false, fmt.Errorf("%w: %s", errImportResourceIDNotFound, row.appID)
		}

		return app, true, nil
	}

	app, ok := b.resolveApplicationByNameLocked(row.appName)

	return app, ok, nil
}

// applyImportApplicationRowLocked merges a re-imported row's
// description/tags onto an existing Application -- the "update" half of
// considerations #4-5.
func applyImportApplicationRowLocked(app *Application, description string, appTags map[string]string) {
	if description != "" {
		app.Description = description
	}

	if app.Tags != nil {
		app.Tags.Merge(appTags)
	}

	app.LastModifiedDateTime = nowRFC3339()
}

// resolveOrCreateServerLocked resolves row's SourceServer via mgn:server:id
// (explicit, fails the row if not found) or falls back to the existing
// mgn:server:user-provided-id dedup convention
// (resolveSourceServerByUserProvidedIDLocked), then attaches applicationID
// (from this row's own Application resolution) when non-empty. Callers must
// hold b.mu and row.hasServer must be true.
func (b *InMemoryBackend) resolveOrCreateServerLocked(row importedRow, applicationID string) (bool, error) {
	existing, err := b.resolveImportServerLocked(row)
	if err != nil {
		return false, err
	}

	seed := sourceServerSeed{
		UserProvidedID:         row.userProvidedID,
		FqdnForActionFramework: row.fqdnForActionFramework,
		SourceProperties:       row.sourceProperties,
		ImportTags:             row.serverTags,
		ApplicationID:          applicationID,
	}

	if existing != nil {
		b.applyImportRowLocked(existing, seed)

		return false, nil
	}

	b.createSourceServerLocked(seed)

	return true, nil
}

func (b *InMemoryBackend) resolveImportServerLocked(row importedRow) (*SourceServer, error) {
	if row.serverID != "" {
		s, ok := b.resolveSourceServerLocked(row.serverID)
		if !ok {
			return nil, fmt.Errorf("%w: %s", errImportResourceIDNotFound, row.serverID)
		}

		return s, nil
	}

	s, _ := b.resolveSourceServerByUserProvidedIDLocked(row.userProvidedID)

	return s, nil
}
