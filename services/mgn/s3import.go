package mgn

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

// This file backs StartImport's real, wire-reachable SourceServer creation
// path (exportimport.go's StartImport/scheduleImportLocked call into it).
// gopherstack-i6oz: the only PUBLIC AWS API that creates a SourceServer is
// StartImport's bulk CSV load, but AWS does not publish that CSV's column
// schema anywhere in this SDK module (types.SourceServer/SourceProperties
// are the wire OUTPUT shape, never an input schema). This package's
// resolution -- an explicit, documented emulator decision, never presented
// as derived AWS behavior -- is the column set below: a header row plus one
// required column ("hostname") and a liberal set of optional columns, each
// mapped onto a real field this backend's SourceServer/SourceProperties
// already models (see models.go). One CPU/Disk/NetworkInterface entry per
// row, not the full multi-value arrays a real per-server inventory tool
// might report -- CSV's one-row-one-record shape does not naturally carry
// repeating groups without a much richer, still-unpublished convention, and
// PARITY.md's own guidance is to pick a defensible simpler shape rather than
// invent one. See PARITY.md's "CSV import schema (this pass)" section.

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

// errImportCSVEmpty/errImportCSVNoHostnameColumn/errImportRowNoHostname are
// this file's static sentinel errors (err113: no ad hoc errors.New at the
// call site) backing parseSourceServerCSV/parseSourceServerRow's own
// whole-parse and per-row failure messages.
var (
	errImportCSVEmpty            = errors.New("parse CSV: source object is empty (no header row)")
	errImportCSVNoHostnameColumn = errors.New(`parse CSV: required column "hostname" not found in header row`)
	errImportRowNoHostname       = errors.New(`required column "hostname" is empty`)
)

// importedSourceServer is one successfully-parsed CSV row, ready to become
// a SourceServer via createSourceServerLocked.
type importedSourceServer struct {
	sourceProperties *SourceProperties
	userProvidedID   string
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

// importCSVColumn names -- matched case-insensitively with surrounding
// space trimmed against the object's first (header) row. "hostname" is the
// only required column; every other column is optional and, if absent or
// blank on a given row, simply leaves the corresponding SourceProperties
// field unset on that row's SourceServer.
const (
	csvColHostname                = "hostname"
	csvColFqdn                    = "fqdn"
	csvColUserProvidedID          = "userprovidedid"
	csvColOperatingSystem         = "operatingsystem"
	csvColRecommendedInstanceType = "recommendedinstancetype"
	csvColCPUCores                = "cpucores"
	csvColCPUModelName            = "cpumodelname"
	csvColRAMBytes                = "rambytes"
	csvColDiskDeviceName          = "diskdevicename"
	csvColDiskBytes               = "diskbytes"
	csvColNetworkInterfaceMac     = "networkinterfacemac"
	csvColNetworkInterfaceIPs     = "networkinterfaceips"
)

// parseSourceServerCSV parses data as this package's documented CSV schema
// (see this file's doc comment and the csvCol* constants above). The first
// row is always the header (StartImport's Input carries no format-options
// field to say otherwise -- confirmed by direct read of S3BucketSource, the
// only input StartImport takes beyond Tags). A missing "hostname" header or
// an empty/unparseable body fails the entire parse (whole-ImportTask
// FAILED, via errImportSourceUnreadable) -- anything past that point is a
// per-row concern: a malformed row is recorded as a real ImportTaskError
// (never silently dropped, never counted as created), while every other row
// still creates its SourceServer.
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

	cols := make(map[string]int, len(rows[0]))
	for i, h := range rows[0] {
		cols[strings.ToLower(strings.TrimSpace(h))] = i
	}

	hostnameIdx, ok := cols[csvColHostname]
	if !ok {
		return nil, errImportCSVNoHostnameColumn
	}

	res := &importCSVResult{}

	for i, row := range rows[1:] {
		// 1-indexed data row, header excluded -- this package's own
		// convention for ImportErrorData.RowNumber (undocumented by AWS).
		rowNumber := int64(i + 1)

		server, rowErr := parseSourceServerRow(row, cols, hostnameIdx)
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

// colValue returns row's trimmed value for the named column, or "" if the
// column is absent from the header or this row is short that many fields
// (a short row is not itself an error -- only an empty *required* column
// is, checked by the row's own caller).
func colValue(row []string, cols map[string]int, name string) string {
	idx, ok := cols[name]
	if !ok || idx >= len(row) {
		return ""
	}

	return strings.TrimSpace(row[idx])
}

// parseOptionalInt64 parses s as an int64, treating "" as 0/absent rather
// than an error -- every numeric CSV column here is optional.
func parseOptionalInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}

	return strconv.ParseInt(s, 10, 64)
}

// parseSourceServerRow builds one row's SourceProperties/UserProvidedID.
// The only required value is a non-empty hostname; every other parse
// failure (a non-numeric optional numeric column) also fails the row, since
// a column present-but-garbled is more likely a real data problem than an
// intentionally blank field.
func parseSourceServerRow(row []string, cols map[string]int, hostnameIdx int) (*importedSourceServer, error) {
	hostname := ""
	if hostnameIdx < len(row) {
		hostname = strings.TrimSpace(row[hostnameIdx])
	}

	if hostname == "" {
		return nil, errImportRowNoHostname
	}

	props := &SourceProperties{
		IdentificationHints:     &IdentificationHints{Hostname: hostname, Fqdn: colValue(row, cols, csvColFqdn)},
		RecommendedInstanceType: colValue(row, cols, csvColRecommendedInstanceType),
	}

	if osStr := colValue(row, cols, csvColOperatingSystem); osStr != "" {
		props.Os = &OS{FullString: osStr}
	}

	var err error

	if props.Cpus, err = parseCSVCPU(row, cols); err != nil {
		return nil, err
	}

	if props.RAMBytes, err = parseOptionalInt64(colValue(row, cols, csvColRAMBytes)); err != nil {
		return nil, fmt.Errorf("%s: %w", csvColRAMBytes, err)
	}

	if props.Disks, err = parseCSVDisk(row, cols); err != nil {
		return nil, err
	}

	props.NetworkInterfaces = parseCSVNetworkInterface(row, cols)

	return &importedSourceServer{
		userProvidedID:   colValue(row, cols, csvColUserProvidedID),
		sourceProperties: props,
	}, nil
}

// parseCSVCPU returns a single-entry Cpus slice from the cpuCores/
// cpuModelName columns, or nil if both are blank on this row.
func parseCSVCPU(row []string, cols map[string]int) ([]CPU, error) {
	cores, model := colValue(row, cols, csvColCPUCores), colValue(row, cols, csvColCPUModelName)
	if cores == "" && model == "" {
		return nil, nil
	}

	n, err := parseOptionalInt64(cores)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", csvColCPUCores, err)
	}

	return []CPU{{ModelName: model, Cores: n}}, nil
}

// parseCSVDisk returns a single-entry Disks slice from the diskBytes/
// diskDeviceName columns, or nil if both are blank on this row.
// DeviceName defaults to "/dev/sda1" (matching createSourceServerLocked's
// own ReplicatedDisks default) if bytes were given without a name.
func parseCSVDisk(row []string, cols map[string]int) ([]Disk, error) {
	bytesStr, device := colValue(row, cols, csvColDiskBytes), colValue(row, cols, csvColDiskDeviceName)
	if bytesStr == "" && device == "" {
		return nil, nil
	}

	n, err := parseOptionalInt64(bytesStr)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", csvColDiskBytes, err)
	}

	if device == "" {
		device = "/dev/sda1"
	}

	return []Disk{{DeviceName: device, Bytes: n}}, nil
}

// parseCSVNetworkInterface returns a single-entry, IsPrimary=true
// NetworkInterfaces slice from the networkInterfaceMac/networkInterfaceIPs
// columns (IPs semicolon-separated, e.g. "10.0.0.5;10.0.0.6"), or nil if no
// MAC was given.
func parseCSVNetworkInterface(row []string, cols map[string]int) []NetworkInterface {
	mac := colValue(row, cols, csvColNetworkInterfaceMac)
	if mac == "" {
		return nil
	}

	var ips []string

	for ip := range strings.SplitSeq(colValue(row, cols, csvColNetworkInterfaceIPs), ";") {
		if ip = strings.TrimSpace(ip); ip != "" {
			ips = append(ips, ip)
		}
	}

	return []NetworkInterface{{MacAddress: mac, Ips: ips, IsPrimary: true}}
}
