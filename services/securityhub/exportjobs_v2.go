package securityhub

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	shtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	maxExportSelectedFields = 50
	maxExportPrefixLen      = 512
	exportJobIDHexLen       = 32
	exportKeyFindingsCSV    = "findings.csv"
	exportKeyFindingsJSON   = "findings.json"
)

// ExportJobV2Input is the validated request of StartExportJobV2.
type ExportJobV2Input struct {
	Filters        map[string]any
	Name           string
	BucketArn      string
	KmsKeyArn      string
	ObjectPrefix   string
	Format         string
	SelectedFields []string
	Scopes         []map[string]string
}

func exportValidationError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidInput, fmt.Sprintf(format, args...))
}

func (b *InMemoryBackend) validateExportInput(in *ExportJobV2Input) error {
	bucket, err := awsarn.Parse(in.BucketArn)
	if err != nil || bucket.Service != "s3" || bucket.Resource == "" || strings.Contains(bucket.Resource, "/") {
		return exportValidationError("Destination.S3.BucketArn must be an S3 bucket ARN")
	}

	key, err := awsarn.Parse(in.KmsKeyArn)
	if err != nil || key.Service != "kms" || !strings.HasPrefix(key.Resource, "key/") ||
		strings.HasPrefix(key.Resource, "key/mrk-") {
		return exportValidationError("Destination.S3.KmsKeyArn must be a full single-Region KMS key ARN")
	}

	if key.Region != b.region {
		return exportValidationError("Destination.S3.KmsKeyArn must be in region %s", b.region)
	}

	if key.AccountID != b.accountID {
		return exportValidationError("Destination.S3.KmsKeyArn must be in account %s", b.accountID)
	}

	if len(in.ObjectPrefix) > maxExportPrefixLen {
		return exportValidationError("Destination.S3.ObjectPrefix must be at most %d characters", maxExportPrefixLen)
	}

	return validateExportOutput(in)
}

func validateExportOutput(in *ExportJobV2Input) error {
	switch in.Format {
	case exportFormatCSV:
		if len(in.SelectedFields) == 0 {
			return exportValidationError("OutputConfiguration.Findings.SelectedFields is required for CSV")
		}
	case exportFormatOCSFJSON:
		if len(in.SelectedFields) > 0 {
			return exportValidationError("OutputConfiguration.Findings.SelectedFields is not supported for OCSF_JSON")
		}
	default:
		return exportValidationError("OutputConfiguration.Findings.Format must be CSV or OCSF_JSON")
	}

	if len(in.SelectedFields) > maxExportSelectedFields {
		return exportValidationError(
			"OutputConfiguration.Findings.SelectedFields allows at most %d fields",
			maxExportSelectedFields,
		)
	}

	known := shtypes.FindingsSelectableField("").Values()
	for _, f := range in.SelectedFields {
		if !slices.Contains(known, shtypes.FindingsSelectableField(f)) {
			return exportValidationError("unsupported SelectedFields entry %q", f)
		}
	}

	return nil
}

// StartExportJobV2 validates the request and runs the export synchronously into the emulated S3.
func (b *InMemoryBackend) StartExportJobV2(in *ExportJobV2Input) (*ExportJobV2, error) {
	if err := b.validateExportInput(in); err != nil {
		return nil, err
	}

	b.mu.Lock("StartExportJobV2")
	defer b.mu.Unlock()

	for _, j := range b.exportJobsV2.All() {
		if j.Status == exportStatusRunning {
			return nil, fmt.Errorf("%w: %s", ErrExportJobRunning, j.ExportJobID)
		}
	}

	job := &ExportJobV2{
		ExportJobID: awsid.NewN("", exportJobIDHexLen), Name: in.Name, DataType: exportDataTypeFindings,
		Status: exportStatusRunning, StartedAt: time.Now().UTC().Format(time.RFC3339),
		BucketArn: in.BucketArn, KmsKeyArn: in.KmsKeyArn, ObjectPrefix: in.ObjectPrefix, Format: in.Format,
		SelectedFields: slices.Clone(in.SelectedFields), Filters: maps.Clone(in.Filters), Scopes: in.Scopes,
	}

	code, msg := b.runExportLocked(context.Background(), job)
	job.EndedAt = time.Now().UTC().Format(time.RFC3339)

	if code != "" {
		job.Status, job.FailureCode, job.FailureMessage = exportStatusFailed, code, msg
	} else {
		job.Status = exportStatusSucceeded
	}

	b.exportJobsV2.Put(job)
	cp := *job

	return &cp, nil
}

// runExportLocked writes the matching findings to the job's bucket; it returns a failure code and message, or "".
func (b *InMemoryBackend) runExportLocked(ctx context.Context, job *ExportJobV2) (string, string) {
	s3, ok := b.s3Backend()
	if !ok {
		return string(shtypes.ExportFailureCodeInternalError), "S3 is not available"
	}

	bucket := job.BucketArn[strings.LastIndex(job.BucketArn, ":")+1:]
	if _, err := s3.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return string(shtypes.ExportFailureCodeResourceNotFound), fmt.Sprintf("bucket %s does not exist", bucket)
	}

	body, name, err := b.renderExportLocked(job)
	if err != nil {
		return string(shtypes.ExportFailureCodeInternalError), err.Error()
	}

	key := path.Join(job.ObjectPrefix, job.ExportJobID, name)
	if _, err = s3.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	}); err != nil {
		return string(shtypes.ExportFailureCodeAccessDenied), err.Error()
	}

	return "", ""
}

func (b *InMemoryBackend) exportFindingsLocked(filters map[string]any) []map[string]any {
	keys := make([]string, 0, len(b.findings))
	for k := range b.findings {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var out []map[string]any

	for _, k := range keys {
		if f := b.findings[k]; matchesFindingFiltersV2(f, filters) {
			row := maps.Clone(f)
			row["metadata"] = map[string]any{"uid": k}
			out = append(out, row)
		}
	}

	return out
}

func (b *InMemoryBackend) renderExportLocked(job *ExportJobV2) ([]byte, string, error) {
	findings := b.exportFindingsLocked(job.Filters)

	if job.Format == exportFormatOCSFJSON {
		var buf bytes.Buffer

		enc := json.NewEncoder(&buf)
		for _, f := range findings {
			if err := enc.Encode(f); err != nil {
				return nil, "", fmt.Errorf("encode finding: %w", err)
			}
		}

		return buf.Bytes(), exportKeyFindingsJSON, nil
	}

	var buf bytes.Buffer

	w := csv.NewWriter(&buf)
	_ = w.Write(job.SelectedFields)

	for _, f := range findings {
		row := make([]string, len(job.SelectedFields))
		for i, field := range job.SelectedFields {
			row[i] = exportCell(f, field)
		}

		_ = w.Write(row)
	}

	w.Flush()

	return buf.Bytes(), exportKeyFindingsCSV, w.Error()
}

// exportCell renders one OCSF field of a finding; fields with no ASFF source render empty.
func exportCell(finding map[string]any, field string) string {
	if vals := ocsfStringValues(finding, field); len(vals) > 0 {
		return strings.Join(vals, ";")
	}

	if key, ok := ocsfDateFieldMap[field]; ok {
		return asFindingString(finding[key])
	}

	if key, ok := ocsfNumberFieldMap[field]; ok {
		if n, isNum := findingNumberValue(finding, key); isNum {
			return strconv.FormatFloat(n, 'f', -1, 64)
		}
	}

	return ""
}

// GetExportJobV2 returns a copy of the export job.
func (b *InMemoryBackend) GetExportJobV2(id string) (*ExportJobV2, error) {
	b.mu.RLock("GetExportJobV2")
	defer b.mu.RUnlock()

	job, ok := b.exportJobsV2.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *job

	return &cp, nil
}

// ListExportJobsV2 lists export jobs newest first, optionally filtered by data type and status.
func (b *InMemoryBackend) ListExportJobsV2(
	dataType, status, nextToken string,
	maxResults int,
) ([]*ExportJobV2, string) {
	b.mu.RLock("ListExportJobsV2")
	defer b.mu.RUnlock()

	var all []*ExportJobV2

	for _, j := range b.exportJobsV2.All() {
		if (dataType == "" || j.DataType == dataType) && (status == "" || j.Status == status) {
			cp := *j
			all = append(all, &cp)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].StartedAt != all[j].StartedAt {
			return all[i].StartedAt > all[j].StartedAt
		}

		return all[i].ExportJobID < all[j].ExportJobID
	})

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

// CancelExportJobV2 cancels a RUNNING job; a CANCELLED job is returned unchanged.
func (b *InMemoryBackend) CancelExportJobV2(id string) (*ExportJobV2, error) {
	b.mu.Lock("CancelExportJobV2")
	defer b.mu.Unlock()

	job, ok := b.exportJobsV2.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	switch job.Status {
	case exportStatusRunning:
		job.Status = exportStatusCancelled
		job.EndedAt = time.Now().UTC().Format(time.RFC3339)
	case exportStatusCancelled:
	default:
		return nil, fmt.Errorf("%w: %s is %s", ErrExportJobFinished, id, job.Status)
	}

	cp := *job

	return &cp, nil
}
