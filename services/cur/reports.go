package cur

import (
	"context"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxReports      = 10
	maxNameLen      = 256
	maxPageSize     = 5
	maxTags         = 50
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxBucketLen    = 256
	maxPrefixLen    = 256
	defaultPageSize = maxPageSize
)

var (
	reportNameRe = regexp.MustCompile(`^[0-9A-Za-z!\-_.*'()]+$`)
	prefixRe     = regexp.MustCompile(`^[0-9A-Za-z!\-_.*'()/]*$`)
)

func regionList() []string {
	return []string{
		"af-south-1", "ap-east-1", "ap-south-1", "ap-south-2", "ap-southeast-1", "ap-southeast-2",
		"ap-southeast-3", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ca-central-1",
		"eu-central-1", "eu-central-2", "eu-west-1", "eu-west-2", "eu-west-3", "eu-north-1",
		"eu-south-1", "eu-south-2", "me-central-1", "me-south-1", "sa-east-1", "us-east-1",
		"us-east-2", "us-west-1", "us-west-2", "cn-north-1", "cn-northwest-1",
	}
}

func oneOf(v string, set ...string) bool { return slices.Contains(set, v) }

func validate(w *reportDefinitionWire) (*Report, error) {
	if w == nil {
		return nil, fail(ErrValidation, "ReportDefinition is required")
	}

	if len(w.ReportName) == 0 || len(w.ReportName) > maxNameLen || !reportNameRe.MatchString(w.ReportName) {
		return nil, fail(ErrValidation, "ReportName %q is invalid", w.ReportName)
	}

	if !oneOf(w.TimeUnit, timeHourly, timeDaily, timeMonthly) {
		return nil, fail(ErrValidation, "TimeUnit %q is invalid", w.TimeUnit)
	}

	if !oneOf(w.Format, formatCSV, formatParquet) {
		return nil, fail(ErrValidation, "Format %q is invalid", w.Format)
	}

	if !oneOf(w.Compression, compGzip, compZip, compParquet) {
		return nil, fail(ErrValidation, "Compression %q is invalid", w.Compression)
	}

	if (w.Format == formatParquet) != (w.Compression == compParquet) {
		return nil, fail(ErrValidation, "Format %s is incompatible with Compression %s", w.Format, w.Compression)
	}

	if err := validateLocation(w); err != nil {
		return nil, err
	}

	if err := validateElements(w); err != nil {
		return nil, err
	}

	r := &Report{
		ReportName:               w.ReportName,
		TimeUnit:                 w.TimeUnit,
		Format:                   w.Format,
		Compression:              w.Compression,
		S3Bucket:                 w.S3Bucket,
		S3Prefix:                 w.S3Prefix,
		S3Region:                 w.S3Region,
		BillingViewArn:           w.BillingViewArn,
		ReportVersioning:         w.ReportVersioning,
		AdditionalSchemaElements: slices.Clone(w.AdditionalSchemaElements),
		AdditionalArtifacts:      slices.Clone(w.AdditionalArtifacts),
		RefreshClosedReports:     true,
	}

	if w.RefreshClosedReports != nil {
		r.RefreshClosedReports = *w.RefreshClosedReports
	}

	if r.ReportVersioning == "" {
		r.ReportVersioning = versionNew
	}

	return r, validateArtifacts(r)
}

func validateLocation(w *reportDefinitionWire) error {
	if w.S3Bucket == "" || len(w.S3Bucket) > maxBucketLen {
		return fail(ErrValidation, "S3Bucket is invalid")
	}

	if len(w.S3Prefix) > maxPrefixLen || !prefixRe.MatchString(w.S3Prefix) {
		return fail(ErrValidation, "S3Prefix %q is invalid", w.S3Prefix)
	}

	if !slices.Contains(regionList(), w.S3Region) {
		return fail(ErrValidation, "S3Region %q is invalid", w.S3Region)
	}

	if w.BillingViewArn != "" && !strings.HasPrefix(w.BillingViewArn, "arn:") {
		return fail(ErrValidation, "BillingViewArn %q is invalid", w.BillingViewArn)
	}

	if w.ReportVersioning != "" && !oneOf(w.ReportVersioning, versionNew, versionOverwrite) {
		return fail(ErrValidation, "ReportVersioning %q is invalid", w.ReportVersioning)
	}

	return nil
}

func validateElements(w *reportDefinitionWire) error {
	for _, e := range w.AdditionalSchemaElements {
		if !oneOf(e, schemaResources, schemaSplit, schemaDiscount) {
			return fail(ErrValidation, "AdditionalSchemaElements value %q is invalid", e)
		}
	}

	seen := map[string]bool{}

	for _, a := range w.AdditionalArtifacts {
		if !oneOf(a, artifactRedshift, artifactQuicksight, artifactAthena) {
			return fail(ErrValidation, "AdditionalArtifacts value %q is invalid", a)
		}

		if seen[a] {
			return fail(ErrValidation, "AdditionalArtifacts value %q is repeated", a)
		}

		seen[a] = true
	}

	return nil
}

func validateArtifacts(r *Report) error {
	for _, a := range r.AdditionalArtifacts {
		switch a {
		case artifactAthena:
			if len(r.AdditionalArtifacts) > 1 {
				return fail(ErrValidation, "ATHENA cannot be combined with other additional artifacts")
			}

			if r.Format != formatParquet {
				return fail(ErrValidation, "ATHENA requires Format Parquet and Compression Parquet")
			}

			if r.ReportVersioning != versionOverwrite {
				return fail(ErrValidation, "ATHENA requires ReportVersioning OVERWRITE_REPORT")
			}
		default:
			if r.Format != formatCSV || r.Compression != compGzip {
				return fail(ErrValidation, "%s requires Format textORcsv and Compression GZIP", a)
			}
		}
	}

	return nil
}

func validateTags(list []tags.KV) (map[string]string, error) {
	if len(list) > maxTags {
		return nil, fail(ErrValidation, "at most %d tags are allowed", maxTags)
	}

	out := make(map[string]string, len(list))

	for _, t := range list {
		if t.Key == "" || len(t.Key) > maxTagKeyLen || len(t.Value) > maxTagValueLen {
			return nil, fail(ErrValidation, "tag %q is invalid", t.Key)
		}

		out[t.Key] = t.Value
	}

	return out, nil
}

func (b *InMemoryBackend) checkBucketLocked(ctx context.Context, bucket string) error {
	if b.objects != nil && !billingexport.BucketExists(ctx, b.objects, bucket) {
		return fail(ErrValidation, "S3 bucket %q does not exist or is not accessible", bucket)
	}

	return nil
}

// PutReportDefinition creates a report definition.
func (b *InMemoryBackend) PutReportDefinition(ctx context.Context, w *reportDefinitionWire, tagList []tags.KV) error {
	r, err := validate(w)
	if err != nil {
		return err
	}

	tm, err := validateTags(tagList)
	if err != nil {
		return err
	}

	b.mu.Lock("PutReportDefinition")
	defer b.mu.Unlock()

	if b.reports.Has(r.ReportName) {
		return fail(ErrDuplicateReportName, "report %q already exists", r.ReportName)
	}

	if b.reports.Len() >= maxReports {
		return fail(ErrReportLimitReached, "an account can have at most %d reports", maxReports)
	}

	if err = b.checkBucketLocked(ctx, r.S3Bucket); err != nil {
		return err
	}

	r.Tags = tm
	r.CreatedAt = b.now()
	b.reports.Put(r)

	return nil
}

// ModifyReportDefinition replaces a report definition, keeping tags and delivery status.
func (b *InMemoryBackend) ModifyReportDefinition(ctx context.Context, name string, w *reportDefinitionWire) error {
	r, err := validate(w)
	if err != nil {
		return err
	}

	if r.ReportName != name {
		return fail(ErrValidation, "ReportDefinition.ReportName must match ReportName")
	}

	b.mu.Lock("ModifyReportDefinition")
	defer b.mu.Unlock()

	old, ok := b.reports.Get(name)
	if !ok {
		return fail(ErrResourceNotFound, "report %q not found", name)
	}

	if err = b.checkBucketLocked(ctx, r.S3Bucket); err != nil {
		return err
	}

	r.Tags = old.Tags
	r.CreatedAt = old.CreatedAt
	r.LastDelivery, r.LastStatus = old.LastDelivery, old.LastStatus
	b.reports.Put(r)

	return nil
}

// DescribeReportDefinitions lists definitions ordered by name.
func (b *InMemoryBackend) DescribeReportDefinitions(
	token string,
	maxResults *int32,
) (page.Page[reportDefinitionWire], error) {
	limit := defaultPageSize

	if maxResults != nil {
		if *maxResults < 1 || int(*maxResults) > maxPageSize {
			return page.Page[reportDefinitionWire]{}, fail(
				ErrValidation,
				"MaxResults must be between 1 and %d",
				maxPageSize,
			)
		}

		limit = int(*maxResults)
	}

	if err := page.ValidateToken(token); err != nil {
		return page.Page[reportDefinitionWire]{}, fail(ErrValidation, "NextToken is invalid")
	}

	b.mu.RLock("DescribeReportDefinitions")
	defer b.mu.RUnlock()

	all := b.reports.All()
	slices.SortFunc(all, func(x, y *Report) int { return strings.Compare(x.ReportName, y.ReportName) })

	wires := make([]reportDefinitionWire, len(all))
	for i, r := range all {
		wires[i] = toWire(r)
	}

	return page.New(wires, token, limit, defaultPageSize), nil
}

// DeleteReportDefinition removes a report definition.
func (b *InMemoryBackend) DeleteReportDefinition(name string) error {
	b.mu.Lock("DeleteReportDefinition")
	defer b.mu.Unlock()

	if !b.reports.Delete(name) {
		return fail(ErrResourceNotFound, "report %q not found", name)
	}

	return nil
}

func (b *InMemoryBackend) reportTags(name string) (*Report, error) {
	r, ok := b.reports.Get(name)
	if !ok {
		return nil, fail(ErrResourceNotFound, "report %q not found", name)
	}

	return r, nil
}

// TagResource adds or replaces tags on a report.
func (b *InMemoryBackend) TagResource(name string, list []tags.KV) error {
	add, err := validateTags(list)
	if err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	r, err := b.reportTags(name)
	if err != nil {
		return err
	}

	merged := maps.Clone(r.Tags)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return fail(ErrValidation, "a report can have at most %d tags", maxTags)
	}

	r.Tags = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(name string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	r, err := b.reportTags(name)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(r.Tags, k)
	}

	return nil
}

// ListTagsForResource returns a report's tags.
func (b *InMemoryBackend) ListTagsForResource(name string) ([]tags.KV, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	r, err := b.reportTags(name)
	if err != nil {
		return nil, err
	}

	return tags.MapToKV(r.Tags), nil
}
