package bcmdataexports

import (
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxExports      = 50
	maxNameLen      = 128
	maxDescLen      = 1024
	maxQueryLen     = 36000
	maxTags         = 50
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxPageSize     = 100
	maxExecutions   = 50
	arnResourcePart = "export/"
)

var nameRe = regexp.MustCompile(`^[0-9A-Za-z\-_]+$`)

func validateExport(w *exportWire) (*Export, error) {
	if w == nil {
		return nil, fail(ErrValidation, "Export is required")
	}

	if w.Name == "" || len(w.Name) > maxNameLen || !nameRe.MatchString(w.Name) {
		return nil, fail(ErrValidation, "Export name %q is invalid", w.Name)
	}

	if len(w.Description) > maxDescLen {
		return nil, fail(ErrValidation, "Description is too long")
	}

	e := &Export{Name: w.Name, Description: w.Description}

	if err := fillDestination(e, w); err != nil {
		return nil, err
	}

	if w.RefreshCadence == nil || w.RefreshCadence.Frequency != freqSync {
		return nil, fail(ErrValidation, "RefreshCadence.Frequency must be %s", freqSync)
	}

	e.Frequency = w.RefreshCadence.Frequency

	if err := fillQuery(e, w); err != nil {
		return nil, err
	}

	return e, nil
}

func fillDestination(e *Export, w *exportWire) error {
	if w.DestinationConfigurations == nil || w.DestinationConfigurations.S3Destination == nil {
		return fail(ErrValidation, "DestinationConfigurations.S3Destination is required")
	}

	d := w.DestinationConfigurations.S3Destination
	if d.S3Bucket == "" || d.S3Region == "" || d.S3OutputConfigurations == nil {
		return fail(ErrValidation, "S3Destination requires S3Bucket, S3Region and S3OutputConfigurations")
	}

	o := d.S3OutputConfigurations
	if o.OutputType != outputCustom {
		return fail(ErrValidation, "OutputType %q is invalid", o.OutputType)
	}

	if !slices.Contains([]string{ovNew, ovOverwrite}, o.Overwrite) {
		return fail(ErrValidation, "Overwrite %q is invalid", o.Overwrite)
	}

	parquet := o.Format == formatParq
	if (o.Format != formatCSV && !parquet) ||
		!slices.Contains([]string{compGzip, compZip, compParquet}, o.Compression) {
		return fail(ErrValidation, "Format %q with Compression %q is invalid", o.Format, o.Compression)
	}

	if parquet != (o.Compression == compParquet) {
		return fail(ErrValidation, "Format %s is incompatible with Compression %s", o.Format, o.Compression)
	}

	e.S3Bucket, e.S3Prefix, e.S3Region, e.S3BucketOwner = d.S3Bucket, d.S3Prefix, d.S3Region, d.S3BucketOwner
	e.OutputType, e.Format, e.Compression, e.Overwrite = o.OutputType, o.Format, o.Compression, o.Overwrite

	return nil
}

func fillQuery(e *Export, w *exportWire) error {
	if w.DataQuery == nil || w.DataQuery.QueryStatement == "" || len(w.DataQuery.QueryStatement) > maxQueryLen {
		return fail(ErrValidation, "DataQuery.QueryStatement is required")
	}

	q, err := parseQuery(w.DataQuery.QueryStatement)
	if err != nil {
		return err
	}

	for name, props := range w.DataQuery.TableConfigurations {
		t, ok := findTable(strings.ToUpper(name))
		if !ok {
			return fail(ErrValidation, "table %q does not exist", name)
		}

		if _, err = t.resolveProps(props); err != nil {
			return err
		}
	}

	if _, _, err = resolveQuery(q, w.DataQuery.TableConfigurations); err != nil {
		return err
	}

	e.Query = w.DataQuery.QueryStatement
	e.TableConfigs = w.DataQuery.TableConfigurations

	return nil
}

// resolveQuery checks the query's table and columns, returning the table definition and resolved properties.
func resolveQuery(q *query, configs map[string]map[string]string) (tableDef, map[string]string, error) {
	t, ok := findTable(q.table)
	if !ok {
		return tableDef{}, nil, fail(ErrValidation, "table %q does not exist", q.table)
	}

	props, err := t.resolveProps(configs[q.table])
	if err != nil {
		return tableDef{}, nil, err
	}

	if _, err = q.run(t.columnsFor(props), nil); err != nil {
		return tableDef{}, nil, err
	}

	return t, props, nil
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

func limitOf(maxResults *int32) (int, error) {
	if maxResults == nil {
		return maxPageSize, nil
	}

	if *maxResults < 1 || int(*maxResults) > maxPageSize {
		return 0, fail(ErrValidation, "MaxResults must be between 1 and %d", maxPageSize)
	}

	return int(*maxResults), nil
}

func tokenOK(token string) error {
	if err := page.ValidateToken(token); err != nil {
		return fail(ErrValidation, "NextToken is invalid")
	}

	return nil
}

func (b *InMemoryBackend) exportLocked(exportArn string) (*Export, error) {
	e, ok := b.exports.Get(exportArn)
	if !ok {
		return nil, fail(ErrResourceNotFound, "export %q not found", exportArn)
	}

	return e, nil
}

func (b *InMemoryBackend) startExecutionLocked(e *Export) {
	id := uuid.NewString()
	now := b.now()
	x := &Execution{
		Key: e.ARN + "|" + id, ID: id, ExportARN: e.ARN, Export: cloneExport(e),
		StatusCode: statusInitiation, CreatedAt: now, LastUpdatedAt: now,
	}
	b.executions.Put(x)

	var mine []*Execution

	for _, o := range b.executions.All() {
		if o.ExportARN == e.ARN {
			mine = append(mine, o)
		}
	}

	if len(mine) <= maxExecutions {
		return
	}

	slices.SortFunc(mine, func(a, c *Execution) int { return a.CreatedAt.Compare(c.CreatedAt) })

	for _, o := range mine[:len(mine)-maxExecutions] {
		if o.terminal() {
			b.executions.Delete(o.Key)
		}
	}
}

// CreateExport creates an export and queues its first execution.
func (b *InMemoryBackend) CreateExport(w *exportWire, tagList []tags.KV) (string, error) {
	e, err := validateExport(w)
	if err != nil {
		return "", err
	}

	tm, err := validateTags(tagList)
	if err != nil {
		return "", err
	}

	b.mu.Lock("CreateExport")
	defer b.mu.Unlock()

	for _, o := range b.exports.All() {
		if o.Name == e.Name {
			return "", fail(ErrValidation, "an export named %q already exists", e.Name)
		}
	}

	if b.exports.Len() >= maxExports {
		return "", fail(ErrServiceQuotaExceeded, "an account can have at most %d exports", maxExports)
	}

	now := b.now()
	e.ARN = arn.Build("bcm-data-exports", b.region, b.accountID, arnResourcePart+e.Name+"-"+uuid.NewString())
	e.Tags = tm
	e.CreatedAt, e.LastUpdatedAt, e.LastRefreshedAt = now, now, now
	e.StatusCode = exportHealthy
	b.exports.Put(e)
	b.startExecutionLocked(e)

	return e.ARN, nil
}

// UpdateExport replaces an export definition and queues a new execution.
func (b *InMemoryBackend) UpdateExport(exportArn string, w *exportWire) (string, error) {
	n, err := validateExport(w)
	if err != nil {
		return "", err
	}

	b.mu.Lock("UpdateExport")
	defer b.mu.Unlock()

	old, err := b.exportLocked(exportArn)
	if err != nil {
		return "", err
	}

	if n.Name != old.Name {
		return "", fail(ErrValidation, "an export cannot be renamed")
	}

	n.ARN, n.Tags, n.CreatedAt = old.ARN, old.Tags, old.CreatedAt
	n.LastRefreshedAt, n.StatusCode, n.StatusReason = old.LastRefreshedAt, old.StatusCode, old.StatusReason
	n.LastUpdatedAt = b.now()
	b.exports.Put(n)
	b.startExecutionLocked(n)

	return n.ARN, nil
}

// DeleteExport removes an export and its executions.
func (b *InMemoryBackend) DeleteExport(exportArn string) (string, error) {
	b.mu.Lock("DeleteExport")
	defer b.mu.Unlock()

	if !b.exports.Delete(exportArn) {
		return "", fail(ErrResourceNotFound, "export %q not found", exportArn)
	}

	for _, x := range b.executions.All() {
		if x.ExportARN == exportArn {
			b.executions.Delete(x.Key)
		}
	}

	return exportArn, nil
}

// GetExport returns an export and its status.
func (b *InMemoryBackend) GetExport(exportArn string) (exportWire, exportStatusWire, error) {
	b.mu.RLock("GetExport")
	defer b.mu.RUnlock()

	e, err := b.exportLocked(exportArn)
	if err != nil {
		return exportWire{}, exportStatusWire{}, err
	}

	return exportToWire(e), exportStatusToWire(e), nil
}

// ListExports lists exports ordered by name.
func (b *InMemoryBackend) ListExports(token string, maxResults *int32) (page.Page[exportRefWire], error) {
	limit, err := limitOf(maxResults)
	if err != nil {
		return page.Page[exportRefWire]{}, err
	}

	if err = tokenOK(token); err != nil {
		return page.Page[exportRefWire]{}, err
	}

	b.mu.RLock("ListExports")
	defer b.mu.RUnlock()

	all := b.exports.All()
	slices.SortFunc(all, func(x, y *Export) int { return strings.Compare(x.Name, y.Name) })

	refs := make([]exportRefWire, len(all))
	for i, e := range all {
		refs[i] = exportRefWire{ExportArn: e.ARN, ExportName: e.Name, ExportStatus: exportStatusToWire(e)}
	}

	return page.New(refs, token, limit, maxPageSize), nil
}

// ListExecutions lists an export's executions, newest first.
func (b *InMemoryBackend) ListExecutions(exportArn, token string, maxResults *int32) (page.Page[execRefWire], error) {
	limit, err := limitOf(maxResults)
	if err != nil {
		return page.Page[execRefWire]{}, err
	}

	if err = tokenOK(token); err != nil {
		return page.Page[execRefWire]{}, err
	}

	b.mu.RLock("ListExecutions")
	defer b.mu.RUnlock()

	if _, err = b.exportLocked(exportArn); err != nil {
		return page.Page[execRefWire]{}, err
	}

	var mine []*Execution

	for _, x := range b.executions.All() {
		if x.ExportARN == exportArn {
			mine = append(mine, x)
		}
	}

	slices.SortFunc(mine, func(a, c *Execution) int {
		if d := c.CreatedAt.Compare(a.CreatedAt); d != 0 {
			return d
		}

		return strings.Compare(a.ID, c.ID)
	})

	refs := make([]execRefWire, len(mine))
	for i, x := range mine {
		refs[i] = execRefWire{ExecutionID: x.ID, ExecutionStatus: execStatusToWire(x)}
	}

	return page.New(refs, token, limit, maxPageSize), nil
}

// GetExecution returns one execution and the definition it ran with.
func (b *InMemoryBackend) GetExecution(exportArn, id string) (getExecResp, error) {
	b.mu.RLock("GetExecution")
	defer b.mu.RUnlock()

	x, ok := b.executions.Get(exportArn + "|" + id)
	if !ok {
		return getExecResp{}, fail(ErrResourceNotFound, "execution %q not found", id)
	}

	return getExecResp{ExecutionID: x.ID, ExecutionStatus: execStatusToWire(x), Export: exportToWire(&x.Export)}, nil
}

// TagResource adds or replaces tags on an export.
func (b *InMemoryBackend) TagResource(resourceArn string, list []tags.KV) error {
	add, err := validateTags(list)
	if err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	e, err := b.exportLocked(resourceArn)
	if err != nil {
		return err
	}

	merged := maps.Clone(e.Tags)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return fail(ErrValidation, "a resource can have at most %d tags", maxTags)
	}

	e.Tags = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	e, err := b.exportLocked(resourceArn)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(e.Tags, k)
	}

	return nil
}

// ListTagsForResource returns an export's tags.
func (b *InMemoryBackend) ListTagsForResource(
	resourceArn, token string,
	maxResults *int32,
) (page.Page[tags.KV], error) {
	limit, err := limitOf(maxResults)
	if err != nil {
		return page.Page[tags.KV]{}, err
	}

	if err = tokenOK(token); err != nil {
		return page.Page[tags.KV]{}, err
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	e, err := b.exportLocked(resourceArn)
	if err != nil {
		return page.Page[tags.KV]{}, err
	}

	return page.New(tags.MapToKV(e.Tags), token, limit, maxPageSize), nil
}

// GetTable returns a table schema under the given properties.
func GetTable(name string, props map[string]string) (getTableResp, error) {
	t, ok := findTable(name)
	if !ok {
		return getTableResp{}, fail(ErrResourceNotFound, "table %q not found", name)
	}

	resolved, err := t.resolveProps(props)
	if err != nil {
		return getTableResp{}, err
	}

	schema := make([]columnWire, 0, len(t.Columns))
	for _, c := range t.columnsFor(resolved) {
		schema = append(schema, columnWire{Name: c.Name, Type: c.Type, Description: c.Description})
	}

	return getTableResp{TableName: t.Name, Description: t.Description, Schema: schema, TableProperties: resolved}, nil
}

// ListTables lists every exportable table.
func ListTables(token string, maxResults *int32) (page.Page[tableWire], error) {
	limit, err := limitOf(maxResults)
	if err != nil {
		return page.Page[tableWire]{}, err
	}

	if err = tokenOK(token); err != nil {
		return page.Page[tableWire]{}, err
	}

	defs := tableDefs()
	out := make([]tableWire, len(defs))

	for i, t := range defs {
		props := make([]tablePropWire, len(t.Properties))
		for j, p := range t.Properties {
			props[j] = tablePropWire{
				Name:         p.Name,
				Description:  p.Description,
				DefaultValue: p.Default,
				ValidValues:  p.Valid,
			}
		}

		out[i] = tableWire{TableName: t.Name, Description: t.Description, TableProperties: props}
	}

	return page.New(out, token, limit, maxPageSize), nil
}
