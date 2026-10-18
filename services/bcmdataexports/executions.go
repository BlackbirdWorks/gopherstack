package bcmdataexports

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type manifestColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type manifest struct {
	ExportName    string           `json:"exportName"`
	ExportArn     string           `json:"exportArn"`
	ExecutionID   string           `json:"executionId"`
	TableName     string           `json:"tableName"`
	BillingPeriod string           `json:"billingPeriod"`
	Format        string           `json:"format"`
	Compression   string           `json:"compression"`
	Columns       []manifestColumn `json:"columns"`
	DataFileKeys  []string         `json:"dataFileKeys"`
	RowCount      int              `json:"rowCount"`
}

type outcome struct {
	code   string
	reason string
}

func nextStatus(code string) string {
	switch code {
	case statusInitiation:
		return statusQueued
	case statusQueued:
		return statusQuerying
	case statusQuerying:
		return statusDelivering
	default:
		return statusSuccess
	}
}

// ProcessExecutions advances every in-flight execution one lifecycle step; delivery writes the result to S3.
func (b *InMemoryBackend) ProcessExecutions(ctx context.Context) {
	b.mu.RLock("ProcessExecutions")
	now := b.now()

	var pending []Execution

	for _, x := range b.executions.All() {
		if !x.terminal() {
			pending = append(pending, *x)
		}
	}

	objects, costs, account := b.objects, b.costs, b.accountID
	b.mu.RUnlock()

	for i := range pending {
		x := &pending[i]
		next := outcome{code: nextStatus(x.StatusCode)}

		switch x.StatusCode {
		case statusQuerying:
			if _, _, err := runQuery(&x.Export, nil, time.Time{}, time.Time{}, ""); err != nil {
				next = outcome{statusQueryFail, reasonInternal}
			}
		case statusDelivering:
			next = deliver(ctx, objects, costs, x, account, now)
		}

		b.apply(x.Key, x.ExportARN, next, now)
	}
}

func (b *InMemoryBackend) apply(key, exportArn string, o outcome, now time.Time) {
	b.mu.Lock("ProcessExecutions")
	defer b.mu.Unlock()

	x, ok := b.executions.Get(key)
	if !ok {
		return
	}

	x.StatusCode, x.StatusReason, x.LastUpdatedAt = o.code, o.reason, now

	if !x.terminal() {
		return
	}

	x.CompletedAt = now

	e, ok := b.exports.Get(exportArn)
	if !ok {
		return
	}

	if o.code == statusSuccess {
		e.StatusCode, e.StatusReason, e.LastRefreshedAt = exportHealthy, "", now
	} else {
		e.StatusCode, e.StatusReason = exportUnhealthy, o.reason
	}
}

func billingPeriod(now time.Time) (time.Time, time.Time) {
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	return start, start.AddDate(0, 1, 0)
}

// runQuery evaluates the export's query; a zero period skips row materialisation (validation only).
func runQuery(
	e *Export, lines []billingexport.CostLine, billStart, billEnd time.Time, payer string,
) (*result, tableDef, error) {
	q, err := parseQuery(e.Query)
	if err != nil {
		return nil, tableDef{}, err
	}

	t, props, err := resolveQuery(q, e.TableConfigs)
	if err != nil {
		return nil, tableDef{}, err
	}

	cols := t.columnsFor(props)

	var rows [][]any
	if !billStart.IsZero() {
		rows = tableRows(t, cols, props, lines, billStart, billEnd, payer)
	}

	res, err := q.run(cols, rows)

	return res, t, err
}

func deliver(
	ctx context.Context, objects billingexport.ObjectStore, costs billingexport.CostSource,
	x *Execution, account string, now time.Time,
) outcome {
	if objects == nil {
		return outcome{statusDelivFail, reasonInternal}
	}

	e := &x.Export
	if !billingexport.BucketExists(ctx, objects, e.S3Bucket) {
		return outcome{statusDelivFail, reasonPermission}
	}

	billStart, billEnd := billingPeriod(now)

	var lines []billingexport.CostLine
	if costs != nil {
		lines = costs.CostLines(billStart, minTime(billEnd, now.Truncate(dayLen).AddDate(0, 0, 1)))
	}

	res, t, err := runQuery(e, lines, billStart, billEnd, account)
	if err != nil {
		return outcome{statusQueryFail, reasonInternal}
	}

	if err = writeResult(ctx, objects, x, t, res, billStart, now); err != nil {
		logger.Load(ctx).WarnContext(ctx, "bcmdataexports: delivery failed", "export", e.Name, "error", err)

		return outcome{statusDelivFail, reasonPermission}
	}

	return outcome{code: statusSuccess}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}

	return b
}

func writeResult(
	ctx context.Context, objects billingexport.ObjectStore, x *Execution, t tableDef, res *result,
	billStart, now time.Time,
) error {
	e := &x.Export
	format, comp := billingexport.FormatCSV, billingexport.CompressNone

	switch {
	case e.Format == formatParq:
		format = billingexport.FormatParquet
	case e.Compression == compGzip:
		comp = billingexport.CompressGzip
	case e.Compression == compZip:
		comp = billingexport.CompressZip
	}

	tbl := &billingexport.Table{Rows: res.rows}
	man := manifest{
		ExportName:  e.Name,
		ExportArn:   e.ARN,
		ExecutionID: x.ID,
		TableName:   t.Name,
		BillingPeriod: billStart.Format(
			"2006-01",
		),
		Format:      e.Format,
		Compression: e.Compression,
		RowCount:    len(res.rows),
	}

	for _, c := range res.cols {
		tbl.Columns = append(tbl.Columns, billingexport.Column{Name: c.Name, Type: columnTypeParquet(c.Type)})
		man.Columns = append(man.Columns, manifestColumn{Name: c.Name, Type: c.Type})
	}

	body, ext, err := billingexport.Encode(tbl, format, comp, e.Name+"-00001")
	if err != nil {
		return err
	}

	prefix := strings.Trim(e.S3Prefix, "/")
	base := path.Join(prefix, e.Name)
	dataDir := path.Join(base, "data", periodLabel(billStart))

	if e.Overwrite == ovNew {
		dataDir = path.Join(dataDir, now.Format("20060102T150405Z")+"-"+x.ID)
	}

	dataKey := path.Join(dataDir, e.Name+"-00001"+ext)
	if err = billingexport.Put(ctx, objects, e.S3Bucket, dataKey, body, ""); err != nil {
		return fmt.Errorf("put %s: %w", dataKey, err)
	}

	man.DataFileKeys = []string{dataKey}

	mb, err := json.Marshal(&man)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	mKey := path.Join(base, "metadata", periodLabel(billStart), e.Name+"-Manifest.json")
	if err = billingexport.Put(ctx, objects, e.S3Bucket, mKey, mb, "application/json"); err != nil {
		return fmt.Errorf("put %s: %w", mKey, err)
	}

	return nil
}
