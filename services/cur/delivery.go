package cur

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	assemblyIDLen = 32
	dayLen        = 24 * time.Hour
)

type manifestColumn struct {
	Category string `json:"category"`
	Name     string `json:"name"`
}

type manifestPeriod struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type artifactKey struct {
	ArtifactType string `json:"artifactType"`
	ArtifactPath string `json:"artifactPath"`
}

type manifest struct {
	AssemblyID             string           `json:"assemblyId"`
	Account                string           `json:"account"`
	Charset                string           `json:"charset"`
	Compression            string           `json:"compression"`
	ContentType            string           `json:"contentType"`
	ReportID               string           `json:"reportId"`
	ReportName             string           `json:"reportName"`
	Bucket                 string           `json:"bucket"`
	Columns                []manifestColumn `json:"columns"`
	BillingPeriod          manifestPeriod   `json:"billingPeriod"`
	ReportKeys             []string         `json:"reportKeys"`
	AdditionalArtifactKeys []artifactKey    `json:"additionalArtifactKeys"`
}

// slotOf identifies the delivery slot a report is in at t; a report is due when its last slot differs.
func slotOf(unit string, t time.Time) string {
	switch unit {
	case timeHourly:
		return t.Format("2006010215")
	case timeMonthly:
		return t.Format("200601")
	default:
		return t.Format("20060102")
	}
}

// DeliverDue delivers every report that has not yet been delivered in its current HOURLY, DAILY or MONTHLY slot.
func (b *InMemoryBackend) DeliverDue(ctx context.Context) {
	b.mu.RLock("DeliverDue")
	now := b.now()

	var due []Report

	for _, r := range b.reports.All() {
		if r.LastSlot != slotOf(r.TimeUnit, now) {
			due = append(due, *r)
		}
	}

	objects, costs, account := b.objects, b.costs, b.accountID
	b.mu.RUnlock()

	if objects == nil {
		return
	}

	slices.SortFunc(due, func(x, y Report) int { return strings.Compare(x.ReportName, y.ReportName) })

	for i := range due {
		status := deliver(ctx, objects, costs, &due[i], account, now)

		b.mu.Lock("DeliverDue")
		if cur, ok := b.reports.Get(due[i].ReportName); ok {
			cur.LastSlot = slotOf(cur.TimeUnit, now)
			cur.LastStatus = status
			cur.LastDelivery = now.Format(time.RFC3339)
		}
		b.mu.Unlock()
	}
}

func deliver(
	ctx context.Context, objects billingexport.ObjectStore, costs billingexport.CostSource,
	r *Report, account string, now time.Time,
) string {
	if !billingexport.BucketExists(ctx, objects, r.S3Bucket) {
		return statusErrNoBucket
	}

	billStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	billEnd := billStart.AddDate(0, 1, 0)

	var lines []billingexport.CostLine
	if costs != nil {
		lines = costs.CostLines(billStart, minTime(billEnd, now.Truncate(dayLen).AddDate(0, 0, 1)))
	}

	if err := writeReport(
		ctx,
		objects,
		r,
		buildTable(r, lines, billStart, billEnd, account),
		billStart,
		billEnd,
		account,
	); err != nil {
		logger.Load(ctx).WarnContext(ctx, "cur: report delivery failed", "report", r.ReportName, "error", err)

		return statusErrPermission
	}

	return statusSuccess
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}

	return b
}

func writeReport(
	ctx context.Context, objects billingexport.ObjectStore, r *Report, t *billingexport.Table,
	billStart, billEnd time.Time, account string,
) error {
	format, comp := billingexport.FormatCSV, billingexport.CompressNone

	switch {
	case r.Format == formatParquet:
		format = billingexport.FormatParquet
	case r.Compression == compGzip:
		comp = billingexport.CompressGzip
	case r.Compression == compZip:
		comp = billingexport.CompressZip
	}

	body, ext, err := billingexport.Encode(t, format, comp, r.ReportName+"-00001")
	if err != nil {
		return err
	}

	prefix := strings.Trim(r.S3Prefix, "/")
	period := billStart.Format("20060102") + "-" + billEnd.Format("20060102")
	periodDir := path.Join(prefix, r.ReportName, period)
	dataDir := periodDir
	assembly := awsid.NewN("", assemblyIDLen)
	athena := slices.Contains(r.AdditionalArtifacts, artifactAthena)

	switch {
	case athena:
		dataDir = path.Join(prefix, r.ReportName, r.ReportName,
			"year="+billStart.Format("2006"), fmt.Sprintf("month=%d", int(billStart.Month())))
	case r.ReportVersioning != versionOverwrite:
		dataDir = path.Join(periodDir, assembly)
	}

	dataKey := path.Join(dataDir, r.ReportName+"-00001"+ext)
	if err = billingexport.Put(ctx, objects, r.S3Bucket, dataKey, body, ""); err != nil {
		return fmt.Errorf("put %s: %w", dataKey, err)
	}

	extras, err := writeArtifacts(ctx, objects, r, t, periodDir, dataDir)
	if err != nil {
		return err
	}

	m := newManifest(r, t, account, assembly, dataKey, billStart, billEnd)
	m.AdditionalArtifactKeys = extras

	return writeManifests(ctx, objects, r, &m, periodDir, dataDir, athena)
}

func newManifest(
	r *Report, t *billingexport.Table, account, assembly, dataKey string, billStart, billEnd time.Time,
) manifest {
	m := manifest{
		AssemblyID: assembly, Account: account, Charset: "UTF-8", Compression: r.Compression,
		ContentType: contentTypeOf(r), ReportID: assembly, ReportName: r.ReportName, Bucket: r.S3Bucket,
		BillingPeriod: manifestPeriod{Start: billStart.Format(manifestTime), End: billEnd.Format(manifestTime)},
		ReportKeys:    []string{dataKey}, AdditionalArtifactKeys: []artifactKey{},
	}

	for _, c := range t.Columns {
		cat, name, _ := strings.Cut(c.Name, "/")
		if name == "" {
			cat, name = "", c.Name
		}

		m.Columns = append(m.Columns, manifestColumn{Category: cat, Name: name})
	}

	return m
}

func writeManifests(
	ctx context.Context,
	objects billingexport.ObjectStore,
	r *Report,
	m *manifest,
	periodDir, dataDir string,
	athena bool,
) error {
	mb, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	keys := []string{path.Join(periodDir, r.ReportName+"-Manifest.json")}
	if r.ReportVersioning != versionOverwrite && !athena {
		keys = append(keys, path.Join(dataDir, r.ReportName+"-Manifest.json"))
	}

	for _, k := range keys {
		if err = billingexport.Put(ctx, objects, r.S3Bucket, k, mb, "application/json"); err != nil {
			return fmt.Errorf("put %s: %w", k, err)
		}
	}

	return nil
}

func contentTypeOf(r *Report) string {
	if r.Format == formatParquet {
		return "application/octet-stream"
	}

	return "text/csv"
}

func writeArtifacts(
	ctx context.Context,
	objects billingexport.ObjectStore,
	r *Report,
	t *billingexport.Table,
	periodDir, dataDir string,
) ([]artifactKey, error) {
	var out []artifactKey

	put := func(typ, key string, body []byte) error {
		if err := billingexport.Put(ctx, objects, r.S3Bucket, key, body, ""); err != nil {
			return fmt.Errorf("put %s: %w", key, err)
		}

		out = append(out, artifactKey{ArtifactType: typ, ArtifactPath: key})

		return nil
	}

	dataURI := "s3://" + r.S3Bucket + "/" + dataDir + "/"

	for _, a := range r.AdditionalArtifacts {
		var err error

		switch a {
		case artifactRedshift:
			if err = put("Redshift", path.Join(periodDir, r.ReportName+"-RedshiftManifest.json"),
				redshiftManifest(dataURI)); err == nil {
				err = put("Redshift", path.Join(periodDir, r.ReportName+"-RedshiftCommands.sql"),
					redshiftCommands(r, t, dataURI))
			}
		case artifactQuicksight:
			err = put("Quicksight", path.Join(periodDir, r.ReportName+"-QuickSightManifest.json"),
				quicksightManifest(dataURI))
		case artifactAthena:
			err = put("Athena", path.Join(periodDir, r.ReportName+"-crawler-cfn.yml"), athenaTemplate(r, dataURI))
		}

		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func redshiftManifest(dataURI string) []byte {
	b, _ := json.Marshal(map[string]any{
		"fileLocations": []map[string]any{{"URIPrefixes": []string{dataURI}}},
		"globalUploadSettings": map[string]string{
			"format": "CSV", "delimiter": ",", "textqualifier": "\"", "containsHeader": "true",
		},
	})

	return b
}

func quicksightManifest(dataURI string) []byte {
	b, _ := json.Marshal(map[string]any{
		"fileLocations": []map[string]any{{"URIPrefixes": []string{dataURI}}},
		"globalUploadSettings": map[string]string{
			"format": "CSV", "delimiter": ",", "textqualifier": "\"", "containsHeader": "true",
		},
	})

	return b
}

func redshiftCommands(r *Report, t *billingexport.Table, dataURI string) []byte {
	var sb strings.Builder

	table := strings.ReplaceAll(strings.ToLower(r.ReportName), "-", "_")
	fmt.Fprintf(&sb, "create table %s (\n", table)

	for i, c := range t.Columns {
		typ := "varchar(256)"
		if c.Type == typeDouble {
			typ = "double precision"
		}

		sep := ","
		if i == len(t.Columns)-1 {
			sep = ""
		}

		fmt.Fprintf(&sb, "  \"%s\" %s%s\n", strings.ReplaceAll(c.Name, "/", "_"), typ, sep)
	}

	fmt.Fprintf(
		&sb,
		");\n\ncopy %s from '%s' credentials 'aws_iam_role=<role>' gzip csv ignoreheader 1;\n",
		table,
		dataURI,
	)

	return []byte(sb.String())
}

func athenaTemplate(r *Report, dataURI string) []byte {
	name := r.ReportName

	return []byte(fmt.Sprintf(`AWSTemplateFormatVersion: '2010-09-09'
Resources:
  AWSCURDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: athenacurcfn_%[1]s
  AWSCURCrawler:
    Type: AWS::Glue::Crawler
    Properties:
      Name: AWSCURCrawler-%[1]s
      DatabaseName: athenacurcfn_%[1]s
      Targets:
        S3Targets:
          - Path: %[2]s
`, strings.ReplaceAll(strings.ToLower(name), "-", "_"), dataURI))
}
