// Command gcp-bigquery-gcs-load uploads a CSV to Cloud Storage, loads it into
// a BigQuery table with a load job and prints an aggregate query result.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	project = "demo-project"
	bucket  = "sales-lake"
	object  = "in/sales.csv"
	dataset = "sales"
	table   = "orders"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

func isConflict(err error) bool {
	if gerr, ok := errors.AsType[*googleapi.Error](err); ok {
		return gerr.Code == http.StatusConflict
	}

	return false
}

func upload(ctx context.Context, csvPath string) error {
	data, err := os.ReadFile(csvPath)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(ctx,
		option.WithEndpoint(env("GCS_URL", "http://localhost:4443")+"/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.Bucket(bucket).Create(ctx, project, nil); err != nil && !isConflict(err) {
		return fmt.Errorf("create bucket: %w", err)
	}

	w := client.Bucket(bucket).Object(object).NewWriter(ctx)
	w.ContentType = "text/csv"

	if _, err = w.Write(data); err != nil {
		return err
	}

	if err = w.Close(); err != nil {
		return fmt.Errorf("upload: %w", err)
	}

	fmt.Printf("uploaded gs://%s/%s (%d bytes)\n", bucket, object, len(data))

	return nil
}

func load(ctx context.Context, bq *bigquery.Client) error {
	if err := bq.Dataset(dataset).Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil && !isConflict(err) {
		return fmt.Errorf("create dataset: %w", err)
	}

	ref := bigquery.NewGCSReference("gs://" + bucket + "/" + object)
	ref.SkipLeadingRows = 1
	ref.Schema = bigquery.Schema{
		{Name: "region", Type: bigquery.StringFieldType},
		{Name: "product", Type: bigquery.StringFieldType},
		{Name: "units", Type: bigquery.IntegerFieldType},
		{Name: "price", Type: bigquery.FloatFieldType},
	}

	loader := bq.Dataset(dataset).Table(table).LoaderFrom(ref)
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if err = status.Err(); err != nil {
		return fmt.Errorf("load job: %w", err)
	}

	md, err := bq.Dataset(dataset).Table(table).Metadata(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("load job done: %s.%s has %d rows\n", dataset, table, md.NumRows)

	return nil
}

const aggregate = "SELECT region, SUM(units) AS units, ROUND(SUM(units * price), 2) AS revenue " +
	"FROM `" + project + "." + dataset + "." + table + "` GROUP BY region ORDER BY revenue DESC"

func query(ctx context.Context, bq *bigquery.Client) (int, error) {
	it, err := bq.Query(aggregate).Read(ctx)
	if err != nil {
		return 0, err
	}

	fmt.Printf("%-8s %6s %10s\n", "region", "units", "revenue")

	rows := 0

	for {
		var row struct {
			Region  string
			Units   int64
			Revenue float64
		}

		if err = it.Next(&row); errors.Is(err, iterator.Done) {
			return rows, nil
		} else if err != nil {
			return rows, err
		}

		fmt.Printf("%-8s %6d %10.2f\n", row.Region, row.Units, row.Revenue)

		rows++
	}
}

func run(ctx context.Context) error {
	csvPath := "sales.csv"
	if len(os.Args) > 1 {
		csvPath = os.Args[1]
	}

	if err := upload(ctx, csvPath); err != nil {
		return err
	}

	bq, err := bigquery.NewClient(ctx, project,
		option.WithEndpoint(env("BIGQUERY_URL", "http://localhost:9050")), option.WithoutAuthentication())
	if err != nil {
		return err
	}
	defer bq.Close()

	if err = load(ctx, bq); err != nil {
		return err
	}

	rows, err := query(ctx, bq)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	fmt.Printf("query returned %d region(s)\n", rows)

	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
