package app

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	s3tablesbackend "github.com/blackbirdworks/gopherstack/services/s3tables"
)

var _ athenabackend.S3TablesSource = (*athenaS3TablesAdapter)(nil)

// athenaS3TablesAdapter serves Athena's s3tablescatalog/<table-bucket> federated catalog from S3 Tables.
type athenaS3TablesAdapter struct {
	handler *s3tablesbackend.Handler
}

// wireAthenaStorage connects Athena to S3 (result objects, table data) and to S3 Tables.
func wireAthenaStorage(byName map[string]service.Registerable) {
	wireAthenaS3(byName["Athena"], byName["S3"])
	wireAthenaS3Tables(byName["Athena"], byName["S3tables"])
}

// wireAthenaS3Tables lets Athena query and append to S3 Tables through the federated catalog.
func wireAthenaS3Tables(athenaReg, s3tablesReg service.Registerable) {
	athenaH, ok := athenaReg.(*athenabackend.Handler)
	if !ok {
		return
	}

	athenaBk, bkOk := athenaH.Backend.(*athenabackend.InMemoryBackend)
	if !bkOk {
		return
	}

	tablesH, tOk := s3tablesReg.(*s3tablesbackend.Handler)
	if !tOk {
		return
	}

	athenaBk.SetS3TablesSource(&athenaS3TablesAdapter{handler: tablesH})
}

func (a *athenaS3TablesAdapter) GetTableInfo(
	region, bucket, namespace, table string,
) (*athenabackend.S3TableInfo, error) {
	bk := a.handler.BackendFor(region)

	t, err := bk.GetTable(bk.TableBucketARN(bucket), []string{namespace}, table)
	if err != nil {
		return nil, err
	}

	return &athenabackend.S3TableInfo{MetadataLocation: t.MetadataLocation, Location: t.WarehouseLocation}, nil
}

func (a *athenaS3TablesAdapter) CommitMetadataLocation(region, bucket, namespace, table, expected, next string) error {
	bk := a.handler.BackendFor(region)
	arn := bk.TableBucketARN(bucket)

	t, err := bk.GetTable(arn, []string{namespace}, table)
	if err != nil {
		return err
	}

	if t.MetadataLocation != expected {
		return athenabackend.ErrCatalogCommitConflict
	}

	if _, err = bk.UpdateTableMetadataLocation(arn, []string{namespace}, table, next, t.VersionToken); err != nil {
		if errors.Is(err, s3tablesbackend.ErrTableVersionConflict) {
			return athenabackend.ErrCatalogCommitConflict
		}

		return err
	}

	return nil
}

func (a *athenaS3TablesAdapter) CreateTable(
	region, bucket, namespace, table string,
) (*athenabackend.S3TableInfo, error) {
	bk := a.handler.BackendFor(region)

	arn := bk.TableBucketARN(bucket)

	t, err := bk.CreateTable(arn, []string{namespace}, table, "ICEBERG", s3tablesbackend.CreateTableOptions{})
	if err != nil {
		switch {
		case errors.Is(err, s3tablesbackend.ErrTableAlreadyExists):
			return nil, fmt.Errorf("%w: %w", athenabackend.ErrCatalogEntityExists, err)
		case errors.Is(err, s3tablesbackend.ErrNamespaceNotFound),
			errors.Is(err, s3tablesbackend.ErrTableBucketNotFound):
			return nil, fmt.Errorf("%w: %w", athenabackend.ErrCatalogEntityNotFound, err)
		}

		return nil, err
	}

	return &athenabackend.S3TableInfo{MetadataLocation: t.MetadataLocation, Location: t.WarehouseLocation}, nil
}

func (a *athenaS3TablesAdapter) DeleteTable(region, bucket, namespace, table string) error {
	bk := a.handler.BackendFor(region)

	return bk.DeleteTable(bk.TableBucketARN(bucket), []string{namespace}, table, "")
}

func (a *athenaS3TablesAdapter) ListNamespaces(region, bucket string) ([]string, error) {
	bk := a.handler.BackendFor(region)

	var (
		out   []string
		token string
	)

	for {
		pg, err := bk.ListNamespaces(
			bk.TableBucketARN(bucket),
			s3tablesbackend.ListNamespacesParams{ContinuationToken: token},
		)
		if err != nil {
			return nil, err
		}

		for _, ns := range pg.Data {
			out = append(out, strings.Join(ns.Namespace, "."))
		}

		if pg.Next == "" {
			return out, nil
		}

		token = pg.Next
	}
}

func (a *athenaS3TablesAdapter) ListTables(region, bucket, namespace string) ([]string, error) {
	bk := a.handler.BackendFor(region)

	var (
		out   []string
		token string
	)

	for {
		pg, err := bk.ListTables(
			bk.TableBucketARN(bucket),
			namespace,
			s3tablesbackend.ListTablesParams{ContinuationToken: token},
		)
		if err != nil {
			return nil, err
		}

		for _, t := range pg.Data {
			out = append(out, t.Name)
		}

		if pg.Next == "" {
			return out, nil
		}

		token = pg.Next
	}
}
