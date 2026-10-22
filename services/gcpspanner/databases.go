package gcpspanner

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxStatements     = 1000
	maxStatementBytes = 1 << 20
	retentionDefault  = "1h"
)

var (
	createStmtRe = regexp.MustCompile("(?is)^\\s*CREATE\\s+DATABASE\\s+[`\"]?([^`\"\\s;]+)[`\"]?\\s*;?\\s*$")
	opIDRe       = regexp.MustCompile(`^[a-z][a-z0-9_]{0,126}[a-z0-9]$`)
)

func validateStatements(stmts []string) error {
	if len(stmts) > maxStatements {
		return errInvalid("At most %d DDL statements are accepted per request.", maxStatements)
	}

	for _, s := range stmts {
		if strings.TrimSpace(s) == "" {
			return errInvalid("DDL statements must not be empty.")
		}

		if len(s) > maxStatementBytes {
			return errInvalid("A DDL statement exceeds %d bytes.", maxStatementBytes)
		}
	}

	return nil
}

func cloneDDL(ddl []string) []string { return slices.Clone(ddl) }

// instanceReadyLocked returns the instance a database call targets. Must hold b.mu.
func (b *InMemoryBackend) instanceLocked(name string) (*instRec, error) {
	rec, ok := b.instances[name]
	if !ok {
		return nil, errInstanceNotFound(name)
	}

	return rec, nil
}

func (b *InMemoryBackend) lookupDB(name string) (string, *dbRec, error) {
	inst, id, err := parseDatabase(name)
	if err != nil {
		return "", nil, err
	}

	b.mu.RLock("lookupDB")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return "", nil, err
	}

	d, ok := rec.dbs[id]
	if !ok {
		return "", nil, errDatabaseNotFound(name)
	}

	return inst, d, nil
}

func (b *InMemoryBackend) precheckCreate(in *databasepb.CreateDatabaseRequest) (string, error) {
	m := createStmtRe.FindStringSubmatch(in.GetCreateStatement())
	if m == nil {
		return "", errInvalid("create_statement must be CREATE DATABASE `{id}`.")
	}

	id := m[1]
	if !databaseIDRe.MatchString(id) {
		return "", errInvalid("Database id %q must be 2-30 characters: lowercase letters, digits, _ or -, starting "+
			"with a letter and ending with a letter or digit.", id)
	}

	if err := validateStatements(in.GetExtraStatements()); err != nil {
		return "", err
	}

	if in.GetEncryptionConfig() != nil {
		return "", errUnimplemented("Customer-managed encryption is not supported by this emulator.")
	}

	b.mu.RLock("CreateDatabase")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return "", err
	}

	if rec.pb.GetState() != instancepb.Instance_READY {
		return "", errPrecondition("Instance %s is not ready.", in.GetParent())
	}

	if _, dup := rec.dbs[id]; dup {
		return "", errExists("Database already exists: %s", databaseName(in.GetParent(), id))
	}

	if len(rec.dbs) >= maxDatabases {
		return "", errExhausted("At most %d databases can exist in an instance.", maxDatabases)
	}

	return id, nil
}

// CreateDatabase creates a database, executing its DDL in the emulator when one is available.
func (b *InMemoryBackend) CreateDatabase(
	ctx context.Context, in *databasepb.CreateDatabaseRequest,
) (*longrunningpb.Operation, error) {
	id, err := b.precheckCreate(in)
	if err != nil {
		return nil, err
	}

	ddl := cloneDDL(in.GetExtraStatements())
	name := databaseName(in.GetParent(), id)

	cc, connErr := b.emu.conn(ctx)

	switch {
	case connErr == nil:
		d := replayDB{id: id, ddl: ddl, dialect: in.GetDatabaseDialect()}
		if err = createDBOnEmulator(ctx, cc, in.GetParent(), d); err != nil {
			return nil, err
		}

		ddl = b.emulatorDDL(ctx, cc, name, ddl)
	case !errors.Is(connErr, errNoDataPlane):
		return nil, connErr
	}

	pb := b.newDatabase(name, in.GetDatabaseDialect())
	if err = b.insertDatabase(in.GetParent(), id, &dbRec{pb: pb, ddl: ddl}); err != nil {
		return nil, err
	}

	meta := &databasepb.CreateDatabaseMetadata{Database: name}

	return b.finished(opName(name, ""), meta, cloneDatabase(pb))
}

func (b *InMemoryBackend) newDatabase(name string, dialect databasepb.DatabaseDialect) *databasepb.Database {
	now := b.ts()

	if dialect == databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED {
		dialect = databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL
	}

	return &databasepb.Database{
		Name: name, State: databasepb.Database_READY, CreateTime: now, EarliestVersionTime: now,
		VersionRetentionPeriod: retentionDefault, DatabaseDialect: dialect,
	}
}

func (b *InMemoryBackend) insertDatabase(instance, id string, d *dbRec) error {
	b.mu.Lock("insertDatabase")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(instance)
	if err != nil {
		return err
	}

	if _, dup := rec.dbs[id]; dup {
		return errExists("Database already exists: %s", databaseName(instance, id))
	}

	rec.dbs[id] = d

	return nil
}

// emulatorDDL reads the DDL the emulator holds for a database, keeping fallback if it cannot be read.
func (b *InMemoryBackend) emulatorDDL(
	ctx context.Context,
	cc grpc.ClientConnInterface,
	db string,
	fallback []string,
) []string {
	resp, err := databasepb.NewDatabaseAdminClient(cc).
		GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: db})
	if err != nil {
		return fallback
	}

	return resp.GetStatements()
}

// GetDatabase returns a database.
func (b *InMemoryBackend) GetDatabase(name string) (*databasepb.Database, error) {
	_, d, err := b.lookupDB(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	return cloneDatabase(d.pb), nil
}

// ListDatabases lists the databases of an instance.
func (b *InMemoryBackend) ListDatabases(
	in *databasepb.ListDatabasesRequest,
) (*databasepb.ListDatabasesResponse, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if err := page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, errInvalid("Invalid page_token.")
	}

	b.mu.RLock("ListDatabases")

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	ids := make([]string, 0, len(rec.dbs))
	for id := range rec.dbs {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	all := make([]*databasepb.Database, 0, len(ids))
	for _, id := range ids {
		all = append(all, cloneDatabase(rec.dbs[id].pb))
	}
	b.mu.RUnlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return &databasepb.ListDatabasesResponse{Databases: pg.Data, NextPageToken: pg.Next}, nil
}

// DropDatabase deletes a database unless drop protection is on.
func (b *InMemoryBackend) DropDatabase(ctx context.Context, name string) (*emptypb.Empty, error) {
	inst, d, err := b.lookupDB(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("DropDatabase")
	protected := d.pb.GetEnableDropProtection()
	b.mu.RUnlock()

	if protected {
		return nil, errPrecondition("Database %s has drop protection enabled.", name)
	}

	if cc := b.emu.current(); cc != nil {
		if _, err = databasepb.NewDatabaseAdminClient(cc).
			DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: name}); err != nil {
			return nil, err //nolint:wrapcheck // emulator status passes through
		}
	}

	b.mu.Lock("DropDatabase")
	defer b.mu.Unlock()

	if rec, ok := b.instances[inst]; ok {
		delete(rec.dbs, strings.TrimPrefix(name, inst+"/databases/"))
	}

	return &emptypb.Empty{}, nil
}

// GetDatabaseDdl returns the DDL statements of a database.
func (b *InMemoryBackend) GetDatabaseDdl(name string) (*databasepb.GetDatabaseDdlResponse, error) {
	_, d, err := b.lookupDB(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetDatabaseDdl")
	defer b.mu.RUnlock()

	return &databasepb.GetDatabaseDdlResponse{Statements: cloneDDL(d.ddl)}, nil
}

func (b *InMemoryBackend) ddlOperation(in *databasepb.UpdateDatabaseDdlRequest) (string, error) {
	id := in.GetOperationId()
	if id == "" {
		return opName(in.GetDatabase(), ""), nil
	}

	if !opIDRe.MatchString(id) {
		return "", errInvalid("operation_id %q must match [a-z][a-z0-9_]*[a-z0-9].", id)
	}

	name := opName(in.GetDatabase(), id)
	if _, dup := b.ops.Lookup(name); dup {
		return "", errExists("Operation already exists: %s", name)
	}

	return name, nil
}

// UpdateDatabaseDdl applies DDL statements, in the emulator when one is available.
func (b *InMemoryBackend) UpdateDatabaseDdl(
	ctx context.Context, in *databasepb.UpdateDatabaseDdlRequest,
) (*longrunningpb.Operation, error) {
	if _, _, err := b.lookupDB(in.GetDatabase()); err != nil {
		return nil, err
	}

	if len(in.GetStatements()) == 0 {
		return nil, errInvalid("At least one DDL statement is required.")
	}

	if err := validateStatements(in.GetStatements()); err != nil {
		return nil, err
	}

	name, err := b.ddlOperation(in)
	if err != nil {
		return nil, err
	}

	res, err := b.applyDDL(ctx, in)
	if err != nil {
		return nil, err
	}

	stamps := make([]*timestamppb.Timestamp, len(in.GetStatements()))
	for i := range stamps {
		stamps[i] = b.ts()
	}

	meta := &databasepb.UpdateDatabaseDdlMetadata{
		Database: in.GetDatabase(), Statements: in.GetStatements(), CommitTimestamps: stamps,
	}

	if res.failure != nil {
		b.failOp(name, func(*timestamppb.Timestamp) proto.Message { return meta }, res.failure)

		op, _ := b.ops.Lookup(name)

		return op, nil
	}

	return b.finished(name, meta, &emptypb.Empty{})
}

// ddlResult carries the DDL failure, if any, to report on the operation.
type ddlResult struct{ failure *status.Status }

// applyDDL runs the statements; the returned status is the DDL failure to report on the operation.
func (b *InMemoryBackend) applyDDL(
	ctx context.Context, in *databasepb.UpdateDatabaseDdlRequest,
) (ddlResult, error) {
	cc, connErr := b.emu.conn(ctx)

	switch {
	case connErr == nil:
		return b.emulatorUpdate(ctx, cc, in)
	case errors.Is(connErr, errNoDataPlane):
		b.appendDDL(in.GetDatabase(), in.GetStatements())

		return ddlResult{}, nil
	default:
		return ddlResult{}, connErr
	}
}

func (b *InMemoryBackend) emulatorUpdate(
	ctx context.Context, cc *grpc.ClientConn, in *databasepb.UpdateDatabaseDdlRequest,
) (ddlResult, error) {
	op, err := databasepb.NewDatabaseAdminClient(cc).UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: in.GetDatabase(), Statements: in.GetStatements(),
	})
	if err != nil {
		return ddlResult{}, err //nolint:wrapcheck // emulator status passes through
	}

	opErr := waitOp(ctx, cc, op)
	b.setDDL(in.GetDatabase(), b.emulatorDDL(ctx, cc, in.GetDatabase(), b.currentDDL(in.GetDatabase())))

	if opErr != nil {
		return ddlResult{failure: statusOf(opErr)}, nil
	}

	return ddlResult{}, nil
}

func (b *InMemoryBackend) currentDDL(db string) []string {
	var out []string

	b.mutateDB(db, func(d *dbRec) { out = cloneDDL(d.ddl) })

	return out
}

func (b *InMemoryBackend) appendDDL(db string, stmts []string) {
	b.mutateDB(db, func(d *dbRec) { d.ddl = append(d.ddl, stmts...) })
}

func (b *InMemoryBackend) setDDL(db string, ddl []string) {
	b.mutateDB(db, func(d *dbRec) { d.ddl = cloneDDL(ddl) })
}

func (b *InMemoryBackend) mutateDB(name string, fn func(*dbRec)) {
	inst, id, err := parseDatabase(name)
	if err != nil {
		return
	}

	b.mu.Lock("mutateDB")
	defer b.mu.Unlock()

	if rec, ok := b.instances[inst]; ok {
		if d, found := rec.dbs[id]; found {
			fn(d)
		}
	}
}

// UpdateDatabase updates enable_drop_protection, the only mutable database field.
func (b *InMemoryBackend) UpdateDatabase(in *databasepb.UpdateDatabaseRequest) (*longrunningpb.Operation, error) {
	pb := in.GetDatabase()
	if pb == nil {
		return nil, errInvalid("database is required.")
	}

	if _, _, err := b.lookupDB(pb.GetName()); err != nil {
		return nil, err
	}

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask is required.")
	}

	for _, p := range paths {
		if p != "enable_drop_protection" {
			return nil, errInvalid("Update mask path %q is not an updatable field.", p)
		}
	}

	b.mutateDB(pb.GetName(), func(d *dbRec) { d.pb.EnableDropProtection = pb.GetEnableDropProtection() })

	got, err := b.GetDatabase(pb.GetName())
	if err != nil {
		return nil, err
	}

	meta := &databasepb.UpdateDatabaseMetadata{Request: in}

	return b.finished(opName(pb.GetName(), ""), meta, got)
}
