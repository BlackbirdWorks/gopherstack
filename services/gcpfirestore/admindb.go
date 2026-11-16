package gcpfirestore

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const versionRetention = time.Hour

type adminServer struct {
	adminpb.UnimplementedFirestoreAdminServer
	b *InMemoryBackend
}

func (b *InMemoryBackend) newDatabaseLocked(
	name, location string,
	typ adminpb.Database_DatabaseType,
) *adminpb.Database {
	now := b.nowTS()

	return &adminpb.Database{
		Name: name, Uid: uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String(), CreateTime: now, UpdateTime: now,
		LocationId: location, Type: typ, ConcurrencyMode: adminpb.Database_PESSIMISTIC,
		VersionRetentionPeriod: durationpb.New(versionRetention), EarliestVersionTime: now,
		PointInTimeRecoveryEnablement: adminpb.Database_POINT_IN_TIME_RECOVERY_DISABLED,
		AppEngineIntegrationMode:      adminpb.Database_DISABLED,
		DeleteProtectionState:         adminpb.Database_DELETE_PROTECTION_DISABLED,
		Etag:                          randHex(idBytes),
	}
}

// databaseLocked returns a registered database, materialising the implicit (default) one on first use.
func (b *InMemoryBackend) databaseLocked(name string) (*adminpb.Database, error) {
	if err := validateDatabase(name); err != nil {
		return nil, err
	}

	if d, ok := b.admin.dbs[name]; ok {
		return d, nil
	}

	if strings.HasSuffix(name, "/databases/"+defaultDBID) {
		d := b.newDatabaseLocked(name, defaultLocation, adminpb.Database_FIRESTORE_NATIVE)
		b.admin.dbs[name] = d

		return d, nil
	}

	return nil, errNotFound("Database %q does not exist.", name)
}

func (s *adminServer) CreateDatabase(
	_ context.Context, in *adminpb.CreateDatabaseRequest,
) (*longrunningpb.Operation, error) {
	id := in.GetDatabaseId()
	if id == "" {
		return nil, errInvalid("database_id is required.")
	}

	if pp := in.GetParent(); projectOf(pp) != pp || pp == "" {
		return nil, errInvalid("Invalid parent %q.", pp)
	}

	name := in.GetParent() + "/databases/" + id
	if err := validateDatabase(name); err != nil {
		return nil, err
	}

	b := s.b
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if _, ok := b.admin.dbs[name]; ok {
		return nil, errAlreadyExists("Database %q already exists.", name)
	}

	typ := in.GetDatabase().GetType()
	if typ == adminpb.Database_DATABASE_TYPE_UNSPECIFIED {
		typ = adminpb.Database_FIRESTORE_NATIVE
	}

	if in.GetDatabase().GetLocationId() == "" {
		return nil, errInvalid("database.location_id is required.")
	}

	d := b.newDatabaseLocked(name, in.GetDatabase().GetLocationId(), typ)
	d.DeleteProtectionState = nonZero(in.GetDatabase().GetDeleteProtectionState(), d.GetDeleteProtectionState())
	b.admin.dbs[name] = d

	return b.finishOpLocked(name, &adminpb.CreateDatabaseMetadata{}, d)
}

func nonZero[T comparable](v, fallback T) T {
	var zero T
	if v == zero {
		return fallback
	}

	return v
}

func (s *adminServer) GetDatabase(_ context.Context, in *adminpb.GetDatabaseRequest) (*adminpb.Database, error) {
	s.b.mu.Lock("GetDatabase")
	defer s.b.mu.Unlock()

	d, err := s.b.databaseLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	return cloneMsg(d), nil
}

func (s *adminServer) ListDatabases(
	_ context.Context, in *adminpb.ListDatabasesRequest,
) (*adminpb.ListDatabasesResponse, error) {
	if pp := in.GetParent(); projectOf(pp) != pp || pp == "" {
		return nil, errInvalid("Invalid parent %q.", pp)
	}

	s.b.mu.Lock("ListDatabases")
	defer s.b.mu.Unlock()

	if _, err := s.b.databaseLocked(in.GetParent() + "/databases/" + defaultDBID); err != nil {
		return nil, err
	}

	all := sortedByName(s.b.admin.dbs, in.GetParent()+"/databases/")
	out := make([]*adminpb.Database, len(all))

	for i, d := range all {
		out[i] = cloneMsg(d)
	}

	return &adminpb.ListDatabasesResponse{Databases: out}, nil
}

func (s *adminServer) UpdateDatabase(
	_ context.Context, in *adminpb.UpdateDatabaseRequest,
) (*longrunningpb.Operation, error) {
	s.b.mu.Lock("UpdateDatabase")
	defer s.b.mu.Unlock()

	d, err := s.b.databaseLocked(in.GetDatabase().GetName())
	if err != nil {
		return nil, err
	}

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{"delete_protection_state", "point_in_time_recovery_enablement", "concurrency_mode"}
	}

	if err = applyDatabaseMask(d, in.GetDatabase(), in.GetUpdateMask(), paths); err != nil {
		return nil, err
	}

	d.UpdateTime, d.Etag = s.b.nowTS(), randHex(idBytes)

	return s.b.finishOpLocked(d.GetName(), &adminpb.UpdateDatabaseMetadata{}, d)
}

func applyDatabaseMask(dst, src *adminpb.Database, _ *fieldmaskpb.FieldMask, paths []string) error {
	for _, p := range paths {
		switch p {
		case "delete_protection_state":
			dst.DeleteProtectionState = nonZero(src.GetDeleteProtectionState(), dst.GetDeleteProtectionState())
		case "point_in_time_recovery_enablement":
			dst.PointInTimeRecoveryEnablement = nonZero(
				src.GetPointInTimeRecoveryEnablement(),
				dst.GetPointInTimeRecoveryEnablement(),
			)
		case "concurrency_mode":
			dst.ConcurrencyMode = nonZero(src.GetConcurrencyMode(), dst.GetConcurrencyMode())
		default:
			return errInvalid("Field %q cannot be updated.", p)
		}
	}

	return nil
}

func (s *adminServer) DeleteDatabase(
	_ context.Context, in *adminpb.DeleteDatabaseRequest,
) (*longrunningpb.Operation, error) {
	b := s.b

	defer b.flushDocEvents()

	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	d, err := b.databaseLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if d.GetDeleteProtectionState() == adminpb.Database_DELETE_PROTECTION_ENABLED {
		return nil, errPrecondition("Database %q has delete protection enabled.", d.GetName())
	}

	if in.GetEtag() != "" && in.GetEtag() != d.GetEtag() {
		return nil, errAborted()
	}

	snapshot := cloneMsg(d)
	snapshot.DeleteTime = b.nowTS()

	b.dropDatabaseLocked(d.GetName())

	return b.finishOpLocked(d.GetName(), &adminpb.DeleteDatabaseMetadata{}, snapshot)
}

// dropDatabaseLocked removes a database with its documents, indexes and field overrides.
func (b *InMemoryBackend) dropDatabaseLocked(name string) {
	prefix := name + "/"

	for n := range b.docs {
		if strings.HasPrefix(n, prefix) {
			delete(b.docs, n)
		}
	}

	delete(b.admin.dbs, name)

	for k := range b.admin.indexes {
		if strings.HasPrefix(k, prefix) {
			delete(b.admin.indexes, k)
		}
	}

	for k := range b.admin.fields {
		if strings.HasPrefix(k, prefix) {
			delete(b.admin.fields, k)
		}
	}

	b.notifyLocked()
}
