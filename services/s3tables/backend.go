package s3tables

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrTableBucketNotFound is returned when a TableBucket does not exist.
	ErrTableBucketNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrTableBucketAlreadyExists is returned when a TableBucket already exists.
	ErrTableBucketAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrNamespaceNotFound is returned when a Namespace does not exist.
	ErrNamespaceNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrNamespaceAlreadyExists is returned when a Namespace already exists.
	ErrNamespaceAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTableNotFound is returned when a Table does not exist.
	ErrTableNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrTableAlreadyExists is returned when a Table already exists.
	ErrTableAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
)

// TableBucket represents an S3 Tables table bucket.
type TableBucket struct {
	ARN                      string
	Name                     string
	OwnerAccountID           string
	CreatedAt                time.Time
	MaintenanceConfiguration map[string]any
	Policy                   string
}

// Namespace represents an S3 Tables namespace.
type Namespace struct {
	CreatedAt      time.Time
	TableBucketARN string
	OwnerAccountID string
	CreatedBy      string
	Policy         string
	NamespaceID    string
	Namespace      []string
}

// Table represents an S3 Tables table.
type Table struct {
	CreatedAt                time.Time
	ModifiedAt               time.Time
	MaintenanceConfiguration map[string]any
	TableBucketARN           string
	Format                   string
	VersionToken             string
	MetadataLocation         string
	WarehouseLocation        string
	ARN                      string
	OwnerAccountID           string
	Policy                   string
	Name                     string
	Namespace                []string
}

// InMemoryBackend is an in-memory store for S3 Tables resources.
type InMemoryBackend struct {
	tableBuckets map[string]*TableBucket // keyed by ARN
	namespaces   map[string]*Namespace   // keyed by tableBucketARN + ":" + namespace
	tables       map[string]*Table       // keyed by ARN
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory S3 Tables backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		tableBuckets: make(map[string]*TableBucket),
		namespaces:   make(map[string]*Namespace),
		tables:       make(map[string]*Table),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("s3tables"),
	}
}

// TableBucketARN builds an ARN for a TableBucket.
func (b *InMemoryBackend) TableBucketARN(name string) string {
	return arn.Build("s3tables", b.region, b.accountID, "bucket/"+name)
}

// TableARN builds an ARN for a Table.
func (b *InMemoryBackend) TableARN(bucketName, namespaceName, tableName string) string {
	return arn.Build("s3tables", b.region, b.accountID,
		"bucket/"+bucketName+"/table/"+namespaceName+"/"+tableName)
}

func namespaceKey(tableBucketARN, namespace string) string {
	return tableBucketARN + "::" + namespace
}

// CreateTableBucket creates a new TableBucket.
func (b *InMemoryBackend) CreateTableBucket(name string) (*TableBucket, error) {
	b.mu.Lock("CreateTableBucket")
	defer b.mu.Unlock()

	bucketARN := b.TableBucketARN(name)
	if _, ok := b.tableBuckets[bucketARN]; ok {
		return nil, fmt.Errorf("%w: table bucket %q already exists", ErrTableBucketAlreadyExists, name)
	}

	tb := &TableBucket{
		ARN:            bucketARN,
		Name:           name,
		OwnerAccountID: b.accountID,
		CreatedAt:      time.Now().UTC(),
	}
	b.tableBuckets[bucketARN] = tb

	return cloneTableBucket(tb), nil
}

// GetTableBucket returns a TableBucket by ARN.
func (b *InMemoryBackend) GetTableBucket(bucketARN string) (*TableBucket, error) {
	b.mu.RLock("GetTableBucket")
	defer b.mu.RUnlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	return cloneTableBucket(tb), nil
}

// DeleteTableBucket deletes a TableBucket by ARN.
func (b *InMemoryBackend) DeleteTableBucket(bucketARN string) error {
	b.mu.Lock("DeleteTableBucket")
	defer b.mu.Unlock()

	if _, ok := b.tableBuckets[bucketARN]; !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	delete(b.tableBuckets, bucketARN)

	return nil
}

// ListTableBuckets returns all TableBuckets sorted by name.
func (b *InMemoryBackend) ListTableBuckets() []*TableBucket {
	b.mu.RLock("ListTableBuckets")
	defer b.mu.RUnlock()

	list := make([]*TableBucket, 0, len(b.tableBuckets))

	for _, tb := range b.tableBuckets {
		list = append(list, cloneTableBucket(tb))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// GetTableBucketMaintenanceConfiguration returns the maintenance config for a bucket.
func (b *InMemoryBackend) GetTableBucketMaintenanceConfiguration(bucketARN string) (map[string]any, error) {
	b.mu.RLock("GetTableBucketMaintenanceConfiguration")
	defer b.mu.RUnlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	cfg := cloneAnyMap(tb.MaintenanceConfiguration)

	return cfg, nil
}

// PutTableBucketMaintenanceConfiguration sets maintenance config for a bucket.
func (b *InMemoryBackend) PutTableBucketMaintenanceConfiguration(
	bucketARN, maintenanceType string,
	value map[string]any,
) error {
	b.mu.Lock("PutTableBucketMaintenanceConfiguration")
	defer b.mu.Unlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	if tb.MaintenanceConfiguration == nil {
		tb.MaintenanceConfiguration = make(map[string]any)
	}

	tb.MaintenanceConfiguration[maintenanceType] = value

	return nil
}

// GetTableBucketPolicy returns the resource policy for a bucket.
func (b *InMemoryBackend) GetTableBucketPolicy(bucketARN string) (string, error) {
	b.mu.RLock("GetTableBucketPolicy")
	defer b.mu.RUnlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return "", fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	if tb.Policy == "" {
		return "", fmt.Errorf("%w: no policy for table bucket %q", ErrTableBucketNotFound, bucketARN)
	}

	return tb.Policy, nil
}

// PutTableBucketPolicy sets the resource policy for a bucket.
func (b *InMemoryBackend) PutTableBucketPolicy(bucketARN, policy string) error {
	b.mu.Lock("PutTableBucketPolicy")
	defer b.mu.Unlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.Policy = policy

	return nil
}

// DeleteTableBucketPolicy removes the resource policy from a bucket.
func (b *InMemoryBackend) DeleteTableBucketPolicy(bucketARN string) error {
	b.mu.Lock("DeleteTableBucketPolicy")
	defer b.mu.Unlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.Policy = ""

	return nil
}

// CreateNamespace creates a new namespace within a table bucket.
func (b *InMemoryBackend) CreateNamespace(tableBucketARN string, namespace []string) (*Namespace, error) {
	b.mu.Lock("CreateNamespace")
	defer b.mu.Unlock()

	if _, ok := b.tableBuckets[tableBucketARN]; !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if _, ok := b.namespaces[key]; ok {
		return nil, fmt.Errorf(
			"%w: namespace %q already exists in bucket %s",
			ErrNamespaceAlreadyExists,
			nsStr,
			tableBucketARN,
		)
	}

	ns := &Namespace{
		Namespace:      cloneStringSlice(namespace),
		TableBucketARN: tableBucketARN,
		OwnerAccountID: b.accountID,
		CreatedBy:      b.accountID,
		CreatedAt:      time.Now().UTC(),
		NamespaceID:    uuid.NewString(),
	}
	b.namespaces[key] = ns

	return cloneNamespace(ns), nil
}

// GetNamespace returns a namespace by bucket ARN and namespace name.
func (b *InMemoryBackend) GetNamespace(tableBucketARN string, namespace []string) (*Namespace, error) {
	b.mu.RLock("GetNamespace")
	defer b.mu.RUnlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	ns, ok := b.namespaces[key]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found in bucket %s", ErrNamespaceNotFound, nsStr, tableBucketARN)
	}

	return cloneNamespace(ns), nil
}

// DeleteNamespace deletes a namespace from a table bucket.
func (b *InMemoryBackend) DeleteNamespace(tableBucketARN string, namespace []string) error {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if _, ok := b.namespaces[key]; !ok {
		return fmt.Errorf("%w: namespace %q not found in bucket %s", ErrNamespaceNotFound, nsStr, tableBucketARN)
	}

	delete(b.namespaces, key)

	return nil
}

// ListNamespaces returns all namespaces in a table bucket sorted by name.
func (b *InMemoryBackend) ListNamespaces(tableBucketARN string) ([]*Namespace, error) {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	if _, ok := b.tableBuckets[tableBucketARN]; !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	list := make([]*Namespace, 0)

	for _, ns := range b.namespaces {
		if ns.TableBucketARN == tableBucketARN {
			list = append(list, cloneNamespace(ns))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return joinNamespace(list[i].Namespace) < joinNamespace(list[j].Namespace)
	})

	return list, nil
}

// CreateTable creates a new table within a namespace.
func (b *InMemoryBackend) CreateTable(tableBucketARN string, namespace []string, name, format string) (*Table, error) {
	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	tb, ok := b.tableBuckets[tableBucketARN]
	if !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	nsStr := joinNamespace(namespace)
	nsKey := namespaceKey(tableBucketARN, nsStr)

	if _, exists := b.namespaces[nsKey]; !exists {
		return nil, fmt.Errorf("%w: namespace %q not found in bucket %s", ErrNamespaceNotFound, nsStr, tableBucketARN)
	}

	tableARN := b.TableARN(tb.Name, nsStr, name)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			return nil, fmt.Errorf("%w: table %q already exists in namespace %s", ErrTableAlreadyExists, name, nsStr)
		}
	}

	now := time.Now().UTC()
	table := &Table{
		ARN:               tableARN,
		Name:              name,
		Namespace:         cloneStringSlice(namespace),
		TableBucketARN:    tableBucketARN,
		Format:            format,
		VersionToken:      uuid.NewString(),
		WarehouseLocation: "s3://" + tb.Name + "/" + nsStr + "/" + name,
		CreatedAt:         now,
		ModifiedAt:        now,
		OwnerAccountID:    b.accountID,
	}
	b.tables[tableARN] = table

	return cloneTable(table), nil
}

// GetTable returns a table by bucket ARN, namespace, and name.
func (b *InMemoryBackend) GetTable(tableBucketARN string, namespace []string, name string) (*Table, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	nsStr := joinNamespace(namespace)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			return cloneTable(t), nil
		}
	}

	return nil, fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

// DeleteTable deletes a table by bucket ARN, namespace, and name.
func (b *InMemoryBackend) DeleteTable(tableBucketARN string, namespace []string, name string) error {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	nsStr := joinNamespace(namespace)

	for tableARN, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			delete(b.tables, tableARN)

			return nil
		}
	}

	return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

// ListTables returns all tables in a table bucket, optionally filtered by namespace.
func (b *InMemoryBackend) ListTables(tableBucketARN, namespace string) ([]*Table, error) {
	b.mu.RLock("ListTables")
	defer b.mu.RUnlock()

	if _, ok := b.tableBuckets[tableBucketARN]; !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	list := make([]*Table, 0)

	for _, t := range b.tables {
		if t.TableBucketARN != tableBucketARN {
			continue
		}

		if namespace != "" && joinNamespace(t.Namespace) != namespace {
			continue
		}

		list = append(list, cloneTable(t))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list, nil
}

// RenameTable renames a table or moves it to a different namespace.
func (b *InMemoryBackend) RenameTable(
	tableBucketARN string,
	namespace []string,
	name, newNamespace, newName string,
) error {
	b.mu.Lock("RenameTable")
	defer b.mu.Unlock()

	nsStr := joinNamespace(namespace)

	var found *Table
	var oldARN string

	for tableARN, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			found = t
			oldARN = tableARN

			break
		}
	}

	if found == nil {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	if newName == "" {
		newName = name
	}

	if newNamespace == "" {
		newNamespace = nsStr
	}

	tb := b.tableBuckets[tableBucketARN]
	newARN := b.TableARN(tb.Name, newNamespace, newName)

	found.Name = newName
	found.Namespace = splitNamespace(newNamespace)
	found.ARN = newARN
	found.ModifiedAt = time.Now().UTC()
	found.VersionToken = uuid.NewString()

	delete(b.tables, oldARN)
	b.tables[newARN] = found

	return nil
}

// UpdateTableMetadataLocation updates the metadata location of a table.
func (b *InMemoryBackend) UpdateTableMetadataLocation(
	tableBucketARN string,
	namespace []string,
	name, metadataLocation, _ string,
) (*Table, error) {
	b.mu.Lock("UpdateTableMetadataLocation")
	defer b.mu.Unlock()

	nsStr := joinNamespace(namespace)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			t.MetadataLocation = metadataLocation
			t.VersionToken = uuid.NewString()
			t.ModifiedAt = time.Now().UTC()

			return cloneTable(t), nil
		}
	}

	return nil, fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

// GetTableMaintenanceConfiguration returns the maintenance config for a table.
func (b *InMemoryBackend) GetTableMaintenanceConfiguration(
	tableBucketARN string,
	namespace []string,
	name string,
) (map[string]any, string, error) {
	b.mu.RLock("GetTableMaintenanceConfiguration")
	defer b.mu.RUnlock()

	nsStr := joinNamespace(namespace)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			return cloneAnyMap(t.MaintenanceConfiguration), t.ARN, nil
		}
	}

	return nil, "", fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

// PutTableMaintenanceConfiguration sets maintenance config for a table.
func (b *InMemoryBackend) PutTableMaintenanceConfiguration(
	tableBucketARN string,
	namespace []string,
	name, maintenanceType string,
	value map[string]any,
) error {
	b.mu.Lock("PutTableMaintenanceConfiguration")
	defer b.mu.Unlock()

	nsStr := joinNamespace(namespace)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			if t.MaintenanceConfiguration == nil {
				t.MaintenanceConfiguration = make(map[string]any)
			}

			t.MaintenanceConfiguration[maintenanceType] = value

			return nil
		}
	}

	return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

// GetTablePolicy returns the resource policy for a table.
func (b *InMemoryBackend) GetTablePolicy(tableBucketARN string, namespace []string, name string) (string, error) {
	b.mu.RLock("GetTablePolicy")
	defer b.mu.RUnlock()

	nsStr := joinNamespace(namespace)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			if t.Policy == "" {
				return "", fmt.Errorf("%w: no policy for table %q", ErrTableNotFound, name)
			}

			return t.Policy, nil
		}
	}

	return "", fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

// PutTablePolicy sets the resource policy for a table.
func (b *InMemoryBackend) PutTablePolicy(tableBucketARN string, namespace []string, name, policy string) error {
	b.mu.Lock("PutTablePolicy")
	defer b.mu.Unlock()

	nsStr := joinNamespace(namespace)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			t.Policy = policy

			return nil
		}
	}

	return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

// DeleteTablePolicy removes the resource policy from a table.
func (b *InMemoryBackend) DeleteTablePolicy(tableBucketARN string, namespace []string, name string) error {
	b.mu.Lock("DeleteTablePolicy")
	defer b.mu.Unlock()

	nsStr := joinNamespace(namespace)

	for _, t := range b.tables {
		if t.TableBucketARN == tableBucketARN && joinNamespace(t.Namespace) == nsStr && t.Name == name {
			t.Policy = ""

			return nil
		}
	}

	return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
}

func cloneTableBucket(tb *TableBucket) *TableBucket {
	cp := *tb
	cp.MaintenanceConfiguration = cloneAnyMap(tb.MaintenanceConfiguration)

	return &cp
}

func cloneNamespace(ns *Namespace) *Namespace {
	cp := *ns
	cp.Namespace = cloneStringSlice(ns.Namespace)

	return &cp
}

func cloneTable(t *Table) *Table {
	cp := *t
	cp.Namespace = cloneStringSlice(t.Namespace)
	cp.MaintenanceConfiguration = cloneAnyMap(t.MaintenanceConfiguration)

	return &cp
}

func cloneStringSlice(s []string) []string {
	if s == nil {
		return []string{}
	}

	out := make([]string, len(s))
	copy(out, s)

	return out
}

func cloneAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	return maps.Clone(m)
}

// joinNamespace joins a namespace slice with "." for use as a key.
func joinNamespace(ns []string) string {
	if len(ns) == 0 {
		return ""
	}

	result := ""

	var resultSb655 strings.Builder
	for i, part := range ns {
		if i > 0 {
			resultSb655.WriteString(".")
		}

		resultSb655.WriteString(part)
	}
	result += resultSb655.String()

	return result
}

// splitNamespace splits a namespace string back into a slice.
func splitNamespace(ns string) []string {
	if ns == "" {
		return []string{}
	}

	return []string{ns}
}
