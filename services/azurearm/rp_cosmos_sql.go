package azurearm

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type cosmosChildKind int

const (
	cosmosKindDatabase cosmosChildKind = iota
	cosmosKindContainer
	cosmosKindDatabaseThroughput
	cosmosKindContainerThroughput
	cosmosKindInvalid
)

const (
	cosmosThroughputName      = "default"
	cosmosFieldResource       = "resource"
	cosmosFieldOptions        = "options"
	cosmosFieldID             = "id"
	cosmosFieldPaths          = "paths"
	cosmosThroughputStep      = 100
	cosmosThroughputMin       = 400
	cosmosAutoscaleStep       = 1000
	cosmosPartKeyVersion      = 2
	cosmosAutoscaleMinDivisor = 10
)

type cosmosContainerExtras struct {
	AnalyticalTTL *int `json:"analyticalTtl,omitempty"`
	PartKeyVer    int  `json:"partitionKeyVersion,omitempty"`
}

func cosmosChildKindOf(id ResourceID) cosmosChildKind {
	t := id.Types

	switch {
	case len(t) == 2 && strings.EqualFold(t[1], cosmosSQLDatabasesType):
		return cosmosKindDatabase
	case len(t) == 3 && strings.EqualFold(t[1], cosmosSQLDatabasesType) && strings.EqualFold(t[2], cosmosContainersType):
		return cosmosKindContainer
	case len(t) == 3 && strings.EqualFold(t[1], cosmosSQLDatabasesType) && strings.EqualFold(t[2], cosmosThroughputType):
		return cosmosKindDatabaseThroughput
	case len(t) == 4 && strings.EqualFold(t[1], cosmosSQLDatabasesType) &&
		strings.EqualFold(t[2], cosmosContainersType) && strings.EqualFold(t[3], cosmosThroughputType):
		return cosmosKindContainerThroughput
	}

	return cosmosKindInvalid
}

func (p *CosmosProvider) accountOf(id ResourceID) (*storedCosmosAccount, error) {
	a, ok := p.owned(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrCosmosAccountNotFound
	}

	return a, nil
}

func throughputKey(account string, parts ...string) string {
	return strings.ToLower(account + "/" + strings.Join(parts, "/"))
}

func (p *CosmosProvider) putChild(id ResourceID, body map[string]any) (map[string]any, error) {
	kind := cosmosChildKindOf(id)
	if kind == cosmosKindInvalid {
		return nil, unsupportedCosmosType(id)
	}

	p.mu.Lock("PutChild")
	defer p.mu.Unlock()

	p.migrateLegacyThroughput()

	a, err := p.accountOf(id)
	if err != nil {
		return nil, err
	}

	switch kind {
	case cosmosKindDatabase:
		return p.putDatabase(id, a, body)
	case cosmosKindContainer:
		return p.putContainer(id, a, body)
	default:
		return p.putThroughput(id, a, kind, body)
	}
}

func (p *CosmosProvider) getChild(id ResourceID) (map[string]any, error) {
	kind := cosmosChildKindOf(id)
	if kind == cosmosKindInvalid {
		return nil, unsupportedCosmosType(id)
	}

	p.mu.RLock("GetChild")
	defer p.mu.RUnlock()

	a, err := p.accountOf(id)
	if err != nil {
		return nil, err
	}

	switch kind {
	case cosmosKindDatabase:
		return p.getDatabase(id, a)
	case cosmosKindContainer:
		return p.getContainer(id, a)
	default:
		return p.getThroughput(id, a, kind)
	}
}

func (p *CosmosProvider) deleteChild(id ResourceID) error {
	kind := cosmosChildKindOf(id)
	if kind == cosmosKindInvalid {
		return unsupportedCosmosType(id)
	}

	p.mu.Lock("DeleteChild")
	defer p.mu.Unlock()

	a, err := p.accountOf(id)
	if err != nil {
		return err
	}

	switch kind {
	case cosmosKindDatabase:
		return p.deleteDatabase(id, a)
	case cosmosKindContainer:
		return p.deleteContainer(id, a)
	default:
		return ErrResourceNotFound
	}
}

func (p *CosmosProvider) listChildren(id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("ListChildren")
	defer p.mu.RUnlock()

	a, err := p.accountOf(id)
	if err != nil {
		return nil, err
	}

	switch {
	case len(id.Types) == 2 && strings.EqualFold(id.Types[1], cosmosSQLDatabasesType):
		return p.listDatabases(a), nil
	case len(id.Types) == 3 && strings.EqualFold(id.Types[2], cosmosContainersType) && len(id.Names) == 2:
		return p.listContainers(id, a)
	}

	return nil, unsupportedCosmosType(id)
}

func (p *CosmosProvider) childID(a *storedCosmosAccount, types []string, names ...string) ResourceID {
	all := append([]string{a.name}, names...)

	return ResourceID{
		SubscriptionID: a.subscription, ResourceGroup: a.resourceGroup,
		Namespace: namespaceMicrosoftDocumentDB,
		Types:     append([]string{cosmosAccountsType}, types...), Names: all,
	}
}

// visible reports whether db may be seen through account a: unowned (data-plane created) or owned by a.
func (p *CosmosProvider) visible(a *storedCosmosAccount, db string) bool {
	owner := p.dbOwner[db]

	return owner == "" || owner == strings.ToLower(a.name)
}

func (p *CosmosProvider) putDatabase(
	id ResourceID,
	a *storedCosmosAccount,
	body map[string]any,
) (map[string]any, error) {
	name := id.LeafName()
	props, _ := body[fieldProperties].(map[string]any)

	if err := checkResourceID(props, name); err != nil {
		return nil, err
	}

	tp, err := parseThroughputOptions(props)
	if err != nil {
		return nil, err
	}

	if !p.visible(a, name) {
		return nil, ErrCosmosConflict
	}

	db, err := p.dataPlane.CreateDatabase(name)

	switch {
	case err == nil:
		p.dbOwner[name] = strings.ToLower(a.name)
	case errors.Is(err, ErrCosmosConflict):
		if db, err = p.dataPlane.GetDatabase(name); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}

	if tp != nil {
		if err = p.dataPlane.PutThroughput(name, "", *tp); err != nil {
			return nil, err
		}
	}

	return databaseBody(id, db), nil
}

func checkResourceID(props map[string]any, name string) error {
	res, _ := props[cosmosFieldResource].(map[string]any)

	got, _ := res[cosmosFieldID].(string)
	if got != "" && got != name {
		return fmt.Errorf("%w: resource id %q does not match name %q", ErrCosmosBadRequest, got, name)
	}

	return nil
}

func (p *CosmosProvider) getDatabase(id ResourceID, a *storedCosmosAccount) (map[string]any, error) {
	name := id.LeafName()
	if !p.visible(a, name) {
		return nil, ErrCosmosDatabaseNotFound
	}

	db, err := p.dataPlane.GetDatabase(name)
	if err != nil {
		return nil, err
	}

	return databaseBody(id, db), nil
}

func (p *CosmosProvider) deleteDatabase(id ResourceID, a *storedCosmosAccount) error {
	name := id.LeafName()
	if !p.visible(a, name) {
		return ErrCosmosDatabaseNotFound
	}

	if err := p.dataPlane.DeleteDatabase(name); err != nil {
		return err
	}

	delete(p.dbOwner, name)

	prefix := throughputKey(a.name, name)
	for k := range p.throughput {
		if k == prefix || strings.HasPrefix(k, prefix+"/") {
			delete(p.throughput, k)
		}
	}

	for k := range p.extras {
		if strings.HasPrefix(k, prefix+"/") {
			delete(p.extras, k)
		}
	}

	return nil
}

func (p *CosmosProvider) listDatabases(a *storedCosmosAccount) []map[string]any {
	dbs := p.dataPlane.ListDatabases()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].ID < dbs[j].ID })

	out := make([]map[string]any, 0, len(dbs))

	for _, db := range dbs {
		if p.visible(a, db.ID) {
			out = append(out, databaseBody(p.childID(a, []string{cosmosSQLDatabasesType}, db.ID), db))
		}
	}

	return out
}

func databaseBody(id ResourceID, db CosmosDatabase) map[string]any {
	return map[string]any{
		"id":      id.ARMID(),
		fieldName: db.ID,
		fieldType: namespaceMicrosoftDocumentDB + "/" + cosmosAccountsType + "/" + cosmosSQLDatabasesType,
		fieldProperties: map[string]any{
			cosmosFieldResource: map[string]any{
				cosmosFieldID: db.ID, "_rid": db.RID, "_ts": db.Modified.Unix(),
				"_self": "dbs/" + db.RID + "/", "_etag": etagOf(db.Modified),
				"_colls": "colls/", "_users": "users/",
			},
			cosmosFieldOptions: map[string]any{},
		},
	}
}

func etagOf(t interface{ UnixNano() int64 }) string {
	return strconv.Quote(strconv.FormatInt(t.UnixNano(), 16))
}

func numberOf(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case string:
		i, err := strconv.Atoi(n)

		return i, err == nil
	}

	return 0, false
}

// parseThroughputOptions reads properties.options; nil means no throughput was requested.
func parseThroughputOptions(props map[string]any) (*cosmosThroughput, error) {
	opts, _ := props[cosmosFieldOptions].(map[string]any)

	if auto, _ := opts["autoscaleSettings"].(map[string]any); auto != nil {
		return autoscaleThroughput(auto["maxThroughput"])
	}

	if raw, ok := opts["throughput"]; ok {
		return manualThroughput(raw)
	}

	return nil, nil //nolint:nilnil // nil means "not requested"
}

func manualThroughput(raw any) (*cosmosThroughput, error) {
	n, ok := numberOf(raw)
	if !ok || n < cosmosThroughputMin || n%cosmosThroughputStep != 0 {
		return nil, fmt.Errorf(
			"%w: throughput must be at least %d and a multiple of %d",
			ErrCosmosBadRequest, cosmosThroughputMin, cosmosThroughputStep)
	}

	return &cosmosThroughput{Throughput: n}, nil
}

func autoscaleThroughput(raw any) (*cosmosThroughput, error) {
	n, ok := numberOf(raw)
	if !ok || n < cosmosAutoscaleStep || n%cosmosAutoscaleStep != 0 {
		return nil, fmt.Errorf(
			"%w: autoscale maxThroughput must be at least %d and a multiple of %d",
			ErrCosmosBadRequest, cosmosAutoscaleStep, cosmosAutoscaleStep)
	}

	return &cosmosThroughput{AutoscaleMax: n}, nil
}

func (p *CosmosProvider) putThroughput(
	id ResourceID, a *storedCosmosAccount, kind cosmosChildKind, body map[string]any,
) (map[string]any, error) {
	if !strings.EqualFold(id.LeafName(), cosmosThroughputName) {
		return nil, ErrCosmosThroughputNotFound
	}

	db, container, err := p.throughputOwner(id, a, kind)
	if err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	res, _ := props[cosmosFieldResource].(map[string]any)

	tp, err := parseThroughputOptions(map[string]any{cosmosFieldOptions: res})
	if err != nil {
		return nil, err
	}

	if tp == nil {
		return nil, fmt.Errorf("%w: throughput or autoscaleSettings is required", ErrCosmosBadRequest)
	}

	if err = p.dataPlane.PutThroughput(db, container, *tp); err != nil {
		return nil, err
	}

	delete(p.throughput, legacyKey(a.name, db, container))

	return throughputBody(id, *tp), nil
}

// throughputOwner verifies the parent database or container exists and returns its data-plane names.
func (p *CosmosProvider) throughputOwner(
	id ResourceID, a *storedCosmosAccount, kind cosmosChildKind,
) (string, string, error) {
	db := id.Names[1]
	if !p.visible(a, db) {
		return "", "", ErrCosmosDatabaseNotFound
	}

	if _, err := p.dataPlane.GetDatabase(db); err != nil {
		return "", "", err
	}

	if kind == cosmosKindDatabaseThroughput {
		return db, "", nil
	}

	if _, err := p.dataPlane.GetContainer(db, id.Names[2]); err != nil {
		return "", "", err
	}

	return db, id.Names[2], nil
}

func (p *CosmosProvider) getThroughput(
	id ResourceID, a *storedCosmosAccount, kind cosmosChildKind,
) (map[string]any, error) {
	if !strings.EqualFold(id.LeafName(), cosmosThroughputName) {
		return nil, ErrCosmosThroughputNotFound
	}

	db, container, err := p.throughputOwner(id, a, kind)
	if err != nil {
		return nil, err
	}

	tp, err := p.dataPlane.GetThroughput(db, container)
	if errors.Is(err, ErrCosmosThroughputNotFound) {
		legacy, ok := p.throughput[legacyKey(a.name, db, container)]
		if !ok {
			return nil, err
		}

		tp, err = legacy, nil
	}

	if err != nil {
		return nil, err
	}

	return throughputBody(id, tp), nil
}

func throughputBody(id ResourceID, tp cosmosThroughput) map[string]any {
	res := map[string]any{"offerReplacePending": "false"}

	if tp.AutoscaleMax > 0 {
		res["autoscaleSettings"] = map[string]any{"maxThroughput": tp.AutoscaleMax}
		res["minimumThroughput"] = strconv.Itoa(tp.AutoscaleMax / cosmosAutoscaleMinDivisor)
	} else {
		res["throughput"] = tp.Throughput
		res["minimumThroughput"] = strconv.Itoa(cosmosThroughputMin)
	}

	return map[string]any{
		"id":            id.ARMID(),
		fieldName:       cosmosThroughputName,
		fieldType:       id.ResourceType(),
		fieldProperties: map[string]any{cosmosFieldResource: res},
	}
}

func (p *CosmosProvider) putContainer(
	id ResourceID,
	a *storedCosmosAccount,
	body map[string]any,
) (map[string]any, error) {
	db, name := id.Names[1], id.LeafName()
	if !p.visible(a, db) {
		return nil, ErrCosmosDatabaseNotFound
	}

	props, _ := body[fieldProperties].(map[string]any)

	spec, extras, err := parseContainerSpec(name, props)
	if err != nil {
		return nil, err
	}

	tp, err := parseThroughputOptions(props)
	if err != nil {
		return nil, err
	}

	c, err := p.dataPlane.PutContainer(db, spec)
	if err != nil {
		return nil, err
	}

	p.extras[throughputKey(a.name, db, name)] = extras

	if tp != nil {
		if err = p.dataPlane.PutThroughput(db, name, *tp); err != nil {
			return nil, err
		}
	}

	return p.containerBody(id, a, c), nil
}

func (p *CosmosProvider) getContainer(id ResourceID, a *storedCosmosAccount) (map[string]any, error) {
	db := id.Names[1]
	if !p.visible(a, db) {
		return nil, ErrCosmosDatabaseNotFound
	}

	c, err := p.dataPlane.GetContainer(db, id.LeafName())
	if err != nil {
		return nil, err
	}

	return p.containerBody(id, a, c), nil
}

func (p *CosmosProvider) deleteContainer(id ResourceID, a *storedCosmosAccount) error {
	db, name := id.Names[1], id.LeafName()
	if !p.visible(a, db) {
		return ErrCosmosDatabaseNotFound
	}

	if err := p.dataPlane.DeleteContainer(db, name); err != nil {
		return err
	}

	key := throughputKey(a.name, db, name)
	delete(p.throughput, key)
	delete(p.extras, key)

	return nil
}

func (p *CosmosProvider) listContainers(id ResourceID, a *storedCosmosAccount) ([]map[string]any, error) {
	db := id.Names[1]
	if !p.visible(a, db) {
		return nil, ErrCosmosDatabaseNotFound
	}

	cs, err := p.dataPlane.ListContainers(db)
	if err != nil {
		return nil, err
	}

	sort.Slice(cs, func(i, j int) bool { return cs[i].ID < cs[j].ID })

	out := make([]map[string]any, 0, len(cs))

	for _, c := range cs {
		cid := p.childID(a, []string{cosmosSQLDatabasesType, cosmosContainersType}, db, c.ID)
		out = append(out, p.containerBody(cid, a, c))
	}

	return out, nil
}

func parseContainerSpec(name string, props map[string]any) (CosmosContainer, cosmosContainerExtras, error) {
	res, _ := props[cosmosFieldResource].(map[string]any)

	if got, _ := res[cosmosFieldID].(string); got != name {
		return CosmosContainer{}, cosmosContainerExtras{},
			fmt.Errorf("%w: resource id %q does not match name %q", ErrCosmosBadRequest, got, name)
	}

	pk, _ := res["partitionKey"].(map[string]any)

	var paths []string

	for _, raw := range asList(pk[cosmosFieldPaths]) {
		if s, ok := raw.(string); ok && s != "" {
			paths = append(paths, s)
		}
	}

	if len(paths) == 0 {
		return CosmosContainer{}, cosmosContainerExtras{},
			fmt.Errorf("%w: partitionKey.paths is required", ErrCosmosBadRequest)
	}

	spec := CosmosContainer{ID: name, PartitionKeyPaths: paths}

	if ttl, ok := numberOf(res["defaultTtl"]); ok {
		spec.DefaultTTL = &ttl
	}

	spec.IndexingPolicy = rawJSON(res["indexingPolicy"])
	spec.UniqueKeyPolicy = rawJSON(res["uniqueKeyPolicy"])
	spec.ConflictResolutionPolicy = rawJSON(res["conflictResolutionPolicy"])

	extras := cosmosContainerExtras{}
	if v, ok := numberOf(pk["version"]); ok {
		extras.PartKeyVer = v
	}

	if v, ok := numberOf(res["analyticalStorageTtl"]); ok {
		extras.AnalyticalTTL = &v
	}

	return spec, extras, nil
}

func rawJSON(v any) []byte {
	if v == nil {
		return nil
	}

	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}

	return b
}

func jsonOr(raw []byte, def map[string]any) map[string]any {
	var m map[string]any
	if len(raw) > 0 && json.Unmarshal(raw, &m) == nil && m != nil {
		return m
	}

	return def
}

func fillIndexingPolicy(raw []byte) map[string]any {
	pol := jsonOr(raw, map[string]any{})

	if _, ok := pol["indexingMode"]; !ok {
		pol["indexingMode"] = "consistent"
	}

	if _, ok := pol["automatic"]; !ok {
		pol["automatic"] = true
	}

	if _, ok := pol["includedPaths"]; !ok {
		pol["includedPaths"] = []any{map[string]any{"path": "/*"}}
	}

	if _, ok := pol["excludedPaths"]; !ok {
		pol["excludedPaths"] = []any{map[string]any{"path": `/"_etag"/?`}}
	}

	return pol
}

func (p *CosmosProvider) containerBody(id ResourceID, a *storedCosmosAccount, c CosmosContainer) map[string]any {
	extras := p.extras[throughputKey(a.name, id.Names[1], c.ID)]

	ver := extras.PartKeyVer
	if ver == 0 {
		ver = cosmosPartKeyVersion
	}

	kind := "Hash"
	if len(c.PartitionKeyPaths) > 1 {
		kind = "MultiHash"
	}

	res := map[string]any{
		cosmosFieldID:    c.ID,
		"indexingPolicy": fillIndexingPolicy(c.IndexingPolicy),
		"partitionKey": map[string]any{
			cosmosFieldPaths: c.PartitionKeyPaths, "kind": kind, "version": ver,
		},
		"uniqueKeyPolicy": jsonOr(c.UniqueKeyPolicy, map[string]any{"uniqueKeys": []any{}}),
		"conflictResolutionPolicy": jsonOr(c.ConflictResolutionPolicy, map[string]any{
			"mode": "LastWriterWins", "conflictResolutionPath": "/_ts", "conflictResolutionProcedure": "",
		}),
		"_rid": c.RID, "_ts": c.Modified.Unix(), "_self": "colls/" + c.RID + "/", "_etag": etagOf(c.Modified),
	}

	if c.DefaultTTL != nil {
		res["defaultTtl"] = *c.DefaultTTL
	}

	if extras.AnalyticalTTL != nil {
		res["analyticalStorageTtl"] = *extras.AnalyticalTTL
	}

	return map[string]any{
		"id":      id.ARMID(),
		fieldName: c.ID,
		fieldType: namespaceMicrosoftDocumentDB + "/" + cosmosAccountsType + "/" + cosmosSQLDatabasesType +
			"/" + cosmosContainersType,
		fieldProperties: map[string]any{cosmosFieldResource: res, cosmosFieldOptions: map[string]any{}},
	}
}
