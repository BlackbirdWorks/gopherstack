package azurearm

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	flexMinRetentionMax = 35
	flexMinPassword     = 8
	flexMaxPassword     = 128
	flexMinPasswordCats = 3
	flexMaxZone         = 3
	flexModeDefault     = "Default"
	flexModeReplica     = "Replica"
	flexModePITR        = "PointInTimeRestore"
	flexModeGeo         = "GeoRestore"
	flexHADisabled      = "Disabled"
	flexHAZoneRedundant = "ZoneRedundant"
	flexHASameZone      = "SameZone"
	flexRoleNone        = "None"
	flexClockSkew       = time.Minute
)

var flexNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{1,61}[a-z0-9])$`)

func flexMap(m map[string]any, k string) map[string]any {
	v, _ := m[k].(map[string]any)

	return v
}

func flexStr(m map[string]any, k string) (string, bool) {
	v, ok := m[k].(string)

	return v, ok
}

func flexInt(m map[string]any, k string) (int, bool) {
	v, ok := m[k].(float64)

	return int(v), ok
}

func validFlexName(name string) error {
	if !flexNameRE.MatchString(name) {
		return flexInvalid("The server name must be 3 to 63 characters of lowercase letters, digits and hyphens, " +
			"and cannot start or end with a hyphen.")
	}

	return nil
}

func (f *flexFlavor) validLogin(login string) error {
	if !f.loginRE.MatchString(login) {
		return flexInvalid("The administrator login is not valid: use up to %d characters; %s.", f.maxLoginLen,
			f.loginRule)
	}

	for _, r := range f.reservedLogins {
		if strings.EqualFold(r, login) {
			return flexInvalid("The administrator login '%s' is reserved.", login)
		}
	}

	return nil
}

func validFlexPassword(pw string) error {
	if len(pw) < flexMinPassword || len(pw) > flexMaxPassword {
		return flexInvalid("The password must be between %d and %d characters long.", flexMinPassword, flexMaxPassword)
	}

	var upper, lower, digit, other bool

	for _, r := range pw {
		switch {
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsLower(r):
			lower = true
		case unicode.IsDigit(r):
			digit = true
		default:
			other = true
		}
	}

	n := 0

	for _, b := range []bool{upper, lower, digit, other} {
		if b {
			n++
		}
	}

	if n < flexMinPasswordCats {
		return flexInvalid("The password must contain characters from three of the following categories: " +
			"uppercase letters, lowercase letters, digits and non-alphanumeric characters.")
	}

	return nil
}

func (p *FlexDBProvider) writeServer(id ResourceID, body map[string]any, patch bool) (flexResult, error) {
	props := flexMap(body, fieldProperties)
	key := p.key(id)

	p.mu.Lock("writeServer")
	defer p.mu.Unlock()

	cur, exists := p.res[key]

	var rec *flexServer

	switch {
	case exists:
		if p.opPending(cur.OpID) {
			return flexResult{}, p.busy(cur.Name, "Update")
		}

		if p.stateOf(cur) != flexStateReady {
			return flexResult{}, p.notReady(cur, "Update")
		}

		rec = cloneFlexServer(cur)
	case patch:
		return flexResult{}, p.notFound(id, "")
	default:
		var err error
		if rec, err = p.newServer(id, body, props); err != nil {
			return flexResult{}, err
		}
	}

	pw, err := p.applyServer(rec, body, props, exists)
	if err != nil {
		return flexResult{}, err
	}

	if err = p.applyRoleChange(rec, props); err != nil {
		return flexResult{}, err
	}

	if !exists && rec.CreateMode == flexModeDefault && pw == "" {
		return flexResult{}, flexInvalid("Invalid value given for parameter administratorLoginPassword.")
	}

	run := p.serverJob(key, rec, pw, exists)

	rec.Transition, rec.State = "Starting", flexStateReady
	if exists {
		rec.Transition = "Updating"
	}

	rec.OpID = p.newOp(flexServerID(p.flavor.ns, id).ARMID(), true, key, run)
	p.res[key] = rec

	return flexResult{body: p.serverBody(id, rec), opID: rec.OpID}, nil
}

// serverJob returns the engine work for a create or update, or nil when there is none.
func (p *FlexDBProvider) serverJob(key string, rec *flexServer, pw string, exists bool) func(context.Context) error {
	eng := p.engine
	if eng == nil {
		return nil
	}

	spec := FlexSpec{Version: rec.Version, AdminLogin: rec.Login, AdminPassword: pw}
	dbs := p.userDatabases(rec)

	switch {
	case !exists:
		return func(ctx context.Context) error {
			if _, err := eng.Ensure(ctx, key, spec); err != nil {
				return err //nolint:wrapcheck // engine errors are reduced to a fixed message by the operation
			}

			return p.createEngineDatabases(ctx, eng, key, dbs)
		}
	case pw != "":
		return func(ctx context.Context) error { return eng.SetPassword(ctx, key, rec.Login, pw) }
	}

	return nil
}

func (p *FlexDBProvider) userDatabases(rec *flexServer) []flexDatabase {
	var out []flexDatabase

	for _, d := range rec.Databases {
		if !p.flavor.isSystemDB(d.Name) {
			out = append(out, d)
		}
	}

	slices.SortFunc(out, func(a, b flexDatabase) int { return strings.Compare(a.Name, b.Name) })

	return out
}

func (p *FlexDBProvider) createEngineDatabases(
	ctx context.Context,
	eng FlexEngine,
	key string,
	dbs []flexDatabase,
) error {
	for _, d := range dbs {
		if err := eng.CreateDatabase(ctx, key, d.Name, d.Charset, d.Collation); err != nil {
			return err //nolint:wrapcheck // reduced to a fixed message by the operation
		}
	}

	return nil
}

func (p *FlexDBProvider) newServer(id ResourceID, body, props map[string]any) (*flexServer, error) {
	name := id.LeafName()
	if err := validFlexName(name); err != nil {
		return nil, err
	}

	for _, r := range p.res {
		if strings.EqualFold(r.Name, name) {
			return nil, flexConflict(flexCodeConflict,
				"The server name '"+name+"' is already in use; server names must be unique.")
		}
	}

	loc, _ := flexStr(body, fieldLocation)
	if loc == "" {
		loc = DefaultLocation
	}

	now := p.now().UTC()
	rec := &flexServer{
		Name: name, ResourceGrp: id.ResourceGroup, Location: loc, CreatedAt: now, State: flexStateReady,
		Version: p.flavor.defaultVersion, HAMode: flexHADisabled, AutoGrow: flexDisabled, GeoBackup: flexDisabled,
		PublicAcc: flexEnabled, StorageGB: p.flavor.defaultStorage, Retention: flexDefaultRetention,
		CreateMode: flexModeDefault, Databases: map[string]flexDatabase{}, Firewall: map[string]flexFirewall{},
	}

	if p.flavor.kind == flexKindMySQL {
		rec.AutoGrow = flexEnabled
	}

	for _, s := range p.flavor.systemDBs {
		rec.Databases[strings.ToLower(s)] = flexDatabase{Name: s, Charset: p.flavor.defaultCharset,
			Collation: p.flavor.defaultCollate}
	}

	mode, _ := flexStr(props, "createMode")
	if err := p.applyCreateMode(rec, mode, props); err != nil {
		return nil, err
	}

	return rec, nil
}

const flexDefaultRetention = 7

func (p *FlexDBProvider) applyCreateMode(rec *flexServer, mode string, props map[string]any) error {
	switch mode {
	case "", flexModeDefault, "Create":
		return nil
	case flexModeReplica, flexModePITR, flexModeGeo:
		rec.CreateMode = mode
	default:
		return flexInvalid(
			"createMode '%s' is not valid; use Default, PointInTimeRestore, GeoRestore or Replica.",
			mode,
		)
	}

	if p.engine != nil {
		return flexInvalid("createMode '%s' is not supported by the docker database engine: a replica or restored "+
			"server would start without the source's data.", mode)
	}

	srcID, _ := flexStr(props, "sourceServerResourceId")

	srcKey, src, err := p.sourceServer(srcID)
	if err != nil {
		return err
	}

	rec.SourceID = srcKey

	return p.inheritFrom(rec, src, props)
}

func (p *FlexDBProvider) sourceServer(srcID string) (string, *flexServer, error) {
	rid, err := ParseGenericResourcePath(srcID)
	if err != nil || !strings.EqualFold(rid.Namespace, p.flavor.ns) || len(rid.Types) != 1 ||
		!strings.EqualFold(rid.Types[0], flexServersType) {
		return "", nil, flexInvalid("sourceServerResourceId must be the resource ID of a %s flexible server.",
			p.flavor.ns)
	}

	src, ok := p.res[p.key(rid)]
	if !ok {
		return "", nil, flexInvalid("The source server '%s' was not found.", rid.Names[0])
	}

	return p.key(rid), src, nil
}

func (p *FlexDBProvider) replicasOf(key string) []*flexServer {
	var out []*flexServer

	for _, r := range p.res {
		if r.CreateMode == flexModeReplica && r.SourceID == key {
			out = append(out, r)
		}
	}

	return out
}

func (p *FlexDBProvider) inheritFrom(rec, src *flexServer, props map[string]any) error {
	if rec.CreateMode == flexModeReplica {
		if err := p.checkReplicaSource(src); err != nil {
			return err
		}

		if len(p.replicasOf(rec.SourceID)) >= p.flavor.maxReplicas {
			return flexInvalid("The source server already has the maximum of %d replicas.", p.flavor.maxReplicas)
		}
	} else if err := p.checkRestorePoint(rec, src, props); err != nil {
		return err
	}

	rec.SKUName, rec.SKUTier, rec.Version, rec.Login = src.SKUName, src.SKUTier, src.Version, src.Login
	rec.StorageGB, rec.Iops, rec.AutoGrow = src.StorageGB, src.Iops, src.AutoGrow
	rec.Retention, rec.GeoBackup = src.Retention, src.GeoBackup
	rec.Subnet, rec.DNSZone, rec.PublicAcc = src.Subnet, src.DNSZone, src.PublicAcc

	if rec.CreateMode != flexModeReplica {
		rec.Databases, rec.Config = maps.Clone(src.Databases), cloneStrMap(src.Config)
	} else {
		rec.Databases = maps.Clone(src.Databases)
	}

	return nil
}

func (p *FlexDBProvider) checkRestorePoint(rec, src *flexServer, props map[string]any) error {
	if rec.CreateMode == flexModeGeo {
		if src.GeoBackup != flexEnabled {
			return flexInvalid("The source server does not have geo-redundant backup enabled.")
		}

		return nil
	}

	raw, _ := flexStr(props, p.flavor.sourceTimeField)
	if raw == "" {
		return flexInvalid("%s is required for createMode PointInTimeRestore.", p.flavor.sourceTimeField)
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return flexInvalid("%s must be an RFC 3339 timestamp.", p.flavor.sourceTimeField)
	}

	if t.Before(src.CreatedAt) || t.After(p.now().Add(flexClockSkew)) {
		return flexInvalid("The restore point must be between the source server's creation time and now.")
	}

	rec.PointInTime = t.UTC()

	return nil
}

// applyServer merges the request onto rec and returns the administrator password it carried, if any.
func (p *FlexDBProvider) applyServer(rec *flexServer, body, props map[string]any, exists bool) (string, error) {
	if t, ok := body[fieldTags]; ok {
		rec.Tags = stringTags(t)
	}

	if id := flexMap(body, "identity"); id != nil {
		rec.Identity = id
	}

	if err := p.applySKU(rec, flexMap(body, "sku")); err != nil {
		return "", err
	}

	pw, err := p.applyIdentityProps(rec, props, exists)
	if err != nil {
		return "", err
	}

	for _, step := range []func(*flexServer, map[string]any, bool) error{
		p.applyStorage, p.applyBackup, p.applyHA, p.applyNetwork, p.applyMisc,
	} {
		if err = step(rec, props, exists); err != nil {
			return "", err
		}
	}

	if rec.SKUName == "" {
		return "", flexInvalid("sku.name and sku.tier are required.")
	}

	if err = p.checkInvariants(rec); err != nil {
		return "", err
	}

	return pw, nil
}

func (p *FlexDBProvider) checkInvariants(rec *flexServer) error {
	ha := rec.HAMode != flexHADisabled

	switch {
	case ha && rec.SKUTier == tierBurstable:
		return flexInvalid("High availability is not supported on the Burstable compute tier.")
	case ha && p.flavor.kind == flexKindPostgres && rec.CreateMode == flexModeReplica:
		return flexInvalid("High availability is not supported on a read replica.")
	case rec.StorageGB > p.flavor.storageCap(rec.SKUTier):
		return flexInvalid("The %s compute tier supports at most %d GiB of storage.", rec.SKUTier,
			p.flavor.storageCap(rec.SKUTier))
	case ha && p.flavor.kind == flexKindMySQL && rec.AutoGrow == flexDisabled:
		return flexInvalid("Storage autogrow cannot be disabled on a server with high availability.")
	}

	return nil
}

func (p *FlexDBProvider) applySKU(rec *flexServer, sku map[string]any) error {
	if sku == nil {
		return nil
	}

	name, _ := flexStr(sku, "name")
	tier, _ := flexStr(sku, "tier")

	s, ok := p.flavor.skuByName(name)
	if !ok {
		return flexInvalid("The compute SKU '%s' is not supported; supported SKUs: %s.", name,
			strings.Join(flexNames(p.flavor.skus), ", "))
	}

	if tier != "" && tier != s.tier {
		return flexInvalid("The SKU '%s' belongs to the %s tier, not %s.", name, s.tier, tier)
	}

	rec.SKUName, rec.SKUTier = s.name, s.tier

	return nil
}

func (p *FlexDBProvider) applyIdentityProps(rec *flexServer, props map[string]any, exists bool) (string, error) {
	if err := p.applyVersion(rec, props, exists); err != nil {
		return "", err
	}

	if v, ok := flexStr(props, "administratorLogin"); ok && v != "" {
		if exists && v != rec.Login {
			return "", flexInvalid("The administrator login cannot be changed after the server is created.")
		}

		if err := p.flavor.validLogin(v); err != nil {
			return "", err
		}

		rec.Login = v
	}

	pw, _ := flexStr(props, "administratorLoginPassword")
	if pw != "" {
		if err := validFlexPassword(pw); err != nil {
			return "", err
		}
	}

	if !exists && rec.CreateMode == flexModeDefault && rec.Login == "" {
		return "", flexInvalid("Invalid value given for parameter administratorLogin.")
	}

	return pw, nil
}

func (p *FlexDBProvider) applyVersion(rec *flexServer, props map[string]any, exists bool) error {
	v, ok := flexStr(props, "version")
	if !ok || v == "" {
		return nil
	}

	if !p.flavor.validVersion(v) {
		return flexInvalid("The server version '%s' is not supported; supported versions: %s.", v,
			strings.Join(p.flavor.versions, ", "))
	}

	if exists && v != rec.Version {
		if err := p.flavor.checkUpgrade(rec.Version, v); err != nil {
			return err
		}
	}

	rec.Version = v

	return nil
}

func (f *flexFlavor) checkUpgrade(from, to string) error {
	if f.kind != flexKindPostgres {
		return flexInvalid("The major version of a MySQL flexible server cannot be changed.")
	}

	a, _ := strconv.Atoi(from)
	b, _ := strconv.Atoi(to)

	if b < a {
		return flexInvalid("The server version cannot be downgraded.")
	}

	return nil
}

func (p *FlexDBProvider) applyStorage(rec *flexServer, props map[string]any, exists bool) error {
	st := flexMap(props, "storage")
	if st == nil {
		return nil
	}

	f := p.flavor

	if gb, ok := flexInt(st, "storageSizeGB"); ok {
		if !f.validStorage(gb) {
			return flexInvalid("storageSizeGB %d is not valid; allowed: %s.", gb, f.storageRule())
		}

		if top := f.storageCap(rec.SKUTier); gb > top {
			return flexInvalid(
				"storageSizeGB %d exceeds the %d GiB maximum of the %s compute tier.",
				gb,
				top,
				rec.SKUTier,
			)
		}

		if exists && gb < rec.StorageGB {
			return flexInvalid("The storage size cannot be reduced.")
		}

		rec.StorageGB = gb
	}

	if v, ok := flexStr(st, "autoGrow"); ok && v != "" {
		if v != flexEnabled && v != flexDisabled {
			return flexInvalid("storage.autoGrow must be Enabled or Disabled.")
		}

		rec.AutoGrow = v
	}

	rec.Iops = 0

	if iops, ok := flexInt(st, "iops"); ok && f.kind == flexKindMySQL {
		if top := p.skuMaxIops(rec); iops < mysqlBaseIops || iops > top {
			return flexInvalid("storage.iops must be between %d and %d.", mysqlBaseIops, top)
		}

		rec.Iops = iops
	}

	return nil
}

func (f *flexFlavor) storageRule() string {
	if f.storageSizes != nil {
		parts := make([]string, len(f.storageSizes))
		for i, s := range f.storageSizes {
			parts[i] = strconv.Itoa(s)
		}

		return strings.Join(parts, ", ")
	}

	return strconv.Itoa(f.minStorage) + " to " + strconv.Itoa(f.maxStorage)
}

func (p *FlexDBProvider) applyBackup(rec *flexServer, props map[string]any, _ bool) error {
	b := flexMap(props, "backup")
	if b == nil {
		return nil
	}

	if d, ok := flexInt(b, "backupRetentionDays"); ok {
		if d < p.flavor.minRetention || d > flexMinRetentionMax {
			return flexInvalid("backupRetentionDays must be between %d and %d.", p.flavor.minRetention,
				flexMinRetentionMax)
		}

		rec.Retention = d
	}

	if v, ok := flexStr(b, "geoRedundantBackup"); ok && v != "" {
		if v != flexEnabled && v != flexDisabled {
			return flexInvalid("backup.geoRedundantBackup must be Enabled or Disabled.")
		}

		rec.GeoBackup = v
	}

	return nil
}

func validZone(z string) bool {
	if z == "" {
		return true
	}

	n, err := strconv.Atoi(z)

	return err == nil && n >= 1 && n <= flexMaxZone
}

func (p *FlexDBProvider) applyHA(rec *flexServer, props map[string]any, exists bool) error {
	if z, ok := flexStr(props, "availabilityZone"); ok {
		if !validZone(z) {
			return flexInvalid("availabilityZone must be 1, 2 or 3.")
		}

		rec.Zone = z
	}

	ha := flexMap(props, "highAvailability")
	if ha == nil {
		return nil
	}

	if mode, ok := flexStr(ha, "mode"); ok && mode != "" {
		if !slices.Contains([]string{flexHADisabled, flexHAZoneRedundant, flexHASameZone}, mode) {
			return flexInvalid("highAvailability.mode must be Disabled, ZoneRedundant or SameZone.")
		}

		if exists && p.flavor.kind == flexKindMySQL && mode == flexHAZoneRedundant && rec.HAMode != mode {
			return flexInvalid("Zone-redundant high availability can only be set when the server is created.")
		}

		rec.HAMode = mode
	}

	if z, ok := flexStr(ha, "standbyAvailabilityZone"); ok {
		if !validZone(z) || (z != "" && z == rec.Zone) {
			return flexInvalid("standbyAvailabilityZone must be 1, 2 or 3 and differ from availabilityZone.")
		}

		rec.StandbyZone = z
	}

	return nil
}

func (p *FlexDBProvider) applyNetwork(rec *flexServer, props map[string]any, exists bool) error {
	n := flexMap(props, "network")
	if n == nil {
		return nil
	}

	if err := p.applySubnet(rec, n, exists); err != nil {
		return err
	}

	pub, hasPub := flexStr(n, "publicNetworkAccess")
	if hasPub && pub != "" && pub != flexEnabled && pub != flexDisabled {
		return flexInvalid("network.publicNetworkAccess must be Enabled or Disabled.")
	}

	switch {
	case rec.Subnet != "":
		if pub == flexEnabled {
			return flexInvalid("Public network access cannot be enabled on a server with virtual network integration.")
		}

		rec.PublicAcc = flexDisabled
	case hasPub && pub != "":
		rec.PublicAcc = pub
	}

	return nil
}

func (p *FlexDBProvider) applySubnet(rec *flexServer, n map[string]any, exists bool) error {
	subnet, hasSubnet := flexStr(n, "delegatedSubnetResourceId")
	dns, _ := flexStr(n, "privateDnsZoneArmResourceId")

	if d, ok := flexStr(n, "privateDnsZoneResourceId"); ok {
		dns = d
	}

	if !hasSubnet || subnet == rec.Subnet {
		return nil
	}

	if exists {
		return flexInvalid("The connectivity method cannot be changed after the server is created.")
	}

	if subnet != "" {
		if err := p.checkSubnet(subnet, dns); err != nil {
			return err
		}
	}

	rec.Subnet, rec.DNSZone = subnet, dns

	return nil
}

func (p *FlexDBProvider) checkSubnet(subnet, dns string) error {
	rid, err := ParseGenericResourcePath(subnet)
	if err != nil || !strings.EqualFold(rid.Namespace, namespaceMicrosoftNetwork) || len(rid.Types) != 2 ||
		!strings.EqualFold(rid.Types[1], netSubnets) {
		return flexInvalid("delegatedSubnetResourceId must be the resource ID of a virtual network subnet.")
	}

	if dns == "" {
		return flexInvalid("privateDnsZoneArmResourceId is required with virtual network integration.")
	}

	if !strings.HasSuffix(strings.ToLower(dns), p.flavor.dnsZoneSuffix) {
		return flexInvalid("The private DNS zone name must end with '%s'.", p.flavor.dnsZoneSuffix)
	}

	if p.network == nil {
		return nil
	}

	found, delegated := p.network.subnetDelegatedTo(subnet, p.flavor.subnetService)

	switch {
	case !found:
		return flexInvalid("The subnet '%s' was not found.", rid.Names[len(rid.Names)-1])
	case !delegated:
		return flexInvalid("The subnet must be delegated to %s.", p.flavor.subnetService)
	}

	if bits, ok := p.network.subnetPrefixBits(subnet); ok && bits > p.flavor.maxSubnetBits {
		return flexInvalid("The delegated subnet must be at least a /%d range.", p.flavor.maxSubnetBits)
	}

	return nil
}

func (p *FlexDBProvider) applyMisc(rec *flexServer, props map[string]any, _ bool) error {
	if mw := flexMap(props, "maintenanceWindow"); mw != nil {
		rec.Maintenance = mw
	}

	if enc := flexMap(props, "dataEncryption"); enc != nil {
		rec.Encryption = enc
	}

	if ac := flexMap(props, "authConfig"); ac != nil && p.flavor.hasAuthConfig {
		rec.AuthConfig = ac
	}

	return nil
}

// applyRoleChange promotes a replica when the request sets its role to None (or the PostgreSQL replica block
// asks to promote it).
func (p *FlexDBProvider) applyRoleChange(rec *flexServer, props map[string]any) error {
	role, _ := flexStr(props, "replicationRole")

	if rep := flexMap(props, "replica"); rep != nil {
		if r, ok := flexStr(rep, "role"); ok && r != "" {
			role = r
		}
	}

	if role == "" {
		return nil
	}

	isReplica := rec.CreateMode == flexModeReplica && rec.SourceID != ""

	switch {
	case role == flexRoleNone || role == flexRolePrimary && isReplica:
		if !isReplica {
			return nil
		}

		rec.SourceID = ""
	case role == p.flavor.roleReplica && isReplica, role == p.flavor.roleSource, role == flexRolePrimary:
	default:
		return flexInvalid("replicationRole '%s' cannot be set on this server.", role)
	}

	return nil
}

func (p *FlexDBProvider) deleteServer(id ResourceID) (string, error) {
	key := p.key(id)

	p.mu.Lock("deleteServer")
	defer p.mu.Unlock()

	if _, ok := p.res[key]; !ok {
		return "", p.notFound(id, "")
	}

	delete(p.res, key)

	for _, r := range p.replicasOf(key) {
		r.SourceID = ""
	}

	var run func(context.Context) error
	if eng := p.engine; eng != nil {
		run = func(ctx context.Context) error { return eng.Delete(ctx, key) }
	}

	return p.newOp(flexServerID(p.flavor.ns, id).ARMID(), false, key, run), nil
}

// Action runs start, stop or restart and returns the operation it started.
func (p *FlexDBProvider) Action(id ResourceID, action string) (string, error) {
	key := p.key(id)

	p.mu.Lock("Action")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return "", err
	}

	if p.opPending(rec.OpID) {
		return "", p.busy(rec.Name, flexActionName(action))
	}

	state := p.stateOf(rec)
	next := *rec

	var run func(context.Context) error

	eng := p.engine

	switch action {
	case aksActionStop:
		if err = p.checkStop(rec, key); err != nil {
			return "", err
		}

		if state == flexStateStopped {
			return p.newOp(id.ARMID(), false, key, nil), nil
		}

		next.State, next.Transition = flexStateStopped, "Stopping"
		next.Pending = nil

		if eng != nil {
			run = func(ctx context.Context) error { return eng.Stop(ctx, key) }
		}
	case aksActionStart:
		if state == flexStateReady {
			return p.newOp(id.ARMID(), false, key, nil), nil
		}

		next.State, next.Transition = flexStateReady, "Starting"
		next.Pending = nil

		if eng != nil {
			run = func(ctx context.Context) error { return eng.Start(ctx, key) }
		}
	default:
		if state != flexStateReady {
			return "", p.notReady(rec, "Restart or Stop")
		}

		next.Transition = "Updating"
		next.Pending = nil

		if eng != nil {
			run = func(ctx context.Context) error { return eng.Restart(ctx, key) }
		}
	}

	next.OpID = p.newOp(id.ARMID(), false, key, run)
	*rec = next

	return next.OpID, nil
}

func (p *FlexDBProvider) checkStop(rec *flexServer, key string) error {
	if p.flavor.kind == flexKindMySQL &&
		(len(p.replicasOf(key)) > 0 || rec.CreateMode == flexModeReplica && rec.SourceID != "") {
		return flexInvalid("A server that has or is a read replica cannot be stopped.")
	}

	return nil
}

func flexActionName(action string) string {
	switch action {
	case aksActionStop:
		return "Stop"
	case aksActionStart:
		return "Start"
	}

	return "Restart"
}

// busy and notReady use the wording shown for stop and restart in
// https://learn.microsoft.com/en-us/azure/postgresql/configure-maintain/how-to-stop-server.
func (p *FlexDBProvider) busy(name, op string) error {
	return flexConflict(flexCodeBusy, fmt.Sprintf(
		"Cannot perform '%s' server operation because server '%s' is busy processing other operation.", op, name))
}

func (p *FlexDBProvider) notReady(rec *flexServer, op string) error {
	return flexConflict(flexCodeNotReady, fmt.Sprintf(
		"%s Server can only be performed on Started servers. Server Name = %s, Current Server State = %s",
		op, rec.Name, p.stateOf(rec)))
}

// checkReplicaSource applies the documented replica rules: no Burstable source, and a replica as source only on
// PostgreSQL 14+ and only one level deep.
func (p *FlexDBProvider) checkReplicaSource(src *flexServer) error {
	if src.SKUTier == tierBurstable {
		return flexInvalid("Read replicas are not supported on the Burstable compute tier.")
	}

	if src.CreateMode != flexModeReplica || src.SourceID == "" {
		return nil
	}

	root, ok := p.res[src.SourceID]
	if p.flavor.kind != flexKindPostgres || (ok && root.CreateMode == flexModeReplica && root.SourceID != "") {
		return flexInvalid("A replica cannot be the source of another replica.")
	}

	if v, err := strconv.Atoi(src.Version); err != nil || v < pgMinCascadeVersion {
		return flexInvalid("Cascading read replicas require PostgreSQL version %d or later.", pgMinCascadeVersion)
	}

	return nil
}
