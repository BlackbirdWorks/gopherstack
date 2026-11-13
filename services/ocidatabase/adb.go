package ocidatabase

import (
	"net/netip"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	defaultBackupDays = 60
	reservedLow       = 3
)

func (b *InMemoryBackend) activeDB(id string) (*ADB, *APIError) {
	db, ok := b.dbs[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return db, nil
}

// liveNames lists databases that still hold their dbName and free tier slot.
func (b *InMemoryBackend) holdsName(db *ADB) bool {
	return db.State != stateTerminated && db.State != stateTerminating
}

func (b *InMemoryBackend) nameTaken(name, except string) bool {
	for _, db := range b.dbs {
		if db.ID != except && b.holdsName(db) && strs.Equal(db.DBName, name) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) freeCount() int {
	n := 0

	for _, db := range b.dbs {
		if db.IsFreeTier && b.holdsName(db) {
			n++
		}
	}

	return n
}

// prepared is a validated create request.
type prepared struct {
	db       *ADB
	password string
}

func (b *InMemoryBackend) prepareCreate(in createIn) (prepared, *APIError) {
	if aerr := b.checkCompartment(deref(in.CompartmentID, "")); aerr != nil {
		return prepared{}, aerr
	}

	if aerr := checkCreateScope(in); aerr != nil {
		return prepared{}, aerr
	}

	name := deref(in.DBName, "")
	if name == "" {
		return prepared{}, errMissing("dbName is required")
	}

	if !validDBName(name) {
		return prepared{}, errParam("dbName must begin with a letter and have at most 30 letters and digits")
	}

	if in.AdminPassword == nil {
		return prepared{}, errMissing("adminPassword is required")
	}

	if aerr := checkAdminPassword(*in.AdminPassword); aerr != nil {
		return prepared{}, aerr
	}

	db, aerr := b.buildCreate(in, name)
	if aerr != nil {
		return prepared{}, aerr
	}

	return prepared{db: db, password: *in.AdminPassword}, nil
}

func checkCreateScope(in createIn) *APIError {
	switch {
	case in.SecretID != nil:
		return errParam("secretId is not supported by the emulator; pass adminPassword")
	case in.Source != nil && *in.Source != "NONE":
		return errParam("source " + *in.Source + " is not supported; only NONE creates a database")
	case deref(in.IsDedicated, false):
		return errParam("dedicated Exadata infrastructure is not supported")
	case deref(in.IsDevTier, false):
		return errParam("isDevTier is not supported; use isFreeTier")
	}

	return nil
}

func (b *InMemoryBackend) buildCreate(in createIn, name string) (*ADB, *APIError) {
	free := deref(in.IsFreeTier, false)

	comp, aerr := resolveCompute(in.ComputeModel, in.ComputeCount, in.OcpuCount, in.CPUCoreCount)
	if aerr != nil {
		return nil, aerr
	}

	gbs, aerr := resolveStorage(in.StorageTBs, in.StorageGBs, gbPerTB)
	if aerr != nil {
		return nil, aerr
	}

	if free {
		if comp, gbs, aerr = freeShape(in, comp, gbs); aerr != nil {
			return nil, aerr
		}
	}

	db := &ADB{
		CompartmentID: *in.CompartmentID, DBName: name, DisplayName: deref(in.DisplayName, name),
		Workload: deref(in.DBWorkload, workloadOLTP), ComputeModel: comp.model, ComputeCount: comp.count,
		LicenseModel: deref(in.LicenseModel, licenseIncl), DBVersion: deref(in.DBVersion, versionNew),
		CharacterSet: deref(in.CharacterSet, charsetDef), NCharacterSet: deref(in.NcharacterSet, ncharsetDef),
		StorageGBs: gbs, BackupDays: deref(in.BackupDays, defaultBackupDays), IsFreeTier: free,
		AutoScaling: deref(in.AutoScaling, false), StorageScales: deref(in.StorageAutoScaling, false),
		Mtls: deref(in.Mtls, false), Whitelisted: in.Whitelisted, NsgIDs: in.NsgIDs,
		FreeformTags: in.FreeformTags, DefinedTags: in.DefinedTags, UniqueID: awsid.NewN("g", uniqueIDLen-1),
		Engine: b.eng != nil,
	}
	db.AccessControl = deref(in.AccessControl, len(in.Whitelisted) > 0)

	if aerr = b.checkCreateFields(db, in); aerr != nil {
		return nil, aerr
	}

	return db, b.placeEndpoint(db, deref(in.SubnetID, ""), deref(in.PrivateEndpointLabel, ""),
		deref(in.PrivateEndpointIP, ""))
}

func (b *InMemoryBackend) checkCreateFields(db *ADB, in createIn) *APIError {
	if aerr := checkWorkload(db.Workload); aerr != nil {
		return aerr
	}

	if aerr := checkVersion(db.DBVersion, db.Engine); aerr != nil {
		return aerr
	}

	if aerr := oneOf("licenseModel", db.LicenseModel, licenseIncl, licenseByol); aerr != nil {
		return aerr
	}

	if aerr := oneOf("ncharacterSet", db.NCharacterSet, ncharsetDef, "UTF8"); aerr != nil {
		return aerr
	}

	if aerr := checkACL(db.Whitelisted); aerr != nil {
		return aerr
	}

	if b.nameTaken(db.DBName, "") {
		return errExists("An Autonomous Database named " + db.DBName + " already exists in the tenancy")
	}

	if db.IsFreeTier {
		return b.checkFreeTier(db, in)
	}

	return nil
}

func (b *InMemoryBackend) checkFreeTier(db *ADB, in createIn) *APIError {
	switch {
	case deref(in.AutoScaling, false) || deref(in.StorageAutoScaling, false):
		return errParam("Always Free databases cannot use auto scaling")
	case deref(in.SubnetID, "") != "":
		return errParam("Always Free databases cannot use a private endpoint")
	case db.LicenseModel != licenseIncl:
		return errParam("Always Free databases use the LICENSE_INCLUDED license model")
	case b.freeCount() >= freeTierMax:
		return errLimit("The tenancy already has the maximum of 2 Always Free Autonomous Databases")
	}

	return nil
}

// freeShape pins the fixed Always Free compute and storage, rejecting any other explicit request.
func freeShape(in createIn, c computeSpec, gbs int) (computeSpec, int, *APIError) {
	want := float32(freeECPU)
	if c.model == modelOCPU {
		want = freeOCPU
	}

	asked := in.ComputeCount != nil || in.OcpuCount != nil || in.CPUCoreCount != nil
	if asked && c.count != want {
		return c, gbs, errParam("Always Free databases have a fixed compute count and cannot be scaled")
	}

	if (in.StorageTBs != nil || in.StorageGBs != nil) && gbs != gbPerTB && gbs != freeStorage {
		return c, gbs, errParam("Always Free databases have fixed storage and cannot be scaled")
	}

	c.count = want

	return c, freeStorage, nil
}

// placeEndpoint applies subnetId, privateEndpointLabel and privateEndpointIp. Callers hold the lock.
func (b *InMemoryBackend) placeEndpoint(db *ADB, subnetID, label, ip string) *APIError {
	if subnetID == "" {
		if label != "" || ip != "" {
			return errParam("privateEndpointLabel and privateEndpointIp need a subnetId")
		}

		db.SubnetID, db.EndpointLabel, db.EndpointIP = "", "", ""

		return nil
	}

	var info SubnetInfo

	if b.subnet != nil {
		var ok bool
		if info, ok = b.subnet(subnetID); !ok {
			return errNotFound(subnetID)
		}
	}

	addr, aerr := b.endpointIP(db.ID, subnetID, ip, info.CIDR)
	if aerr != nil {
		return aerr
	}

	db.SubnetID, db.EndpointLabel, db.EndpointIP = subnetID, label, addr

	return nil
}

func (b *InMemoryBackend) endpointIP(self, subnetID, want, cidr string) (string, *APIError) {
	if cidr == "" {
		if want != "" {
			return "", errParam("privateEndpointIp cannot be checked without OCI Core")
		}

		return "", nil
	}

	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return "", nil //nolint:nilerr // an unparseable CIDR from OCI Core assigns no address
	}

	used := b.usedAddrs(self, subnetID)

	if want != "" {
		return checkWantedIP(prefix, want, used)
	}

	a := prefix.Masked().Addr()
	for range reservedLow {
		a = a.Next()
	}

	for ; prefix.Contains(a); a = a.Next() {
		if !used[a] {
			return a.String(), nil
		}
	}

	return "", errLimit("The subnet has no free addresses")
}

func (b *InMemoryBackend) usedAddrs(self, subnetID string) map[netip.Addr]bool {
	used := map[netip.Addr]bool{}

	for _, o := range b.dbs {
		if o.ID == self || o.SubnetID != subnetID || !b.holdsName(o) {
			continue
		}

		if a, err := netip.ParseAddr(o.EndpointIP); err == nil {
			used[a] = true
		}
	}

	return used
}

func checkWantedIP(prefix netip.Prefix, want string, used map[netip.Addr]bool) (string, *APIError) {
	a, err := netip.ParseAddr(want)
	if err != nil || !prefix.Contains(a) {
		return "", errParam("privateEndpointIp must be an address inside the subnet " + prefix.String())
	}

	if used[a] {
		return "", errConflict("privateEndpointIp " + want + " is already in use")
	}

	return want, nil
}

// existingByToken returns the result of an earlier create with the same retry token.
func (b *InMemoryBackend) existingByToken(token string) (adbWire, string, string, bool) {
	wr := b.workReqs[b.tokens[token]]
	if wr == nil {
		return adbWire{}, "", "", false
	}

	db := b.dbs[wr.ResourceID]
	if db == nil {
		return adbWire{}, "", "", false
	}

	return b.view(db), wr.ID, db.Etag, true
}

// CreateDatabase provisions an Autonomous Database and returns it with its work request.
func (b *InMemoryBackend) CreateDatabase(in createIn, token string) (adbWire, string, string, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if token != "" {
		if view, wrID, etag, ok := b.existingByToken(token); ok {
			return view, wrID, etag, nil
		}
	}

	p, aerr := b.prepareCreate(in)
	if aerr != nil {
		return adbWire{}, "", "", aerr
	}

	db := p.db
	db.ID, db.Created, db.Seq = newOCID("autonomousdatabase"), b.nowUTC(), b.nextSeq()

	if b.engineOn(db) {
		if _, err := b.securityLocked(db.ID); err != nil {
			return adbWire{}, "", "", err
		}

		b.eng.passwords[db.ID] = p.password
	}

	b.dbs[db.ID] = db
	wr := b.begin(db, step{
		typ: "CREATE_AUTONOMOUS_DATABASE", action: "CREATED", state: statePending, target: stateAvailable,
		work: b.launchWork(db.ID),
	})

	if token != "" {
		b.tokens[token] = wr.ID
	}

	return b.view(db), wr.ID, db.Etag, nil
}

// GetDatabase returns one Autonomous Database with its etag.
func (b *InMemoryBackend) GetDatabase(id string) (adbWire, string, error) {
	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	db, aerr := b.activeDB(id)
	if aerr != nil {
		return adbWire{}, "", aerr
	}

	return b.view(db), db.Etag, nil
}

// DatabaseFilter selects Autonomous Databases in a list.
type DatabaseFilter struct {
	FreeTier    *bool
	Compartment string
	State       string
	StateNot    string
	Workload    string
	DBVersion   string
	DisplayName string
}

// ListDatabases lists the Autonomous Databases of a compartment.
func (b *InMemoryBackend) ListDatabases(f DatabaseFilter, o ListOpts) (page.Page[adbWire], error) {
	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(f.Compartment); aerr != nil {
		return page.Page[adbWire]{}, aerr
	}

	var all []*ADB

	for _, db := range b.dbs {
		if db.CompartmentID == f.Compartment && f.matches(db) {
			all = append(all, db)
		}
	}

	pg := paginate(all, o,
		func(d *ADB) string { return d.DisplayName },
		func(d *ADB) (time.Time, int64) { return d.Created, d.Seq },
	)

	out := make([]adbWire, 0, len(pg.Data))
	for _, db := range pg.Data {
		out = append(out, b.view(db))
	}

	return page.Page[adbWire]{Data: out, Next: pg.Next}, nil
}

func (f DatabaseFilter) matches(db *ADB) bool {
	switch {
	case f.State != "" && db.State != f.State:
		return false
	case f.StateNot != "" && db.State == f.StateNot:
		return false
	case f.Workload != "" && db.Workload != f.Workload:
		return false
	case f.DBVersion != "" && db.DBVersion != f.DBVersion:
		return false
	case f.DisplayName != "" && db.DisplayName != f.DisplayName:
		return false
	case f.FreeTier != nil && db.IsFreeTier != *f.FreeTier:
		return false
	}

	return true
}
