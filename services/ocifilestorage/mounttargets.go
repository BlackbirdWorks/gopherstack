package ocifilestorage

import (
	"net/netip"
	"regexp"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	idmapNone          = "NONE"
	idmapLDAP          = "LDAP"
	schemaRFC2307      = "RFC2307"
	schemaRFC2307BIS   = "RFC2307BIS"
	maxNsgs            = 5
	baseThroughput     = 1
	shape20            = 20
	shape40            = 40
	shape80            = 80
	billingCycleDays   = 30
	defaultLDAPCache   = 300
	maxHostnameLabel   = 63
	defaultFsStatBytes = 23843202333929
	defaultFsStatFiles = 7156920187
)

var hostLabelRe = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

type ldapIdmapIn struct {
	CacheRefreshIntervalSeconds  *int   `json:"cacheRefreshIntervalSeconds"`
	CacheLifetimeSeconds         *int   `json:"cacheLifetimeSeconds"`
	NegativeCacheLifetimeSeconds *int   `json:"negativeCacheLifetimeSeconds"`
	SchemaType                   string `json:"schemaType"`
	UserSearchBase               string `json:"userSearchBase"`
	GroupSearchBase              string `json:"groupSearchBase"`
	OutboundConnector1ID         string `json:"outboundConnector1Id"`
	OutboundConnector2ID         string `json:"outboundConnector2Id"`
}

type kerberosIn struct {
	CurrentKeyTabSecretVersion *int    `json:"currentKeyTabSecretVersion"`
	BackupKeyTabSecretVersion  *int    `json:"backupKeyTabSecretVersion"`
	IsKerberosEnabled          *bool   `json:"isKerberosEnabled"`
	KerberosRealm              *string `json:"kerberosRealm"`
	KeyTabSecretID             *string `json:"keyTabSecretId"`
}

// CreateMountTargetIn is the CreateMountTarget request body.
type CreateMountTargetIn = createMTIn

type createMTIn struct {
	tagsIn
	DisplayName         *string                   `json:"displayName"`
	HostnameLabel       *string                   `json:"hostnameLabel"`
	IPAddress           *string                   `json:"ipAddress"`
	IdmapType           string                    `json:"idmapType"`
	LdapIdmap           *ldapIdmapIn              `json:"ldapIdmap"`
	Kerberos            *kerberosIn               `json:"kerberos"`
	RequestedThroughput *int64                    `json:"requestedThroughput"`
	SecurityAttributes  map[string]map[string]any `json:"securityAttributes"`
	AvailabilityDomain  string                    `json:"availabilityDomain"`
	CompartmentID       string                    `json:"compartmentId"`
	SubnetID            string                    `json:"subnetId"`
	NsgIDs              []string                  `json:"nsgIds"`
	Locks               []lock                    `json:"locks"`
}

type updateMTIn struct {
	tagsIn
	DisplayName        *string                   `json:"displayName"`
	LdapIdmap          *ldapIdmapIn              `json:"ldapIdmap"`
	Kerberos           *kerberosIn               `json:"kerberos"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes"`
	NsgIDs             *[]string                 `json:"nsgIds"`
	IdmapType          string                    `json:"idmapType"`
}

type updateSetIn struct {
	DisplayName    *string `json:"displayName"`
	MaxFsStatBytes *int64  `json:"maxFsStatBytes"`
	MaxFsStatFiles *int64  `json:"maxFsStatFiles"`
}

type throughputIn struct {
	RequestedThroughput *int64 `json:"requestedThroughput"`
}

// MTFilter selects mount targets and export sets in a list.
type MTFilter struct {
	Compartment, AD, State, DisplayName, ID, ExportSet string
}

func (b *InMemoryBackend) mtView(e *entry[*MountTarget]) MountTarget {
	v := *e.W
	v.Int, v.base = nil, cloneBase(e.W.base)
	v.PrivateIPIDs = slices.Clone(v.PrivateIPIDs)
	v.NsgIDs = slices.Clone(v.NsgIDs)

	return v
}

func mtSummary(v MountTarget) map[string]any {
	return project(
		v,
		"compartmentId",
		"displayName",
		"id",
		"lifecycleState",
		"privateIpIds",
		"subnetId",
		"timeCreated",
		"availabilityDomain",
		"exportSetId",
		"mountTargetIpv6Ids",
		"nsgIds",
		"timeBillingCycleEnd",
		"observedThroughput",
		"requestedThroughput",
		"reservedStorageCapacity",
		"locks",
		"freeformTags",
		"definedTags",
		"systemTags",
		"securityAttributes",
	)
}

func (b *InMemoryBackend) checkLdap(idmap string, in *ldapIdmapIn) (*ldapIdmap, *APIError) {
	if idmap != "" && idmap != idmapNone && idmap != idmapLDAP {
		return nil, errParam("idmapType must be NONE or LDAP")
	}

	if idmap != idmapLDAP {
		if in != nil {
			return nil, errParam("ldapIdmap requires idmapType LDAP")
		}

		return nil, nil
	}

	if in == nil || in.OutboundConnector1ID == "" || in.UserSearchBase == "" || in.GroupSearchBase == "" {
		return nil, errMissing("ldapIdmap with outboundConnector1Id, userSearchBase and groupSearchBase is required")
	}

	if aerr := firstErr(
		checkSchema(
			in.SchemaType,
		),
		b.checkConnector(in.OutboundConnector1ID),
		b.checkConnector(in.OutboundConnector2ID),
		checkCacheTimes(in.CacheRefreshIntervalSeconds, in.CacheLifetimeSeconds, in.NegativeCacheLifetimeSeconds),
	); aerr != nil {
		return nil, aerr
	}

	schema := in.SchemaType
	if schema == "" {
		schema = schemaRFC2307
	}

	return &ldapIdmap{
		SchemaType: schema, UserSearchBase: in.UserSearchBase, GroupSearchBase: in.GroupSearchBase,
		CacheRefreshIntervalSeconds:  cacheOrDefault(in.CacheRefreshIntervalSeconds),
		CacheLifetimeSeconds:         cacheOrDefault(in.CacheLifetimeSeconds),
		NegativeCacheLifetimeSeconds: cacheOrDefault(in.NegativeCacheLifetimeSeconds),
		OutboundConnector1ID:         in.OutboundConnector1ID, OutboundConnector2ID: in.OutboundConnector2ID,
	}, nil
}

func checkSchema(s string) *APIError {
	if s != "" && s != schemaRFC2307 && s != schemaRFC2307BIS {
		return errParam("schemaType must be RFC2307 or RFC2307BIS")
	}

	return nil
}

func (b *InMemoryBackend) checkConnector(id string) *APIError {
	if id == "" {
		return nil
	}

	c, aerr := b.conns.usable(id)
	if aerr != nil {
		return aerr
	}

	if c.W.State != stActive {
		return errState("The outbound connector " + id + " is in state " + c.W.State)
	}

	return nil
}

func checkCacheTimes(vals ...*int) *APIError {
	for _, p := range vals {
		if p != nil && *p < 0 {
			return errParam("LDAP cache durations must not be negative")
		}
	}

	return nil
}

func cacheOrDefault(p *int) *int {
	v := deref(p, defaultLDAPCache)

	return &v
}

func (b *InMemoryBackend) checkKerberos(in *kerberosIn, cur *kerberos) (*kerberos, *APIError) {
	if in == nil {
		return cur, nil
	}

	k := kerberos{}
	if cur != nil {
		k = *cur
	}

	if in.KerberosRealm != nil {
		k.KerberosRealm = *in.KerberosRealm
	}

	if in.KeyTabSecretID != nil {
		k.KeyTabSecretID = *in.KeyTabSecretID
	}

	if in.IsKerberosEnabled != nil {
		k.IsKerberosEnabled = *in.IsKerberosEnabled
	}

	if in.CurrentKeyTabSecretVersion != nil {
		k.CurrentKeyTabSecretVersion = in.CurrentKeyTabSecretVersion
	}

	if in.BackupKeyTabSecretVersion != nil {
		k.BackupKeyTabSecretVersion = in.BackupKeyTabSecretVersion
	}

	if k.KerberosRealm == "" || k.KeyTabSecretID == "" {
		return nil, errMissing("kerberos requires kerberosRealm and keyTabSecretId")
	}

	if k.KerberosRealm != upperASCII(k.KerberosRealm) {
		return nil, errParam("kerberosRealm must be upper case")
	}

	if aerr := b.checkKeytab(k.KeyTabSecretID, k.CurrentKeyTabSecretVersion, k.KerberosRealm); aerr != nil {
		return nil, aerr
	}

	if k.BackupKeyTabSecretVersion != nil {
		if aerr := b.checkKeytab(k.KeyTabSecretID, k.BackupKeyTabSecretVersion, k.KerberosRealm); aerr != nil {
			return nil, aerr
		}
	}

	return &k, nil
}

func upperASCII(s string) string {
	out := []byte(s)

	for i, c := range out {
		if c >= 'a' && c <= 'z' {
			out[i] = c - 'a' + 'A'
		}
	}

	return string(out)
}

// keytabEntries reads and parses a secret holding a keytab.
func (b *InMemoryBackend) keytabEntries(secretID string, version *int) ([]keytabEntry, *APIError) {
	if b.secret == nil {
		return nil, errNotFound(secretID)
	}

	data, ok := b.secret(secretID, version)
	if !ok {
		return nil, errNotFound(secretID)
	}

	ents, err := parseKeytab(data)
	if err != nil {
		return nil, errParam("The secret " + secretID + " does not hold a valid keytab: " + err.Error())
	}

	return ents, nil
}

func (b *InMemoryBackend) checkKeytab(secretID string, version *int, realm string) *APIError {
	ents, aerr := b.keytabEntries(secretID, version)
	if aerr != nil {
		return aerr
	}

	for _, e := range ents {
		if realm != "" && !hasSuffixFold(e.Principal, "@"+realm) {
			return errParam("keytab principal " + e.Principal + " is not in the realm " + realm)
		}
	}

	return nil
}

func hasSuffixFold(s, suffix string) bool {
	return len(s) >= len(suffix) && eqFold(s[len(s)-len(suffix):], suffix)
}

// CreateMountTarget creates a mount target, its VNIC and its export set.
func (b *InMemoryBackend) CreateMountTarget(in createMTIn, token string) (MountTarget, string, error) {
	if in.AvailabilityDomain == "" || in.SubnetID == "" {
		return MountTarget{}, "", errMissing("availabilityDomain and subnetId are required")
	}

	b.mu.Lock("CreateMountTarget")
	defer b.mu.Unlock()

	if id := b.retryHit(kindMT, token); id != "" {
		if e, ok := b.mts[id]; ok {
			return b.mtView(e), e.Etag, nil
		}
	}

	mt, sub, aerr := b.buildMountTarget(in)
	if aerr != nil {
		return MountTarget{}, "", aerr
	}

	nic, err := b.net.CreateNic(NicRequest{
		CompartmentID: in.CompartmentID, AD: in.AvailabilityDomain, SubnetID: in.SubnetID, DisplayName: mt.DisplayName,
		Hostname: deref(in.HostnameLabel, ""), IPAddress: deref(in.IPAddress, ""), NsgIDs: in.NsgIDs,
	})
	if err != nil {
		return MountTarget{}, "", nicErr(err)
	}

	mt.PrivateIPIDs, mt.Int = []string{nic.PrivateIPID}, &mtInt{VnicID: nic.VnicID}

	statBytes, statFiles := int64(defaultFsStatBytes), int64(defaultFsStatFiles)
	set := &ExportSet{
		MaxFsStatBytes: &statBytes, MaxFsStatFiles: &statFiles,
		base: b.newBase(kindExportSet, tagsIn{}, nil), CompartmentID: in.CompartmentID,
		DisplayName: "export set - " + mt.DisplayName, VcnID: sub.VcnID, AvailabilityDomain: in.AvailabilityDomain,
		Int: &esInt{MountTargetID: mt.ID},
	}
	mt.ExportSetID = set.ID

	put(b, b.sets, set, stActive)

	e := put(b, b.mts, mt, stActive)
	b.remember(kindMT, token, mt.ID)

	if e.W.State == stActive {
		b.mountTargetSettled(e)
	}

	return b.mtView(e), e.Etag, nil
}

func nicErr(err error) *APIError {
	if ae, ok := err.(*APIError); ok { //nolint:errorlint // own type, never wrapped
		return ae
	}

	return errParam(err.Error())
}

func (b *InMemoryBackend) buildMountTarget(in createMTIn) (*MountTarget, SubnetInfo, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, SubnetInfo{}, aerr
	}

	if b.net == nil || b.net.Subnet == nil {
		return nil, SubnetInfo{}, errNotFound(in.SubnetID)
	}

	sub, ok := b.net.Subnet(in.SubnetID)
	if !ok {
		return nil, SubnetInfo{}, errNotFound(in.SubnetID)
	}

	if sub.AD != "" && sub.AD != in.AvailabilityDomain {
		return nil, SubnetInfo{}, errParam(
			"The subnet " + in.SubnetID + " is specific to availability domain " + sub.AD,
		)
	}

	name := "MountTarget-" + b.nowUTC().Format("20060102-150405")
	if in.DisplayName != nil {
		name = *in.DisplayName
	}

	if aerr := checkName(name); aerr != nil {
		return nil, SubnetInfo{}, aerr
	}

	if aerr := checkMTNetwork(in); aerr != nil {
		return nil, SubnetInfo{}, aerr
	}

	for _, l := range in.Locks {
		if aerr := checkLock(l); aerr != nil {
			return nil, SubnetInfo{}, aerr
		}
	}

	ldap, aerr := b.checkLdap(in.IdmapType, in.LdapIdmap)
	if aerr != nil {
		return nil, SubnetInfo{}, aerr
	}

	krb, aerr := b.checkKerberos(in.Kerberos, nil)
	if aerr != nil {
		return nil, SubnetInfo{}, aerr
	}

	idmap := in.IdmapType
	if idmap == "" {
		idmap = idmapNone
	}

	mt := &MountTarget{
		base: b.newBase(kindMT, in.tagsIn, in.Locks), CompartmentID: in.CompartmentID, DisplayName: name,
		SubnetID: in.SubnetID, AvailabilityDomain: in.AvailabilityDomain, IdmapType: idmap, LdapIdmap: ldap,
		Kerberos: krb, NsgIDs: append([]string{}, in.NsgIDs...), SecurityAttributes: in.SecurityAttributes,
		RequestedThroughput: baseThroughput, ObservedThroughput: baseThroughput, PrivateIPIDs: []string{},
	}

	return mt, sub, nil
}

func checkMTNetwork(in createMTIn) *APIError {
	if in.HostnameLabel != nil &&
		(len(*in.HostnameLabel) > maxHostnameLabel || !hostLabelRe.MatchString(*in.HostnameLabel)) {
		return errParam("hostnameLabel must be a valid DNS label")
	}

	if in.IPAddress != nil {
		if _, err := netip.ParseAddr(*in.IPAddress); err != nil {
			return errParam("ipAddress is not a valid IP address")
		}
	}

	if len(in.NsgIDs) > maxNsgs {
		return errParam("at most 5 nsgIds are allowed")
	}

	if in.RequestedThroughput != nil && *in.RequestedThroughput != baseThroughput {
		return errParam("Only 1 Gbps of requestedThroughput is supported when creating a mount target")
	}

	return nil
}

// mountTargetSettled starts the NFS server of a mount target that became ACTIVE. Callers hold the write lock.
func (b *InMemoryBackend) mountTargetSettled(e *entry[*MountTarget]) {
	if b.dp == nil || e.W.State != stActive {
		return
	}

	if err := b.dp.StartMount(e.W.ID, b.mountExports); err != nil {
		e.W.State, e.W.LifecycleDetails = stFailed, err.Error()
	}
}

// GetMountTarget returns one mount target.
func (b *InMemoryBackend) GetMountTarget(id string) (MountTarget, string, error) {
	b.mu.RLock("GetMountTarget")
	defer b.mu.RUnlock()

	e, aerr := b.mts.get(id)
	if aerr != nil {
		return MountTarget{}, "", aerr
	}

	return b.mtView(e), e.Etag, nil
}

// ListMountTargets lists mount targets of a compartment and availability domain.
func (b *InMemoryBackend) ListMountTargets(f MTFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListMountTargets")
	defer b.mu.RUnlock()

	return listScoped(b, b.mts, f, o, scope[*MountTarget]{
		fields: func(w *MountTarget) (string, string, string) {
			return w.CompartmentID, w.AvailabilityDomain, w.DisplayName
		},
		extra:  func(w *MountTarget) bool { return f.ExportSet == "" || w.ExportSetID == f.ExportSet },
		render: func(e *entry[*MountTarget]) map[string]any { return mtSummary(b.mtView(e)) },
	})
}

// UpdateMountTarget changes a mount target.
func (b *InMemoryBackend) UpdateMountTarget(
	id, ifMatch string,
	override bool,
	in updateMTIn,
) (MountTarget, string, error) {
	b.mu.Lock("UpdateMountTarget")
	defer b.mu.Unlock()

	e, aerr := b.mts.usable(id)
	if aerr != nil {
		return MountTarget{}, "", aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, override)); aerr != nil {
		return MountTarget{}, "", aerr
	}

	if aerr = b.applyMTUpdate(e.W, in); aerr != nil {
		return MountTarget{}, "", aerr
	}

	applyTags(&e.W.base, in.tagsIn)
	e.Etag = newEtag()

	return b.mtView(e), e.Etag, nil
}

func (b *InMemoryBackend) applyMTUpdate(mt *MountTarget, in updateMTIn) *APIError {
	if in.DisplayName != nil {
		if aerr := checkName(*in.DisplayName); aerr != nil {
			return aerr
		}
	}

	if in.NsgIDs != nil && len(*in.NsgIDs) > maxNsgs {
		return errParam("at most 5 nsgIds are allowed")
	}

	idmap := mt.IdmapType
	if in.IdmapType != "" {
		idmap = in.IdmapType
	}

	ldapIn := in.LdapIdmap
	if ldapIn == nil && idmap == idmapLDAP && mt.LdapIdmap != nil && in.IdmapType == "" {
		ldapIn = ldapToIn(mt.LdapIdmap)
	}

	ldap, aerr := b.checkLdap(idmap, ldapIn)
	if aerr != nil {
		return aerr
	}

	krb, aerr := b.checkKerberos(in.Kerberos, mt.Kerberos)
	if aerr != nil {
		return aerr
	}

	if in.DisplayName != nil {
		mt.DisplayName = *in.DisplayName
	}

	if in.NsgIDs != nil {
		mt.NsgIDs = slices.Clone(*in.NsgIDs)
	}

	if in.SecurityAttributes != nil {
		mt.SecurityAttributes = in.SecurityAttributes
	}

	mt.IdmapType, mt.LdapIdmap, mt.Kerberos = idmap, ldap, krb

	return nil
}

func ldapToIn(l *ldapIdmap) *ldapIdmapIn {
	return &ldapIdmapIn{
		CacheRefreshIntervalSeconds: l.CacheRefreshIntervalSeconds, CacheLifetimeSeconds: l.CacheLifetimeSeconds,
		NegativeCacheLifetimeSeconds: l.NegativeCacheLifetimeSeconds, SchemaType: l.SchemaType,
		UserSearchBase: l.UserSearchBase, GroupSearchBase: l.GroupSearchBase,
		OutboundConnector1ID: l.OutboundConnector1ID, OutboundConnector2ID: l.OutboundConnector2ID,
	}
}

// DeleteMountTarget deletes a mount target with its export set, exports and VNIC.
func (b *InMemoryBackend) DeleteMountTarget(id, ifMatch string, override bool) error {
	b.mu.Lock("DeleteMountTarget")
	e, aerr := b.mts.usable(id)

	if aerr == nil {
		aerr = firstErr(checkEtag(e.Etag, ifMatch), guardDelete(&e.W.base, override))
	}

	if aerr != nil {
		b.mu.Unlock()

		return aerr
	}

	for _, x := range b.exps {
		if x.W.ExportSetID == e.W.ExportSetID && x.W.State != stDeleted {
			begin(b, x, stDeleting, stDeleted)
		}
	}

	if s, ok := b.sets[e.W.ExportSetID]; ok {
		begin(b, s, stDeleting, stDeleted)
	}

	vnic := ""
	if e.W.Int != nil {
		vnic = e.W.Int.VnicID
	}

	dp := b.dp
	begin(b, e, stDeleting, stDeleted)
	b.mu.Unlock()

	if dp != nil {
		dp.StopMount(id)
	}

	if vnic != "" && b.net != nil && b.net.DeleteNic != nil {
		b.net.DeleteNic(vnic)
	}

	return nil
}

// ChangeMountTargetCompartment moves a mount target and its export set.
func (b *InMemoryBackend) ChangeMountTargetCompartment(id, ifMatch, compartment string) error {
	b.mu.Lock("ChangeMountTargetCompartment")
	defer b.mu.Unlock()

	e, aerr := b.mts.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(
		checkEtag(e.Etag, ifMatch),
		guardChange(&e.W.base, false),
		b.checkCompartment(compartment),
	); aerr != nil {
		return aerr
	}

	e.W.CompartmentID = compartment
	e.Etag = newEtag()

	if s, ok := b.sets[e.W.ExportSetID]; ok {
		s.W.CompartmentID = compartment
		s.Etag = newEtag()
	}

	return nil
}

// UpgradeShape raises the requested throughput of a mount target.
func (b *InMemoryBackend) UpgradeShape(id, ifMatch string, in throughputIn) (MountTarget, string, error) {
	b.mu.Lock("UpgradeShape")
	defer b.mu.Unlock()

	e, aerr := b.shapeTarget(id, ifMatch, in)
	if aerr != nil {
		return MountTarget{}, "", aerr
	}

	if *in.RequestedThroughput <= e.W.RequestedThroughput {
		return MountTarget{}, "", errParam(
			"requestedThroughput must exceed the current throughput of " + itoa(e.W.RequestedThroughput) + " Gbps",
		)
	}

	e.W.RequestedThroughput, e.W.ObservedThroughput = *in.RequestedThroughput, *in.RequestedThroughput
	e.W.Int.DowngradeTo, e.W.Int.DowngradeAfter = nil, nil
	b.rollBilling(e.W)

	begin(b, e, stUpdating, stActive)

	return b.mtView(e), e.Etag, nil
}

func (b *InMemoryBackend) rollBilling(mt *MountTarget) {
	end := b.nowUTC().AddDate(0, 0, billingCycleDays)
	mt.TimeBillingCycleEnd = &end
}

func (b *InMemoryBackend) shapeTarget(id, ifMatch string, in throughputIn) (*entry[*MountTarget], *APIError) {
	e, aerr := b.mts.usable(id)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, false)); aerr != nil {
		return nil, aerr
	}

	if e.W.State != stActive {
		return nil, errState("The mount target " + id + " is in state " + e.W.State)
	}

	if in.RequestedThroughput == nil {
		return nil, errMissing("requestedThroughput is required")
	}

	if !validShape(*in.RequestedThroughput) {
		return nil, errParam("requestedThroughput must be one of 1, 20, 40 or 80 Gbps")
	}

	if e.W.Int == nil {
		e.W.Int = &mtInt{}
	}

	return e, nil
}

// ScheduleDowngradeShape schedules a throughput reduction for the end of the billing cycle.
func (b *InMemoryBackend) ScheduleDowngradeShape(id, ifMatch string, in throughputIn) (MountTarget, string, error) {
	b.mu.Lock("ScheduleDowngradeShape")
	defer b.mu.Unlock()

	e, aerr := b.shapeTarget(id, ifMatch, in)
	if aerr != nil {
		return MountTarget{}, "", aerr
	}

	if *in.RequestedThroughput >= e.W.RequestedThroughput {
		return MountTarget{}, "", errParam(
			"requestedThroughput must be below the current throughput of " + itoa(e.W.RequestedThroughput) + " Gbps",
		)
	}

	if e.W.TimeBillingCycleEnd == nil {
		b.rollBilling(e.W)
	}

	to, after := *in.RequestedThroughput, *e.W.TimeBillingCycleEnd
	e.W.Int.DowngradeTo, e.W.Int.DowngradeAfter = &to, &after
	e.Etag = newEtag()

	return b.mtView(e), e.Etag, nil
}

// CancelDowngradeShape cancels a scheduled throughput reduction.
func (b *InMemoryBackend) CancelDowngradeShape(id, ifMatch string) (MountTarget, string, error) {
	b.mu.Lock("CancelDowngradeShape")
	defer b.mu.Unlock()

	e, aerr := b.mts.usable(id)
	if aerr != nil {
		return MountTarget{}, "", aerr
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return MountTarget{}, "", aerr
	}

	if e.W.Int == nil || e.W.Int.DowngradeTo == nil {
		return MountTarget{}, "", errState("The mount target " + id + " has no scheduled shape downgrade")
	}

	e.W.Int.DowngradeTo, e.W.Int.DowngradeAfter = nil, nil
	e.Etag = newEtag()

	return b.mtView(e), e.Etag, nil
}

// sweepMountTargets applies due scheduled downgrades.
func (b *InMemoryBackend) sweepMountTargets(now time.Time) {
	for _, e := range b.mts {
		in := e.W.Int
		if in == nil || in.DowngradeTo == nil || in.DowngradeAfter == nil || in.DowngradeAfter.After(now) {
			continue
		}

		e.W.RequestedThroughput, e.W.ObservedThroughput = *in.DowngradeTo, *in.DowngradeTo
		in.DowngradeTo, in.DowngradeAfter = nil, nil
		b.rollBilling(e.W)
		e.Etag = newEtag()
	}
}

type validateKeyTabsIn struct {
	KeyTabSecretDetails *struct {
		CurrentKeyTabSecretVersion *int   `json:"currentKeyTabSecretVersion"`
		BackupKeyTabSecretVersion  *int   `json:"backupKeyTabSecretVersion"`
		KeyTabSecretID             string `json:"keyTabSecretId"`
	} `json:"keyTabSecretDetails"`
	MountTargetID string `json:"mountTargetId"`
}

// KeytabValidation is the ValidateKeyTabs result.
type KeytabValidation struct {
	Current []keytabEntry `json:"currentKerberosKeytabEntries"`
	Backup  []keytabEntry `json:"backupKerberosKeytabEntries,omitempty"`
}

// ValidateKeyTabs parses the keytab secret and reports its principals.
func (b *InMemoryBackend) ValidateKeyTabs(in validateKeyTabsIn) (KeytabValidation, error) {
	if in.KeyTabSecretDetails == nil || in.KeyTabSecretDetails.KeyTabSecretID == "" {
		return KeytabValidation{}, errMissing("keyTabSecretDetails.keyTabSecretId is required")
	}

	b.mu.RLock("ValidateKeyTabs")
	defer b.mu.RUnlock()

	realm := ""

	if in.MountTargetID != "" {
		mt, aerr := b.mts.usable(in.MountTargetID)
		if aerr != nil {
			return KeytabValidation{}, aerr
		}

		if mt.W.Kerberos != nil {
			realm = mt.W.Kerberos.KerberosRealm
		}
	}

	d := in.KeyTabSecretDetails

	cur, aerr := b.keytabEntries(d.KeyTabSecretID, d.CurrentKeyTabSecretVersion)
	if aerr != nil {
		return KeytabValidation{}, aerr
	}

	out := KeytabValidation{Current: cur}

	if d.BackupKeyTabSecretVersion != nil {
		if out.Backup, aerr = b.keytabEntries(d.KeyTabSecretID, d.BackupKeyTabSecretVersion); aerr != nil {
			return KeytabValidation{}, aerr
		}
	}

	for _, e := range append(slices.Clone(out.Current), out.Backup...) {
		if realm != "" && !hasSuffixFold(e.Principal, "@"+realm) {
			return KeytabValidation{}, errParam("keytab principal " + e.Principal + " is not in the realm " + realm)
		}
	}

	return out, nil
}

// ---- export sets ----

func (b *InMemoryBackend) setView(e *entry[*ExportSet]) ExportSet {
	v := *e.W
	v.Int, v.base = nil, cloneBase(e.W.base)

	return v
}

func setSummary(v ExportSet) map[string]any {
	return project(
		v,
		"compartmentId",
		"displayName",
		"id",
		"lifecycleState",
		"timeCreated",
		"vcnId",
		"availabilityDomain",
	)
}

// GetExportSet returns one export set.
func (b *InMemoryBackend) GetExportSet(id string) (ExportSet, string, error) {
	b.mu.RLock("GetExportSet")
	defer b.mu.RUnlock()

	e, aerr := b.sets.get(id)
	if aerr != nil {
		return ExportSet{}, "", aerr
	}

	return b.setView(e), e.Etag, nil
}

// ListExportSets lists export sets of a compartment and availability domain.
func (b *InMemoryBackend) ListExportSets(f MTFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListExportSets")
	defer b.mu.RUnlock()

	return listScoped(b, b.sets, f, o, scope[*ExportSet]{
		fields: func(w *ExportSet) (string, string, string) {
			return w.CompartmentID, w.AvailabilityDomain, w.DisplayName
		},
		render: func(e *entry[*ExportSet]) map[string]any { return setSummary(b.setView(e)) },
	})
}

// UpdateExportSet changes an export set's name and FSSTAT limits.
func (b *InMemoryBackend) UpdateExportSet(id, ifMatch string, in updateSetIn) (ExportSet, string, error) {
	b.mu.Lock("UpdateExportSet")
	defer b.mu.Unlock()

	e, aerr := b.sets.usable(id)
	if aerr != nil {
		return ExportSet{}, "", aerr
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return ExportSet{}, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = checkName(*in.DisplayName); aerr != nil {
			return ExportSet{}, "", aerr
		}
	}

	for _, p := range []*int64{in.MaxFsStatBytes, in.MaxFsStatFiles} {
		if p != nil && *p < 0 {
			return ExportSet{}, "", errParam("maxFsStatBytes and maxFsStatFiles must not be negative")
		}
	}

	if in.DisplayName != nil {
		e.W.DisplayName = *in.DisplayName
	}

	if in.MaxFsStatBytes != nil {
		e.W.MaxFsStatBytes = in.MaxFsStatBytes
	}

	if in.MaxFsStatFiles != nil {
		e.W.MaxFsStatFiles = in.MaxFsStatFiles
	}

	e.Etag = newEtag()

	return b.setView(e), e.Etag, nil
}

// validShape reports whether gbps is a mount target performance level (Content/File/Tasks/managingmounttargets.htm).
func validShape(gbps int64) bool {
	switch gbps {
	case baseThroughput, shape20, shape40, shape80:
		return true
	default:
		return false
	}
}
