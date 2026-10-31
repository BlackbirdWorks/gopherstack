package ocimysql

import (
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	minStorageGB      = 50
	maxStorageGB      = 131072
	minPasswordLen    = 8
	maxPasswordLen    = 32
	maxUsernameLen    = 32
	defaultMySQLPort  = 3306
	defaultMySQLXPort = 33060
	minRetentionDays  = 1
	maxRetentionDays  = 35
	defaultRetention  = 7
	defaultWindow     = "00:00"
	sourceNone        = "NONE"
	sourceBackup      = "BACKUP"
	enabled           = "ENABLED"
	disabled          = "DISABLED"
	modeReadWrite     = "READ_WRITE"
	modeReadOnly      = "READ_ONLY"
	accessUnrestr     = "UNRESTRICTED"
	accessRestricted  = "RESTRICTED"
	skipFinalBackup   = "SKIP_FINAL_BACKUP"
	requireFinal      = "REQUIRE_FINAL_BACKUP"
	retainBackups     = "RETAIN"
	deleteBackups     = "DELETE"
	hostLabelMax      = 63
)

//nolint:gochecknoglobals // immutable reserved-name list and label pattern
var (
	reservedUsers = []string{
		"administrator", "ociadmin", "ocidbm", "ocirest", "ocirpl", "oracle-cloud-agent", "mysql.sys",
		"mysql.session", "mysql.infoschema", "mysql_option_tracker_persister", "mysql_rest_service_admin",
		"mysql_rest_service_data_provider", "mysql_rest_service_dev", "mysql_rest_service_meta_provider",
		"mysql_rest_service_schema_admin", "mysql_rest_service_user", "mysql_task_admin", "mysql_task_user",
		"rrhhuser", "mysqlexport", "root",
	}
	hostLabelRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$`)
	extraKeys   = []string{
		"rest", "databaseConsole", "secureConnections", "encryptData", "customerContacts", "readEndpoint",
		"telemetryConfiguration", "securityAttributes", "nsgIds", "ipv6AddressIpv6SubnetCidrPairDetails",
		"isIpv6Enabled",
	}
)

// BackupPolicyIn is the create/update backup policy.
type BackupPolicyIn struct {
	Tags
	IsEnabled       *bool                      `json:"isEnabled"`
	PitrPolicy      *struct{ IsEnabled *bool } `json:"pitrPolicy"`
	WindowStartTime *string                    `json:"windowStartTime"`
	RetentionInDays *int                       `json:"retentionInDays"`
	SoftDelete      string                     `json:"softDelete"`
	CopyPolicies    json.RawMessage            `json:"copyPolicies"`
}

// DeletionPolicyIn is the create/update deletion policy.
type DeletionPolicyIn struct {
	IsDeleteProtected        *bool  `json:"isDeleteProtected"`
	AutomaticBackupRetention string `json:"automaticBackupRetention"`
	FinalBackup              string `json:"finalBackup"`
}

// DataStorageIn is dataStorage.
type DataStorageIn struct {
	IsAutoExpandStorageEnabled *bool `json:"isAutoExpandStorageEnabled"`
	MaxStorageSizeInGBs        *int  `json:"maxStorageSizeInGBs"`
}

// SourceIn is the create source details.
type SourceIn struct {
	SourceType string `json:"sourceType"`
	BackupID   string `json:"backupId"`
}

// CreateDBSystemIn is the CreateDBSystem body.
type CreateDBSystemIn struct {
	Tags
	Extra             map[string]json.RawMessage `json:"-"`
	IsHighlyAvailable *bool                      `json:"isHighlyAvailable"`
	DataStorageGBs    *int                       `json:"dataStorageSizeInGBs"`
	Port              *int                       `json:"port"`
	PortX             *int                       `json:"portX"`
	DataStorage       *DataStorageIn             `json:"dataStorage"`
	BackupPolicy      *BackupPolicyIn            `json:"backupPolicy"`
	Source            *SourceIn                  `json:"source"`
	DeletionPolicy    *DeletionPolicyIn          `json:"deletionPolicy"`
	Maintenance       map[string]any             `json:"maintenance"`
	Description       string                     `json:"description"`
	AdminPassword     string                     `json:"adminPassword"`
	DisplayName       string                     `json:"displayName"`
	ShapeName         string                     `json:"shapeName"`
	AvailabilityDom   string                     `json:"availabilityDomain"`
	FaultDomain       string                     `json:"faultDomain"`
	ConfigurationID   string                     `json:"configurationId"`
	MysqlVersion      string                     `json:"mysqlVersion"`
	AdminUsername     string                     `json:"adminUsername"`
	SubnetID          string                     `json:"subnetId"`
	HostnameLabel     string                     `json:"hostnameLabel"`
	IPAddress         string                     `json:"ipAddress"`
	CrashRecovery     string                     `json:"crashRecovery"`
	DBManagement      string                     `json:"databaseManagement"`
	DatabaseMode      string                     `json:"databaseMode"`
	AccessMode        string                     `json:"accessMode"`
	CompartmentID     string                     `json:"compartmentId"`
}

func validPassword(p string) bool {
	if len(p) < minPasswordLen || len(p) > maxPasswordLen {
		return false
	}

	var digit, lower, upper, special bool

	for _, r := range p {
		switch {
		case unicode.IsDigit(r):
			digit = true
		case unicode.IsLower(r):
			lower = true
		case unicode.IsUpper(r):
			upper = true
		default:
			special = true
		}
	}

	return digit && lower && upper && special
}

func validAdmin(user, pass string) *APIError {
	switch {
	case user == "":
		return errMissing("adminUsername is required")
	case len(user) > maxUsernameLen:
		return errParam("adminUsername must be at most 32 characters")
	case containsFold(reservedUsers, user):
		return errParam("adminUsername " + user + " is reserved and cannot be used")
	case strings.ContainsAny(user, "'\"\\`@% \t\r\n"):
		return errParam("adminUsername must not contain quotes, backslashes, @, %, or whitespace")
	case pass == "":
		return errMissing("adminPassword is required")
	case !validPassword(pass):
		return errParam("adminPassword must be 8 to 32 characters long and contain at least 1 numeric, 1 lowercase, " +
			"1 uppercase, and 1 special (nonalphanumeric) character")
	}

	return nil
}

func validStorage(gbs int) *APIError {
	if gbs < minStorageGB || gbs > maxStorageGB {
		return errParam("dataStorageSizeInGBs must be between 50 and 131072")
	}

	return nil
}

func validEnum(field, v string, allowed ...string) *APIError {
	if v != "" && !containsFold(allowed, v) {
		return errParam(field + " must be one of " + strings.Join(allowed, ", "))
	}

	return nil
}

func validWindow(w string) (string, *APIError) {
	for _, layout := range []string{"15:04", "15:04:05", "15:04:05Z07:00", "15:04Z07:00", "15:04:05Z"} {
		if t, err := time.Parse(layout, w); err == nil {
			return t.Format("15:04"), nil
		}
	}

	return "", errParam("backupPolicy.windowStartTime must be the time portion of an RFC3339 timestamp, e.g. 03:00")
}

func newBackupPolicy() BackupPolicy {
	return BackupPolicy{
		IsEnabled:       true,
		WindowStartTime: defaultWindow,
		RetentionInDays: defaultRetention,
		SoftDelete:      disabled,
	}
}

func mergeBackupPolicy(cur BackupPolicy, in *BackupPolicyIn) (BackupPolicy, *APIError) {
	if in == nil {
		return cur, nil
	}

	if aerr := validEnum("backupPolicy.softDelete", in.SoftDelete, enabled, disabled); aerr != nil {
		return cur, aerr
	}

	if in.IsEnabled != nil {
		cur.IsEnabled = *in.IsEnabled
	}

	if in.WindowStartTime != nil {
		w, aerr := validWindow(*in.WindowStartTime)
		if aerr != nil {
			return cur, aerr
		}

		cur.WindowStartTime = w
	}

	if in.RetentionInDays != nil {
		if *in.RetentionInDays < minRetentionDays || *in.RetentionInDays > maxRetentionDays {
			return cur, errParam("backupPolicy.retentionInDays must be between 1 and 35")
		}

		cur.RetentionInDays = *in.RetentionInDays
	}

	if in.SoftDelete != "" {
		cur.SoftDelete = strings.ToUpper(in.SoftDelete)
	}

	if in.PitrPolicy != nil && in.PitrPolicy.IsEnabled != nil {
		cur.PitrEnabled = *in.PitrPolicy.IsEnabled
	}

	if in.CopyPolicies != nil {
		cur.CopyPolicies = in.CopyPolicies
	}

	cur.Tags.merge(in.Tags)

	return cur, nil
}

func mergeDeletion(cur DeletionPolicy, in *DeletionPolicyIn) (DeletionPolicy, *APIError) {
	if in == nil {
		return cur, nil
	}

	if aerr := firstErr(
		validEnum("deletionPolicy.automaticBackupRetention", in.AutomaticBackupRetention, deleteBackups, retainBackups),
		validEnum("deletionPolicy.finalBackup", in.FinalBackup, skipFinalBackup, requireFinal),
	); aerr != nil {
		return cur, aerr
	}

	if in.AutomaticBackupRetention != "" {
		cur.AutomaticBackupRetention = strings.ToUpper(in.AutomaticBackupRetention)
	}

	if in.FinalBackup != "" {
		cur.FinalBackup = strings.ToUpper(in.FinalBackup)
	}

	if in.IsDeleteProtected != nil {
		cur.IsDeleteProtected = *in.IsDeleteProtected
	}

	return cur, nil
}

func validPorts(port, portX int) *APIError {
	switch {
	case port < 1 || port > 65535 || portX < 1 || portX > 65535:
		return errParam("port and portX must be between 1 and 65535")
	case port == portX:
		return errParam("port and portX must differ")
	}

	return nil
}

func validHostLabel(l string) *APIError {
	if l != "" && (len(l) > hostLabelMax || !hostLabelRe.MatchString(l)) {
		return errParam("hostnameLabel must be an RFC 1123 label of at most 63 characters")
	}

	return nil
}

// checkSubnet resolves the subnet and returns its CIDR and availability domain.
func (b *InMemoryBackend) checkSubnet(id string) (SubnetInfo, *APIError) {
	if id == "" {
		return SubnetInfo{}, errMissing("subnetId is required")
	}

	if !strings.HasPrefix(id, "ocid1.subnet.") {
		return SubnetInfo{}, errParam("subnetId must be the OCID of a subnet")
	}

	if b.net == nil {
		return SubnetInfo{ID: id}, nil
	}

	sub, ok := b.net.Subnet(id)
	if !ok {
		return SubnetInfo{}, errParam("subnetId " + id + " does not reference an existing subnet")
	}

	return sub, nil
}

// allocIP picks the DB system's address in its subnet: want when given and free, else the first free host.
func (b *InMemoryBackend) allocIP(sub SubnetInfo, want, exceptID string) (string, *APIError) {
	taken := map[string]bool{}

	for _, s := range b.systems {
		if s.SubnetID == sub.ID && s.State != stateDeleted && s.ID != exceptID {
			taken[s.IP] = true
		}
	}

	if want != "" {
		switch {
		case taken[want]:
			return "", errParam("ipAddress " + want + " is already in use in the subnet")
		case sub.CIDR != "" && !inCIDR(sub.CIDR, want):
			return "", errParam("ipAddress " + want + " is not within the subnet CIDR block " + sub.CIDR)
		}

		return want, nil
	}

	if sub.CIDR == "" {
		return documentationAddr(b.nextSeq()), nil
	}

	for off := firstHostOffset; ; off++ {
		ip := hostIn(sub.CIDR, off)
		if ip == "" {
			return "", errParam("the subnet has no free addresses")
		}

		if !taken[ip] {
			return ip, nil
		}
	}
}

func (b *InMemoryBackend) hostnameTaken(subnet, label, exceptID string) bool {
	for _, s := range b.systems {
		if s.SubnetID == subnet && s.State != stateDeleted && s.ID != exceptID && strings.EqualFold(s.Hostname, label) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) lookupConfig(id string) (*Configuration, bool) {
	if c, ok := b.defaults[id]; ok {
		return c, true
	}

	c, ok := b.configs[id]

	return c, ok && c.State == stateActive
}

func (b *InMemoryBackend) resolveConfig(id, shape string, ha bool) (string, *APIError) {
	if id == "" {
		return defaultConfigID(shape, ha && shape != shapeFree), nil
	}

	c, ok := b.lookupConfig(id)

	switch {
	case !ok:
		return "", errParam("configurationId " + id + " does not reference an existing configuration")
	case c.Shape != shape:
		return "", errParam("configuration " + id + " is for shape " + c.Shape + ", not " + shape)
	}

	return id, nil
}

type createPlan struct {
	sys    *DBSystem
	backup *Backup
	dump   []byte
}

func (b *InMemoryBackend) checkSource(in *SourceIn) (*Source, *Backup, *APIError) {
	if in == nil || in.SourceType == "" || strings.EqualFold(in.SourceType, sourceNone) {
		return &Source{SourceType: sourceNone}, nil, nil
	}

	if !strings.EqualFold(in.SourceType, sourceBackup) {
		return nil, nil, errParam("source.sourceType " + in.SourceType + " is not supported; use NONE or BACKUP")
	}

	if in.BackupID == "" {
		return nil, nil, errMissing("source.backupId is required for sourceType BACKUP")
	}

	bk, ok := b.backups[in.BackupID]
	if !ok || bk.State == stateDeleted {
		return nil, nil, errParam("source.backupId " + in.BackupID + " does not reference an existing backup")
	}

	if bk.State != stateActive {
		return nil, nil, errState("backup " + in.BackupID + " is in state " + bk.State + "; it must be ACTIVE")
	}

	return &Source{SourceType: sourceBackup, BackupID: bk.ID}, bk, nil
}

func (b *InMemoryBackend) buildSystem(in CreateDBSystemIn) (*createPlan, *APIError) {
	if aerr := firstErr(b.checkCompartment(in.CompartmentID), b.validateCreateFields(in)); aerr != nil {
		return nil, aerr
	}

	src, bk, aerr := b.checkSource(in.Source)
	if aerr != nil {
		return nil, aerr
	}

	user, pass := in.AdminUsername, in.AdminPassword
	if bk == nil || user != "" || pass != "" {
		if aerr = validAdmin(user, pass); aerr != nil {
			return nil, aerr
		}
	} else if bk.Snapshot != nil {
		user, pass = bk.Snapshot.AdminUser, bk.Snapshot.AdminPass
	}

	return b.assemble(in, src, bk, user, pass)
}

func (b *InMemoryBackend) validateCreateFields(in CreateDBSystemIn) *APIError {
	sh, ok := shapeByName(in.ShapeName)

	switch {
	case in.ShapeName == "":
		return errMissing("shapeName is required")
	case !ok || sh.support != supportDBSystem:
		return errParam("shapeName " + in.ShapeName + " is not a MySQL DB system shape; see ListShapes")
	case in.IsHighlyAvailable != nil && *in.IsHighlyAvailable && in.ShapeName == shapeFree:
		return errParam("shape " + shapeFree + " does not support high availability")
	case in.MysqlVersion != "" && !isKnownVersion(in.MysqlVersion):
		return errParam("mysqlVersion " + in.MysqlVersion + " is not supported; see ListVersions")
	}

	if in.DisplayName != "" {
		if aerr := validateName(in.DisplayName); aerr != nil {
			return aerr
		}
	}

	return firstErr(
		validHostLabel(in.HostnameLabel),
		validEnum("crashRecovery", in.CrashRecovery, enabled, disabled),
		validEnum("databaseManagement", in.DBManagement, enabled, disabled),
		validEnum("databaseMode", in.DatabaseMode, modeReadWrite, modeReadOnly),
		validEnum("accessMode", in.AccessMode, accessUnrestr, accessRestricted),
	)
}

func isKnownVersion(v string) bool {
	_, ok := familyOf(v)

	return ok
}

func (b *InMemoryBackend) assemble(
	in CreateDBSystemIn, src *Source, bk *Backup, user, pass string,
) (*createPlan, *APIError) {
	sub, aerr := b.checkSubnet(in.SubnetID)
	if aerr != nil {
		return nil, aerr
	}

	gbs, aerr := storageSize(in, bk)
	if aerr != nil {
		return nil, aerr
	}

	port, portX := defaultMySQLPort, defaultMySQLXPort
	if in.Port != nil {
		port = *in.Port
	}

	if in.PortX != nil {
		portX = *in.PortX
	}

	policy, aerr2 := mergeBackupPolicy(newBackupPolicy(), in.BackupPolicy)
	del, aerr3 := mergeDeletion(DeletionPolicy{
		AutomaticBackupRetention: deleteBackups, FinalBackup: skipFinalBackup,
	}, in.DeletionPolicy)

	if aerr = firstErr(validPorts(port, portX), aerr2, aerr3); aerr != nil {
		return nil, aerr
	}

	ha := in.IsHighlyAvailable != nil && *in.IsHighlyAvailable

	cfg, aerr := b.resolveConfig(in.ConfigurationID, in.ShapeName, ha)
	if aerr != nil {
		return nil, aerr
	}

	if in.HostnameLabel != "" && b.hostnameTaken(sub.ID, in.HostnameLabel, "") {
		return nil, errParam("hostnameLabel " + in.HostnameLabel + " is already used in the subnet")
	}

	ip, aerr := b.allocIP(sub, in.IPAddress, "")
	if aerr != nil {
		return nil, aerr
	}

	return b.finishSystem(in, systemParts{
		src: src, bk: bk, user: user, pass: pass, sub: sub, gbs: gbs, port: port, portX: portX, policy: policy,
		del: del, cfg: cfg, ip: ip, ha: ha,
	}), nil
}

type systemParts struct {
	bk     *Backup
	src    *Source
	sub    SubnetInfo
	cfg    string
	ip     string
	user   string
	pass   string
	del    DeletionPolicy
	policy BackupPolicy
	gbs    int
	port   int
	portX  int
	ha     bool
}

func storageSize(in CreateDBSystemIn, bk *Backup) (int, *APIError) {
	gbs := minStorageGB
	if in.DataStorageGBs != nil {
		gbs = *in.DataStorageGBs
	} else if bk != nil && bk.Snapshot != nil {
		gbs = bk.Snapshot.DataGB
	}

	if aerr := validStorage(gbs); aerr != nil {
		return 0, aerr
	}

	if ds := in.DataStorage; ds != nil && ds.MaxStorageSizeInGBs != nil {
		if *ds.MaxStorageSizeInGBs <= gbs || *ds.MaxStorageSizeInGBs > maxStorageGB {
			return 0, errParam(
				"dataStorage.maxStorageSizeInGBs must be greater than the initial size and at most 131072",
			)
		}
	}

	return gbs, nil
}

func (b *InMemoryBackend) finishSystem(in CreateDBSystemIn, p systemParts) *createPlan {
	now := b.nowUTC()
	version := in.MysqlVersion

	if version == "" {
		version = defaultVersion
	}

	name := in.DisplayName
	if name == "" {
		name = "MySQLDBSystem" + now.Format("20060102150405")
	}

	ad := orStr(in.AvailabilityDom, p.sub.AvailabilityDomain, defaultAD)

	s := &DBSystem{
		ID: newOCID("mysqldbsystem"), CompartmentID: in.CompartmentID, Name: name, Description: in.Description,
		SubnetID: p.sub.ID, Shape: in.ShapeName, Version: version, ConfigID: p.cfg, Hostname: in.HostnameLabel,
		AD: ad, FD: orStr(in.FaultDomain, "", defaultFD), State: stateCreating, Etag: newEtag(),
		AdminUser: p.user, AdminPass: p.pass, RootSecret: newSecret(),
		DatabaseMode:  orStr(strings.ToUpper(in.DatabaseMode), "", modeReadWrite),
		AccessMode:    orStr(strings.ToUpper(in.AccessMode), "", accessUnrestr),
		CrashRecovery: orStr(strings.ToUpper(in.CrashRecovery), "", enabled),
		DBManagement:  orStr(strings.ToUpper(in.DBManagement), "", disabled),
		IP:            p.ip, DataGB: p.gbs, Port: p.port, PortX: p.portX, HA: p.ha, AutoExpand: p.ha,
		BackupPolicy: p.policy, Deletion: p.del, Source: p.src, Tags: in.Tags,
		TimeCreated: now, TimeUpdated: now, LastAutoBackup: now, Seq: b.nextSeq(),
		Maintenance: mergeMaintenance(in.Maintenance), Extra: in.Extra,
	}

	if ds := in.DataStorage; ds != nil {
		if ds.IsAutoExpandStorageEnabled != nil {
			s.AutoExpand = *ds.IsAutoExpandStorageEnabled
		}

		if ds.MaxStorageSizeInGBs != nil {
			s.MaxGB = *ds.MaxStorageSizeInGBs
		}
	}

	plan := &createPlan{sys: s, backup: p.bk}
	if p.bk != nil {
		plan.dump = p.bk.Dump
	}

	return plan
}

func mergeMaintenance(in map[string]any) map[string]any {
	out := map[string]any{
		"windowStartTime": "SUNDAY 03:00", "versionPreference": "OLDEST",
		"versionTrackPreference": "LONG_TERM_SUPPORT", "maintenanceScheduleType": "REGULAR",
	}
	maps.Copy(out, in)

	return out
}

func orStr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}

// CreateDBSystem creates a DB system in CREATING state and returns its body, etag and work request.
func (b *InMemoryBackend) CreateDBSystem(in CreateDBSystemIn) (Doc, string, string, error) {
	b.mu.Lock("CreateDbSystem")
	defer b.mu.Unlock()

	plan, aerr := b.buildSystem(in)
	if aerr != nil {
		return nil, "", "", aerr
	}

	s := plan.sys
	s.TransitionAt = b.transitionAt()

	if b.eng != nil {
		s.TransitionAt = nil
	}

	wr := b.newWR(wrCreateDBSystem, s.CompartmentID, entityDBSystem, s.ID, actionCreated, s.TransitionAt)
	b.systems[s.ID] = s

	if b.eng != nil {
		b.spawn(s.ID, wr, b.launchWork(s.ID, launchIn{
			admin: s.AdminUser, password: s.AdminPass, dump: plan.dump, fresh: true,
		}), func(s *DBSystem) { s.State = stateActive })
	}

	return b.systemDoc(s), s.Etag, wr, nil
}

// Doc is a JSON object response body.
type Doc = map[string]any

func newSecret() string { return newEtag() + newEtag() }

func (b *InMemoryBackend) endpointFor(s *DBSystem) (string, int) {
	if s.EngineHost != "" && s.EnginePort != 0 {
		return s.EngineHost, s.EnginePort
	}

	return s.IP, s.Port
}

func (b *InMemoryBackend) endpointDoc(s *DBSystem) Doc {
	ip, port := b.endpointFor(s)
	status := "ACTIVE"

	switch s.State {
	case stateInactive, stateFailed, stateDeleted:
		status = "INACTIVE"
	case stateUpdating, stateCreating:
		status = "UPDATING"
	}

	d := Doc{
		"ipAddress": ip, "port": port, "portX": s.PortX, "ipAddressVersion": "IPV4",
		"modes": []string{"READ", "WRITE"}, "status": status, "resourceType": "DBSYSTEM", "resourceId": s.ID,
	}
	if s.Hostname != "" {
		d["hostname"] = s.Hostname
	}

	return d
}

func (b *InMemoryBackend) policyDoc(p BackupPolicy) Doc {
	d := Doc{
		keyIsEnabled: p.IsEnabled, "windowStartTime": p.WindowStartTime, "retentionInDays": p.RetentionInDays,
		"softDelete": p.SoftDelete, "pitrPolicy": Doc{keyIsEnabled: p.PitrEnabled},
	}
	putTags(d, p.Tags)

	if p.CopyPolicies != nil {
		d["copyPolicies"] = p.CopyPolicies
	}

	return d
}

func putTags(d Doc, t Tags) {
	if t.FreeformTags != nil {
		d["freeformTags"] = t.FreeformTags
	}

	if t.DefinedTags != nil {
		d["definedTags"] = t.DefinedTags
	}
}

func (b *InMemoryBackend) deletionDoc(p DeletionPolicy) Doc {
	return Doc{
		"automaticBackupRetention": p.AutomaticBackupRetention, "finalBackup": p.FinalBackup,
		"isDeleteProtected": p.IsDeleteProtected,
	}
}

func (b *InMemoryBackend) sourceDoc(s *Source) Doc {
	if s == nil || s.SourceType == sourceNone {
		return Doc{keySourceType: sourceNone}
	}

	return Doc{keySourceType: s.SourceType, "backupId": s.BackupID}
}

func (b *InMemoryBackend) channelsOf(id string) []Doc {
	out := []Doc{}

	for _, c := range b.channels {
		if c.Target.DBSystemID == id && c.State != stateDeleted {
			out = append(out, b.channelSummary(c))
		}
	}

	return out
}

func (b *InMemoryBackend) systemBase(s *DBSystem) Doc {
	d := Doc{
		"id": s.ID, keyDisplayName: s.Name, keyCompartment: s.CompartmentID, keyLifecycle: s.State,
		"mysqlVersion": s.Version, keyTimeCreated: s.TimeCreated, keyTimeUpdated: s.TimeUpdated,
		"databaseMode": s.DatabaseMode, "accessMode": s.AccessMode, "isHighlyAvailable": s.HA,
		"isHeatWaveClusterAttached": false, "availabilityDomain": s.AD, "faultDomain": s.FD, "shapeName": s.Shape,
		"currentPlacement": Doc{"availabilityDomain": s.AD, "faultDomain": s.FD},
		"endpoints":        []Doc{b.endpointDoc(s)}, "deletionPolicy": b.deletionDoc(s.Deletion),
		"backupPolicy": b.policyDoc(s.BackupPolicy), "crashRecovery": s.CrashRecovery,
		"databaseManagement": s.DBManagement,
	}
	putTags(d, s.Tags)

	if s.Description != "" {
		d["description"] = s.Description
	}

	if s.Details != "" {
		d["lifecycleDetails"] = s.Details
	}

	return d
}

func (b *InMemoryBackend) systemDoc(s *DBSystem) Doc {
	d := b.systemBase(s)
	ip, port := b.endpointFor(s)
	maxGB := max(s.MaxGB, s.DataGB)

	d["subnetId"], d["dataStorageSizeInGBs"] = s.SubnetID, s.DataGB
	d["dataStorage"] = Doc{
		"isAutoExpandStorageEnabled": s.AutoExpand, "maxStorageSizeInGBs": maxGB, "allocatedStorageSizeInGBs": s.DataGB,
		"dataStorageSizeInGBs": s.DataGB, "dataStorageSizeLimitInGBs": max(maxGB, maxStorageGB),
	}
	d["maintenance"], d["source"] = s.Maintenance, b.sourceDoc(s.Source)
	d["configurationId"], d["ipAddress"], d["port"], d["portX"] = s.ConfigID, ip, port, s.PortX
	d["channels"] = b.channelsOf(s.ID)

	if s.Hostname != "" {
		d["hostnameLabel"] = s.Hostname
	}

	if s.BackupPolicy.PitrEnabled {
		d["pointInTimeRecoveryDetails"] = Doc{
			"timeEarliestRecoveryPoint": s.TimeCreated, "timeLatestRecoveryPoint": b.nowUTC(),
		}
	}

	for k, v := range s.Extra {
		d[k] = v
	}

	return d
}

func (b *InMemoryBackend) systemSummary(s *DBSystem) Doc {
	return b.systemBase(s)
}

// GetDBSystem returns one DB system.
func (b *InMemoryBackend) GetDBSystem(id string) (Doc, string, error) {
	b.mu.Lock("GetDbSystem")
	defer b.mu.Unlock()

	b.settleID(id)

	s, ok := b.systems[id]
	if !ok {
		return nil, "", errNotFound(id)
	}

	return b.systemDoc(s), s.Etag, nil
}

// DBSystemListIn are the ListDBSystems filters.
type DBSystemListIn struct {
	Compartment     string
	ID              string
	Name            string
	State           string
	ConfigurationID string
	HeatWave        *bool
	ListOpts
}

func (in DBSystemListIn) matches(s *DBSystem) bool {
	if s.CompartmentID != in.Compartment || (in.ID != "" && s.ID != in.ID) ||
		(in.Name != "" && s.Name != in.Name) || (in.ConfigurationID != "" && s.ConfigID != in.ConfigurationID) ||
		(in.HeatWave != nil && *in.HeatWave) {
		return false
	}

	return stateMatches(s.State, in.State)
}

// stateMatches filters by lifecycleState; without one, DELETED resources are hidden.
func stateMatches(state, want string) bool {
	if want == "" {
		return state != stateDeleted
	}

	return strings.EqualFold(state, want)
}

// ListDBSystems lists the DB systems of a compartment.
func (b *InMemoryBackend) ListDBSystems(in DBSystemListIn) (page.Page[Doc], error) {
	if aerr := firstErr(
		b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, keyDisplayName, keyTimeCreated),
		validEnum(keyLifecycle, in.State, stateCreating, stateActive, stateInactive, stateUpdating,
			stateDeleting, stateDeleted, stateFailed),
	); aerr != nil {
		return page.Page[Doc]{}, aerr
	}

	b.mu.Lock("ListDbSystems")
	defer b.mu.Unlock()

	b.settleAll()

	var all []*DBSystem

	for _, s := range b.systems {
		if in.matches(s) {
			all = append(all, s)
		}
	}

	pg := paginate(all, in.ListOpts, func(s *DBSystem) sortKeys {
		return sortKeys{created: s.TimeCreated, updated: s.TimeUpdated, name: s.Name, seq: s.Seq}
	})

	out := make([]Doc, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, b.systemSummary(s))
	}

	return page.Page[Doc]{Data: out, Next: pg.Next}, nil
}

func (h *Handler) createDBSystem(q *req) error {
	var in CreateDBSystemIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.Extra = extras(q.body)

	d, etag, wr, err := h.Backend.CreateDBSystem(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}

func extras(body []byte) map[string]json.RawMessage {
	var all map[string]json.RawMessage
	if json.Unmarshal(body, &all) != nil {
		return nil
	}

	out := map[string]json.RawMessage{}

	for _, k := range extraKeys {
		if v, ok := all[k]; ok && string(v) != "null" {
			out[k] = v
		}
	}

	return out
}

func (h *Handler) getDBSystem(q *req) error {
	d, etag, err := h.Backend.GetDBSystem(q.param(paramDBSystem))

	return reply(q, d, etag, err)
}

func (h *Handler) listDBSystems(q *req) error {
	in := DBSystemListIn{
		Compartment: q.query(keyCompartment), ID: q.query(paramDBSystem), Name: q.query(keyDisplayName),
		State: q.query(keyLifecycle), ConfigurationID: q.query("configurationId"), ListOpts: q.listOpts(),
	}
	if v := q.query("isHeatWaveClusterAttached"); v != "" {
		t := strings.EqualFold(v, "true")
		in.HeatWave = &t
	}

	pg, err := h.Backend.ListDBSystems(in)

	return collectionReply(q, pg, err)
}
