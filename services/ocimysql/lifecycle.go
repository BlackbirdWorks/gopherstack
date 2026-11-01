package ocimysql

import (
	"context"
	"encoding/json"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"
)

const (
	hoursPerDay       = 24
	shutdownImmediate = "IMMEDIATE"
)

func inCIDR(cidr, ip string) bool {
	p, err := netip.ParsePrefix(cidr)
	if err != nil {
		return false
	}

	a, err := netip.ParseAddr(ip)

	return err == nil && p.Contains(a)
}

func due(t *time.Time, now time.Time) bool { return t != nil && !t.After(now) }

// Sweep applies due lifecycle transitions, automatic backups and backup retention against the backend clock.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	b.settleAll()
	b.autoBackups()
	b.expireBackups()
}

func (b *InMemoryBackend) settleAll() {
	now := b.nowUTC()

	for _, s := range b.systems {
		settleSystem(s, now)
	}

	for _, bk := range b.backups {
		settleBackup(bk, now)
	}

	for _, c := range b.channels {
		settleChannel(c, now)
	}
}

func (b *InMemoryBackend) settleID(id string) {
	now := b.nowUTC()

	if s, ok := b.systems[id]; ok {
		settleSystem(s, now)
	}

	if bk, ok := b.backups[id]; ok {
		settleBackup(bk, now)
	}

	if c, ok := b.channels[id]; ok {
		settleChannel(c, now)
	}
}

func settleSystem(s *DBSystem, now time.Time) {
	if !due(s.TransitionAt, now) {
		return
	}

	s.TransitionAt = nil

	switch {
	case s.State == stateDeleting:
		s.State = stateDeleted
	case s.After != "":
		s.State = s.After
	default:
		s.State = stateActive
	}

	s.After = ""
}

func settleBackup(bk *Backup, now time.Time) {
	if !due(bk.TransitionAt, now) {
		return
	}

	bk.TransitionAt = nil

	if bk.State == stateDeleting {
		bk.State, bk.Dump = stateDeleted, nil

		return
	}

	bk.State = stateActive
}

func settleChannel(c *Channel, now time.Time) {
	if !due(c.TransitionAt, now) {
		return
	}

	c.TransitionAt = nil

	switch {
	case c.State == stateDeleting:
		c.State = stateDeleted
	case c.After != "":
		c.State = c.After
	default:
		c.State = stateActive
	}

	c.After = ""
}

// begin moves a DB system into a transitional state; the clock settles it unless the engine does.
func (b *InMemoryBackend) begin(s *DBSystem, op, after string, byEngine bool) string {
	s.State, s.After, s.TimeUpdated, s.Etag = stateUpdating, after, b.nowUTC(), newEtag()
	s.TransitionAt = nil

	if !byEngine {
		s.TransitionAt = b.transitionAt()
	}

	return b.newWR(op, s.CompartmentID, entityDBSystem, s.ID, actionUpdated, s.TransitionAt)
}

func (b *InMemoryBackend) mutable(id, ifMatch string, states ...string) (*DBSystem, *APIError) {
	b.settleID(id)

	s, ok := b.systems[id]
	if !ok || s.State == stateDeleted {
		return nil, errNotFound(id)
	}

	if aerr := checkEtag(s.Etag, ifMatch); aerr != nil {
		return nil, aerr
	}

	if slices.Contains(states, s.State) {
		return s, nil
	}

	return nil, errState("DB system " + id + " is in state " + s.State + "; operation requires " +
		strings.Join(states, " or "))
}

// UpdateDBSystemIn is the UpdateDBSystem body.
type UpdateDBSystemIn struct {
	Tags
	DataStorageGBs *int                       `json:"dataStorageSizeInGBs"`
	HostnameLabel  *string                    `json:"hostnameLabel"`
	SubnetID       *string                    `json:"subnetId"`
	ShapeName      *string                    `json:"shapeName"`
	MysqlVersion   *string                    `json:"mysqlVersion"`
	ConfigID       *string                    `json:"configurationId"`
	AdminUsername  *string                    `json:"adminUsername"`
	Port           *int                       `json:"port"`
	Extra          map[string]json.RawMessage `json:"-"`
	IPAddress      *string                    `json:"ipAddress"`
	Description    *string                    `json:"description"`
	HA             *bool                      `json:"isHighlyAvailable"`
	AdminPassword  *string                    `json:"adminPassword"`
	PortX          *int                       `json:"portX"`
	DataStorage    *DataStorageIn             `json:"dataStorage"`
	BackupPolicy   *BackupPolicyIn            `json:"backupPolicy"`
	Deletion       *DeletionPolicyIn          `json:"deletionPolicy"`
	Maintenance    map[string]any             `json:"maintenance"`
	DisplayName    *string                    `json:"displayName"`
	DBManagement   string                     `json:"databaseManagement"`
	DatabaseMode   string                     `json:"databaseMode"`
	AccessMode     string                     `json:"accessMode"`
	CrashRecovery  string                     `json:"crashRecovery"`
}

type updateEffect struct {
	structural bool
	admin      bool
	version    bool
}

func (b *InMemoryBackend) applyUpdate(n *DBSystem, in UpdateDBSystemIn) (updateEffect, *APIError) {
	var fx updateEffect

	if aerr := firstErr(
		validEnum("crashRecovery", in.CrashRecovery, enabled, disabled),
		validEnum("databaseManagement", in.DBManagement, enabled, disabled),
		validEnum("databaseMode", in.DatabaseMode, modeReadWrite, modeReadOnly),
		validEnum("accessMode", in.AccessMode, accessUnrestr, accessRestricted),
		b.applyIdentity(n, in),
	); aerr != nil {
		return fx, aerr
	}

	if aerr := firstErr(b.applyShapeAndVersion(n, in, &fx), applyStorage(n, in, &fx), b.applyAdmin(n, in, &fx),
		b.applyNetwork(n, in), applyPorts(n, in)); aerr != nil {
		return fx, aerr
	}

	pol, aerr := mergeBackupPolicy(n.BackupPolicy, in.BackupPolicy)
	del, aerr2 := mergeDeletion(n.Deletion, in.Deletion)

	if aerr = firstErr(aerr, aerr2); aerr != nil {
		return fx, aerr
	}

	n.BackupPolicy, n.Deletion = pol, del
	n.Tags.merge(in.Tags)

	for k, v := range in.Maintenance {
		if n.Maintenance == nil {
			n.Maintenance = map[string]any{}
		}

		n.Maintenance[k] = v
	}

	for k, v := range in.Extra {
		if n.Extra == nil {
			n.Extra = map[string]json.RawMessage{}
		}

		n.Extra[k] = v
	}

	setNonEmpty(&n.CrashRecovery, strings.ToUpper(in.CrashRecovery))
	setNonEmpty(&n.DBManagement, strings.ToUpper(in.DBManagement))
	setNonEmpty(&n.DatabaseMode, strings.ToUpper(in.DatabaseMode))
	setNonEmpty(&n.AccessMode, strings.ToUpper(in.AccessMode))

	return fx, nil
}

func setNonEmpty(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}

func (b *InMemoryBackend) applyIdentity(n *DBSystem, in UpdateDBSystemIn) *APIError {
	if in.DisplayName != nil {
		if aerr := validateName(*in.DisplayName); aerr != nil {
			return aerr
		}

		n.Name = *in.DisplayName
	}

	if in.Description != nil {
		n.Description = *in.Description
	}

	return nil
}

func (b *InMemoryBackend) applyShapeAndVersion(n *DBSystem, in UpdateDBSystemIn, fx *updateEffect) *APIError {
	oldShape, oldHA := n.Shape, n.HA

	if in.ShapeName != nil && *in.ShapeName != n.Shape {
		sh, ok := shapeByName(*in.ShapeName)
		if !ok || sh.support != supportDBSystem {
			return errParam("shapeName " + *in.ShapeName + " is not a MySQL DB system shape; see ListShapes")
		}

		n.Shape, fx.structural = *in.ShapeName, true
	}

	if in.HA != nil && *in.HA != n.HA {
		n.HA, fx.structural = *in.HA, true
	}

	if n.HA && n.Shape == shapeFree {
		return errParam("shape " + shapeFree + " does not support high availability")
	}

	if in.MysqlVersion != nil && *in.MysqlVersion != n.Version {
		switch {
		case !isKnownVersion(*in.MysqlVersion):
			return errParam("mysqlVersion " + *in.MysqlVersion + " is not supported; see ListVersions")
		case versionBefore(*in.MysqlVersion, n.Version):
			return errParam("mysqlVersion cannot be downgraded from " + n.Version)
		}

		n.Version, fx.structural, fx.version = *in.MysqlVersion, true, true
	}

	return b.applyConfig(n, in, oldShape, oldHA, fx)
}

func (b *InMemoryBackend) applyConfig(
	n *DBSystem,
	in UpdateDBSystemIn,
	oldShape string,
	oldHA bool,
	fx *updateEffect,
) *APIError {
	cur, curKnown := b.lookupConfig(n.ConfigID)
	curDefault := curKnown && cur.Type == configDefault

	switch {
	case in.ConfigID != nil && *in.ConfigID != n.ConfigID:
		id, aerr := b.resolveConfig(*in.ConfigID, n.Shape, n.HA)
		if aerr != nil {
			return aerr
		}

		n.ConfigID, fx.structural = id, true
	case n.Shape != oldShape && curDefault:
		n.ConfigID = defaultConfigID(n.Shape, n.HA)
	case n.Shape != oldShape:
		return errParam(
			"configuration " + n.ConfigID + " is for shape " + oldShape + "; set configurationId for " + n.Shape,
		)
	case n.HA != oldHA && curDefault:
		n.ConfigID = defaultConfigID(n.Shape, n.HA)
	}

	return nil
}

func applyStorage(n *DBSystem, in UpdateDBSystemIn, fx *updateEffect) *APIError {
	if in.DataStorageGBs != nil {
		if aerr := validStorage(*in.DataStorageGBs); aerr != nil {
			return aerr
		}

		if *in.DataStorageGBs < n.DataGB {
			return errParam("dataStorageSizeInGBs cannot be decreased")
		}
	}

	if ds := in.DataStorage; ds != nil {
		if ds.IsAutoExpandStorageEnabled != nil {
			n.AutoExpand = *ds.IsAutoExpandStorageEnabled
		}

		if ds.MaxStorageSizeInGBs != nil {
			if *ds.MaxStorageSizeInGBs <= n.DataGB || *ds.MaxStorageSizeInGBs > maxStorageGB {
				return errParam(
					"dataStorage.maxStorageSizeInGBs must be greater than the current size and at most 131072",
				)
			}

			n.MaxGB = *ds.MaxStorageSizeInGBs
		}
	}

	if in.DataStorageGBs != nil && *in.DataStorageGBs != n.DataGB {
		n.DataGB, fx.structural = *in.DataStorageGBs, true
	}

	return nil
}

func (b *InMemoryBackend) applyAdmin(n *DBSystem, in UpdateDBSystemIn, fx *updateEffect) *APIError {
	if in.AdminUsername == nil && in.AdminPassword == nil {
		return nil
	}

	if in.AdminPassword == nil {
		return errMissing("adminPassword is required to change the administrator")
	}

	user, pass := n.AdminUser, *in.AdminPassword
	if in.AdminUsername != nil {
		user = *in.AdminUsername
	}

	if aerr := validAdmin(user, pass); aerr != nil {
		return aerr
	}

	n.AdminUser, n.AdminPass, fx.admin, fx.structural = user, pass, true, true

	return nil
}

func (b *InMemoryBackend) applyNetwork(n *DBSystem, in UpdateDBSystemIn) *APIError {
	subnetChanged := in.SubnetID != nil && *in.SubnetID != n.SubnetID
	if !subnetChanged && in.IPAddress == nil && in.HostnameLabel == nil {
		return nil
	}

	sub, aerr := b.checkSubnet(orStr(deref(in.SubnetID), n.SubnetID))
	if aerr != nil {
		return aerr
	}

	label := n.Hostname
	if in.HostnameLabel != nil {
		label = *in.HostnameLabel
	}

	if aerr = validHostLabel(label); aerr != nil {
		return aerr
	}

	if label != "" && b.hostnameTaken(sub.ID, label, n.ID) {
		return errParam("hostnameLabel " + label + " is already used in the subnet")
	}

	want := deref(in.IPAddress)
	if want == "" && !subnetChanged {
		want = n.IP
	}

	ip, aerr := b.allocIP(sub, want, n.ID)
	if aerr != nil {
		return aerr
	}

	n.SubnetID, n.Hostname, n.IP = sub.ID, label, ip

	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func applyPorts(n *DBSystem, in UpdateDBSystemIn) *APIError {
	port, portX := n.Port, n.PortX
	if in.Port != nil {
		port = *in.Port
	}

	if in.PortX != nil {
		portX = *in.PortX
	}

	if aerr := validPorts(port, portX); aerr != nil {
		return aerr
	}

	n.Port, n.PortX = port, portX

	return nil
}

// UpdateDBSystem updates a DB system and returns its work request.
func (b *InMemoryBackend) UpdateDBSystem(id, ifMatch string, in UpdateDBSystemIn) (string, error) {
	b.mu.Lock("UpdateDbSystem")
	defer b.mu.Unlock()

	s, aerr := b.mutable(id, ifMatch, stateActive, stateInactive)
	if aerr != nil {
		return "", aerr
	}

	n := *s

	fx, aerr := b.applyUpdate(&n, in)
	if aerr != nil {
		return "", aerr
	}

	if (fx.admin || fx.version) && b.eng != nil && s.State != stateActive {
		return "", errState("DB system " + id + " must be ACTIVE to change the administrator or MySQL version")
	}

	prev := s.State
	*s = n

	if !fx.structural {
		s.TimeUpdated, s.Etag = b.nowUTC(), newEtag()
		wr := b.newWR(wrUpdateDBSystem, s.CompartmentID, entityDBSystem, s.ID, actionUpdated, nil)
		b.finishWR(wr, "")

		return wr, nil
	}

	engineJob := b.eng != nil && (fx.admin || fx.version)
	wr := b.begin(s, wrUpdateDBSystem, prev, engineJob)

	if engineJob {
		b.spawn(s.ID, wr, b.updateWork(s, fx), func(s *DBSystem) { s.State = prev })
	}

	return wr, nil
}

func (b *InMemoryBackend) updateWork(s *DBSystem, fx updateEffect) func(ctx context.Context) error {
	id, user, pass := s.ID, s.AdminUser, s.AdminPass
	oldInst := b.eng.live[id]

	return func(ctx context.Context) error {
		if fx.version {
			if oldInst != nil {
				oldInst.Close(ctx, false)
			}

			b.mu.Lock("updateWork")
			delete(b.eng.live, id)
			b.mu.Unlock()

			return b.launchWork(id, launchIn{admin: user, password: pass})(ctx)
		}

		if oldInst == nil {
			return errNoServer
		}

		return oldInst.EnsureAdmin(ctx, user, pass) //nolint:wrapcheck // engine errors carry the cause
	}
}

func parseShutdown(q *req) error {
	var in struct {
		ShutdownType string `json:"shutdownType"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	if in.ShutdownType == "" {
		return errMissing("shutdownType is required")
	}

	if aerr := validEnum("shutdownType", in.ShutdownType, shutdownImmediate, "FAST", "SLOW"); aerr != nil {
		return aerr
	}

	return nil
}

// StopDBSystem stops an ACTIVE DB system; data is kept.
func (b *InMemoryBackend) StopDBSystem(id, ifMatch string) (string, error) {
	b.mu.Lock("StopDbSystem")
	defer b.mu.Unlock()

	s, aerr := b.mutable(id, ifMatch, stateActive)
	if aerr != nil {
		return "", aerr
	}

	wr := b.begin(s, wrStopDBSystem, stateInactive, b.eng != nil)

	if b.eng != nil {
		b.spawn(id, wr, func(ctx context.Context) error {
			if inst := b.instance(id); inst != nil {
				return inst.Stop(ctx) //nolint:wrapcheck // engine errors carry the cause
			}

			return nil
		}, func(s *DBSystem) { s.State = stateInactive })
	}

	return wr, nil
}

// StartDBSystem starts an INACTIVE DB system.
func (b *InMemoryBackend) StartDBSystem(id, ifMatch string) (string, error) {
	b.mu.Lock("StartDbSystem")
	defer b.mu.Unlock()

	s, aerr := b.mutable(id, ifMatch, stateInactive)
	if aerr != nil {
		return "", aerr
	}

	wr := b.begin(s, wrStartDBSystem, stateActive, b.eng != nil)

	if b.eng != nil {
		b.spawn(id, wr, b.startWork(s), func(s *DBSystem) { s.State = stateActive })
	}

	return wr, nil
}

// startWork starts the stopped server or, when it is gone, launches a new one on the same data volume.
func (b *InMemoryBackend) startWork(s *DBSystem) func(ctx context.Context) error {
	id, user, pass := s.ID, s.AdminUser, s.AdminPass

	return func(ctx context.Context) error {
		if inst := b.instance(id); inst != nil {
			return inst.Start(ctx) //nolint:wrapcheck // engine errors carry the cause
		}

		return b.launchWork(id, launchIn{admin: user, password: pass})(ctx)
	}
}

// RestartDBSystem restarts an ACTIVE DB system.
func (b *InMemoryBackend) RestartDBSystem(id, ifMatch string) (string, error) {
	b.mu.Lock("RestartDbSystem")
	defer b.mu.Unlock()

	s, aerr := b.mutable(id, ifMatch, stateActive)
	if aerr != nil {
		return "", aerr
	}

	wr := b.begin(s, wrRestartDBSystem, stateActive, b.eng != nil)

	if b.eng != nil {
		start := b.startWork(s)

		b.spawn(id, wr, func(ctx context.Context) error {
			if inst := b.instance(id); inst != nil {
				if err := inst.Stop(ctx); err != nil {
					return err //nolint:wrapcheck // engine errors carry the cause
				}
			}

			return start(ctx)
		}, func(s *DBSystem) { s.State = stateActive })
	}

	return wr, nil
}

// DeleteDBSystem deletes a DB system, honouring its deletion policy.
func (b *InMemoryBackend) DeleteDBSystem(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteDbSystem")
	defer b.mu.Unlock()

	s, aerr := b.mutable(id, ifMatch, stateActive, stateInactive, stateFailed)
	if aerr != nil {
		return "", aerr
	}

	if s.Deletion.IsDeleteProtected {
		return "", newErr(http.StatusConflict, "Conflict",
			"DB system "+id+" is delete protected; set deletionPolicy.isDeleteProtected to false first")
	}

	wasStopped := s.State == stateInactive
	s.State, s.After, s.TimeUpdated, s.Etag = stateDeleting, "", b.nowUTC(), newEtag()
	s.TransitionAt = nil

	if s.Deletion.AutomaticBackupRetention == deleteBackups {
		b.dropAutomaticBackups(id)
	}

	final := s.Deletion.FinalBackup == requireFinal

	if b.eng == nil {
		s.TransitionAt = b.transitionAt()
		wr := b.newWR(wrDeleteDBSystem, s.CompartmentID, entityDBSystem, id, actionDeleted, s.TransitionAt)

		if final {
			b.newBackupRecord(s, creationManual, "Final backup of "+s.Name, s.BackupPolicy.RetentionInDays, nil)
		}

		return wr, nil
	}

	inst := b.eng.live[id]
	delete(b.eng.live, id)

	wr := b.newWR(wrDeleteDBSystem, s.CompartmentID, entityDBSystem, id, actionDeleted, nil)
	snap := *s
	b.spawn(id, wr, func(ctx context.Context) error {
		return b.teardown(ctx, &snap, inst, final, wasStopped)
	}, func(s *DBSystem) { s.State = stateDeleted })

	return wr, nil
}

// teardown takes the final backup, if required, and removes the server and its data volume.
func (b *InMemoryBackend) teardown(ctx context.Context, s *DBSystem, inst Instance, final, wasStopped bool) error {
	var err error

	if inst == nil {
		b.eng.launcher.RemoveVolume(ctx, s.ID)

		return nil
	}

	if final {
		if wasStopped {
			err = inst.Start(ctx)
		}

		var dump []byte

		if err == nil {
			dump, err = inst.Dump(ctx)
		}

		if err == nil {
			b.mu.Lock("teardown")
			b.newBackupRecord(s, creationManual, "Final backup of "+s.Name, s.BackupPolicy.RetentionInDays, dump)
			b.mu.Unlock()
		}
	}

	inst.Close(ctx, true)

	return err //nolint:wrapcheck // engine errors carry the cause
}

func (h *Handler) updateDBSystem(q *req) error {
	var in UpdateDBSystemIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.Extra = extras(q.body)

	wr, err := h.Backend.UpdateDBSystem(q.param(paramDBSystem), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteDBSystem(q *req) error {
	wr, err := h.Backend.DeleteDBSystem(q.param(paramDBSystem), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) startDBSystem(q *req) error {
	wr, err := h.Backend.StartDBSystem(q.param(paramDBSystem), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) stopDBSystem(q *req) error {
	if err := parseShutdown(q); err != nil {
		return err
	}

	wr, err := h.Backend.StopDBSystem(q.param(paramDBSystem), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) restartDBSystem(q *req) error {
	if err := parseShutdown(q); err != nil {
		return err
	}

	wr, err := h.Backend.RestartDBSystem(q.param(paramDBSystem), q.ifMatch())

	return acceptedWR(q, wr, err)
}
