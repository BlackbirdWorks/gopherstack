package gcpsql

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultRegion  = "us-central1"
	defaultVersion = "MYSQL_8_0"
	passwordBytes  = 18
	maxNameLen     = 98
)

var instanceNameRe = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

// InstanceInput is the writable part of sql#instance.
type InstanceInput struct {
	Settings           map[string]any `json:"settings"`
	Name               string         `json:"name"`
	Region             string         `json:"region"`
	DatabaseVersion    string         `json:"databaseVersion"`
	RootPassword       string         `json:"rootPassword"`
	InstanceType       string         `json:"instanceType"`
	MasterInstanceName string         `json:"masterInstanceName"`
	MaintenanceVersion string         `json:"maintenanceVersion"`
}

func randomPassword() string {
	b := make([]byte, passwordBytes)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func validateInstance(in *InstanceInput) error {
	if len(in.Name) > maxNameLen || !instanceNameRe.MatchString(in.Name) {
		return errInvalid("Invalid instance name: %q.", in.Name)
	}

	if _, _, err := parseVersion(in.DatabaseVersion); err != nil {
		return versionError(in.DatabaseVersion)
	}

	if settingString(in.Settings, "tier") == "" {
		return errInvalid("Missing parameter: Tier.")
	}

	return nil
}

func serviceAccount(project, name string) string {
	sum := sha256.Sum256([]byte(project + "/" + name))

	return "p" + hex.EncodeToString(sum[:5]) + "-" + hex.EncodeToString(sum[5:8]) +
		"@gcp-sa-cloud-sql.iam.gserviceaccount.com"
}

func (b *InMemoryBackend) newInstance(project string, in *InstanceInput) (*instRec, error) {
	caPEM, keyPEM, ca, err := newServerCA(project, in.Name, b.now())
	if err != nil {
		return nil, err
	}

	settings := asMap(deepCopy(in.Settings))
	fillDefaults(settings, settingsDefaults(in.DatabaseVersion, in.Region))

	if in.RootPassword == "" {
		in.RootPassword = randomPassword()
	}

	rec := &instRec{
		ServerID:  randomServerID(),
		fresh:     true,
		Databases: map[string]*Database{}, Users: map[string]*userRec{}, Backups: map[string]*backupRec{},
		Certs: map[string]*certRec{}, RootPassword: in.RootPassword, CACert: caPEM, CAKey: keyPEM,
		Inst: Instance{
			Kind: kindInstance, State: statePending, DatabaseVersion: in.DatabaseVersion, Settings: settings,
			InstanceType: "CLOUD_SQL_INSTANCE", Project: project, BackendType: "SECOND_GEN", Name: in.Name,
			Region: in.Region, GceZone: settingString(asMap(settings["locationPreference"]), "zone"),
			SelfLink: instanceLink(project, in.Name), ConnectionName: project + ":" + in.Region + ":" + in.Name,
			CreateTime: stamp(b.now()), ServerCaCert: ca, MaintenanceVersion: in.MaintenanceVersion,
			ServiceAccountEmailAddress: serviceAccount(
				project,
				in.Name,
			), SQLNetworkArchitecture: "NEW_NETWORK_ARCHITECTURE",
		},
	}
	seedAccounts(rec)
	rec.Inst.Etag = etagOf(rec.Inst)

	return rec, nil
}

// InsertInstance creates an instance, or a read replica when masterInstanceName is set, and starts its engine.
func (b *InMemoryBackend) InsertInstance(project string, in InstanceInput) (*Operation, error) {
	replica := in.MasterInstanceName != "" || in.InstanceType == instanceTypeReplica

	b.mu.Lock("InsertInstance")
	defer b.mu.Unlock()

	var prim *instRec

	if replica {
		var err error
		if prim, err = b.prepareReplicaLocked(project, &in); err != nil {
			return nil, err
		}
	}

	if in.Region == "" {
		in.Region = defaultRegion
	}

	if in.DatabaseVersion == "" {
		in.DatabaseVersion = defaultVersion
	}

	if err := validateInstance(&in); err != nil {
		return nil, err
	}

	rec, err := b.newInstance(project, &in)
	if err != nil {
		return nil, err
	}

	if _, exists := b.instances[instKey(project, in.Name)]; exists {
		return nil, errConflict("instanceAlreadyExists",
			"The Cloud SQL instance already exists. When you delete an instance, you can't reuse the name of the "+
				"deleted instance until one week from the deletion date.")
	}

	b.instances[instKey(project, in.Name)] = rec

	opType := "CREATE"
	if replica {
		b.linkReplicaLocked(prim, rec)

		opType = "CREATE_REPLICA"
	}

	return b.submitLocked(rec, project, opType, opMeta{}, func(ctx context.Context) error {
		return b.bringUp(ctx, rec)
	}), nil
}

func (b *InMemoryBackend) viewLocked(rec *instRec) *Instance {
	cp := rec.Inst
	cp.Settings = asMap(deepCopy(rec.Inst.Settings))
	cp.IPAddresses = append([]IPMapping(nil), rec.Inst.IPAddresses...)
	cp.ReplicaNames = append([]string(nil), rec.Inst.ReplicaNames...)

	return &cp
}

// GetInstance returns an instance.
func (b *InMemoryBackend) GetInstance(project, name string) (*Instance, error) {
	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, err
	}

	return b.viewLocked(rec), nil
}

// ListInstances lists the instances of a project sorted by name.
func (b *InMemoryBackend) ListInstances(project, token string, limit int) page.Page[*Instance] {
	b.mu.RLock("ListInstances")

	var all []*Instance

	for _, rec := range b.instances {
		if rec.Inst.Project == project && !rec.gone {
			all = append(all, b.viewLocked(rec))
		}
	}
	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	return page.New(all, token, limit, defaultPage)
}

func (b *InMemoryBackend) touchLocked(rec *instRec) { rec.Inst.Etag = etagOf(rec.Inst) }

// UpdateInstance patches (merge) or replaces (PUT) the settings of an instance.
func (b *InMemoryBackend) UpdateInstance(project, name string, in InstanceInput, replace bool) (*Operation, error) {
	b.mu.Lock("UpdateInstance")
	defer b.mu.Unlock()

	rec, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, err
	}

	if rec.Inst.State == stateDeleting {
		return nil, errNotRunnable()
	}

	if in.DatabaseVersion != "" && in.DatabaseVersion != rec.Inst.DatabaseVersion {
		return nil, errInvalid("Changing the database version is not supported by the emulator.")
	}

	old := rec.Inst.Settings
	next, err := nextSettings(rec, in, replace)
	if err != nil {
		return nil, err
	}

	rec.Inst.Settings = next
	b.touchLocked(rec)

	task := b.settingsTask(rec, old, next)

	return b.submitLocked(rec, project, "UPDATE", opMeta{}, task), nil
}

func nextSettings(rec *instRec, in InstanceInput, replace bool) (map[string]any, error) {
	cur := rec.Inst.Settings
	next := asMap(deepCopy(cur))

	if replace {
		if settingString(in.Settings, "tier") == "" {
			return nil, errInvalid("Missing parameter: Tier.")
		}

		if v, ok := in.Settings["settingsVersion"].(string); ok && v != settingString(cur, "settingsVersion") {
			return nil, newErr(http.StatusPreconditionFailed, "conditionNotMet", "Precondition check failed.")
		}

		next = asMap(deepCopy(in.Settings))
		fillDefaults(next, settingsDefaults(rec.Inst.DatabaseVersion, rec.Inst.Region))
	} else {
		mergePatch(next, in.Settings)
	}

	next["settingsVersion"] = bumpVersion(settingString(cur, "settingsVersion"))

	return next, nil
}

func bumpVersion(v string) string {
	n, _ := strconv.Atoi(v)

	return strconv.Itoa(n + 1)
}

// settingsTask applies the engine-visible part of a settings change.
func (b *InMemoryBackend) settingsTask(rec *instRec, old, next map[string]any) func(context.Context) error {
	oldPolicy, newPolicy := settingString(old, "activationPolicy"), settingString(next, "activationPolicy")
	flagsChanged := flagsSig(flagsOf(old)) != flagsSig(flagsOf(next))
	newFlags := flagsOf(next)
	version := rec.Inst.DatabaseVersion
	pitrChanged := pitrOn(version, old) != pitrOn(version, next)
	tierChanged := settingString(old, "tier") != settingString(next, "tier")

	return func(ctx context.Context) error {
		switch {
		case oldPolicy != policyNever && newPolicy == policyNever:
			return b.suspend(ctx, rec)
		case oldPolicy == policyNever && newPolicy != policyNever:
			return b.resume(ctx, rec)
		}

		if tierChanged {
			if relaunched, err := b.relaunch(ctx, rec); relaunched || err != nil {
				return err
			}
		}

		if flagsChanged {
			if err := b.restartEngine(ctx, rec, newFlags); err != nil {
				return err
			}
		}

		if pitrChanged {
			return b.syncPITR(ctx, rec, b.engineOf(rec))
		}

		return nil
	}
}

// relaunch recreates the engine container so a new tier's CPU and memory limits apply. It only does so when the
// data survives in a volume and no replica is tied to the container; otherwise the limits apply at the next launch.
func (b *InMemoryBackend) relaunch(ctx context.Context, rec *instRec) (bool, error) {
	eng := b.engineOf(rec)

	p, ok := eng.(interface{ Persistent() bool })
	if !ok || !p.Persistent() {
		return false, nil
	}

	b.mu.Lock("relaunch")
	tied := len(rec.Inst.ReplicaNames) > 0 || b.isReplica(rec)
	if !tied {
		rec.eng = nil
	}
	b.mu.Unlock()

	if tied {
		return false, nil
	}

	eng.Close(ctx)

	return true, b.bringUp(ctx, rec)
}

// RestartInstance restarts the engine, applying the configured flags.
func (b *InMemoryBackend) RestartInstance(project, name string) (*Operation, error) {
	b.mu.Lock("RestartInstance")
	defer b.mu.Unlock()

	rec, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, err
	}

	if rec.Inst.State == stateSuspended || rec.Inst.State == stateDeleting {
		return nil, errNotRunnable()
	}

	flags := flagsOf(rec.Inst.Settings)

	return b.submitLocked(rec, project, "RESTART", opMeta{}, func(ctx context.Context) error {
		return b.restartEngine(ctx, rec, flags)
	}), nil
}

// DeleteInstance removes an instance and its engine.
func (b *InMemoryBackend) DeleteInstance(project, name string) (*Operation, error) {
	b.mu.Lock("DeleteInstance")
	defer b.mu.Unlock()

	rec, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, err
	}

	if rec.Inst.State == stateDeleting {
		return nil, errNotRunnable()
	}

	if v, _ := rec.Inst.Settings["deletionProtectionEnabled"].(bool); v {
		return nil, errInvalid("The instance cannot be deleted while deletion protection is enabled.")
	}

	if len(rec.Inst.ReplicaNames) > 0 {
		return nil, errInvalid("The instance has read replicas; delete them before deleting the primary.")
	}

	b.unlinkReplicaLocked(rec)
	rec.Inst.State = stateDeleting
	b.touchLocked(rec)

	return b.submitLocked(rec, project, "DELETE", opMeta{}, func(ctx context.Context) error {
		b.mu.Lock("DeleteInstance.task")
		eng := rec.eng
		rec.eng, rec.gone = nil, true
		delete(b.instances, instKey(project, name))
		b.mu.Unlock()

		purgeEngine(ctx, eng)

		return nil
	}), nil
}

// purgeEngine closes eng and deletes its persistent storage.
func purgeEngine(ctx context.Context, eng Engine) {
	switch p := eng.(type) {
	case nil:
	case Purger:
		p.Purge(ctx)
	default:
		eng.Close(ctx)
	}
}

func (b *InMemoryBackend) engineOf(rec *instRec) Engine {
	b.mu.RLock("engineOf")
	defer b.mu.RUnlock()

	return rec.eng
}

func (b *InMemoryBackend) setState(rec *instRec, state string) {
	b.mu.Lock("setState")
	rec.Inst.State = state
	b.touchLocked(rec)
	b.mu.Unlock()
}

func (b *InMemoryBackend) restartEngine(ctx context.Context, rec *instRec, flags []Flag) error {
	eng := b.engineOf(rec)
	if eng == nil {
		return nil
	}

	return eng.Restart(ctx, flags)
}

func (b *InMemoryBackend) suspend(ctx context.Context, rec *instRec) error {
	if eng := b.engineOf(rec); eng != nil {
		if err := eng.Stop(ctx); err != nil {
			return err
		}
	}

	b.setState(rec, stateSuspended)

	return nil
}

func (b *InMemoryBackend) resume(ctx context.Context, rec *instRec) error {
	if eng := b.engineOf(rec); eng != nil {
		if err := eng.Start(ctx); err != nil {
			return err
		}

		b.setState(rec, stateRunnable)

		return nil
	}

	return b.bringUp(ctx, rec)
}

// launchSpec describes the engine of rec; for a replica it also readies the primary. A replica with a nil
// source is state-only like its primary.
func (b *InMemoryBackend) launchSpec(ctx context.Context, rec *instRec) (LaunchSpec, Engine, bool, error) {
	b.mu.RLock("launchSpec")
	spec := LaunchSpec{
		ID: rec.Inst.Name, Project: rec.Inst.Project, Version: rec.Inst.DatabaseVersion, Password: rec.RootPassword,
		ServerID: rec.ServerID, Persist: true, Recovery: rec.recovery, Fresh: rec.fresh,
	}
	tier := settingString(rec.Inst.Settings, "tier")
	replica := b.isReplica(rec)
	b.mu.RUnlock()

	spec.NanoCPUs, spec.MemoryBytes = tierLimits(tier)

	if !replica {
		return spec, nil, false, nil
	}

	src, err := b.primaryEngine(ctx, rec)
	if err != nil || src == nil {
		return spec, nil, true, err
	}

	rp, ok := src.(Replicator)
	if !ok {
		return spec, nil, true, errNoReplication
	}

	rs, err := rp.ReplicationSource(ctx)
	if err != nil {
		return spec, nil, true, err
	}

	spec.Replica = &rs

	return spec, src, true, nil
}

func (b *InMemoryBackend) sweepVolumes(ctx context.Context, l Launcher) {
	sw, ok := l.(VolumeSweeper)

	b.mu.Lock("sweepVolumes")
	first := !b.swept
	b.swept = true

	keep := map[string]bool{}
	for _, r := range b.instances {
		keep[volumeName("data", r.Inst.Project, r.Inst.Name)] = true
		keep[volumeName("pitr", r.Inst.Project, r.Inst.Name)] = true
	}
	b.mu.Unlock()

	if ok && first {
		sw.SweepVolumes(ctx, keep)
	}
}

// bringUp launches the engine of rec (or runs state-only) and replays databases, users and flags.
func (b *InMemoryBackend) bringUp(ctx context.Context, rec *instRec) error {
	b.mu.RLock("bringUp")
	l := b.launcher
	suspended := settingString(rec.Inst.Settings, "activationPolicy") == policyNever
	b.mu.RUnlock()

	if suspended {
		b.setState(rec, stateSuspended)

		return nil
	}

	b.sweepVolumes(ctx, l)

	spec, src, replica, err := b.launchSpec(ctx, rec)
	if err != nil {
		b.setState(rec, stateFailed)

		return err
	}

	var eng Engine
	if !replica || src != nil {
		eng, err = b.launch(ctx, l, spec)
		if err != nil {
			b.setState(rec, stateFailed)

			return err
		}
	}

	if eng != nil {
		if err = b.finishEngine(ctx, rec, eng, spec, src); err != nil {
			purgeAfterFailure(ctx, eng, spec)
			b.setState(rec, stateFailed)

			return err
		}
	} else if err = b.syncPITR(ctx, rec, nil); err != nil {
		b.setState(rec, stateFailed)

		return err
	}

	b.mu.Lock("bringUp")
	rec.eng, rec.recovery, rec.fresh = eng, nil, false
	rec.Inst.IPAddresses, rec.Inst.GopherstackEndpoint = addresses(eng)
	rec.Inst.State = stateRunnable
	b.touchLocked(rec)
	b.mu.Unlock()

	return nil
}

// purgeAfterFailure drops a half-built engine; volumes holding data a restart could still use are kept.
func purgeAfterFailure(ctx context.Context, eng Engine, spec LaunchSpec) {
	if spec.Recovery != nil || spec.Replica != nil {
		purgeEngine(ctx, eng)

		return
	}

	eng.Close(ctx)
}

// finishEngine brings a launched engine to the state the records describe.
func (b *InMemoryBackend) finishEngine(
	ctx context.Context, rec *instRec, eng Engine, spec LaunchSpec, src Engine,
) error {
	if spec.Replica != nil {
		return b.attachReplica(ctx, rec, eng, spec, src)
	}

	if spec.Recovery != nil {
		if rc, ok := eng.(Recoverer); ok {
			if err := rc.ApplyRecovery(ctx, *spec.Recovery); err != nil {
				return err
			}
		}

		if err := b.alignDatabases(ctx, rec, eng); err != nil {
			return err
		}
	}

	if err := b.replay(ctx, rec, eng); err != nil {
		return err
	}

	return b.syncPITR(ctx, rec, eng)
}

func (b *InMemoryBackend) attachReplica(
	ctx context.Context, rec *instRec, eng Engine, spec LaunchSpec, src Engine,
) error {
	rp, ok := eng.(Replicator)
	if !ok {
		return errNoReplication
	}

	var seed []byte

	if !isPG(rec.Inst.DatabaseVersion) {
		if err := b.replay(ctx, rec, eng); err != nil {
			return err
		}

		primary, err := b.primaryRec(rec)
		if err != nil {
			return err
		}

		srcRepl, isRepl := src.(Replicator)
		if !isRepl {
			return errNoReplication
		}

		if seed, err = srcRepl.ReplicaSeed(ctx, b.exportableDBs(primary, nil)); err != nil {
			return err
		}
	}

	if err := rp.AttachReplica(ctx, *spec.Replica, seed); err != nil {
		return err
	}

	b.mu.RLock("attachReplica")
	paused := rec.ReplicaPaused
	b.mu.RUnlock()

	if paused {
		return rp.SetReplication(ctx, false)
	}

	return nil
}

func (b *InMemoryBackend) primaryRec(rec *instRec) (*instRec, error) {
	name, err := masterName(rec.Inst.Project, rec.Inst.MasterInstanceName)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("primaryRec")
	defer b.mu.RUnlock()

	return b.lookupLocked(rec.Inst.Project, name)
}

func (b *InMemoryBackend) launch(ctx context.Context, l Launcher, spec LaunchSpec) (Engine, error) {
	if l == nil {
		return nil, nil //nolint:nilnil // no launcher means state-only
	}

	eng, err := l.Launch(ctx, spec)
	if errors.Is(err, ErrRuntimeUnavailable) {
		logger.Load(ctx).WarnContext(ctx, "gcpsql: no container runtime, instance is state-only",
			"instance", spec.ID, "error", err)

		return nil, nil //nolint:nilnil // state-only fallback
	}

	return eng, err
}

func addresses(eng Engine) ([]IPMapping, string) {
	ips := []IPMapping{{Type: "PRIMARY", IPAddress: "127.0.0.1"}}
	if eng == nil {
		return ips, ""
	}

	host, port := eng.Endpoint()
	ips[0].IPAddress = host

	return ips, host + ":" + strconv.Itoa(port)
}

func (b *InMemoryBackend) replay(ctx context.Context, rec *instRec, eng Engine) error {
	b.mu.RLock("replay")
	dbs := make([]*Database, 0, len(rec.Databases))
	users := make([]userRec, 0, len(rec.Users))

	for _, d := range rec.Databases {
		if !d.System {
			dbs = append(dbs, d)
		}
	}

	for _, u := range rec.Users {
		users = append(users, *u)
	}

	flags := flagsOf(rec.Inst.Settings)
	b.mu.RUnlock()

	for _, u := range users {
		if isBuiltinAdmin(rec.Inst.DatabaseVersion, u.User.Name) {
			continue
		}

		if err := eng.UpsertUser(
			ctx,
			UserSpec{Name: u.User.Name, Host: u.User.Host, Password: u.Password},
		); err != nil {
			return err
		}
	}

	existing, err := eng.ListDatabases(ctx)
	if err != nil {
		return err
	}

	for _, d := range dbs {
		if contains(existing, d.Name) {
			continue
		}

		if err = eng.CreateDatabase(ctx, d.Name, d.Charset, d.Collation); err != nil {
			return err
		}
	}

	if len(flags) > 0 {
		return eng.Restart(ctx, flags)
	}

	return nil
}

func isBuiltinAdmin(version, name string) bool {
	if strings.HasPrefix(version, "POSTGRES") {
		return name == pgAdminUser
	}

	return name == mysqlAdminUser
}
