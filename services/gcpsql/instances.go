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

	if in.MasterInstanceName != "" || in.InstanceType == "READ_REPLICA_INSTANCE" {
		return errInvalid("Read replicas are not supported by the emulator.")
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

// InsertInstance creates an instance and starts its engine.
func (b *InMemoryBackend) InsertInstance(project string, in InstanceInput) (*Operation, error) {
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

	b.mu.Lock("InsertInstance")
	defer b.mu.Unlock()

	if _, exists := b.instances[instKey(project, in.Name)]; exists {
		return nil, errConflict("instanceAlreadyExists",
			"The Cloud SQL instance already exists. When you delete an instance, you can't reuse the name of the "+
				"deleted instance until one week from the deletion date.")
	}

	b.instances[instKey(project, in.Name)] = rec

	return b.submitLocked(rec, project, "CREATE", opMeta{}, func(ctx context.Context) error {
		return b.bringUp(ctx, rec)
	}), nil
}

func (b *InMemoryBackend) viewLocked(rec *instRec) *Instance {
	cp := rec.Inst
	cp.Settings = asMap(deepCopy(rec.Inst.Settings))
	cp.IPAddresses = append([]IPMapping(nil), rec.Inst.IPAddresses...)

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

	return func(ctx context.Context) error {
		switch {
		case oldPolicy != policyNever && newPolicy == policyNever:
			return b.suspend(ctx, rec)
		case oldPolicy == policyNever && newPolicy != policyNever:
			return b.resume(ctx, rec)
		case flagsChanged:
			return b.restartEngine(ctx, rec, newFlags)
		}

		return nil
	}
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

	rec.Inst.State = stateDeleting
	b.touchLocked(rec)

	return b.submitLocked(rec, project, "DELETE", opMeta{}, func(ctx context.Context) error {
		b.mu.Lock("DeleteInstance.task")
		eng := rec.eng
		rec.eng, rec.gone = nil, true
		delete(b.instances, instKey(project, name))
		b.mu.Unlock()

		if eng != nil {
			eng.Close(ctx)
		}

		return nil
	}), nil
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

// bringUp launches the engine of rec (or runs state-only) and replays databases, users and flags.
func (b *InMemoryBackend) bringUp(ctx context.Context, rec *instRec) error {
	b.mu.RLock("bringUp")
	l := b.launcher
	spec := LaunchSpec{ID: rec.Inst.Name, Version: rec.Inst.DatabaseVersion, Password: rec.RootPassword}
	suspended := settingString(rec.Inst.Settings, "activationPolicy") == policyNever
	b.mu.RUnlock()

	if suspended {
		b.setState(rec, stateSuspended)

		return nil
	}

	eng, err := b.launch(ctx, l, spec)
	if err != nil {
		b.setState(rec, stateFailed)

		return err
	}

	if eng != nil {
		if err = b.replay(ctx, rec, eng); err != nil {
			eng.Close(ctx)
			b.setState(rec, stateFailed)

			return err
		}
	}

	b.mu.Lock("bringUp")
	rec.eng = eng
	rec.Inst.IPAddresses, rec.Inst.GopherstackEndpoint = addresses(eng)
	rec.Inst.State = stateRunnable
	b.touchLocked(rec)
	b.mu.Unlock()

	return nil
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

	for _, d := range dbs {
		if err := eng.CreateDatabase(ctx, d.Name, d.Charset, d.Collation); err != nil {
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
