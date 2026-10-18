package redshift

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	workgroupUnitPrefix  = "workgroup/"
	workgroupDefaultUser = "admin"
	workgroupDefaultDB   = "dev"
)

func workgroupUnitKey(name string) string { return workgroupUnitPrefix + name }

// rememberNamespacePasswordLocked keeps the admin password for the engine login only; it is never persisted.
func (b *InMemoryBackend) rememberNamespacePasswordLocked(namespace, password string) {
	if b.engine == nil || password == "" {
		return
	}

	if b.slNsPasswords == nil {
		b.slNsPasswords = make(map[string]string)
	}

	b.slNsPasswords[namespace] = password
}

// provisionWorkgroupLocked starts the Postgres container behind wg; the workgroup stays CREATING until it answers.
func (b *InMemoryBackend) provisionWorkgroupLocked(wg *Workgroup) {
	e := b.engine
	if e == nil || b.closed {
		return
	}

	user, dbName := workgroupDefaultUser, workgroupDefaultDB

	if ns, ok := b.slNamespaces.Get(wg.NamespaceName); ok {
		if ns.AdminUsername != "" {
			user = ns.AdminUsername
		}

		if ns.DBName != "" {
			dbName = ns.DBName
		}
	}

	password := b.slNsPasswords[wg.NamespaceName]
	if password == "" {
		password = randomPassword()
	}

	key := workgroupUnitKey(wg.WorkgroupName)
	ctx, cancel := context.WithCancel(context.Background())
	lb := &liveCluster{ctx: ctx, cancel: cancel, id: key, user: user, password: password, dbName: dbName}
	e.units[key] = lb
	wg.Status = slStatusCreating
	delete(b.slPending, slPendingKey(slKindWorkgroup, wg.WorkgroupName))

	e.wg.Go(func() { b.runUnit(lb) })
}

func (b *InMemoryBackend) workgroupStartingLocked(name string) bool {
	lb := b.unitLocked(workgroupUnitKey(name))

	return lb != nil && !lb.ready
}

// markWorkgroupReadyLocked publishes the live endpoint on the workgroup behind lb.
func (b *InMemoryBackend) markWorkgroupReadyLocked(lb *liveCluster) {
	lb.ready = true

	wg, ok := b.slWorkgroups.Get(strings.TrimPrefix(lb.id, workgroupUnitPrefix))
	if !ok {
		return
	}

	wg.Endpoint.Address, wg.Endpoint.Port, wg.Port = b.engine.cfg.Host, lb.port, lb.port

	if wg.Status == slStatusCreating {
		wg.Status = slStatusAvailable
	}
}

// failWorkgroupLocked drops a workgroup whose engine never started; Serverless has no failed status.
func (b *InMemoryBackend) failWorkgroupLocked(lb *liveCluster) {
	name := strings.TrimPrefix(lb.id, workgroupUnitPrefix)
	b.slWorkgroups.Delete(name)
	b.slWorkgroupIdx.remove(name)
	delete(b.slPending, slPendingKey(slKindWorkgroup, name))

	logger.Load(lb.ctx).WarnContext(lb.ctx, "redshift: workgroup engine failed to start",
		"code", engineStartFailedCode, "workgroup", name)
}

// WorkgroupEngineLogin returns the admin login of the engine behind workgroup name; ok is false without a ready one.
func (b *InMemoryBackend) WorkgroupEngineLogin(name string) (EngineLogin, bool) {
	return b.EngineLogin(workgroupUnitKey(name))
}

// WorkgroupEngineLogin returns the admin login of the engine behind workgroup name in region.
func (h *ServerlessHandler) WorkgroupEngineLogin(region, name string) (EngineLogin, bool) {
	return h.BackendFor(region).WorkgroupEngineLogin(name)
}

func (b *InMemoryBackend) relaunchWorkgroupsLocked() {
	for _, wg := range b.slWorkgroups.All() {
		b.provisionWorkgroupLocked(wg)
	}
}

func (b *InMemoryBackend) dropWorkgroupUnitsLocked() {
	if b.engine == nil {
		return
	}

	for id := range b.engine.units {
		if strings.HasPrefix(id, workgroupUnitPrefix) {
			b.dropUnitLocked(id)
		}
	}
}
