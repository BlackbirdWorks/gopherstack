package azurearm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

var (
	errACAVault    = errors.New("key vault is not available")
	errACAUnsupVol = errors.New("volume type is not supported by this emulator")
)

const (
	acaCodeRevisionMode = "ContainerAppInvalidRevisionMode"
	acaCodeRevisionOff  = "ContainerAppRevisionInactive"
)

func acaIntOf(m map[string]any, k string, def int) int {
	if f, ok := acaNum(m, k); ok {
		return int(f)
	}

	return def
}

func acaRuleConcurrency(rules []any) int {
	for _, e := range rules {
		r := mapOf(e)

		kinds := []struct{ rule, key string }{
			{acaScaleHTTP, "concurrentRequests"}, {acaScaleTCP, "concurrentConnections"},
		}

		for _, kind := range kinds {
			if md := mapOf(mapOf(r[kind.rule])["metadata"]); md != nil {
				if n, err := strconv.Atoi(strOf(md, kind.key)); err == nil && n > 0 {
					return n
				}
			}
		}
	}

	return acaDefaultConc
}

// resolveSecrets returns the secret values by name; keyVaultUrl secrets are read from Key Vault.
func (p *ContainerAppsProvider) resolveSecrets(a *storedACAApp) (map[string]string, error) {
	out := map[string]string{}

	for _, e := range acaList(a.Config[fieldSecrets]) {
		s := mapOf(e)
		name := strOf(s, fieldName)

		if kv := strOf(s, fieldKeyVaultURL); kv != "" {
			ref, _ := parseVaultURL(kv)

			if p.vault == nil {
				return nil, fmt.Errorf("secret %q: %w", name, errACAVault)
			}

			v, err := p.vault.SecretValue(ref.vault, ref.secret, ref.version)
			if err != nil {
				return nil, fmt.Errorf("secret %q: reading %s from key vault %q: %w", name, ref.secret, ref.vault, err)
			}

			out[name] = v

			continue
		}

		out[name] = strOf(s, fieldValue)
	}

	return out, nil
}

func acaVolumeWires(t map[string]any, env *storedACAEnv, secrets map[string]string) ([]aciVolumeWire, error) {
	var out []aciVolumeWire

	for _, e := range acaList(t[fieldVolumes]) {
		v := mapOf(e)
		w := aciVolumeWire{Name: strOf(v, fieldName)}

		switch strings.ToLower(strOf(v, "storageType")) {
		case "", "emptydir":
			raw := json.RawMessage("{}")
			w.EmptyDir = &raw
		case "secret":
			w.Secret = acaSecretVolume(acaList(v[fieldSecrets]), secrets)
		case "azurefile":
			st := mapOf(env.Storages[strings.ToLower(strOf(v, fieldStorageName))].Props["azureFile"])
			w.AzureFile = &aciAzureFileWire{
				ShareName: strOf(st, "shareName"), StorageAccountName: strOf(st, "accountName"),
				ReadOnly: strOf(st, "accessMode") == "ReadOnly",
			}
		default:
			return nil, fmt.Errorf("volume %q (%s): %w", w.Name, strOf(v, "storageType"), errACAUnsupVol)
		}

		out = append(out, w)
	}

	return out, nil
}

func acaSecretVolume(items []any, secrets map[string]string) map[string]string {
	out := map[string]string{}

	if len(items) == 0 {
		for k, v := range secrets {
			out[k] = base64.StdEncoding.EncodeToString([]byte(v))
		}

		return out
	}

	for _, e := range items {
		it := mapOf(e)
		path := strOf(it, "path")

		if path == "" {
			path = strOf(it, "secretRef")
		}

		out[path] = base64.StdEncoding.EncodeToString([]byte(secrets[strOf(it, "secretRef")]))
	}

	return out
}

func acaContainerWire(c map[string]any, secrets map[string]string, port int) aciContainerWire {
	w := aciContainerWire{Name: strOf(c, fieldName), Properties: aciContainerPropsWire{Image: strOf(c, acaFieldImage)}}

	for _, a := range acaList(c["command"]) {
		w.Properties.Command = append(w.Properties.Command, fmt.Sprint(a))
	}

	for _, a := range acaList(c["args"]) {
		w.Properties.Args = append(w.Properties.Args, fmt.Sprint(a))
	}

	for _, e := range acaList(c["env"]) {
		ev := mapOf(e)
		val := strOf(ev, fieldValue)

		if ref := strOf(ev, "secretRef"); ref != "" {
			val = secrets[ref]
		}

		w.Properties.EnvironmentVariables = append(w.Properties.EnvironmentVariables,
			aciEnvWire{Name: strOf(ev, fieldName), Value: &val})
	}

	if s, err := acaContainerSizing(c); err == nil {
		w.Properties.Resources = &aciResourcesWire{Limits: &aciResourceWire{CPU: s.cpu, MemoryInGB: s.memGi}}
	}

	for _, m := range acaList(c["volumeMounts"]) {
		mm := mapOf(m)
		w.Properties.VolumeMounts = append(w.Properties.VolumeMounts, aciMountWire{
			Name: strOf(mm, "volumeName"), MountPath: strOf(mm, "mountPath"),
		})
	}

	if port > 0 {
		w.Properties.Ports = []aciPortWire{{Port: port, Protocol: aciProtoTCP}}
	}

	return w
}

// buildSpec captures how to run rev. The caller holds p.mu.
func (p *ContainerAppsProvider) buildSpec(a *storedACAApp, rev *storedACARevision) (*acaRevSpec, error) {
	env := p.envByID(a.EnvironmentID)
	if env == nil {
		return nil, fmt.Errorf("managed environment %s: %w", a.EnvironmentID, ErrResourceNotFound)
	}

	secrets, err := p.resolveSecrets(a)
	if err != nil {
		return nil, err
	}

	vols, err := acaVolumeWires(rev.Template, env, secrets)
	if err != nil {
		return nil, err
	}

	port := 0
	if in := a.ingress(); in != nil {
		port = acaIntOf(in, "targetPort", 0)
	}

	props := aciPropsWire{OSType: aciOSLinux, RestartPolicy: aciRestartAlways, Volumes: vols}

	for i, e := range acaList(rev.Template[fieldContainers]) {
		cport := 0
		if i == 0 {
			cport = port
		}

		props.Containers = append(props.Containers, acaContainerWire(mapOf(e), secrets, cport))
	}

	if port > 0 {
		props.IPAddress = &aciIPWire{Type: aciIPPublic, Ports: []aciPortWire{{Port: port, Protocol: aciProtoTCP}}}
	}

	for _, e := range acaList(a.Config[fieldRegistries]) {
		r := mapOf(e)
		if ref := strOf(r, "passwordSecretRef"); ref != "" {
			props.ImageRegistryCredentials = append(props.ImageRegistryCredentials,
				aciCredWire{Server: strOf(r, "server"), Username: strOf(r, "username"), Password: secrets[ref]})
		}
	}

	sc := mapOf(rev.Template[fieldScale])

	return &acaRevSpec{
		Props: props, Key: acaRevKey(a, rev.Name), App: a.Name, Revision: rev.Name, Domain: env.DefaultDomain,
		TargetPort: port, Min: acaIntOf(sc, fieldMinReplicas, 0), Max: acaIntOf(sc, fieldMaxReplicas, acaDefaultMax),
		Conc:     acaRuleConcurrency(acaList(sc["rules"])),
		Cooldown: time.Duration(acaIntOf(sc, "cooldownPeriod", int(acaDefaultCooldown.Seconds()))) * time.Second,
	}, nil
}

// readyRevision is the newest active revision that has not failed to start.
func (p *ContainerAppsProvider) readyRevision(a *storedACAApp) string {
	for _, r := range slices.Backward(a.Revisions) {
		if r.Active && p.rt.failureOf(acaRevKey(a, r.Name)) == "" {
			return r.Name
		}
	}

	return ""
}

// reconcile starts new active revisions and, in Single mode, retires the others once the newest one is ready. The
// caller holds opMu.
func (p *ContainerAppsProvider) reconcile(ctx context.Context, key string) {
	type job struct {
		err  error
		spec *acaRevSpec
		key  string
	}

	p.mu.RLock("reconcile")
	a, ok := p.apps[key]

	if !ok {
		p.mu.RUnlock()

		return
	}

	var jobs []job

	var stop []string

	for _, r := range a.Revisions {
		rk := acaRevKey(a, r.Name)

		switch {
		case a.Stopped || !r.Active:
			stop = append(stop, rk)
		case !p.rt.hasPool(rk):
			spec, err := p.buildSpec(a, r)
			jobs = append(jobs, job{spec: spec, key: rk, err: err})
		}
	}
	p.mu.RUnlock()

	p.rt.closeRevisions(stop)

	for _, j := range jobs {
		if j.err != nil {
			p.rt.setFailure(j.key, j.err.Error())

			continue
		}

		if err := p.rt.ensure(ctx, j.spec, max(j.spec.Min, 1)); err != nil {
			p.rt.setFailure(j.key, err.Error())
		}
	}

	p.retireOld(key)
}

func (p *ContainerAppsProvider) retireOld(key string) {
	p.mu.Lock("retireOld")

	a, ok := p.apps[key]
	if !ok {
		p.mu.Unlock()

		return
	}

	var closing []string

	if a.mode() == acaModeSingle {
		closing = p.retireBefore(a, p.readyRevision(a))
	}

	for _, n := range a.pruneInactive() {
		closing = append(closing, acaRevKey(a, n))
	}

	p.rebuildHostsLocked()
	p.mu.Unlock()

	p.rt.closeRevisions(closing)
}

func (p *ContainerAppsProvider) setStopped(ctx context.Context, id ResourceID, stopped bool) (map[string]any, error) {
	p.opMu.Lock("setStopped")
	defer p.opMu.Unlock()

	key := acaKey(id.ResourceGroup, id.LeafName())

	p.mu.Lock("setStopped")

	a, ok := p.apps[key]
	if !ok {
		p.mu.Unlock()

		return nil, ErrResourceNotFound
	}

	a.Stopped, a.Modified = stopped, p.now().UTC()
	p.rebuildHostsLocked()
	p.mu.Unlock()

	p.reconcile(ctx, key)

	p.mu.RLock("setStopped")
	defer p.mu.RUnlock()

	return p.appBody(id, a), nil
}

// revisionAction runs activate, deactivate or restart on a revision.
func (p *ContainerAppsProvider) revisionAction(ctx context.Context, id ResourceID, rev, action string) error {
	p.opMu.Lock("revisionAction")
	defer p.opMu.Unlock()

	key := acaKey(id.ResourceGroup, id.LeafName())

	p.mu.Lock("revisionAction")

	a, ok := p.apps[key]
	if !ok {
		p.mu.Unlock()

		return ErrResourceNotFound
	}

	r := a.revision(rev)
	if r == nil {
		p.mu.Unlock()

		return ErrResourceNotFound
	}

	if err := acaActionAllowed(a, r, action); err != nil {
		p.mu.Unlock()

		return err
	}

	rk := acaRevKey(a, r.Name)

	switch action {
	case acaActActivate:
		r.Active, r.LastActive = true, p.now().UTC()
	case acaActDeactivate:
		r.Active = false
	}

	p.rebuildHostsLocked()
	p.mu.Unlock()

	if action != acaActActivate {
		p.rt.closeRevisions([]string{rk})
	}

	p.reconcile(ctx, key)

	return nil
}

func acaActionAllowed(a *storedACAApp, r *storedACARevision, action string) error {
	if action != acaActRestart && a.mode() == acaModeSingle {
		return netErr(http.StatusBadRequest, acaCodeRevisionMode,
			"Revisions can only be %sd when activeRevisionsMode is Multiple or Labels.", action)
	}

	if action == acaActRestart && !r.Active {
		return netErr(
			http.StatusBadRequest,
			acaCodeRevisionOff,
			"Revision '%s' is inactive and cannot be restarted.",
			r.Name,
		)
	}

	return nil
}

// retireBefore deactivates the active revisions older than ready and returns their runtime keys.
func (p *ContainerAppsProvider) retireBefore(a *storedACAApp, ready string) []string {
	var closing []string

	if ready == "" {
		return nil
	}

	for _, r := range a.Revisions {
		if strings.EqualFold(r.Name, ready) {
			break
		}

		if r.Active {
			r.Active = false

			closing = append(closing, acaRevKey(a, r.Name))
		}
	}

	return closing
}
