package azurearm

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	acaKeyPrefix       = "~aca/"
	acaReadyTimeout    = 90 * time.Second
	acaDefaultCooldown = 300 * time.Second
	acaDefaultConc     = 10
	acaReplicaIDLen    = 5
	acaReplicaHashLen  = 10
)

// ACAClock is the clock that drives scale-to-zero.
type ACAClock = gcpscale.Clock

var errACAStart = errors.New("replica failed to start")

type acaReplica struct {
	Created  time.Time
	readyErr error
	Name     string
	Key      string
	ready    sync.Once
	Port     int
}

func (r *acaReplica) waitReady() error {
	r.ready.Do(func() {
		if r.Port == 0 {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), acaReadyTimeout)
		defer cancel()

		if err := gcpserve.WaitReady(ctx, r.Port); err != nil {
			r.readyErr = fmt.Errorf("replica %s did not answer on its target port: %w", r.Name, err)
		}
	})

	return r.readyErr
}

// acaRevSpec is everything needed to run replicas of one revision, captured when its pool is created so that new
// secret values apply only after a restart.
type acaRevSpec struct {
	Key        string
	App        string
	Revision   string
	Domain     string
	Props      aciPropsWire
	Cooldown   time.Duration
	TargetPort int
	Min        int
	Max        int
	Conc       int
}

type acaPool struct {
	pool     *gcpscale.Pool[*acaReplica]
	spec     *acaRevSpec
	replicas map[string]*acaReplica
	failure  string
}

type acaRuntime struct {
	engine    *ACIEngine
	clock     gcpscale.Clock
	pick      func(int) int
	transport *http.Transport
	mu        *lockmetrics.RWMutex
	pools     map[string]*acaPool
	failures  map[string]string
}

func newACARuntime(engine *ACIEngine) *acaRuntime {
	return &acaRuntime{
		engine: engine, clock: gcpscale.RealClock{}, pick: rand.IntN, transport: httpclient.NewTransport(),
		mu: lockmetrics.New("azurearm.containerapps.runtime"), pools: map[string]*acaPool{},
		failures: map[string]string{},
	}
}

func (rt *acaRuntime) setClock(c gcpscale.Clock) {
	rt.mu.Lock("setClock")
	defer rt.mu.Unlock()

	rt.clock = c
}

func (rt *acaRuntime) setPick(fn func(int) int) {
	rt.mu.Lock("setPick")
	defer rt.mu.Unlock()

	rt.pick = fn
}

func (rt *acaRuntime) draw(n int) int {
	rt.mu.RLock("draw")
	fn := rt.pick
	rt.mu.RUnlock()

	return fn(n)
}

func (rt *acaRuntime) failureOf(key string) string {
	rt.mu.RLock("failureOf")
	defer rt.mu.RUnlock()

	if ap := rt.pools[key]; ap != nil && ap.failure != "" {
		return ap.failure
	}

	return rt.failures[key]
}

func (rt *acaRuntime) setFailure(key, msg string) {
	rt.mu.Lock("setFailure")
	defer rt.mu.Unlock()

	if msg == "" {
		delete(rt.failures, key)
	} else {
		rt.failures[key] = msg
	}
}

func (rt *acaRuntime) hasPool(key string) bool {
	rt.mu.RLock("hasPool")
	defer rt.mu.RUnlock()

	return rt.pools[key] != nil
}

// poolFor returns key's pool, creating it from spec when missing; created reports a fresh pool.
func (rt *acaRuntime) poolFor(spec *acaRevSpec) (*acaPool, bool) {
	rt.mu.Lock("poolFor")
	defer rt.mu.Unlock()

	if ap := rt.pools[spec.Key]; ap != nil {
		return ap, false
	}

	ap := &acaPool{spec: spec, replicas: map[string]*acaReplica{}}
	ap.pool = gcpscale.New(strings.ReplaceAll(spec.Key, "/", "."), gcpscale.Config[*acaReplica]{
		Start: func(context.Context) (*acaReplica, error) { return rt.launch(ap) },
		Stop:  func(r *acaReplica) { rt.stopReplica(ap, r) },
		Limits: func() gcpscale.Limits {
			return gcpscale.Limits{Min: spec.Min, Max: spec.Max, Concurrency: spec.Conc}
		},
		Clock:       rt.clock,
		IdleTimeout: spec.Cooldown,
	})
	rt.pools[spec.Key] = ap
	delete(rt.failures, spec.Key)

	return ap, true
}

// ensure starts n replicas when the revision has no pool yet (scaled-down revisions are left alone).
func (rt *acaRuntime) ensure(ctx context.Context, spec *acaRevSpec, n int) error {
	ap, created := rt.poolFor(spec)
	if !created {
		return nil
	}

	return ap.pool.Ensure(ctx, n)
}

func (rt *acaRuntime) acquire(
	ctx context.Context, key string, build func() (*acaRevSpec, error),
) (*acaReplica, func(), error) {
	rt.mu.RLock("acquire")
	ap := rt.pools[key]
	rt.mu.RUnlock()

	if ap == nil {
		spec, err := build()
		if err != nil {
			rt.setFailure(key, err.Error())

			return nil, nil, err
		}

		ap, _ = rt.poolFor(spec)
	}

	rep, release, err := ap.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire replica: %w", err)
	}

	return rep, release, nil
}

func (rt *acaRuntime) launch(ap *acaPool) (*acaReplica, error) {
	spec := ap.spec
	name := fmt.Sprintf("%s-%s-%s", spec.Revision, awsid.NewN("", acaReplicaHashLen), awsid.NewN("", acaReplicaIDLen))
	key := spec.Key + "/" + name

	rt.engine.Deploy(aciConfig{Key: key, Name: name, Props: spec.replicaProps(name)})

	view, ok := rt.engine.View(key)
	if !ok || view.Failure != "" {
		rt.engine.Remove(key)

		reason := view.Failure
		if reason == "" {
			reason = "the container engine did not start the replica"
		}

		rt.mu.Lock("launch")
		ap.failure = reason
		rt.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", errACAStart, reason)
	}

	rep := &acaReplica{Name: name, Key: key, Port: view.HostPorts[spec.TargetPort], Created: time.Now().UTC()}

	rt.mu.Lock("launch")
	ap.failure = ""
	ap.replicas[name] = rep
	rt.mu.Unlock()

	return rep, nil
}

func (rt *acaRuntime) stopReplica(ap *acaPool, r *acaReplica) {
	rt.mu.Lock("stopReplica")
	delete(ap.replicas, r.Name)
	rt.mu.Unlock()

	rt.engine.Remove(r.Key)
}

func (rt *acaRuntime) closeRevisions(keys []string) {
	for _, k := range keys {
		rt.mu.Lock("closeRevisions")
		ap := rt.pools[k]
		delete(rt.pools, k)
		delete(rt.failures, k)
		rt.mu.Unlock()

		if ap != nil {
			ap.pool.Close()
		}
	}
}

func (rt *acaRuntime) closeAll() {
	rt.mu.RLock("closeAll")
	keys := make([]string, 0, len(rt.pools))

	for k := range rt.pools {
		keys = append(keys, k)
	}
	rt.mu.RUnlock()

	rt.closeRevisions(keys)

	rt.mu.Lock("closeAll")
	rt.failures = map[string]string{}
	rt.mu.Unlock()

	rt.transport.CloseIdleConnections()
	rt.engine.RemovePrefixed(acaKeyPrefix)
}

func (rt *acaRuntime) removeKeys(keys []string) {
	for _, k := range keys {
		rt.engine.Remove(k)
	}
}

// replicasOf lists the revision's live replicas, oldest first.
func (rt *acaRuntime) replicasOf(key string) []*acaReplica {
	rt.mu.RLock("replicasOf")
	defer rt.mu.RUnlock()

	ap := rt.pools[key]
	if ap == nil {
		return nil
	}

	out := make([]*acaReplica, 0, len(ap.replicas))
	for _, r := range ap.replicas {
		out = append(out, r)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Created.Equal(out[j].Created) {
			return out[i].Name < out[j].Name
		}

		return out[i].Created.Before(out[j].Created)
	})

	return out
}

// replicaProps returns the group properties for one replica: the revision's containers plus the platform variables.
func (s *acaRevSpec) replicaProps(replica string) aciPropsWire {
	p := s.Props
	p.Containers = make([]aciContainerWire, 0, len(s.Props.Containers))

	for _, c := range s.Props.Containers {
		env := append([]aciEnvWire(nil), c.Properties.EnvironmentVariables...)

		for _, kv := range [][2]string{
			{"CONTAINER_APP_NAME", s.App}, {"CONTAINER_APP_REVISION", s.Revision},
			{"CONTAINER_APP_REPLICA_NAME", replica}, {"CONTAINER_APP_HOSTNAME", s.Revision + "." + s.Domain},
			{"CONTAINER_APP_ENV_DNS_SUFFIX", s.Domain},
		} {
			env = append(env, aciEnvWire{Name: kv[0], Value: new(kv[1])})
		}

		if s.TargetPort > 0 {
			env = append(env, aciEnvWire{Name: "CONTAINER_APP_PORT", Value: new(strconv.Itoa(s.TargetPort))})
		}

		c.Properties.EnvironmentVariables = env
		p.Containers = append(p.Containers, c)
	}

	return p
}
