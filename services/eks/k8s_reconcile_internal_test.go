package eks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const rbacPrefix = "/apis/rbac.authorization.k8s.io/v1/"

// fakeK8s is an in-memory stand-in for the k3s API server covering what the reconciler touches.
type fakeK8s struct {
	srv          *httptest.Server
	crbs         map[string]bool
	rbs          map[string]bool
	nodes        map[string]bool
	patches      map[string][]string
	deleted      []string
	mu           sync.Mutex
	corednsAvail int
}

func newFakeK8s(t *testing.T) *fakeK8s {
	t.Helper()

	f := &fakeK8s{
		crbs: map[string]bool{}, rbs: map[string]bool{}, nodes: map[string]bool{},
		patches: map[string][]string{}, corednsAvail: 1,
	}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)

	return f
}

func (f *fakeK8s) addNode(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nodes[name] = true
}

func (f *fakeK8s) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	path := r.URL.Path

	switch {
	case r.Method == http.MethodGet && path == "/api/v1/nodes":
		items := make([]map[string]any, 0, len(f.nodes))

		for name := range f.nodes {
			items = append(items, map[string]any{
				"metadata": map[string]any{"name": name},
				"status":   map[string]any{"conditions": []map[string]string{{"type": "Ready", "status": "True"}}},
			})
		}

		writeJSON(w, map[string]any{"items": items})
	case r.Method == http.MethodGet && path == rbacPrefix+"clusterrolebindings":
		writeJSON(w, f.listBindings(f.crbs, ""))
	case r.Method == http.MethodGet && path == rbacPrefix+"rolebindings":
		writeJSON(w, f.listBindings(f.rbs, "ns"))
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/deployments/coredns"):
		writeJSON(w, map[string]any{"status": map[string]any{"availableReplicas": f.corednsAvail}})
	case r.Method == http.MethodPatch && strings.HasPrefix(path, "/api/v1/nodes/"):
		body, _ := io.ReadAll(r.Body)
		node := strings.TrimPrefix(path, "/api/v1/nodes/")
		f.patches[node] = append(f.patches[node], string(body))
	case r.Method == http.MethodPatch && strings.Contains(path, "clusterrolebindings/"):
		f.crbs[path[strings.LastIndex(path, "/")+1:]] = true
	case r.Method == http.MethodPatch && strings.Contains(path, "/rolebindings/"):
		f.rbs[rbKey(path)] = true
	case r.Method == http.MethodDelete && strings.Contains(path, "clusterrolebindings/"):
		delete(f.crbs, path[strings.LastIndex(path, "/")+1:])
	case r.Method == http.MethodDelete && strings.Contains(path, "/rolebindings/"):
		delete(f.rbs, rbKey(path))
	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/api/v1/nodes/"):
		node := strings.TrimPrefix(path, "/api/v1/nodes/")
		f.deleted = append(f.deleted, node)
		delete(f.nodes, node)
	case r.Method == http.MethodPatch:
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func rbKey(path string) string {
	parts := strings.Split(path, "/")

	return parts[len(parts)-3] + "/" + parts[len(parts)-1]
}

func (f *fakeK8s) listBindings(set map[string]bool, nsMode string) map[string]any {
	items := make([]map[string]any, 0, len(set))

	for k := range set {
		meta := map[string]any{"name": k}

		if nsMode != "" {
			ns, name, _ := strings.Cut(k, "/")
			meta = map[string]any{"name": name, "namespace": ns}
		}

		items = append(items, map[string]any{"metadata": meta})
	}

	return map[string]any{"items": items}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// engineRuntime records containers and registers a node in the fake API for every agent started.
type engineRuntime struct {
	k8s     *fakeK8s
	specs   []container.Spec
	removed []string
	mu      sync.Mutex
}

func (r *engineRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.specs = append(r.specs, spec)

	if i := slices.Index(spec.Entrypoint, "--node-name"); i >= 0 {
		r.k8s.addNode(spec.Entrypoint[i+1])
	}

	return "ctr-" + strconv.Itoa(len(r.specs)), nil
}

func (r *engineRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removed = append(r.removed, id)

	return nil
}

func (r *engineRuntime) Exec(context.Context, string, []string) (container.ExecResult, error) {
	return container.ExecResult{Stdout: "10.1.2.3\n"}, nil
}

func (r *engineRuntime) agentSpecs() []container.Spec {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.specs)
}

func (r *engineRuntime) removedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.removed)
}

func newReconcileEnv(t *testing.T, rt ClusterRuntime, k8s *fakeK8s) (*InMemoryBackend, *reconciler) {
	t.Helper()

	b := NewInMemoryBackend(t.Context(), testAccount, "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.EnableClusters(ClusterEngineConfig{Runtime: rt, Token: testAdminToken}))

	api, err := newK8sAPI(k8s.srv.Listener.Addr().String(), testAdminToken, certPEM(k8s.srv.Certificate()))
	require.NoError(t, err)

	t.Cleanup(api.close)

	lc := &liveCluster{
		name: "c1", containerID: "server", image: "rancher/k3s:test", joinToken: "jt",
		kick: make(chan struct{}, 1), cancel: func() {}, api: api,
	}

	b.mu.Lock("test")
	b.clusterEng.clusters["c1"] = lc
	b.mu.Unlock()

	return b, &reconciler{
		b:       b,
		e:       b.clusterEng,
		lc:      lc,
		api:     api,
		name:    "c1",
		version: "1.32",
		warned:  map[string]string{},
	}
}

func nodegroupStatus(b *InMemoryBackend, name string) string {
	ng, err := b.DescribeNodegroup("c1", name)
	if err != nil {
		return err.Error()
	}

	return ng.Status
}

func TestReconcile_NodegroupLifecycle(t *testing.T) {
	t.Parallel()

	k8s := newFakeK8s(t)
	rt := &engineRuntime{k8s: k8s}
	b, r := newReconcileEnv(t, rt, k8s)

	_, err := b.CreateNodegroup("c1", "ng1", "", "", "SPOT", "", "", nil, 2, 1, 3, NodegroupInput{
		Labels: map[string]string{"role": "web"},
		Taints: []NodegroupTaint{{Key: "dedicated", Value: "gpu", Effect: "NO_SCHEDULE"}},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, statusCreating, nodegroupStatus(b, "ng1"), "no stub timer on a live cluster")

	r.pass(t.Context())

	specs := rt.agentSpecs()
	require.Len(t, specs, 2)
	assert.Equal(t, statusCreating, nodegroupStatus(b, "ng1"), "nodes are not Ready until observed")

	spec := specs[0]
	assert.True(t, spec.Privileged)
	assert.Equal(t, "rancher/k3s:test", spec.Image)
	assert.Equal(t, []string{"K3S_URL=https://10.1.2.3:6443", "K3S_TOKEN=jt"}, spec.Env)
	assert.Equal(t, []string{"/bin/k3s", "agent"}, spec.Entrypoint[:2])

	joined := strings.Join(spec.Entrypoint, " ")
	assert.Contains(t, joined, "--node-label eks.amazonaws.com/nodegroup=ng1")
	assert.Contains(t, joined, "--node-label eks.amazonaws.com/capacityType=SPOT")
	assert.Contains(t, joined, "--node-taint dedicated=gpu:NoSchedule")

	r.pass(t.Context())
	assert.Equal(t, statusActive, nodegroupStatus(b, "ng1"))

	k8s.mu.Lock()
	patched := 0

	for _, ps := range k8s.patches {
		for _, p := range ps {
			if strings.Contains(p, `"role":"web"`) {
				patched++
			}
		}
	}
	k8s.mu.Unlock()

	assert.Equal(t, 2, patched, "user labels are applied to both nodes through the API")

	one := int32(1)
	_, err = b.UpdateNodegroupConfig("c1", "ng1", NodegroupConfigUpdate{DesiredSize: &one})
	require.NoError(t, err)
	r.pass(t.Context())

	assert.Len(t, rt.removedIDs(), 1, "scale-down stops one agent")
	k8s.mu.Lock()
	assert.Len(t, k8s.deleted, 1, "scale-down deletes the Node object")
	k8s.mu.Unlock()

	_, err = b.DeleteNodegroup("c1", "ng1")
	require.NoError(t, err)
	r.pass(t.Context())
	assert.Len(t, rt.removedIDs(), 2, "deleting the nodegroup removes the last agent")
}

func TestReconcile_ZeroDesiredIsActive(t *testing.T) {
	t.Parallel()

	k8s := newFakeK8s(t)
	rt := &engineRuntime{k8s: k8s}
	b, r := newReconcileEnv(t, rt, k8s)

	_, err := b.CreateNodegroup("c1", "empty", "", "", "", "", "", nil, 0, 0, 1, NodegroupInput{}, nil)
	require.NoError(t, err)
	r.pass(t.Context())

	assert.Equal(t, statusActive, nodegroupStatus(b, "empty"))
	assert.Empty(t, rt.agentSpecs())
}

func TestReconcile_NodegroupWithoutExec(t *testing.T) {
	t.Parallel()

	k8s := newFakeK8s(t)
	b, r := newReconcileEnv(t, noopRuntime{}, k8s)

	_, err := b.CreateNodegroup("c1", "ng1", "", "", "", "", "", nil, 1, 1, 1, NodegroupInput{}, nil)
	require.NoError(t, err)
	r.pass(t.Context())

	assert.Equal(t, statusCreating, nodegroupStatus(b, "ng1"))
}

func TestReconcile_AccessPolicyBindings(t *testing.T) {
	t.Parallel()

	k8s := newFakeK8s(t)
	b, r := newReconcileEnv(t, &engineRuntime{k8s: k8s}, k8s)

	k8s.crbs["stale"] = true

	_, err := b.CreateAccessEntry("c1", roleARN, "STANDARD", "", nil, nil)
	require.NoError(t, err)

	policy := "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy"
	_, err = b.AssociateAccessPolicy("c1", roleARN, policy, map[string]any{"type": "cluster"})
	require.NoError(t, err)
	r.pass(t.Context())

	p, _ := parsePrincipal(roleARN)
	want := "gopherstack-eks-" + stableID("c1/"+p.canonical()) + "-amazonekseditpolicy"

	k8s.mu.Lock()
	assert.True(t, k8s.crbs[want], "binding %s applied; have %v", want, k8s.crbs)
	assert.False(t, k8s.crbs["stale"], "unmanaged leftovers with our label are pruned")
	k8s.mu.Unlock()

	require.NoError(t, b.DisassociateAccessPolicy("c1", roleARN, policy))
	r.pass(t.Context())

	k8s.mu.Lock()
	assert.Empty(t, k8s.crbs, "disassociation removes the binding")
	k8s.mu.Unlock()
}

func TestReconcile_CoreDNSAddonHealth(t *testing.T) {
	t.Parallel()

	k8s := newFakeK8s(t)
	b, r := newReconcileEnv(t, &engineRuntime{k8s: k8s}, k8s)

	_, err := b.CreateAddon("c1", "coredns", "", "", "", "", "", nil, nil)
	require.NoError(t, err)

	status := func() *Addon {
		a, derr := b.DescribeAddon("c1", "coredns")
		require.NoError(t, derr)

		return a
	}

	assert.Equal(t, statusCreating, status().Status)

	r.pass(t.Context())
	assert.Equal(t, statusActive, status().Status)

	k8s.mu.Lock()
	k8s.corednsAvail = 0
	k8s.mu.Unlock()
	r.pass(t.Context())

	a := status()
	assert.Equal(t, statusDegraded, a.Status)
	require.Len(t, a.Health.Issues, 1)
	assert.Equal(t, issueNoReplicas, a.Health.Issues[0]["code"])

	k8s.mu.Lock()
	k8s.corednsAvail = 2
	k8s.mu.Unlock()
	r.pass(t.Context())

	a = status()
	assert.Equal(t, statusActive, a.Status)
	assert.Empty(t, a.Health.Issues)
}

func TestNextStatusTransitions(t *testing.T) {
	t.Parallel()

	old := addonReadyLimit + time.Minute

	addonTests := []struct {
		cur     string
		want    string
		age     time.Duration
		healthy bool
	}{
		{cur: statusCreating, healthy: true, age: 0, want: statusActive},
		{cur: statusCreating, healthy: false, age: 0, want: statusCreating},
		{cur: statusCreating, healthy: false, age: old, want: statusCreateFail},
		{cur: statusActive, healthy: false, age: 0, want: statusDegraded},
		{cur: statusActive, healthy: true, age: 0, want: statusActive},
		{cur: statusDegraded, healthy: true, age: 0, want: statusActive},
		{cur: statusUpdating, healthy: false, age: 0, want: statusUpdating},
	}

	for i, tt := range addonTests {
		t.Run(fmt.Sprintf("addon_%d_%s", i, tt.cur), func(t *testing.T) {
			t.Parallel()

			got, _ := nextAddonStatus(tt.cur, tt.healthy, tt.age)
			assert.Equal(t, tt.want, got)
		})
	}

	ngTests := []struct {
		name  string
		want  string
		ng    ngDesired
		ready int
	}{
		{
			name:  "ready",
			ng:    ngDesired{status: statusCreating, desired: 2, createdAt: time.Now()},
			ready: 2,
			want:  statusActive,
		},
		{name: "waiting", ng: ngDesired{status: statusCreating, desired: 2, createdAt: time.Now()}, ready: 1},
		{
			name: "timeout", want: statusCreateFail,
			ng: ngDesired{status: statusCreating, desired: 2, createdAt: time.Now().Add(-time.Hour)},
		},
		{name: "already_active", ng: ngDesired{status: statusActive, desired: 2}},
	}

	for _, tt := range ngTests {
		t.Run("nodegroup_"+tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, nodegroupNextStatus(tt.ng, tt.ready))
		})
	}
}
