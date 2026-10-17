package route53recoverycontrolconfig

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	maxNameLen     = 64
	defaultPanel   = "DefaultControlPanel"
	listDefault    = 100
	listMax        = 1000
	endpointPrefix = "/route53-recovery-cluster/"
)

// clusterRegions are the five regions every deployed cluster exposes a data-plane endpoint in.
func clusterRegions() []string {
	return []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-2", "ap-northeast-1"}
}

func validName(kind, name string) error {
	if name == "" || len(name) > maxNameLen {
		return restroute.Validation("%s name must be between 1 and %d characters", kind, maxNameLen)
	}

	return nil
}

func validNetworkType(t string) bool { return t == networkIPv4 || t == networkDualstack }

func idOf(arn string) string {
	if _, after, ok := strings.CutLast(arn, "/"); ok {
		return after
	}

	return arn
}

type requestBaseCtxKey struct{}

// requestBase returns scheme://host for the address the caller reached the server on, or false when it is not a
// plain host[:port].
func requestBase(scheme, host string) (string, bool) {
	if scheme != "http" && scheme != "https" {
		return "", false
	}

	u, err := url.Parse(scheme + "://" + host)
	if err != nil || host == "" || u.Host != host || u.User != nil || u.Path != "" || u.RawQuery != "" ||
		u.Fragment != "" {
		return "", false
	}

	return scheme + "://" + host, true
}

// endpointBase is the URL cluster endpoints hang from: the configured external endpoint, else the scheme and
// host the caller reached this server on, else the server's own listen address.
func (b *InMemoryBackend) endpointBase(ctx context.Context) string {
	if ext := b.externalEndpoint.Load(); ext != nil && *ext != "" {
		return *ext
	}

	if base, ok := ctx.Value(requestBaseCtxKey{}).(string); ok {
		return base
	}

	if ep, ok := b.appConfig.(interface{ GetEndpoint() string }); ok {
		return strings.TrimRight(ep.GetEndpoint(), "/")
	}

	return "http://localhost:4566"
}

// SetExternalEndpoint pins the base URL of cluster endpoints, overriding the caller's host.
func (b *InMemoryBackend) SetExternalEndpoint(base string) {
	base = strings.TrimRight(base, "/")
	b.externalEndpoint.Store(&base)
}

func (b *InMemoryBackend) clusterView(ctx context.Context, c *Cluster) wireCluster {
	v := wireCluster{
		ClusterArn:  c.ARN,
		Name:        c.Name,
		NetworkType: c.NetworkType,
		Owner:       b.accountID,
		Status:      b.statusOf(c.CreatedAt, c.DeletedAt),
	}

	if v.Status == statusDeployed {
		for _, region := range clusterRegions() {
			v.ClusterEndpoints = append(v.ClusterEndpoints, wireEndpoint{
				Endpoint: b.endpointBase(ctx) + endpointPrefix + idOf(c.ARN) + "/" + region,
				Region:   region,
			})
		}
	}

	return v
}

// CreateCluster creates a cluster together with its default control panel.
func (b *InMemoryBackend) CreateCluster(
	ctx context.Context,
	name, networkType, token string,
	tags map[string]string,
) (wireCluster, error) {
	if err := validName("cluster", name); err != nil {
		return wireCluster{}, err
	}

	if networkType == "" {
		networkType = networkIPv4
	}

	if !validNetworkType(networkType) {
		return wireCluster{}, restroute.Validation("NetworkType must be IPV4 or DUALSTACK")
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if existing, ok := b.idempotent("cluster", token); ok {
		if c, err := b.getCluster(existing); err == nil {
			return b.clusterView(ctx, c), nil
		}
	}

	now := time.Now().UTC()
	id := newID()
	cluster := &Cluster{
		ARN: b.clusterARN(id), Name: name, NetworkType: networkType, CreatedAt: now, Tags: cloneTags(tags),
	}
	b.clusters.Put(cluster)
	b.panels.Put(&ControlPanel{
		ARN: b.panelARN(newID()), ClusterARN: cluster.ARN, Name: defaultPanel, Default: true, CreatedAt: now,
	})
	b.rememberToken("cluster", token, cluster.ARN)

	return b.clusterView(ctx, cluster), nil
}

// DescribeCluster returns one cluster.
func (b *InMemoryBackend) DescribeCluster(ctx context.Context, arn string) (wireCluster, error) {
	b.mu.RLock("DescribeCluster")
	defer b.mu.RUnlock()

	c, err := b.getCluster(arn)
	if err != nil {
		return wireCluster{}, err
	}

	return b.clusterView(ctx, c), nil
}

// ListClusters lists clusters.
func (b *InMemoryBackend) ListClusters(
	ctx context.Context,
	token string,
	maxResults int,
) ([]wireCluster, string, error) {
	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	var all []*Cluster

	for _, c := range b.clusters.All() {
		if b.alive(c.DeletedAt) {
			all = append(all, c)
		}
	}

	pg, err := restroute.Paginate(sortedBy(all, func(c *Cluster) string { return c.Name + c.ARN }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]wireCluster, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, b.clusterView(ctx, c))
	}

	return out, pg.Next, nil
}

// UpdateCluster changes the network type.
func (b *InMemoryBackend) UpdateCluster(ctx context.Context, arn, networkType string) (wireCluster, error) {
	if !validNetworkType(networkType) {
		return wireCluster{}, restroute.Validation("NetworkType must be IPV4 or DUALSTACK")
	}

	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, err := b.getCluster(arn)
	if err != nil {
		return wireCluster{}, err
	}

	c.NetworkType = networkType

	return b.clusterView(ctx, c), nil
}

// DeleteCluster removes a cluster; it must not hold custom control panels, routing controls or safety rules.
func (b *InMemoryBackend) DeleteCluster(arn string) error {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, err := b.getCluster(arn)
	if err != nil {
		return err
	}

	var defaultArn string

	for _, p := range b.panels.All() {
		if p.ClusterARN != arn || !b.alive(p.DeletedAt) {
			continue
		}

		if !p.Default {
			return restroute.Conflict("Cluster %s still has control panel %q; delete it first", arn, p.Name)
		}

		defaultArn = p.ARN
	}

	if b.panelHasChildren(defaultArn) {
		return restroute.Conflict(
			"Cluster %s still has routing controls or safety rules in its default control panel",
			arn,
		)
	}

	b.markDeleted(&c.DeletedAt, func() { b.clusters.Delete(arn) })

	if defaultArn != "" {
		if p, ok := b.panels.Get(defaultArn); ok {
			b.markDeleted(&p.DeletedAt, func() { b.panels.Delete(defaultArn) })
		}
	}

	return nil
}
