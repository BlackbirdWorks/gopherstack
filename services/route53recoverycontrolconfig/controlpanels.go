package route53recoverycontrolconfig

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (b *InMemoryBackend) controlsIn(panelArn string) []*RoutingControl {
	var out []*RoutingControl

	for _, c := range b.controls.All() {
		if c.PanelARN == panelArn && b.alive(c.DeletedAt) {
			out = append(out, c)
		}
	}

	return out
}

func (b *InMemoryBackend) rulesIn(panelArn string) []*SafetyRule {
	var out []*SafetyRule

	for _, r := range b.rules.All() {
		if r.PanelARN == panelArn && b.alive(r.DeletedAt) {
			out = append(out, r)
		}
	}

	return out
}

func (b *InMemoryBackend) panelHasChildren(panelArn string) bool {
	return panelArn != "" && (len(b.controlsIn(panelArn)) > 0 || len(b.rulesIn(panelArn)) > 0)
}

func (b *InMemoryBackend) panelView(p *ControlPanel) wireControlPanel {
	return wireControlPanel{
		ClusterArn:          p.ClusterARN,
		ControlPanelArn:     p.ARN,
		Name:                p.Name,
		Owner:               b.accountID,
		Status:              b.statusOf(p.CreatedAt, p.DeletedAt),
		DefaultControlPanel: p.Default,
		RoutingControlCount: int32(len(b.controlsIn(p.ARN))), //nolint:gosec // bounded by listMax-scale quotas
	}
}

// CreateControlPanel creates a custom control panel in a cluster.
func (b *InMemoryBackend) CreateControlPanel(
	clusterArn, name, token string,
	tags map[string]string,
) (wireControlPanel, error) {
	if err := validName("control panel", name); err != nil {
		return wireControlPanel{}, err
	}

	b.mu.Lock("CreateControlPanel")
	defer b.mu.Unlock()

	if existing, ok := b.idempotent("panel", token); ok {
		if p, err := b.getPanel(existing); err == nil {
			return b.panelView(p), nil
		}
	}

	if err := b.activeCluster(clusterArn); err != nil {
		return wireControlPanel{}, err
	}

	p := &ControlPanel{
		ARN: b.panelARN(
			newID(),
		),
		ClusterARN: clusterArn,
		Name:       name,
		CreatedAt:  time.Now().UTC(),
		Tags:       cloneTags(tags),
	}
	b.panels.Put(p)
	b.rememberToken("panel", token, p.ARN)

	return b.panelView(p), nil
}

// DescribeControlPanel returns one control panel.
func (b *InMemoryBackend) DescribeControlPanel(arn string) (wireControlPanel, error) {
	b.mu.RLock("DescribeControlPanel")
	defer b.mu.RUnlock()

	p, err := b.getPanel(arn)
	if err != nil {
		return wireControlPanel{}, err
	}

	return b.panelView(p), nil
}

// ListControlPanels lists panels, optionally restricted to one cluster.
func (b *InMemoryBackend) ListControlPanels(
	clusterArn, token string,
	maxResults int,
) ([]wireControlPanel, string, error) {
	b.mu.RLock("ListControlPanels")
	defer b.mu.RUnlock()

	var all []*ControlPanel

	for _, p := range b.panels.All() {
		if b.alive(p.DeletedAt) && (clusterArn == "" || p.ClusterARN == clusterArn) {
			all = append(all, p)
		}
	}

	pg, err := restroute.Paginate(sortedBy(all, func(p *ControlPanel) string { return p.Name + p.ARN }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]wireControlPanel, 0, len(pg.Data))
	for _, p := range pg.Data {
		out = append(out, b.panelView(p))
	}

	return out, pg.Next, nil
}

// UpdateControlPanel renames a control panel.
func (b *InMemoryBackend) UpdateControlPanel(arn, name string) (wireControlPanel, error) {
	if err := validName("control panel", name); err != nil {
		return wireControlPanel{}, err
	}

	b.mu.Lock("UpdateControlPanel")
	defer b.mu.Unlock()

	p, err := b.getPanel(arn)
	if err != nil {
		return wireControlPanel{}, err
	}

	p.Name = name

	return b.panelView(p), nil
}

// DeleteControlPanel deletes an empty, non-default control panel.
func (b *InMemoryBackend) DeleteControlPanel(arn string) error {
	b.mu.Lock("DeleteControlPanel")
	defer b.mu.Unlock()

	p, err := b.getPanel(arn)
	if err != nil {
		return err
	}

	if p.Default {
		return restroute.Conflict("The default control panel %s cannot be deleted", arn)
	}

	if b.panelHasChildren(arn) {
		return restroute.Conflict("Control panel %s still has routing controls or safety rules", arn)
	}

	b.markDeleted(&p.DeletedAt, func() { b.panels.Delete(arn) })

	return nil
}

// defaultPanelOf returns the default control panel ARN of a cluster.
func (b *InMemoryBackend) defaultPanelOf(clusterArn string) (string, error) {
	for _, p := range b.panels.All() {
		if p.ClusterARN == clusterArn && p.Default && b.alive(p.DeletedAt) {
			return p.ARN, nil
		}
	}

	return "", restroute.NotFound("Cluster %s has no default control panel", clusterArn)
}
