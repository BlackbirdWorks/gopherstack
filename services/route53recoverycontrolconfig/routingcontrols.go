package route53recoverycontrolconfig

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (b *InMemoryBackend) controlView(c *RoutingControl) wireRoutingControl {
	return wireRoutingControl{
		ControlPanelArn:   c.PanelARN,
		Name:              c.Name,
		Owner:             b.accountID,
		RoutingControlArn: c.ARN,
		Status:            b.statusOf(c.CreatedAt, c.DeletedAt),
	}
}

// CreateRoutingControl creates a routing control (state Off) in the given or the cluster's default control panel.
func (b *InMemoryBackend) CreateRoutingControl(clusterArn, panelArn, name, token string) (wireRoutingControl, error) {
	if err := validName("routing control", name); err != nil {
		return wireRoutingControl{}, err
	}

	b.mu.Lock("CreateRoutingControl")
	defer b.mu.Unlock()

	if existing, ok := b.idempotent("control", token); ok {
		if c, err := b.getControl(existing); err == nil {
			return b.controlView(c), nil
		}
	}

	if _, err := b.getCluster(clusterArn); err != nil {
		return wireRoutingControl{}, err
	}

	if panelArn == "" {
		def, err := b.defaultPanelOf(clusterArn)
		if err != nil {
			return wireRoutingControl{}, err
		}

		panelArn = def
	}

	panel, err := b.getPanel(panelArn)
	if err != nil {
		return wireRoutingControl{}, err
	}

	if panel.ClusterARN != clusterArn {
		return wireRoutingControl{}, restroute.Validation(
			"Control panel %s does not belong to cluster %s",
			panelArn,
			clusterArn,
		)
	}

	c := &RoutingControl{
		ARN: arn.BuildGlobal(
			arnService,
			"",
			b.accountID,
			"controlpanel/"+idOf(panelArn)+"/routingcontrol/"+newID()[:12],
		),
		PanelARN:   panelArn,
		ClusterARN: clusterArn,
		Name:       name,
		State:      stateOff,
		CreatedAt:  time.Now().UTC(),
	}
	b.controls.Put(c)
	b.rememberToken("control", token, c.ARN)

	return b.controlView(c), nil
}

// DescribeRoutingControl returns one routing control.
func (b *InMemoryBackend) DescribeRoutingControl(arn string) (wireRoutingControl, error) {
	b.mu.RLock("DescribeRoutingControl")
	defer b.mu.RUnlock()

	c, err := b.getControl(arn)
	if err != nil {
		return wireRoutingControl{}, err
	}

	return b.controlView(c), nil
}

// ListRoutingControls lists the routing controls of a control panel.
func (b *InMemoryBackend) ListRoutingControls(
	panelArn, token string,
	maxResults int,
) ([]wireRoutingControl, string, error) {
	b.mu.RLock("ListRoutingControls")
	defer b.mu.RUnlock()

	if _, err := b.getPanel(panelArn); err != nil {
		return nil, "", err
	}

	pg, err := restroute.Paginate(
		sortedBy(b.controlsIn(panelArn), func(c *RoutingControl) string { return c.Name + c.ARN }),
		token,
		maxResults,
		listDefault,
		listMax,
	)
	if err != nil {
		return nil, "", err
	}

	out := make([]wireRoutingControl, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, b.controlView(c))
	}

	return out, pg.Next, nil
}

// UpdateRoutingControl renames a routing control.
func (b *InMemoryBackend) UpdateRoutingControl(arn, name string) (wireRoutingControl, error) {
	if err := validName("routing control", name); err != nil {
		return wireRoutingControl{}, err
	}

	b.mu.Lock("UpdateRoutingControl")
	defer b.mu.Unlock()

	c, err := b.getControl(arn)
	if err != nil {
		return wireRoutingControl{}, err
	}

	c.Name = name

	return b.controlView(c), nil
}

// DeleteRoutingControl deletes a routing control that no safety rule references.
func (b *InMemoryBackend) DeleteRoutingControl(arn string) error {
	b.mu.Lock("DeleteRoutingControl")
	defer b.mu.Unlock()

	c, err := b.getControl(arn)
	if err != nil {
		return err
	}

	for _, r := range b.rulesIn(c.PanelARN) {
		if r.governs(arn) {
			return restroute.Conflict("Routing control %s is used by safety rule %q (%s)", arn, r.Name, r.ARN)
		}
	}

	b.markDeleted(&c.DeletedAt, func() { b.controls.Delete(arn) })

	return nil
}
