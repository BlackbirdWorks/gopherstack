package azurearm

import (
	"strings"
	"time"
)

const (
	flexThreatType = "advancedThreatProtectionSettings"
	flexThreatName = "Default"
	flexDetachVNet = "detachvnet"
	flexPropState  = "state"
)

func (p *FlexDBProvider) threatState(rec *flexServer) string {
	if rec.ThreatState == "" {
		return flexDisabled
	}

	return rec.ThreatState
}

func (p *FlexDBProvider) threatBody(id ResourceID, rec *flexServer) map[string]any {
	rid := id
	rid.Types, rid.Names = []string{flexServersType, flexThreatType}, []string{id.Names[0], flexThreatName}

	created := rec.CreatedAt
	if !rec.ThreatChanged.IsZero() {
		created = rec.ThreatChanged
	}

	return p.childEnvelope(rid, flexThreatType, map[string]any{
		flexPropState: p.threatState(rec), "creationTime": created.UTC().Format(time.RFC3339Nano),
		fieldProvisioningState: provisioningStateSucceeded,
	})
}

// writeThreat applies a PUT or PATCH of the Default advanced threat protection setting of a MySQL server; the
// state is the only writable property (armmysqlflexibleservers AdvancedThreatProtectionUpdateProperties).
func (p *FlexDBProvider) writeThreat(id ResourceID, body map[string]any) (flexResult, error) {
	if p.flavor.kind != flexKindMySQL {
		return flexResult{}, errUnsupportedFlexType(id)
	}

	if !strings.EqualFold(id.LeafName(), flexThreatName) {
		return flexResult{}, flexInvalid("The advanced threat protection setting name must be '%s'.", flexThreatName)
	}

	state, _ := flexStr(flexMap(body, fieldProperties), flexPropState)

	canon, ok := canonicalIn([]string{flexEnabled, flexDisabled}, state)
	if !ok {
		return flexResult{}, flexInvalid("properties.state must be Enabled or Disabled.")
	}

	p.mu.Lock("writeThreat")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return flexResult{}, err
	}

	if p.stateOf(rec) != flexStateReady {
		return flexResult{}, p.notReady(rec, "Update advanced threat protection")
	}

	rec.ThreatState, rec.ThreatChanged = canon, p.now().UTC()

	return flexResult{body: p.threatBody(id, rec), opID: p.newOp(id.ARMID(), true, "", nil)}, nil
}

// DetachVNet moves a MySQL server from virtual network integration to public access
// (armmysqlflexibleservers ServersClient.BeginDetachVNet).
func (p *FlexDBProvider) DetachVNet(id ResourceID, body map[string]any) (string, error) {
	if p.flavor.kind != flexKindMySQL {
		return "", errUnsupportedFlexType(id)
	}

	access := flexEnabled

	if v, ok := flexStr(body, "publicNetworkAccess"); ok && v != "" {
		canon, valid := canonicalIn([]string{flexEnabled, flexDisabled}, v)
		if !valid {
			return "", flexInvalid("publicNetworkAccess must be Enabled or Disabled.")
		}

		access = canon
	}

	key := p.key(id)

	p.mu.Lock("DetachVNet")
	defer p.mu.Unlock()

	rec, err := p.idleServer(id, flexDetachVNet)
	if err != nil {
		return "", err
	}

	if p.stateOf(rec) != flexStateReady {
		return "", p.notReady(rec, "Detach virtual network")
	}

	if rec.Subnet == "" {
		return "", flexInvalid("Server '%s' does not have virtual network integration.", rec.Name)
	}

	rec.Subnet, rec.DNSZone, rec.PublicAcc = "", "", access
	rec.Transition = flexTransitionUpdating
	rec.OpID = p.newOp(id.ARMID(), true, key, nil)

	return rec.OpID, nil
}

func (p *FlexDBProvider) threatList(id ResourceID, rec *flexServer) ([]map[string]any, error) {
	if p.flavor.kind != flexKindMySQL {
		return nil, errUnsupportedFlexType(id)
	}

	return []map[string]any{p.threatBody(id, rec)}, nil
}
