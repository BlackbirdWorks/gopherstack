package azurearm

import (
	"encoding/json/v2"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	auxRolling           = "rollingUpgrade"
	cmpFieldRollingPol   = "rollingUpgradePolicy"
	rollingActionOS      = "osrollingupgrade"
	rollingActionExt     = "extensionrollingupgrade"
	rollingStatusDone    = "Completed"
	rollingStatusRunning = "RollingForward"
	rollingActionStart   = "Start"
	rollingDefaultPct    = 20
	rollingMaxPct        = 100
	rollingPauseDefault  = "PT0S"
	rollingLatest        = "latest"
)

//nolint:gochecknoglobals // static read-only key list
var rollingPercentKeys = []string{
	"maxBatchInstancePercent", "maxUnhealthyInstancePercent", "maxUnhealthyUpgradedInstancePercent",
}

type rollingRecord struct {
	Started    time.Time      `json:"started"`
	LastAction time.Time      `json:"lastAction"`
	Policy     map[string]any `json:"policy,omitempty"`
	Code       string         `json:"code"`
	Action     string         `json:"action"`
	Successful int            `json:"successful"`
}

// normalizeRollingPolicy fills the documented rolling upgrade defaults (20 percent, PT0S) and range-checks the
// percentages (learn.microsoft.com/rest/api/compute/virtual-machine-scale-sets/create-or-update, RollingUpgradePolicy).
func normalizeRollingPolicy(up map[string]any) error {
	raw, present := up[cmpFieldRollingPol].(map[string]any)
	if !present && strOf(up, "mode") != upgradeRolling {
		return nil
	}

	pol := cloneProps(raw)

	for _, k := range rollingPercentKeys {
		v, set := pol[k]
		if !set {
			pol[k] = rollingDefaultPct

			continue
		}

		if n := intOf(v); n < 1 || n > rollingMaxPct {
			return cmpInvalid("The rollingUpgradePolicy.%s %d is not valid; it must be between 1 and %d.",
				k, n, rollingMaxPct)
		}
	}

	setDefault(pol, "pauseTimeBetweenBatches", rollingPauseDefault)
	up[cmpFieldRollingPol] = pol

	return nil
}

func recordRollingUpgrade(r *cmpRes, upgraded int) {
	now := time.Now().UTC()
	rec := rollingRecord{
		Started: now, LastAction: now, Code: rollingStatusDone, Action: rollingActionStart, Successful: upgraded,
		Policy: mapOf(mapOf(r.Props[cmpFieldUpgrade])[cmpFieldRollingPol]),
	}

	if b, err := json.Marshal(rec, jsonx.V1()); err == nil {
		r.Aux[auxRolling] = string(b)
	}
}

func loadRolling(r *cmpRes) (rollingRecord, bool) {
	var rec rollingRecord

	raw := r.Aux[auxRolling]
	if raw == "" || json.Unmarshal([]byte(raw), &rec, jsonx.V1()) != nil {
		return rec, false
	}

	return rec, true
}

// startRollingUpgrade serves osRollingUpgrade and extensionRollingUpgrade: every unprotected instance moves to the
// current model, and the upgrade is recorded as the latest one.
func (p *ComputeProvider) startRollingUpgrade(r *cmpRes) error {
	if err := normalizeRollingPolicy(mapOf(r.Props[cmpFieldUpgrade])); err != nil {
		return err
	}

	n := 0

	for _, in := range r.Instances {
		if in.ProtectActions {
			continue
		}

		if err := p.upgradeInstance(r, in); err != nil {
			return err
		}

		n++
	}

	recordRollingUpgrade(r, n)

	return nil
}

// RollingUpgradeCancel cancels the running rolling upgrade; upgrades finish before the call returns, so there is
// never one to cancel and the answer is a conflict.
func (p *ComputeProvider) RollingUpgradeCancel(id ResourceID) error {
	p.mu.Lock("RollingUpgradeCancel")
	defer p.mu.Unlock()

	r, ok := p.top(cmpVMSS, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	rec, found := loadRolling(r)
	if !found || rec.Code != rollingStatusRunning {
		return cmpNotAllowed("Cannot cancel the rolling upgrade of scale set %s: no rolling upgrade is in progress.",
			r.Name)
	}

	return nil
}

// RollingUpgradeLatest returns the status of the most recent rolling upgrade of the scale set.
func (p *ComputeProvider) RollingUpgradeLatest(id ResourceID) (map[string]any, error) {
	p.mu.RLock("RollingUpgradeLatest")
	defer p.mu.RUnlock()

	r, ok := p.top(cmpVMSS, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	rec, found := loadRolling(r)
	if !found {
		return nil, ErrResourceNotFound
	}

	pending := 0

	for _, in := range r.Instances {
		if in.Model != atoiOr(r.Aux[auxModel], 1) {
			pending++
		}
	}

	return map[string]any{
		cmpFieldID: r.armID() + "/rollingUpgrades/" + rollingLatest, fieldName: rollingLatest,
		fieldType:     strings.Join([]string{namespaceMicrosoftCompute, cmpVMSS, "rollingUpgrades"}, "/"),
		fieldLocation: r.Location,
		fieldProperties: map[string]any{
			"policy": rec.Policy,
			"runningStatus": map[string]any{
				aksFieldCode: rec.Code, apimOpStartTime: rec.Started.Format(time.RFC3339Nano),
				"lastAction": rec.Action, "lastActionTime": rec.LastAction.Format(time.RFC3339Nano),
			},
			"progress": map[string]any{
				"successfulInstanceCount": rec.Successful, "failedInstanceCount": 0,
				"inProgressInstanceCount": 0, "pendingInstanceCount": pending,
			},
		},
	}, nil
}
