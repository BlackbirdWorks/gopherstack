package azurearm

import (
	"context"
	"maps"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

const (
	netWatchers   = "networkWatchers"
	wFlowLogs     = "flowLogs"
	flPropStorage = "storageId"
	flPropTarget  = "targetResourceId"
	flPropRetain  = "retentionPolicy"
	flPropFormat  = "format"
	flPropAnalyt  = "flowAnalyticsConfiguration"
	flMaxRetain   = 365
)

func (p *NetworkProvider) putWatcher(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	it, _, created := p.upsertTop(netWatchers, id, body)
	it.Obj.Props = takeProps(body, fieldProvisioningState, "runningOperationIds")

	return p.bodyWatcher(id, it), created, nil
}

func (p *NetworkProvider) bodyWatcher(id ResourceID, it *netParent) map[string]any {
	props := maps.Clone(it.Obj.Props)
	props[fieldProvisioningState] = netStatusSucceeded

	return p.envelope(id, netWatchers, &it.Obj, props)
}

// flowTarget resolves the resource a flow log monitors and returns its resource GUID.
func (p *NetworkProvider) flowTarget(tid string) (string, bool) {
	for _, typ := range []string{netNSGs, netVNets, netNICs} {
		if t, ok := p.netRef(tid, typ); ok {
			return t.Obj.GUID, true
		}
	}

	if _, sn, ok := p.subnetByID(tid); ok {
		return sn.GUID, true
	}

	return "", false
}

func (p *NetworkProvider) checkFlowStorage(sid string) error {
	rid, err := ParseGenericResourcePath(sid)
	if err != nil || len(rid.Types) != 1 || !strings.EqualFold(rid.Namespace, "Microsoft.Storage") ||
		!strings.EqualFold(rid.Types[0], "storageAccounts") {
		return netBad("storageId '%s' is not the id of a storage account.", sid)
	}

	if p.lookup == nil {
		return nil
	}

	if _, gerr := p.lookup(context.Background(), rid); gerr != nil {
		return refMissing(sid)
	}

	return nil
}

func checkFlowSettings(props map[string]any) error {
	if err := checkFlowRetention(propsMap(props[flPropRetain])); err != nil {
		return err
	}

	if err := checkFlowFormat(propsMap(props[flPropFormat])); err != nil {
		return err
	}

	return checkFlowAnalytics(propsMap(propsMap(props[flPropAnalyt])["networkWatcherFlowAnalyticsConfiguration"]))
}

func checkFlowRetention(r map[string]any) error {
	if d, set := r["days"].(float64); set && (d < 0 || d > flMaxRetain || d != float64(int(d))) {
		return netBad("retentionPolicy.days must be between 0 and %d.", flMaxRetain)
	}

	return nil
}

func checkFlowFormat(f map[string]any) error {
	if t := strOf(f, "type"); t != "" && !strings.EqualFold(t, "JSON") {
		return netBad("format.type must be JSON.")
	}

	if v, set := f["version"].(float64); set && v != 1 && v != 2 {
		return netBad("format.version must be 1 or 2.")
	}

	return nil
}

func checkFlowAnalytics(ta map[string]any) error {
	if enabled, _ := ta["enabled"].(bool); !enabled {
		return nil
	}

	for _, k := range []string{"workspaceId", "workspaceRegion", "workspaceResourceId"} {
		if strOf(ta, k) == "" {
			return netBad("Traffic analytics requires %s when enabled.", k)
		}
	}

	if v, set := ta["trafficAnalyticsInterval"].(float64); set && v != 10 && v != 60 {
		return netBad("trafficAnalyticsInterval must be 10 or 60.")
	}

	return nil
}

func (p *NetworkProvider) writeFlowLog(w *netParent, name string, body map[string]any) (bool, error) {
	props := propsOf(body)

	tid := strOf(props, flPropTarget)
	if tid == "" || strOf(props, flPropStorage) == "" {
		return false, netBad("Flow log %s requires targetResourceId and storageId.", name)
	}

	guid, ok := p.flowTarget(tid)
	if !ok {
		return false, refMissing(tid)
	}

	if err := p.checkFlowStorage(strOf(props, flPropStorage)); err != nil {
		return false, err
	}

	if err := checkFlowSettings(props); err != nil {
		return false, err
	}

	prev, idx := w.kid(wFlowLogs, name)

	loc := normLocation(strOf(body, fieldLocation))
	if loc == "" && prev != nil {
		loc = prev.Location
	}

	if loc == "" {
		return false, netErr(
			http.StatusBadRequest,
			netCodeLocation,
			"The location property is required for this definition.",
		)
	}

	o := &netObj{
		Name: name, Etag: newEtag(), GUID: uuid.NewString(), Location: loc, Tags: stringTags(body[fieldTags]),
		Props: takeProps(body, fieldProvisioningState, "targetResourceGuid"),
		Aux:   map[string]string{"target": guid},
	}
	if _, set := o.Props["enabled"].(bool); !set {
		o.Props["enabled"] = true
	}

	if id, has := body["identity"]; has {
		o.Extra = map[string]any{"identity": id}
	}

	if prev != nil {
		o.GUID = prev.GUID
		w.Kids[wFlowLogs][idx] = o
	} else {
		w.Kids[wFlowLogs] = append(w.Kids[wFlowLogs], o)
	}

	p.touch(w)

	return prev == nil, nil
}

func (p *NetworkProvider) bodyFlowLog(id ResourceID, f *netObj) map[string]any {
	props := maps.Clone(f.Props)
	props[fieldProvisioningState] = netStatusSucceeded
	props["targetResourceGuid"] = f.Aux["target"]

	return p.envelope(id, netWatchers+"/"+wFlowLogs, f, props)
}

// pruneFlowLogsLocked drops flow logs whose monitored resource no longer exists.
func (p *NetworkProvider) pruneFlowLogsLocked() {
	for _, w := range p.ofType(netWatchers) {
		kept := w.Kids[wFlowLogs][:0]

		for _, f := range w.Kids[wFlowLogs] {
			if _, ok := p.flowTarget(strOf(f.Props, flPropTarget)); ok || f.Aux["legacy"] != "" {
				kept = append(kept, f)
			}
		}

		w.Kids[wFlowLogs] = kept
	}
}

// patchFlowLogTags replaces the tags of a flow log, as FlowLogs UpdateTags does.
func (p *NetworkProvider) patchFlowLogTags(id ResourceID, tags map[string]string) (map[string]any, error) {
	w, ok := p.top(netWatchers, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	f, idx := w.kid(wFlowLogs, id.Names[1])
	if idx < 0 {
		return nil, ErrResourceNotFound
	}

	f.Tags = tags
	f.Etag = newEtag()

	return p.bodyFlowLog(id, f), nil
}
