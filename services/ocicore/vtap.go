package ocicore

import (
	"net/http"
	"net/netip"
	"slices"
)

const (
	kindVtap          = "vtap"
	kindCaptureFilter = "capturefilter"

	filterVtap       = "VTAP"
	filterFlowLog    = "FLOWLOG"
	flowAll          = "ALL"
	srcVnic          = "VNIC"
	srcSubnet        = "SUBNET"
	tgtVnic          = "VNIC"
	tgtNLB           = "NETWORK_LOAD_BALANCER"
	tgtIP            = "IP_ADDRESS"
	encapVxlan       = "VXLAN"
	modeDefault      = "DEFAULT"
	modePriority     = "PRIORITY"
	ruleInclude      = "INCLUDE"
	ruleExclude      = "EXCLUDE"
	detailRunning    = "RUNNING"
	detailStopped    = "STOPPED"
	defaultMaxPacket = 9000
	maxVni           = 16777215
	maxSampling      = 100
)

// CaptureRule is a VTAP or flow log capture filter rule.
type CaptureRule struct {
	IcmpOptions      *IcmpOptions `json:"icmpOptions,omitempty"`
	TCPOptions       *PortOptions `json:"tcpOptions,omitempty"`
	UDPOptions       *PortOptions `json:"udpOptions,omitempty"`
	IsEnabled        *bool        `json:"isEnabled,omitempty"`
	Priority         *int         `json:"priority,omitempty"`
	SamplingRate     *int         `json:"samplingRate,omitempty"`
	TrafficDirection string       `json:"trafficDirection,omitempty"`
	RuleAction       string       `json:"ruleAction,omitempty"`
	SourceCidr       string       `json:"sourceCidr,omitempty"`
	DestinationCidr  string       `json:"destinationCidr,omitempty"`
	Protocol         string       `json:"protocol,omitempty"`
	FlowLogType      string       `json:"flowLogType,omitempty"`
}

// CaptureFilter selects the traffic a VTAP mirrors or a VCN flow log records.
type CaptureFilter struct {
	Base
	FilterType   string        `json:"filterType"`
	VtapRules    []CaptureRule `json:"vtapCaptureFilterRules,omitempty"`
	FlowLogRules []CaptureRule `json:"flowLogCaptureFilterRules,omitempty"`
}

// Vtap is a virtual test access point; traffic mirroring is recorded, not performed.
type Vtap struct {
	Base
	VcnID                         string `json:"vcnId"`
	SourceID                      string `json:"sourceId"`
	CaptureFilterID               string `json:"captureFilterId"`
	TargetID                      string `json:"targetId,omitempty"`
	TargetIP                      string `json:"targetIp,omitempty"`
	LifecycleDetails              string `json:"lifecycleStateDetails"`
	EncapsulationProtocol         string `json:"encapsulationProtocol"`
	SourceType                    string `json:"sourceType"`
	TargetType                    string `json:"targetType"`
	TrafficMode                   string `json:"trafficMode"`
	SourcePrivateEndpointIP       string `json:"sourcePrivateEndpointIp,omitempty"`
	SourcePrivateEndpointSubnetID string `json:"sourcePrivateEndpointSubnetId,omitempty"`
	VxlanNetworkIdentifier        int64  `json:"vxlanNetworkIdentifier"`
	MaxPacketSize                 int    `json:"maxPacketSize"`
	IsVtapEnabled                 bool   `json:"isVtapEnabled"`
}

func (v *Vtap) vcnRef() string          { return v.VcnID }
func (v *CaptureFilter) vcnRef() string { return "" }

func (v *Vtap) view() Resource {
	c := *v
	c.Internal = nil

	return &c
}

func (v *CaptureFilter) view() Resource {
	c := *v
	c.Internal = nil
	c.VtapRules, c.FlowLogRules = slices.Clone(v.VtapRules), slices.Clone(v.FlowLogRules)

	return &c
}

func validCaptureCidr(c string) *APIError {
	if c == "" {
		return nil
	}

	if _, ok := parseAnyCidr(c); !ok {
		return errParam(c + " is not a valid CIDR block")
	}

	return nil
}

func validVtapRuleFields(r CaptureRule) *APIError {
	if r.TrafficDirection != dirIngress && r.TrafficDirection != dirEgress {
		return errParam("trafficDirection must be INGRESS or EGRESS")
	}

	if r.IsEnabled != nil || r.Priority != nil || r.SamplingRate != nil || r.FlowLogType != "" {
		return errParam("flow log fields are not valid in a VTAP capture filter rule")
	}

	return nil
}

func validFlowRuleFields(r CaptureRule) *APIError {
	if r.TrafficDirection != "" {
		return errParam("trafficDirection is not valid in a flow log capture filter rule")
	}

	if r.SamplingRate != nil && (*r.SamplingRate < 1 || *r.SamplingRate > maxSampling) {
		return errParam("samplingRate must be between 1 and 100")
	}

	if r.FlowLogType != "" && !slices.Contains([]string{flowAll, "REJECT", "ACCEPT"}, r.FlowLogType) {
		return errParam("flowLogType must be ALL, REJECT or ACCEPT")
	}

	return nil
}

func validCaptureRule(r CaptureRule, kind string) *APIError {
	check := validFlowRuleFields
	if kind == filterVtap {
		check = validVtapRuleFields
	}

	if aerr := check(r); aerr != nil {
		return aerr
	}

	if r.RuleAction != "" && r.RuleAction != ruleInclude && r.RuleAction != ruleExclude {
		return errParam("ruleAction must be INCLUDE or EXCLUDE")
	}

	for _, c := range []string{r.SourceCidr, r.DestinationCidr} {
		if aerr := validCaptureCidr(c); aerr != nil {
			return aerr
		}
	}

	if r.Protocol == "" {
		if r.IcmpOptions != nil || r.TCPOptions != nil || r.UDPOptions != nil {
			return errParam("protocol options need a protocol")
		}

		return nil
	}

	return ruleOptions(r.Protocol, r.IcmpOptions, r.TCPOptions, r.UDPOptions)
}

func checkCaptureRules(kind string, vtap, flow []CaptureRule) *APIError {
	own, other := vtap, flow
	if kind == filterFlowLog {
		own, other = flow, vtap
	}

	if len(other) > 0 {
		return errParam("rules of the other filterType are not allowed in a " + kind + " capture filter")
	}

	for _, r := range own {
		if aerr := validCaptureRule(r, kind); aerr != nil {
			return aerr
		}
	}

	return nil
}

// CreateCaptureFilterIn is the CreateCaptureFilter request body.
type CreateCaptureFilterIn struct {
	TagsIn
	CompartmentID string        `json:"compartmentId"`
	DisplayName   string        `json:"displayName"`
	FilterType    string        `json:"filterType"`
	VtapRules     []CaptureRule `json:"vtapCaptureFilterRules"`
	FlowLogRules  []CaptureRule `json:"flowLogCaptureFilterRules"`
}

// CreateCaptureFilter creates a VTAP or flow log capture filter.
func (b *InMemoryBackend) CreateCaptureFilter(in CreateCaptureFilterIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.FilterType != filterVtap && in.FilterType != filterFlowLog {
		return nil, "", errParam("filterType must be VTAP or FLOWLOG")
	}

	if aerr := checkCaptureRules(in.FilterType, in.VtapRules, in.FlowLogRules); aerr != nil {
		return nil, "", aerr
	}

	f := &CaptureFilter{
		Base:       b.newBase(kindCaptureFilter, in.CompartmentID, in.DisplayName, in.TagsIn),
		FilterType: in.FilterType, VtapRules: in.VtapRules, FlowLogRules: in.FlowLogRules,
	}
	b.add(f)

	return f.view(), f.Internal.Etag, nil
}

// UpdateCaptureFilterIn is the UpdateCaptureFilter request body.
type UpdateCaptureFilterIn struct {
	UpdateIn
	VtapRules    *[]CaptureRule `json:"vtapCaptureFilterRules"`
	FlowLogRules *[]CaptureRule `json:"flowLogCaptureFilterRules"`
}

// UpdateCaptureFilter renames a filter or replaces its rules.
func (b *InMemoryBackend) UpdateCaptureFilter(
	id, ifMatch string,
	in UpdateCaptureFilterIn,
) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(f *CaptureFilter) *APIError {
		vt, fl := f.VtapRules, f.FlowLogRules
		if in.VtapRules != nil {
			vt = *in.VtapRules
		}

		if in.FlowLogRules != nil {
			fl = *in.FlowLogRules
		}

		if aerr := checkCaptureRules(f.FilterType, vt, fl); aerr != nil {
			return aerr
		}

		f.VtapRules, f.FlowLogRules = vt, fl

		return nil
	})
}

// CreateVtapIn is the CreateVtap request body.
type CreateVtapIn struct {
	TagsIn
	VxlanNetworkIdentifier        *int64 `json:"vxlanNetworkIdentifier"`
	IsVtapEnabled                 *bool  `json:"isVtapEnabled"`
	MaxPacketSize                 *int   `json:"maxPacketSize"`
	CompartmentID                 string `json:"compartmentId"`
	DisplayName                   string `json:"displayName"`
	VcnID                         string `json:"vcnId"`
	SourceID                      string `json:"sourceId"`
	CaptureFilterID               string `json:"captureFilterId"`
	TargetID                      string `json:"targetId"`
	TargetIP                      string `json:"targetIp"`
	EncapsulationProtocol         string `json:"encapsulationProtocol"`
	SourceType                    string `json:"sourceType"`
	TargetType                    string `json:"targetType"`
	TrafficMode                   string `json:"trafficMode"`
	SourcePrivateEndpointIP       string `json:"sourcePrivateEndpointIp"`
	SourcePrivateEndpointSubnetID string `json:"sourcePrivateEndpointSubnetId"`
}

func (b *InMemoryBackend) checkVtapSource(vcnID, typ, id string) *APIError {
	switch typ {
	case srcVnic:
		vn, aerr := getLive[*Vnic](b, id)
		if aerr != nil {
			return aerr
		}

		if s, ok := b.res[vn.SubnetID].(*Subnet); !ok || s.VcnID != vcnID {
			return errParam("The source VNIC " + id + " is not in VCN " + vcnID)
		}
	case srcSubnet:
		return refInVcn[*Subnet](b, id, vcnID, "source subnet")
	case "LOAD_BALANCER", "DB_SYSTEM", "EXADATA_VM_CLUSTER", "AUTONOMOUS_DATA_WAREHOUSE", "NETWORK_FIREWALL":
		if id == "" {
			return errMissing("sourceId is required")
		}
	default:
		return errParam("sourceType " + typ + " is not a valid VTAP source type")
	}

	return nil
}

func (b *InMemoryBackend) checkVtapTarget(vcn *Vcn, typ, id, ip string) *APIError {
	switch typ {
	case tgtVnic:
		vn, aerr := getLive[*Vnic](b, id)
		if aerr != nil {
			return aerr
		}

		if s, ok := b.res[vn.SubnetID].(*Subnet); !ok || s.VcnID != vcn.ID {
			return errParam("The target VNIC " + id + " is not in VCN " + vcn.ID)
		}
	case tgtNLB:
		if id == "" {
			return errMissing("targetId is required for a network load balancer target")
		}
	case tgtIP:
		a, err := netip.ParseAddr(ip)
		if err != nil || !a.Is4() {
			return errParam("targetIp must be an IPv4 address")
		}

		if !slices.ContainsFunc(vcn.CidrBlocks, func(c string) bool { return netip.MustParsePrefix(c).Contains(a) }) {
			return errParam("targetIp " + ip + " is outside the CIDR blocks of VCN " + vcn.ID)
		}
	default:
		return errParam("targetType " + typ + " is not a valid VTAP target type")
	}

	return nil
}

func (b *InMemoryBackend) vtapChecks(in *CreateVtapIn) *APIError {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return aerr
	}

	if in.VcnID == "" || in.SourceID == "" || in.CaptureFilterID == "" {
		return errMissing("vcnId, sourceId and captureFilterId are required")
	}

	vcn, aerr := getLive[*Vcn](b, in.VcnID)
	if aerr != nil {
		return aerr
	}

	in.SourceType, in.TargetType = cmpOrStr(in.SourceType, srcVnic), cmpOrStr(in.TargetType, tgtVnic)
	if in.TargetIP != "" && in.TargetID == "" {
		in.TargetType = tgtIP
	}

	if aerr = b.checkVtapSource(vcn.ID, in.SourceType, in.SourceID); aerr != nil {
		return aerr
	}

	if aerr = b.checkVtapTarget(vcn, in.TargetType, in.TargetID, in.TargetIP); aerr != nil {
		return aerr
	}

	if in.TargetType == tgtVnic && in.TargetID == in.SourceID {
		return errParam("A VTAP target cannot be its own source")
	}

	f, aerr := getLive[*CaptureFilter](b, in.CaptureFilterID)
	if aerr != nil {
		return aerr
	}

	if f.FilterType != filterVtap {
		return errParam("Capture filter " + f.ID + " is not a VTAP capture filter")
	}

	return nil
}

func vtapTuning(in *CreateVtapIn) *APIError {
	in.EncapsulationProtocol = cmpOrStr(in.EncapsulationProtocol, encapVxlan)
	in.TrafficMode = cmpOrStr(in.TrafficMode, modeDefault)

	switch {
	case in.EncapsulationProtocol != encapVxlan:
		return errParam("encapsulationProtocol must be VXLAN")
	case in.TrafficMode != modeDefault && in.TrafficMode != modePriority:
		return errParam("trafficMode must be DEFAULT or PRIORITY")
	case in.VxlanNetworkIdentifier != nil && (*in.VxlanNetworkIdentifier < 0 || *in.VxlanNetworkIdentifier > maxVni):
		return errParam("vxlanNetworkIdentifier must be between 0 and 16777215")
	case in.MaxPacketSize != nil && (*in.MaxPacketSize < 1 || *in.MaxPacketSize > defaultMaxPacket):
		return errParam("maxPacketSize must be between 1 and 9000")
	}

	return nil
}

// CreateVtap creates a VTAP; mirroring itself is recorded, not performed.
func (b *InMemoryBackend) CreateVtap(in CreateVtapIn) (Resource, string, *APIError) {
	if aerr := b.vtapChecks(&in); aerr != nil {
		return nil, "", aerr
	}

	if aerr := vtapTuning(&in); aerr != nil {
		return nil, "", aerr
	}

	v := &Vtap{
		Base:                          b.newBase(kindVtap, in.CompartmentID, in.DisplayName, in.TagsIn),
		VcnID:                         in.VcnID,
		SourceID:                      in.SourceID,
		CaptureFilterID:               in.CaptureFilterID,
		TargetID:                      in.TargetID,
		TargetIP:                      in.TargetIP,
		EncapsulationProtocol:         in.EncapsulationProtocol,
		SourceType:                    in.SourceType,
		TargetType:                    in.TargetType,
		TrafficMode:                   in.TrafficMode,
		MaxPacketSize:                 defaultMaxPacket,
		SourcePrivateEndpointIP:       in.SourcePrivateEndpointIP,
		SourcePrivateEndpointSubnetID: in.SourcePrivateEndpointSubnetID,
		LifecycleDetails:              detailStopped,
	}

	if in.VxlanNetworkIdentifier != nil {
		v.VxlanNetworkIdentifier = *in.VxlanNetworkIdentifier
	}

	if in.MaxPacketSize != nil {
		v.MaxPacketSize = *in.MaxPacketSize
	}

	v.setEnabled(in.IsVtapEnabled != nil && *in.IsVtapEnabled)
	b.add(v)

	return v.view(), v.Internal.Etag, nil
}

func (v *Vtap) setEnabled(on bool) {
	v.IsVtapEnabled, v.LifecycleDetails = on, detailStopped
	if on {
		v.LifecycleDetails = detailRunning
	}
}

// UpdateVtapIn is the UpdateVtap request body.
type UpdateVtapIn struct {
	UpdateIn
	VxlanNetworkIdentifier *int64  `json:"vxlanNetworkIdentifier"`
	IsVtapEnabled          *bool   `json:"isVtapEnabled"`
	MaxPacketSize          *int    `json:"maxPacketSize"`
	SourceID               *string `json:"sourceId"`
	TargetID               *string `json:"targetId"`
	TargetIP               *string `json:"targetIp"`
	CaptureFilterID        *string `json:"captureFilterId"`
	TrafficMode            *string `json:"trafficMode"`
	SourceType             *string `json:"sourceType"`
	TargetType             *string `json:"targetType"`
}

func str(p *string, def string) string {
	if p == nil {
		return def
	}

	return *p
}

// UpdateVtap changes the endpoints, filter and tuning of a VTAP.
func (b *InMemoryBackend) UpdateVtap(id, ifMatch string, in UpdateVtapIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(v *Vtap) *APIError {
		c := CreateVtapIn{
			CompartmentID: v.CompartmentID, VcnID: v.VcnID, SourceID: str(in.SourceID, v.SourceID),
			CaptureFilterID: str(in.CaptureFilterID, v.CaptureFilterID), TargetID: str(in.TargetID, v.TargetID),
			TargetIP: str(in.TargetIP, v.TargetIP), SourceType: str(in.SourceType, v.SourceType),
			TargetType: str(in.TargetType, v.TargetType), TrafficMode: str(in.TrafficMode, v.TrafficMode),
			EncapsulationProtocol: v.EncapsulationProtocol, VxlanNetworkIdentifier: in.VxlanNetworkIdentifier,
			MaxPacketSize: in.MaxPacketSize,
		}

		if aerr := b.vtapChecks(&c); aerr != nil {
			return aerr
		}

		if aerr := vtapTuning(&c); aerr != nil {
			return aerr
		}

		v.SourceID, v.CaptureFilterID, v.TargetID, v.TargetIP = c.SourceID, c.CaptureFilterID, c.TargetID, c.TargetIP
		v.SourceType, v.TargetType, v.TrafficMode = c.SourceType, c.TargetType, c.TrafficMode

		if in.VxlanNetworkIdentifier != nil {
			v.VxlanNetworkIdentifier = *in.VxlanNetworkIdentifier
		}

		if in.MaxPacketSize != nil {
			v.MaxPacketSize = *in.MaxPacketSize
		}

		if in.IsVtapEnabled != nil {
			v.setEnabled(*in.IsVtapEnabled)
		}

		return nil
	})
}

func vtapFilter(q *req) func(Resource) bool {
	return and(
		fieldEq[*Vtap](q, "vcnId", func(x *Vtap) string { return x.VcnID }),
		fieldEq[*Vtap](q, "source", func(x *Vtap) string { return x.SourceID }),
		fieldEq[*Vtap](q, "targetId", func(x *Vtap) string { return x.TargetID }),
		fieldEq[*Vtap](q, "targetIp", func(x *Vtap) string { return x.TargetIP }),
		func(r Resource) bool {
			want := q.query("isVtapEnabled")
			x, _ := r.(*Vtap)

			return want == "" || want == boolStr(x.IsVtapEnabled)
		},
	)
}

func boolStr(v bool) string {
	if v {
		return "true"
	}

	return "false"
}

func captureFilterFilter(q *req) func(Resource) bool {
	return fieldEq[*CaptureFilter](q, "filterType", func(x *CaptureFilter) string { return x.FilterType })
}

func vtapEntries() []entry {
	vt, cf := idPath("/vtaps", "vtapId"), idPath("/captureFilters", "captureFilterId")

	return []entry{
		{createOp((*InMemoryBackend).CreateVtap), http.MethodPost, "/vtaps", "CreateVtap"},
		{listOp[*Vtap](vtapFilter), http.MethodGet, "/vtaps", "ListVtaps"},
		{getOp[*Vtap]("vtapId"), http.MethodGet, vt, "GetVtap"},
		{updateOp("vtapId", (*InMemoryBackend).UpdateVtap), http.MethodPut, vt, "UpdateVtap"},
		{deleteOp[*Vtap]("vtapId"), http.MethodDelete, vt, "DeleteVtap"},
		{changeOp[*Vtap]("vtapId"), http.MethodPost, vt + "/actions/changeCompartment", "ChangeVtapCompartment"},
		{createOp((*InMemoryBackend).CreateCaptureFilter), http.MethodPost, "/captureFilters", "CreateCaptureFilter"},
		{listOp[*CaptureFilter](captureFilterFilter), http.MethodGet, "/captureFilters", "ListCaptureFilters"},
		{getOp[*CaptureFilter]("captureFilterId"), http.MethodGet, cf, "GetCaptureFilter"},
		{
			updateOp("captureFilterId", (*InMemoryBackend).UpdateCaptureFilter),
			http.MethodPut,
			cf,
			"UpdateCaptureFilter",
		},
		{deleteOp[*CaptureFilter]("captureFilterId"), http.MethodDelete, cf, "DeleteCaptureFilter"},
		{
			changeOp[*CaptureFilter]("captureFilterId"), http.MethodPost, cf + "/actions/changeCompartment",
			"ChangeCaptureFilterCompartment",
		},
	}
}
