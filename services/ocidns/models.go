package ocidns

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

const (
	scopeGlobal  = "GLOBAL"
	scopePrivate = "PRIVATE"

	zonePrimary   = "PRIMARY"
	zoneSecondary = "SECONDARY"

	modeStatic = "STATIC"

	dnssecEnabled  = "ENABLED"
	dnssecDisabled = "DISABLED"

	stateActive = "ACTIVE"

	endpointVnic = "VNIC"
	actionFwd    = "FORWARD"
)

type meta struct {
	Modified time.Time `json:"modified"`
	ETagSeq  int64     `json:"etagSeq"`
}

func (m *meta) touch(now time.Time) {
	m.ETagSeq++
	m.Modified = now
}

func (m *meta) etag(id string) string {
	sum := sha256.Sum256([]byte(id + "/" + strconv.FormatInt(m.ETagSeq, 10)))

	return hex.EncodeToString(sum[:16])
}

// Tags are the shared freeform and defined tag sets of a resource.
type Tags struct {
	FreeformTags map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags  map[string]map[string]any `json:"definedTags,omitempty"`
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}
}

func (t *Tags) free() map[string]string {
	if t.FreeformTags == nil {
		return map[string]string{}
	}

	return t.FreeformTags
}

func (t *Tags) defined() map[string]map[string]any {
	if t.DefinedTags == nil {
		return map[string]map[string]any{}
	}

	return t.DefinedTags
}

// ExtServer is an external master or downstream nameserver of a zone.
type ExtServer struct {
	Port      *int   `json:"port,omitempty"`
	Address   string `json:"address"`
	TsigKeyID string `json:"tsigKeyId,omitempty"`
}

// RRSet is the records of one domain and type; they share one TTL.
type RRSet struct {
	Domain  string   `json:"domain"`
	Rtype   string   `json:"rtype"`
	Rdata   []string `json:"rdata"`
	TTL     int      `json:"ttl"`
	Version int64    `json:"version"`
}

// Zone is a DNS zone with its records.
type Zone struct {
	Created time.Time         `json:"created"`
	Records map[string]*RRSet `json:"records"`
	meta
	Tags
	ID                  string      `json:"id"`
	Name                string      `json:"name"`
	ZoneType            string      `json:"zoneType"`
	CompartmentID       string      `json:"compartmentId"`
	Scope               string      `json:"scope"`
	ViewID              string      `json:"viewId,omitempty"`
	ResolutionMode      string      `json:"resolutionMode"`
	DnssecState         string      `json:"dnssecState"`
	ExternalMasters     []ExtServer `json:"externalMasters"`
	ExternalDownstreams []ExtServer `json:"externalDownstreams"`
	Version             int64       `json:"version"`
	Seq                 int64       `json:"seq"`
}

// View is a private DNS view.
type View struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	meta
	Tags
	ID            string `json:"id"`
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
	Seq           int64  `json:"seq"`
	Protected     bool   `json:"protected"`
}

// ResolverRule is a resolver forwarding rule.
type ResolverRule struct {
	SourceEndpointName      string   `json:"sourceEndpointName"`
	ClientAddressConditions []string `json:"clientAddressConditions"`
	QnameCoverConditions    []string `json:"qnameCoverConditions"`
	DestinationAddresses    []string `json:"destinationAddresses"`
}

// Resolver is the private resolver of a VCN.
type Resolver struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	meta
	Tags
	ID            string         `json:"id"`
	CompartmentID string         `json:"compartmentId"`
	DisplayName   string         `json:"displayName"`
	VcnID         string         `json:"vcnId"`
	DefaultViewID string         `json:"defaultViewId"`
	AttachedViews []string       `json:"attachedViews"`
	Rules         []ResolverRule `json:"rules"`
	Seq           int64          `json:"seq"`
}

// Endpoint is a VNIC resolver endpoint.
type Endpoint struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	meta
	Tags
	ID                string   `json:"id"`
	ResolverID        string   `json:"resolverId"`
	CompartmentID     string   `json:"compartmentId"`
	Name              string   `json:"name"`
	SubnetID          string   `json:"subnetId"`
	ForwardingAddress string   `json:"forwardingAddress,omitempty"`
	ListeningAddress  string   `json:"listeningAddress,omitempty"`
	NsgIDs            []string `json:"nsgIds"`
	Seq               int64    `json:"seq"`
	IsForwarding      bool     `json:"isForwarding"`
	IsListening       bool     `json:"isListening"`
}

// TsigKey is a TSIG key.
type TsigKey struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	meta
	Tags
	ID            string `json:"id"`
	CompartmentID string `json:"compartmentId"`
	Name          string `json:"name"`
	Algorithm     string `json:"algorithm"`
	Secret        string `json:"secret"`
	Seq           int64  `json:"seq"`
}

// SteeringPolicy is a stored traffic steering policy; it is not evaluated.
type SteeringPolicy struct {
	Created time.Time `json:"created"`
	meta
	Tags
	ID                   string           `json:"id"`
	CompartmentID        string           `json:"compartmentId"`
	DisplayName          string           `json:"displayName"`
	Template             string           `json:"template"`
	HealthCheckMonitorID string           `json:"healthCheckMonitorId,omitempty"`
	Answers              []map[string]any `json:"answers"`
	Rules                []map[string]any `json:"rules"`
	TTL                  int              `json:"ttl"`
	Seq                  int64            `json:"seq"`
}

// Attachment binds a steering policy to a domain of a GLOBAL zone.
type Attachment struct {
	Created time.Time `json:"created"`
	meta
	ID               string   `json:"id"`
	CompartmentID    string   `json:"compartmentId"`
	DisplayName      string   `json:"displayName"`
	SteeringPolicyID string   `json:"steeringPolicyId"`
	ZoneID           string   `json:"zoneId"`
	Domain           string   `json:"domain"`
	Rtypes           []string `json:"rtypes"`
	Seq              int64    `json:"seq"`
}
