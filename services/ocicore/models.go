package ocicore

import "time"

// Lifecycle states.
const (
	statProvisioning = "PROVISIONING"
	statAvailable    = "AVAILABLE"
	statTerminating  = "TERMINATING"
	statTerminated   = "TERMINATED"
	statAssigned     = "ASSIGNED"
)

// Resource kinds, as they appear in OCIDs (ocid1.<kind>.oc1.iad.<id>).
const (
	kindVcn      = "vcn"
	kindSubnet   = "subnet"
	kindIGW      = "internetgateway"
	kindNAT      = "natgateway"
	kindSGW      = "servicegateway"
	kindRT       = "routetable"
	kindSL       = "securitylist"
	kindNSG      = "networksecuritygroup"
	kindDHCP     = "dhcpoptions"
	kindLPG      = "localpeeringgateway"
	kindPublicIP = "publicip"
	kindService  = "service"
	kindDRG      = "drg"
)

// Internal is bookkeeping that never reaches the wire.
type Internal struct {
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	TimeDeleted  *time.Time `json:"timeDeleted,omitempty"`
	Etag         string     `json:"etag"`
	Seq          int64      `json:"seq"`
}

// Base is the field set every Core networking resource shares.
type Base struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	Internal       *Internal                 `json:"internal,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	LifecycleState string                    `json:"lifecycleState"`
}

func (b *Base) base() *Base { return b }

type resource interface {
	base() *Base
	vcnRef() string
	view() resource
}

// Vcn is a virtual cloud network.
type Vcn struct {
	Base
	CidrBlock            string   `json:"cidrBlock"`
	DefaultDhcpOptionsID string   `json:"defaultDhcpOptionsId"`
	DefaultRouteTableID  string   `json:"defaultRouteTableId"`
	DefaultSecurityList  string   `json:"defaultSecurityListId"`
	DNSLabel             string   `json:"dnsLabel,omitempty"`
	VcnDomainName        string   `json:"vcnDomainName,omitempty"`
	CidrBlocks           []string `json:"cidrBlocks"`
	Ipv6CidrBlocks       []string `json:"ipv6CidrBlocks,omitempty"`
	Ipv6PrivateCidrs     []string `json:"ipv6PrivateCidrBlocks,omitempty"`
}

// Subnet is a subnet of a VCN.
type Subnet struct {
	Base
	VcnID                   string   `json:"vcnId"`
	AvailabilityDomain      *string  `json:"availabilityDomain"`
	CidrBlock               string   `json:"cidrBlock"`
	DhcpOptionsID           string   `json:"dhcpOptionsId"`
	DNSLabel                string   `json:"dnsLabel,omitempty"`
	SubnetDomainName        string   `json:"subnetDomainName,omitempty"`
	Ipv6CidrBlock           string   `json:"ipv6CidrBlock,omitempty"`
	Ipv6VirtualRouterIP     string   `json:"ipv6VirtualRouterIp,omitempty"`
	RouteTableID            string   `json:"routeTableId"`
	VirtualRouterIP         string   `json:"virtualRouterIp"`
	VirtualRouterMac        string   `json:"virtualRouterMac"`
	Ipv4CidrBlocks          []string `json:"ipv4CidrBlocks"`
	Ipv6CidrBlocks          []string `json:"ipv6CidrBlocks,omitempty"`
	SecurityListIDs         []string `json:"securityListIds"`
	ProhibitInternetIngress bool     `json:"prohibitInternetIngress"`
	ProhibitPublicIPOnVnic  bool     `json:"prohibitPublicIpOnVnic"`
}

// InternetGateway is an internet gateway.
type InternetGateway struct {
	Base
	VcnID        string `json:"vcnId"`
	RouteTableID string `json:"routeTableId,omitempty"`
	IsEnabled    bool   `json:"isEnabled"`
}

// NatGateway is a NAT gateway.
type NatGateway struct {
	Base
	VcnID        string `json:"vcnId"`
	NatIP        string `json:"natIp"`
	PublicIPID   string `json:"publicIpId,omitempty"`
	RouteTableID string `json:"routeTableId,omitempty"`
	BlockTraffic bool   `json:"blockTraffic"`
}

// ServiceRef names a service attached to a service gateway.
type ServiceRef struct {
	ServiceID   string `json:"serviceId"`
	ServiceName string `json:"serviceName"`
}

// ServiceGateway is a service gateway.
type ServiceGateway struct {
	Base
	VcnID        string       `json:"vcnId"`
	RouteTableID string       `json:"routeTableId,omitempty"`
	Services     []ServiceRef `json:"services"`
	BlockTraffic bool         `json:"blockTraffic"`
}

// RouteRule is one route table rule.
type RouteRule struct {
	CidrBlock       string `json:"cidrBlock,omitempty"`
	Destination     string `json:"destination,omitempty"`
	DestinationType string `json:"destinationType,omitempty"`
	NetworkEntityID string `json:"networkEntityId"`
	RouteType       string `json:"routeType,omitempty"`
	Description     string `json:"description,omitempty"`
}

// RouteTable is a route table.
type RouteTable struct {
	Base
	VcnID      string      `json:"vcnId"`
	RouteRules []RouteRule `json:"routeRules"`
}

// PortRange is an inclusive port range.
type PortRange struct {
	Max int `json:"max"`
	Min int `json:"min"`
}

// PortOptions are TCP or UDP match options.
type PortOptions struct {
	DestinationPortRange *PortRange `json:"destinationPortRange,omitempty"`
	SourcePortRange      *PortRange `json:"sourcePortRange,omitempty"`
}

// IcmpOptions are ICMP match options.
type IcmpOptions struct {
	Code *int `json:"code,omitempty"`
	Type int  `json:"type"`
}

// IngressRule is a security list ingress rule.
type IngressRule struct {
	IcmpOptions *IcmpOptions `json:"icmpOptions,omitempty"`
	TCPOptions  *PortOptions `json:"tcpOptions,omitempty"`
	UDPOptions  *PortOptions `json:"udpOptions,omitempty"`
	IsStateless *bool        `json:"isStateless,omitempty"`
	Protocol    string       `json:"protocol"`
	Source      string       `json:"source"`
	SourceType  string       `json:"sourceType,omitempty"`
	Description string       `json:"description,omitempty"`
}

// EgressRule is a security list egress rule.
type EgressRule struct {
	IcmpOptions     *IcmpOptions `json:"icmpOptions,omitempty"`
	TCPOptions      *PortOptions `json:"tcpOptions,omitempty"`
	UDPOptions      *PortOptions `json:"udpOptions,omitempty"`
	IsStateless     *bool        `json:"isStateless,omitempty"`
	Protocol        string       `json:"protocol"`
	Destination     string       `json:"destination"`
	DestinationType string       `json:"destinationType,omitempty"`
	Description     string       `json:"description,omitempty"`
}

// SecurityList is a security list.
type SecurityList struct {
	Base
	VcnID        string        `json:"vcnId"`
	IngressRules []IngressRule `json:"ingressSecurityRules"`
	EgressRules  []EgressRule  `json:"egressSecurityRules"`
}

// NetworkSecurityGroup is a network security group.
type NetworkSecurityGroup struct {
	Base
	VcnID string `json:"vcnId"`
}

// SecurityRule is a network security group rule.
type SecurityRule struct {
	TimeCreated     time.Time    `json:"timeCreated"`
	IcmpOptions     *IcmpOptions `json:"icmpOptions,omitempty"`
	TCPOptions      *PortOptions `json:"tcpOptions,omitempty"`
	UDPOptions      *PortOptions `json:"udpOptions,omitempty"`
	IsStateless     *bool        `json:"isStateless,omitempty"`
	ID              string       `json:"id"`
	Direction       string       `json:"direction"`
	Protocol        string       `json:"protocol"`
	Source          string       `json:"source,omitempty"`
	SourceType      string       `json:"sourceType,omitempty"`
	Destination     string       `json:"destination,omitempty"`
	DestinationType string       `json:"destinationType,omitempty"`
	Description     string       `json:"description,omitempty"`
	IsValid         bool         `json:"isValid"`
}

// NSGRule is a stored network security group rule.
type NSGRule struct {
	NsgID string `json:"nsgId"`
	Seq   int64  `json:"seq"`
	SecurityRule
}

// DhcpOption is one DHCP option; Type selects the populated fields.
type DhcpOption struct {
	Type              string   `json:"type"`
	ServerType        string   `json:"serverType,omitempty"`
	CustomDNSServers  []string `json:"customDnsServers,omitempty"`
	SearchDomainNames []string `json:"searchDomainNames,omitempty"`
}

// DhcpOptions is a set of DHCP options.
type DhcpOptions struct {
	Base
	VcnID          string       `json:"vcnId"`
	DomainNameType string       `json:"domainNameType,omitempty"`
	Options        []DhcpOption `json:"options"`
}

// LocalPeeringGateway is a local peering gateway.
type LocalPeeringGateway struct {
	Base
	VcnID                 string   `json:"vcnId"`
	PeerID                string   `json:"peerId,omitempty"`
	PeeringStatus         string   `json:"peeringStatus"`
	PeeringStatusDetails  string   `json:"peeringStatusDetails,omitempty"`
	PeerAdvertisedCidr    string   `json:"peerAdvertisedCidr,omitempty"`
	RouteTableID          string   `json:"routeTableId,omitempty"`
	PeerAdvertisedDetails []string `json:"peerAdvertisedCidrDetails,omitempty"`
	IsCrossTenancyPeering bool     `json:"isCrossTenancyPeering"`
}

// PublicIP is a public IP address.
type PublicIP struct {
	Base
	IPAddress          string  `json:"ipAddress"`
	Lifetime           string  `json:"lifetime"`
	Scope              string  `json:"scope"`
	AssignedEntityID   string  `json:"assignedEntityId,omitempty"`
	AssignedEntityType string  `json:"assignedEntityType,omitempty"`
	AvailabilityDomain *string `json:"availabilityDomain"`
	PrivateIPID        string  `json:"privateIpId,omitempty"`
	PublicIPPoolID     string  `json:"publicIpPoolId,omitempty"`
}

// Service is an Oracle service reachable through a service gateway.
type Service struct {
	CidrBlock   string `json:"cidrBlock"`
	Description string `json:"description"`
	ID          string `json:"id"`
	Name        string `json:"name"`
}

func (v *Vcn) vcnRef() string                  { return "" }
func (v *Subnet) vcnRef() string               { return v.VcnID }
func (v *InternetGateway) vcnRef() string      { return v.VcnID }
func (v *NatGateway) vcnRef() string           { return v.VcnID }
func (v *ServiceGateway) vcnRef() string       { return v.VcnID }
func (v *RouteTable) vcnRef() string           { return v.VcnID }
func (v *SecurityList) vcnRef() string         { return v.VcnID }
func (v *NetworkSecurityGroup) vcnRef() string { return v.VcnID }
func (v *DhcpOptions) vcnRef() string          { return v.VcnID }
func (v *LocalPeeringGateway) vcnRef() string  { return v.VcnID }
func (v *PublicIP) vcnRef() string             { return "" }

func (v *Vcn) view() resource                  { c := *v; c.Internal = nil; return &c }
func (v *Subnet) view() resource               { c := *v; c.Internal = nil; return &c }
func (v *InternetGateway) view() resource      { c := *v; c.Internal = nil; return &c }
func (v *NatGateway) view() resource           { c := *v; c.Internal = nil; return &c }
func (v *ServiceGateway) view() resource       { c := *v; c.Internal = nil; return &c }
func (v *RouteTable) view() resource           { c := *v; c.Internal = nil; return &c }
func (v *SecurityList) view() resource         { c := *v; c.Internal = nil; return &c }
func (v *NetworkSecurityGroup) view() resource { c := *v; c.Internal = nil; return &c }
func (v *DhcpOptions) view() resource          { c := *v; c.Internal = nil; return &c }
func (v *LocalPeeringGateway) view() resource  { c := *v; c.Internal = nil; return &c }
func (v *PublicIP) view() resource             { c := *v; c.Internal = nil; return &c }
