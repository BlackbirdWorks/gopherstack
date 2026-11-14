package azureredis

// Spec is the part of a cache's ARM configuration the Redis servers act on.
type Spec struct {
	SKU               string             `json:"sku"`
	MinTLS            string             `json:"minTls"`
	Firewall          []IPRange          `json:"firewall,omitempty"`
	Assignments       []AccessAssignment `json:"accessAssignments,omitempty"`
	EnableNonSSLPort  bool               `json:"enableNonSslPort"`
	Persistence       bool               `json:"persistence"`
	AuthNotRequired   bool               `json:"authNotRequired,omitempty"`
	DisableAccessKeys bool               `json:"disableAccessKeys,omitempty"`
	EntraEnabled      bool               `json:"entraEnabled,omitempty"`
}

// AccessAssignment grants a principal the permissions of a data access policy; Alias is also the user name
// accepted for token authentication.
type AccessAssignment struct {
	ObjectID    string `json:"objectId"`
	Alias       string `json:"alias"`
	Permissions string `json:"permissions"`
}

// Endpoint is where a cache's servers listen. Port is 0 while the non-TLS port is disabled.
type Endpoint struct {
	Host    string
	Port    int
	SSLPort int
}

// Keys are a cache's two access keys; either authenticates.
type Keys struct {
	Primary   string
	Secondary string
}

// Reboot types accepted by Reboot.
const (
	RebootPrimary   = "PrimaryNode"
	RebootSecondary = "SecondaryNode"
	RebootAll       = "AllNodes"
)

// Access key names accepted by RegenerateKey.
const (
	KeyPrimary   = "Primary"
	KeySecondary = "Secondary"
)

// SKU names.
const (
	SKUBasic    = "Basic"
	SKUStandard = "Standard"
	SKUPremium  = "Premium"
)
