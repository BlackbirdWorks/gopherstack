package azureredis

// Spec is the part of a cache's ARM configuration the Redis servers act on.
type Spec struct {
	// SKU is Basic, Standard or Premium.
	SKU string `json:"sku"`
	// MinTLS is "1.0", "1.1" or "1.2".
	MinTLS string `json:"minTls"`
	// EnableNonSSLPort serves plaintext Redis on a second port.
	EnableNonSSLPort bool `json:"enableNonSslPort"`
	// Persistence is true when RDB or AOF backup is enabled, so a full reboot keeps the data.
	Persistence bool `json:"persistence"`
	// AuthNotRequired (redisConfiguration authnotrequired) lets clients connect without a key.
	AuthNotRequired bool `json:"authNotRequired,omitempty"`
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
