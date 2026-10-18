package ses

import "time"

// Settings holds service-level configuration for the SES backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	InboundAddr     string        `json:"inbound_addr"     env:"SES_INBOUND_ADDR"     default:"127.0.0.1:2525" help:"Listen address of the inbound SMTP server."`                                      //nolint:lll // Kong struct tag makes this line long
	JanitorInterval time.Duration `json:"janitor_interval" env:"SES_JANITOR_INTERVAL" default:"1m"             help:"Janitor tick interval."`                                                          //nolint:lll // Kong struct tag makes this line long
	EmailTTL        time.Duration `json:"email_ttl"        env:"SES_EMAIL_TTL"        default:"24h"            help:"TTL for stored sent emails before they are evicted."`                             //nolint:lll // Kong struct tag makes this line long
	InboundMaxSize  int64         `json:"inbound_max_size" env:"SES_INBOUND_MAX_SIZE" default:"41943040"       help:"Maximum inbound message size in bytes."`                                          //nolint:lll // Kong struct tag makes this line long
	InboundEnabled  bool          `json:"inbound_enabled"  env:"SES_INBOUND_ENABLED"  default:"false"          help:"Run an SMTP listener that applies the active receipt rule set to received mail."` //nolint:lll // Kong struct tag makes this line long
}
