package amp

// Documented per-workspace ingestion quotas (AMP_quotas.html, Service quotas).
const (
	DefaultIngestionRate   = 1666666
	DefaultActiveSeries    = 50000000
	maxLabelsPerSeries     = 150
	maxLabelBytesPerSeries = 7 * 1024
	activeSeriesWindowMs   = int64(2 * 60 * 60 * 1000)
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.AMP.
type Settings struct {
	IngestionRate          int  `json:"ingestionRate"          env:"AMP_INGESTION_RATE"           default:"1666666"  name:"ingestion-rate"           help:"Samples per second one workspace may ingest; also the token bucket refill rate."`                                              //nolint:lll // config struct tags are intentionally verbose
	IngestionBurst         int  `json:"ingestionBurst"         env:"AMP_INGESTION_BURST"                             name:"ingestion-burst"          help:"Token bucket size in samples; 0 means one second of the ingestion rate."`                                                      //nolint:lll // config struct tags are intentionally verbose
	ActiveSeries           int  `json:"activeSeries"           env:"AMP_ACTIVE_SERIES"            default:"50000000" name:"active-series"            help:"Active series (a sample in the last 2 hours) one workspace may hold."`                                                         //nolint:lll // config struct tags are intentionally verbose
	EnforceIngestionQuotas bool `json:"enforceIngestionQuotas" env:"AMP_ENFORCE_INGESTION_QUOTAS"                    name:"enforce-ingestion-quotas" help:"Reject remote write beyond the ingestion rate (429) and active series (400) quotas; off by default so bulk loads never fail."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{IngestionRate: DefaultIngestionRate, ActiveSeries: DefaultActiveSeries}
}

// ConfigProvider extracts AMP configuration from the AppContext Config.
type ConfigProvider interface {
	GetAMPSettings() Settings
}
