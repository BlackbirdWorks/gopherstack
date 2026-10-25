package s3

import "time"

const defaultCompressionMinBytes = 1024

// DefaultSettings returns a Settings struct populated with the documented defaults.
// This is used when no ConfigProvider is available at init time.
func DefaultSettings() Settings {
	return Settings{
		DefaultRegion:       "us-east-1",
		JanitorInterval:     defaultJanitorInterval,
		CompressionMinBytes: defaultCompressionMinBytes,
	}
}

// Settings holds service-level configuration for the S3 backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	DefaultRegion        string        `json:"default_region"          env:"S3_REGION"                                                         default:"us-east-1" help:"Default region for S3."`                                                                                                                                                                                                  //nolint:lll // Kong struct tag makes this line long
	JanitorInterval      time.Duration `json:"janitor_interval"        env:"S3_JANITOR_INTERVAL"                                               default:"500ms"     help:"Janitor tick interval."`                                                                                                                                                                                                  //nolint:lll // Kong struct tag makes this line long
	SkipKMSKeyValidation bool          `json:"skip_kms_key_validation" env:"GOPHERSTACK_S3_SKIP_KMS_KEY_VALIDATION,S3_SKIP_KMS_KEY_VALIDATION"                     help:"Accept SSE-KMS key ids that do not exist in KMS instead of failing with KMS.NotFoundException (LocalStack S3_SKIP_KMS_KEY_VALIDATION)."                                                   name:"skip-kms-key-validation"` //nolint:lll // config struct tags are intentionally verbose
	ValidateSignatures   bool          `json:"validate_signatures"     env:"GOPHERSTACK_S3_VALIDATE_SIGNATURES,S3_VALIDATE_SIGNATURES"                             help:"Cryptographically verify S3 request and presigned-URL signatures against --sigv4-secret, for S3 only (LocalStack S3_VALIDATE_SIGNATURES; S3_SKIP_SIGNATURE_VALIDATION=0 enables it too)." name:"validate-signatures"`     //nolint:lll // config struct tags are intentionally verbose
	DisableCustomCORS    bool          `json:"disable_custom_cors"     env:"GOPHERSTACK_S3_DISABLE_CUSTOM_CORS,DISABLE_CUSTOM_CORS_S3"                             help:"Ignore per-bucket CORS configuration so only the global CORS settings apply (LocalStack DISABLE_CUSTOM_CORS_S3)."                                                                         name:"disable-custom-cors"`     //nolint:lll // config struct tags are intentionally verbose
	CompressionMinBytes  int           `json:"compression_min_bytes"   env:"S3_COMPRESSION_MIN_BYTES"                                          default:"1024"      help:"Minimum object size in bytes for zstd compression. Set to 0 to compress all objects regardless of size."                                                                                  name:"compression-min-bytes"`   //nolint:lll // config struct tags are intentionally verbose
}
