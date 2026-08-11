package transcribe

import (
	"fmt"
	"regexp"
	"slices"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
)

// jobNameRe validates TranscriptionJobName per AWS: alphanumeric, dots, underscores, hyphens, 1-200 chars.
var jobNameRe = regexp.MustCompile(`^[0-9a-zA-Z._-]{1,200}$`)

// mediaSampleRateHertzMin is the minimum sample rate accepted by Transcribe.
const mediaSampleRateHertzMin = 8000

// mediaSampleRateHertzMax is the maximum sample rate accepted by Transcribe.
const mediaSampleRateHertzMax = 48000

// maxLanguageOptions is the maximum number of language options for language identification.
const maxLanguageOptions = 10

// minLanguageOptions is the minimum number of language options for language identification.
const minLanguageOptions = 2

// maxLanguageIDSettings is the documented map-size limit for LanguageIdSettings
// ("Map Entries: Maximum number of 5 items.").
const maxLanguageIDSettings = 5

// supportedLanguageCodes derives the set of language codes Amazon Transcribe accepts
// from the pinned SDK's types.LanguageCode enum, so it can't drift from a hand-copied list again.
func supportedLanguageCodes() []string {
	values := sdktypes.LanguageCode("").Values()
	codes := make([]string, len(values))

	for i, v := range values {
		codes[i] = string(v)
	}

	return codes
}

// supportedMediaFormats returns the set of media formats accepted by Amazon Transcribe.
func supportedMediaFormats() []string {
	return []string{"mp3", "mp4", "wav", "flac", "ogg", "amr", "webm", "m4a"}
}

// validateJobName checks that a job name matches the AWS-allowed pattern.
func validateJobName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: TranscriptionJobName is required", ErrValidation)
	}

	if !jobNameRe.MatchString(name) {
		return fmt.Errorf("%w: TranscriptionJobName must match ^[0-9a-zA-Z._-]{1,200}$", ErrValidation)
	}

	return nil
}

// validateLanguageCode checks that a language code is in the supported set.
// Empty is allowed when identify flags are set (caller must check).
func validateLanguageCode(code string) error {
	if code == "" {
		return nil
	}

	if !slices.Contains(supportedLanguageCodes(), code) {
		return fmt.Errorf("%w: unsupported LanguageCode %q", ErrValidation, code)
	}

	return nil
}

// validateMediaFormat checks that a media format is in the supported set.
func validateMediaFormat(format string) error {
	if format == "" {
		return nil
	}

	if !slices.Contains(supportedMediaFormats(), format) {
		return fmt.Errorf(
			"%w: unsupported MediaFormat %q; must be one of %v",
			ErrValidation,
			format,
			supportedMediaFormats(),
		)
	}

	return nil
}

// validateMediaSampleRateHertz checks that the sample rate is in [8000, 48000].
func validateMediaSampleRateHertz(rate int32) error {
	if rate == 0 {
		return nil
	}

	if rate < mediaSampleRateHertzMin || rate > mediaSampleRateHertzMax {
		return fmt.Errorf("%w: MediaSampleRateHertz must be between %d and %d",
			ErrValidation, mediaSampleRateHertzMin, mediaSampleRateHertzMax)
	}

	return nil
}

// validateLanguageOptions checks that LanguageOptions is within allowed range.
func validateLanguageOptions(opts []string) error {
	if len(opts) == 0 {
		return nil
	}

	if len(opts) < minLanguageOptions || len(opts) > maxLanguageOptions {
		return fmt.Errorf("%w: LanguageOptions must contain %d-%d codes, got %d",
			ErrValidation, minLanguageOptions, maxLanguageOptions, len(opts))
	}

	for _, code := range opts {
		if !slices.Contains(supportedLanguageCodes(), code) {
			return fmt.Errorf("%w: unsupported LanguageCode %q in LanguageOptions", ErrValidation, code)
		}
	}

	return nil
}

// validateLanguageIDSettings checks LanguageIdSettings against the documented rules:
// a map of at most 5 entries, each keyed by a supported language code, and (per
// StartTranscriptionJob docs) "multi-language identification (IdentifyMultipleLanguages)
// doesn't support custom language models" -- so a LanguageModelName sub-parameter is
// rejected when IdentifyMultipleLanguages is set. Note that AWS only *recommends*
// (does not require) pairing LanguageIdSettings with LanguageOptions, so that pairing
// is intentionally not enforced here.
func validateLanguageIDSettings(settings map[string]LanguageIDSettings, identifyMultipleLanguages bool) error {
	if len(settings) == 0 {
		return nil
	}

	if len(settings) > maxLanguageIDSettings {
		return fmt.Errorf("%w: LanguageIdSettings must contain at most %d entries, got %d",
			ErrValidation, maxLanguageIDSettings, len(settings))
	}

	codes := make([]string, 0, len(settings))
	for code := range settings {
		codes = append(codes, code)
	}

	slices.Sort(codes)

	for _, code := range codes {
		if !slices.Contains(supportedLanguageCodes(), code) {
			return fmt.Errorf("%w: unsupported LanguageCode %q in LanguageIdSettings", ErrValidation, code)
		}

		if identifyMultipleLanguages && settings[code].LanguageModelName != "" {
			return fmt.Errorf(
				"%w: LanguageIdSettings[%q].LanguageModelName is not supported with IdentifyMultipleLanguages",
				ErrValidation, code,
			)
		}
	}

	return nil
}
