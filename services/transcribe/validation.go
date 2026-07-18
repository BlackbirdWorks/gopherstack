package transcribe

import (
	"fmt"
	"regexp"
	"slices"
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

// supportedLanguageCodes returns the set of language codes supported by Amazon Transcribe.
func supportedLanguageCodes() []string {
	return []string{
		"af-ZA", "ar-AE", "ar-SA", "da-DK", "de-CH", "de-DE", "en-AB", "en-AU", "en-GB",
		"en-IE", "en-IN", "en-NZ", "en-US", "en-WL", "en-ZA", "es-ES", "es-US", "eu-ES",
		"fa-IR", "fi-FI", "fr-CA", "fr-FR", "he-IL", "hi-IN", "id-ID", "it-IT", "ja-JP",
		"ko-KR", "ms-MY", "nl-NL", "pt-BR", "pt-PT", "ru-RU", "sv-SE", "ta-IN", "te-IN",
		"th-TH", "tr-TR", "uk-UA", "vi-VN", "zh-CN", "zh-TW",
	}
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
