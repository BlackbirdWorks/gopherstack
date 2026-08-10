package polly

import (
	"fmt"
	"slices"
)

// DescribeVoices lists built-in voices matching filters.
func (b *InMemoryBackend) DescribeVoices(filter DescribeVoicesFilter) ([]Voice, error) {
	if filter.Engine != "" && !slices.Contains(validEngines(), filter.Engine) {
		return nil, fmt.Errorf("%w: invalid Engine %q", ErrValidation, filter.Engine)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]Voice, 0, len(b.voices))
	for _, voice := range b.voices {
		if !matchesVoice(voice, filter) {
			continue
		}

		copyVoice := voice
		copyVoice.AdditionalLanguageCodes = slices.Clone(voice.AdditionalLanguageCodes)
		copyVoice.SupportedEngines = slices.Clone(voice.SupportedEngines)
		if !filter.IncludeAdditionalLanguageCodes {
			copyVoice.AdditionalLanguageCodes = nil
		}
		out = append(out, copyVoice)
	}

	return out, nil
}

func matchesVoice(voice Voice, filter DescribeVoicesFilter) bool {
	if filter.Engine != "" && !slices.Contains(voice.SupportedEngines, filter.Engine) {
		return false
	}
	if filter.Gender != "" && voice.Gender != filter.Gender {
		return false
	}
	if filter.LanguageCode == "" || voice.LanguageCode == filter.LanguageCode {
		return true
	}

	return filter.IncludeAdditionalLanguageCodes && slices.Contains(voice.AdditionalLanguageCodes, filter.LanguageCode)
}

// Language name constants used in the built-in voice catalogue.
const (
	langArabic          = "Arabic"
	langArabicGulf      = "Arabic (Gulf)"
	langAustrEnglish    = "Australian English"
	langBelgDutch       = "Belgian Dutch"
	langBelgFrench      = "Belgian French"
	langBrazPortuguese  = "Brazilian Portuguese"
	langBritEnglish     = "British English"
	langCanadFrench     = "Canadian French"
	langCantonese       = "Chinese (Cantonese)"
	langCastilSpanish   = "Castilian Spanish"
	langChinaMandarin   = "Chinese Mandarin"
	langCzech           = "Czech"
	langDanish          = "Danish"
	langDutch           = "Dutch"
	langEuropPortuguese = "European Portuguese"
	langFrench          = "French"
	langGerman          = "German"
	langIndianEnglish   = "Indian English"
	langIrEnglish       = "Irish English"
	langItalian         = "Italian"
	langJapanese        = "Japanese"
	langKorean          = "Korean"
	langMexSpanish      = "Mexican Spanish"
	langNorwegian       = "Norwegian"
	langNZEnglish       = "New Zealand English"
	langPolish          = "Polish"
	langRussian         = "Russian"
	langSingEnglish     = "Singaporean English"
	langSwedish         = "Swedish"
	langSwissGerman     = "Swiss German"
	langTurkish         = "Turkish"
	langUSEnglish       = "US English"
	langUSSpanish       = "US Spanish"
	langWelshEnglish    = "Welsh English"
	langZAEnglish       = "South African English"
)

// Language code constants used in the built-in voice catalogue.
const (
	lcArabic          = "arb"
	lcArabicGulf      = "ar-AE"
	lcAustrEnglish    = "en-AU"
	lcBelgDutch       = "nl-BE"
	lcBelgFrench      = "fr-BE"
	lcBrazPortuguese  = "pt-BR"
	lcBritEnglish     = "en-GB"
	lcCanadFrench     = "fr-CA"
	lcCantonese       = "yue-CN"
	lcCastilSpanish   = "es-ES"
	lcCatalan         = "ca-ES"
	lcChinaMandarin   = "cmn-CN"
	lcCzech           = "cs-CZ"
	lcDanish          = "da-DK"
	lcDutch           = "nl-NL"
	lcEuropPortuguese = "pt-PT"
	lcFinnish         = "fi-FI"
	lcFrench          = "fr-FR"
	lcGerman          = "de-DE"
	lcAustrGerman     = "de-AT"
	lcHindi           = "hi-IN"
	lcIcelandic       = "is-IS"
	lcIndianEnglish   = "en-IN"
	lcIrEnglish       = "en-IE"
	lcItalian         = "it-IT"
	lcJapanese        = "ja-JP"
	lcKorean          = "ko-KR"
	lcMexSpanish      = "es-MX"
	lcNorwegian       = "nb-NO"
	lcNZEnglish       = "en-NZ"
	lcPolish          = "pl-PL"
	lcRomanian        = "ro-RO"
	lcRussian         = "ru-RU"
	lcSingEnglish     = "en-SG"
	lcSwedish         = "sv-SE"
	lcSwissGerman     = "de-CH"
	lcTurkish         = "tr-TR"
	lcUSSpanish       = "es-US"
	lcWelsh           = "cy-GB"
	lcWelshEnglish    = "en-GB-WLS"
	lcZAEnglish       = "en-ZA"
)

const builtInVoicesCap = 106

// builtInVoices returns the full built-in voice catalogue, field-diffed
// against the "Available voices" table at
// https://docs.aws.amazon.com/polly/latest/dg/voicelist.html and against the
// VoiceId enum in aws-sdk-go-v2/service/polly/types (pinned SDK version, see
// PARITY.md). Every VoiceId enum value present in the pinned SDK is
// represented here with its real Gender/LanguageCode/SupportedEngines.
//
// Three voices documented on that page (Patrick, Alba, Raúl) are NOT part of
// the VoiceId enum in the pinned aws-sdk-go-v2/service/polly@v1.60.4 module
// (a newer, unreleased-at-pin-time AWS addition) and are intentionally
// omitted -- adding them would let this backend accept a VoiceId no real
// client built against this SDK version could ever send or receive.
func builtInVoices() []Voice {
	voices := make([]Voice, 0, builtInVoicesCap)
	voices = append(voices, builtInVoicesEnglishUS()...)
	voices = append(voices, builtInVoicesEnglishGlobal()...)
	voices = append(voices, builtInVoicesGermanic()...)
	voices = append(voices, builtInVoicesFrenchPortuguese()...)
	voices = append(voices, builtInVoicesSpanishItalian()...)
	voices = append(voices, builtInVoicesOther()...)
	voices = append(voices, builtInVoicesArabic()...)

	return voices
}

func builtInVoicesEnglishUS() []Voice {
	std := defaultEngine
	neu := engineNeural
	lng := engineLongForm
	gen := engineGenerative

	return []Voice{
		{
			ID: "Danielle", Name: "Danielle", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu, lng, gen},
		},
		{
			ID: "Gregory", Name: "Gregory", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu, lng},
		},
		{
			ID: "Ivy", Name: "Ivy", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Joanna", Name: "Joanna", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Kendra", Name: "Kendra", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Kimberly", Name: "Kimberly", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Salli", Name: "Salli", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Ruth", Name: "Ruth", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu, lng, gen},
		},
		{
			ID: "Tiffany", Name: "Tiffany", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{gen},
		},
		{
			ID: "Joey", Name: "Joey", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Justin", Name: "Justin", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Kevin", Name: "Kevin", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Matthew", Name: "Matthew", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu, gen},
		},
		{
			ID: "Stephen", Name: "Stephen", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu, gen},
		},
	}
}

func builtInVoicesEnglishGlobal() []Voice {
	std := defaultEngine
	neu := engineNeural
	gen := engineGenerative

	return []Voice{
		// English (AU)
		{
			ID: "Nicole", Name: "Nicole", Gender: genderFemale,
			LanguageCode: lcAustrEnglish, LanguageName: langAustrEnglish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Olivia", Name: "Olivia", Gender: genderFemale,
			LanguageCode: lcAustrEnglish, LanguageName: langAustrEnglish,
			SupportedEngines: []string{neu, gen},
		},
		{
			ID: "Russell", Name: "Russell", Gender: genderMale,
			LanguageCode: lcAustrEnglish, LanguageName: langAustrEnglish,
			SupportedEngines: []string{std},
		},
		// English (GB)
		{
			ID: "Amy", Name: "Amy", Gender: genderFemale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Emma", Name: "Emma", Gender: genderFemale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Brian", Name: "Brian", Gender: genderMale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Arthur", Name: "Arthur", Gender: genderMale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{neu},
		},
		// English (Welsh) -- en-GB-WLS, distinct from the Welsh (cy-GB) language below.
		{
			ID: "Geraint", Name: "Geraint", Gender: genderMale,
			LanguageCode: lcWelshEnglish, LanguageName: langWelshEnglish,
			SupportedEngines: []string{std},
		},
		// English (IN)
		{
			ID: "Aditi", Name: "Aditi", Gender: genderFemale,
			LanguageCode: lcIndianEnglish, LanguageName: langIndianEnglish,
			AdditionalLanguageCodes: []string{lcHindi},
			SupportedEngines:        []string{std},
		},
		{
			ID: "Kajal", Name: "Kajal", Gender: genderFemale,
			LanguageCode: lcIndianEnglish, LanguageName: langIndianEnglish,
			AdditionalLanguageCodes: []string{lcHindi},
			SupportedEngines:        []string{neu, gen},
		},
		{
			ID: "Raveena", Name: "Raveena", Gender: genderFemale,
			LanguageCode: lcIndianEnglish, LanguageName: langIndianEnglish,
			SupportedEngines: []string{std},
		},
		// English (IE)
		{
			ID: "Niamh", Name: "Niamh", Gender: genderFemale,
			LanguageCode: lcIrEnglish, LanguageName: langIrEnglish,
			SupportedEngines: []string{neu, gen},
		},
		// English (NZ)
		{
			ID: "Aria", Name: "Aria", Gender: genderFemale,
			LanguageCode: lcNZEnglish, LanguageName: langNZEnglish,
			SupportedEngines: []string{neu, gen},
		},
		// English (SG)
		{
			ID: "Jasmine", Name: "Jasmine", Gender: genderFemale,
			LanguageCode: lcSingEnglish, LanguageName: langSingEnglish,
			SupportedEngines: []string{neu, gen},
		},
		// English (ZA)
		{
			ID: "Ayanda", Name: "Ayanda", Gender: genderFemale,
			LanguageCode: lcZAEnglish, LanguageName: langZAEnglish,
			SupportedEngines: []string{neu, gen},
		},
		// Welsh
		{
			ID: "Gwyneth", Name: "Gwyneth", Gender: genderFemale,
			LanguageCode: lcWelsh, LanguageName: "Welsh",
			SupportedEngines: []string{std},
		},
	}
}

// builtInVoicesGermanic covers the Nordic languages (Danish, Icelandic,
// Norwegian, Swedish, Finnish) plus Dutch and German, split across two
// sub-functions to keep each under the funlen limit.
func builtInVoicesGermanic() []Voice {
	voices := builtInVoicesNordic()

	return append(voices, builtInVoicesDutchGerman()...)
}

func builtInVoicesNordic() []Voice {
	std := defaultEngine
	neu := engineNeural

	return []Voice{
		// Danish
		{
			ID: "Naja", Name: "Naja", Gender: genderFemale,
			LanguageCode: lcDanish, LanguageName: langDanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Mads", Name: "Mads", Gender: genderMale,
			LanguageCode: lcDanish, LanguageName: langDanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Sofie", Name: "Sofie", Gender: genderFemale,
			LanguageCode: lcDanish, LanguageName: langDanish,
			SupportedEngines: []string{neu},
		},
		// Finnish
		{
			ID: "Suvi", Name: "Suvi", Gender: genderFemale,
			LanguageCode: lcFinnish, LanguageName: "Finnish",
			SupportedEngines: []string{neu},
		},
		// Icelandic
		{
			ID: "Dora", Name: "Dóra", Gender: genderFemale,
			LanguageCode: lcIcelandic, LanguageName: "Icelandic",
			SupportedEngines: []string{std},
		},
		{
			ID: "Karl", Name: "Karl", Gender: genderMale,
			LanguageCode: lcIcelandic, LanguageName: "Icelandic",
			SupportedEngines: []string{std},
		},
		// Norwegian
		{
			ID: "Liv", Name: "Liv", Gender: genderFemale,
			LanguageCode: lcNorwegian, LanguageName: langNorwegian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Ida", Name: "Ida", Gender: genderFemale,
			LanguageCode: lcNorwegian, LanguageName: langNorwegian,
			SupportedEngines: []string{neu},
		},
		// Swedish
		{
			ID: "Astrid", Name: "Astrid", Gender: genderFemale,
			LanguageCode: lcSwedish, LanguageName: langSwedish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Elin", Name: "Elin", Gender: genderFemale,
			LanguageCode: lcSwedish, LanguageName: langSwedish,
			SupportedEngines: []string{neu},
		},
	}
}

func builtInVoicesDutchGerman() []Voice {
	std := defaultEngine
	neu := engineNeural
	gen := engineGenerative

	return []Voice{
		// Dutch (NL)
		{
			ID: "Laura", Name: "Laura", Gender: genderFemale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{neu, gen},
		},
		{
			ID: "Lotte", Name: "Lotte", Gender: genderFemale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{std},
		},
		{
			ID: "Ruben", Name: "Ruben", Gender: genderMale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{std},
		},
		// Dutch (BE)
		{
			ID: "Lisa", Name: "Lisa", Gender: genderFemale,
			LanguageCode: lcBelgDutch, LanguageName: langBelgDutch,
			SupportedEngines: []string{neu, gen},
		},
		// German (AT)
		{
			ID: "Hannah", Name: "Hannah", Gender: genderFemale,
			LanguageCode: lcAustrGerman, LanguageName: "Austrian German",
			SupportedEngines: []string{neu, gen},
		},
		// German (CH)
		{
			ID: "Sabrina", Name: "Sabrina", Gender: genderFemale,
			LanguageCode: lcSwissGerman, LanguageName: langSwissGerman,
			SupportedEngines: []string{neu, gen},
		},
		// German (DE)
		{
			ID: "Marlene", Name: "Marlene", Gender: genderFemale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{std},
		},
		{
			ID: "Vicki", Name: "Vicki", Gender: genderFemale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Hans", Name: "Hans", Gender: genderMale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{std},
		},
		{
			ID: "Daniel", Name: "Daniel", Gender: genderMale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{neu, gen},
		},
		{
			ID: "Lennart", Name: "Lennart", Gender: genderMale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{gen},
		},
	}
}

func builtInVoicesFrenchPortuguese() []Voice {
	std := defaultEngine
	neu := engineNeural
	gen := engineGenerative

	return []Voice{
		// French (BE)
		{
			ID: "Isabelle", Name: "Isabelle", Gender: genderFemale,
			LanguageCode: lcBelgFrench, LanguageName: langBelgFrench,
			SupportedEngines: []string{neu, gen},
		},
		// French (CA)
		{
			ID: "Chantal", Name: "Chantal", Gender: genderFemale,
			LanguageCode: lcCanadFrench, LanguageName: langCanadFrench,
			SupportedEngines: []string{std},
		},
		{
			ID: "Gabrielle", Name: "Gabrielle", Gender: genderFemale,
			LanguageCode: lcCanadFrench, LanguageName: langCanadFrench,
			SupportedEngines: []string{neu, gen},
		},
		{
			ID: "Liam", Name: "Liam", Gender: genderMale,
			LanguageCode: lcCanadFrench, LanguageName: langCanadFrench,
			SupportedEngines: []string{neu, gen},
		},
		// French (FR)
		{
			ID: "Ambre", Name: "Ambre", Gender: genderFemale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{gen},
		},
		{
			ID: "Celine", Name: "Céline", Gender: genderFemale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{std},
		},
		{
			ID: "Florian", Name: "Florian", Gender: genderMale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{gen},
		},
		{
			ID: "Lea", Name: "Léa", Gender: genderFemale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Mathieu", Name: "Mathieu", Gender: genderMale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{std},
		},
		{
			ID: "Remi", Name: "Rémi", Gender: genderMale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{neu, gen},
		},
		// Portuguese (BR)
		{
			ID: "Camila", Name: "Camila", Gender: genderFemale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Vitoria", Name: "Vitória", Gender: genderFemale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Ricardo", Name: "Ricardo", Gender: genderMale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{std},
		},
		{
			ID: "Thiago", Name: "Thiago", Gender: genderMale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{neu},
		},
		// Portuguese (PT)
		{
			ID: "Ines", Name: "Inês", Gender: genderFemale,
			LanguageCode: lcEuropPortuguese, LanguageName: langEuropPortuguese,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Cristiano", Name: "Cristiano", Gender: genderMale,
			LanguageCode: lcEuropPortuguese, LanguageName: langEuropPortuguese,
			SupportedEngines: []string{std},
		},
		// Romanian
		{
			ID: "Carmen", Name: "Carmen", Gender: genderFemale,
			LanguageCode: lcRomanian, LanguageName: "Romanian",
			SupportedEngines: []string{std},
		},
	}
}

func builtInVoicesSpanishItalian() []Voice {
	std := defaultEngine
	neu := engineNeural
	gen := engineGenerative

	return []Voice{
		// Italian
		{
			ID: "Beatrice", Name: "Beatrice", Gender: genderFemale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{gen},
		},
		{
			ID: "Carla", Name: "Carla", Gender: genderFemale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Bianca", Name: "Bianca", Gender: genderFemale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Lorenzo", Name: "Lorenzo", Gender: genderMale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{gen},
		},
		{
			ID: "Giorgio", Name: "Giorgio", Gender: genderMale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Adriano", Name: "Adriano", Gender: genderMale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{neu},
		},
		// Spanish (ES)
		{
			ID: "Conchita", Name: "Conchita", Gender: genderFemale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Lucia", Name: "Lucia", Gender: genderFemale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Enrique", Name: "Enrique", Gender: genderMale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Sergio", Name: "Sergio", Gender: genderMale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{neu, gen},
		},
		// Spanish (MX)
		{
			ID: "Mia", Name: "Mia", Gender: genderFemale,
			LanguageCode: lcMexSpanish, LanguageName: langMexSpanish,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Andres", Name: "Andrés", Gender: genderMale,
			LanguageCode: lcMexSpanish, LanguageName: langMexSpanish,
			SupportedEngines: []string{neu, gen},
		},
		// Spanish (US)
		{
			ID: "Lupe", Name: "Lupe", Gender: genderFemale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Penelope", Name: "Penélope", Gender: genderFemale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Miguel", Name: "Miguel", Gender: genderMale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Pedro", Name: "Pedro", Gender: genderMale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{neu, gen},
		},
	}
}

// builtInVoicesOther covers the remaining language families (East Asian and
// Eastern European) not grouped elsewhere, split across two sub-functions to
// keep each under the funlen limit.
func builtInVoicesOther() []Voice {
	voices := builtInVoicesEastAsian()

	return append(voices, builtInVoicesEasternEuropean()...)
}

func builtInVoicesEastAsian() []Voice {
	std := defaultEngine
	neu := engineNeural
	gen := engineGenerative

	return []Voice{
		// Catalan
		{
			ID: "Arlet", Name: "Arlet", Gender: genderFemale,
			LanguageCode: lcCatalan, LanguageName: "Catalan",
			SupportedEngines: []string{neu},
		},
		// Chinese (Cantonese)
		{
			ID: "Hiujin", Name: "Hiujin", Gender: genderFemale,
			LanguageCode: lcCantonese, LanguageName: langCantonese,
			SupportedEngines: []string{neu},
		},
		// Chinese Mandarin
		{
			ID: "Zhiyu", Name: "Zhiyu", Gender: genderFemale,
			LanguageCode: lcChinaMandarin, LanguageName: langChinaMandarin,
			SupportedEngines: []string{std, neu},
		},
		// Japanese
		{
			ID: "Mizuki", Name: "Mizuki", Gender: genderFemale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{std},
		},
		{
			ID: "Kazuha", Name: "Kazuha", Gender: genderFemale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Tomoko", Name: "Tomoko", Gender: genderFemale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Takumi", Name: "Takumi", Gender: genderMale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{std, neu},
		},
		// Korean
		{
			ID: "Seoyeon", Name: "Seoyeon", Gender: genderFemale,
			LanguageCode: lcKorean, LanguageName: langKorean,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Jihye", Name: "Jihye", Gender: genderFemale,
			LanguageCode: lcKorean, LanguageName: langKorean,
			SupportedEngines: []string{neu},
		},
	}
}

func builtInVoicesEasternEuropean() []Voice {
	std := defaultEngine
	neu := engineNeural
	gen := engineGenerative

	return []Voice{
		// Czech
		{
			ID: "Jitka", Name: "Jitka", Gender: genderFemale,
			LanguageCode: lcCzech, LanguageName: langCzech,
			SupportedEngines: []string{neu},
		},
		// Polish
		{
			ID: "Ewa", Name: "Ewa", Gender: genderFemale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Maja", Name: "Maja", Gender: genderFemale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Ola", Name: "Ola", Gender: genderFemale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{neu, gen},
		},
		{
			ID: "Jacek", Name: "Jacek", Gender: genderMale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Jan", Name: "Jan", Gender: genderMale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		// Russian
		{
			ID: "Tatyana", Name: "Tatyana", Gender: genderFemale,
			LanguageCode: lcRussian, LanguageName: langRussian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Maxim", Name: "Maxim", Gender: genderMale,
			LanguageCode: lcRussian, LanguageName: langRussian,
			SupportedEngines: []string{std},
		},
		// Turkish
		{
			ID: "Filiz", Name: "Filiz", Gender: genderFemale,
			LanguageCode: lcTurkish, LanguageName: langTurkish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Burcu", Name: "Burcu", Gender: genderFemale,
			LanguageCode: lcTurkish, LanguageName: langTurkish,
			SupportedEngines: []string{neu},
		},
	}
}

// builtInVoicesArabic covers Modern Standard Arabic (Zeina, standard-only)
// and the two fully bilingual Gulf Arabic voices (Hala, Zayd), which speak
// both ar-AE and arb per
// https://docs.aws.amazon.com/polly/latest/dg/bilingual-voices.html.
func builtInVoicesArabic() []Voice {
	neu := engineNeural

	return []Voice{
		{
			ID: "Zeina", Name: "Zeina", Gender: genderFemale,
			LanguageCode: lcArabic, LanguageName: langArabic,
			SupportedEngines: []string{defaultEngine},
		},
		{
			ID: "Hala", Name: "Hala", Gender: genderFemale,
			LanguageCode: lcArabicGulf, LanguageName: langArabicGulf,
			AdditionalLanguageCodes: []string{lcArabic},
			SupportedEngines:        []string{neu},
		},
		{
			ID: "Zayd", Name: "Zayd", Gender: genderMale,
			LanguageCode: lcArabicGulf, LanguageName: langArabicGulf,
			AdditionalLanguageCodes: []string{lcArabic},
			SupportedEngines:        []string{neu},
		},
	}
}
