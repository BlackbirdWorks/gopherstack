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
	langAustrEnglish    = "Australian English"
	langBelgDutch       = "Belgian Dutch"
	langBelgFrench      = "Belgian French"
	langBrazPortuguese  = "Brazilian Portuguese"
	langBritEnglish     = "British English"
	langCanadFrench     = "Canadian French"
	langCastilSpanish   = "Castilian Spanish"
	langChinaMandarin   = "Chinese Mandarin"
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
	langSwedish         = "Swedish"
	langTurkish         = "Turkish"
	langUSEnglish       = "US English"
	langUSSpanish       = "US Spanish"
	langZAEnglish       = "South African English"
)

// Language code constants used in the built-in voice catalogue.
const (
	lcAustrEnglish    = "en-AU"
	lcBelgDutch       = "nl-BE"
	lcBelgFrench      = "fr-BE"
	lcBrazPortuguese  = "pt-BR"
	lcBritEnglish     = "en-GB"
	lcCanadFrench     = "fr-CA"
	lcCastilSpanish   = "es-ES"
	lcCatalan         = "ca-ES"
	lcChinaMandarin   = "cmn-CN"
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
	lcSwedish         = "sv-SE"
	lcTurkish         = "tr-TR"
	lcUSSpanish       = "es-US"
	lcWelsh           = "cy-GB"
	lcZAEnglish       = "en-ZA"
)

const builtInVoicesCap = 90

func builtInVoices() []Voice {
	voices := make([]Voice, 0, builtInVoicesCap)
	voices = append(voices, builtInVoicesEnglishUS()...)
	voices = append(voices, builtInVoicesEnglishGlobal()...)
	voices = append(voices, builtInVoicesGermanic()...)
	voices = append(voices, builtInVoicesFrenchPortuguese()...)
	voices = append(voices, builtInVoicesSpanishItalian()...)
	voices = append(voices, builtInVoicesOther()...)

	return voices
}

func builtInVoicesEnglishUS() []Voice {
	std := defaultEngine
	neu := engineNeural
	lng := engineLongForm
	gen := engineGenerative

	return []Voice{
		{
			ID: "Ivy", Name: "Ivy", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Joanna", Name: "Joanna", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu, lng, gen},
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
			SupportedEngines: []string{lng, gen},
		},
		{
			ID: "Joey", Name: "Joey", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Justin", Name: "Justin", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Kevin", Name: "Kevin", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Matthew", Name: "Matthew", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu, lng, gen},
		},
		{
			ID: "Stephen", Name: "Stephen", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{lng, gen},
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
			SupportedEngines: []string{neu},
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
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Arthur", Name: "Arthur", Gender: genderMale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{neu},
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
			SupportedEngines:        []string{neu},
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
			SupportedEngines: []string{neu},
		},
		// English (NZ)
		{
			ID: "Aria", Name: "Aria", Gender: genderFemale,
			LanguageCode: lcNZEnglish, LanguageName: langNZEnglish,
			SupportedEngines: []string{neu},
		},
		// English (ZA)
		{
			ID: "Ayanda", Name: "Ayanda", Gender: genderFemale,
			LanguageCode: lcZAEnglish, LanguageName: langZAEnglish,
			SupportedEngines: []string{neu},
		},
		// Welsh
		{
			ID: "Gwyneth", Name: "Gwyneth", Gender: genderFemale,
			LanguageCode: lcWelsh, LanguageName: "Welsh",
			SupportedEngines: []string{std},
		},
	}
}

func builtInVoicesGermanic() []Voice {
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
		// Dutch (NL)
		{
			ID: "Lotte", Name: "Lotte", Gender: genderFemale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Ruben", Name: "Ruben", Gender: genderMale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{std},
		},
		{
			ID: "Laura", Name: "Laura", Gender: genderFemale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{neu},
		},
		// Dutch (BE)
		{
			ID: "Lisa", Name: "Lisa", Gender: genderFemale,
			LanguageCode: lcBelgDutch, LanguageName: langBelgDutch,
			SupportedEngines: []string{neu},
		},
		// Finnish
		{
			ID: "Suvi", Name: "Suvi", Gender: genderFemale,
			LanguageCode: lcFinnish, LanguageName: "Finnish",
			SupportedEngines: []string{neu},
		},
		// German (AT)
		{
			ID: "Hannah", Name: "Hannah", Gender: genderFemale,
			LanguageCode: lcAustrGerman, LanguageName: "Austrian German",
			SupportedEngines: []string{neu},
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
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Hans", Name: "Hans", Gender: genderMale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{std},
		},
		{
			ID: "Daniel", Name: "Daniel", Gender: genderMale,
			LanguageCode: lcGerman, LanguageName: langGerman,
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

func builtInVoicesFrenchPortuguese() []Voice {
	std := defaultEngine
	neu := engineNeural

	return []Voice{
		// French (BE)
		{
			ID: "Isabelle", Name: "Isabelle", Gender: genderFemale,
			LanguageCode: lcBelgFrench, LanguageName: langBelgFrench,
			SupportedEngines: []string{neu},
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
			SupportedEngines: []string{neu},
		},
		{
			ID: "Liam", Name: "Liam", Gender: genderMale,
			LanguageCode: lcCanadFrench, LanguageName: langCanadFrench,
			SupportedEngines: []string{neu},
		},
		// French (FR)
		{
			ID: "Celine", Name: "Céline", Gender: genderFemale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{std},
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
			SupportedEngines: []string{neu},
		},
		// Portuguese (BR)
		{
			ID: "Camila", Name: "Camila", Gender: genderFemale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{std, neu},
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

	return []Voice{
		// Italian
		{
			ID: "Carla", Name: "Carla", Gender: genderFemale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Bianca", Name: "Bianca", Gender: genderFemale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std, neu},
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
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Enrique", Name: "Enrique", Gender: genderMale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Sergio", Name: "Sergio", Gender: genderMale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{neu},
		},
		// Spanish (MX)
		{
			ID: "Mia", Name: "Mia", Gender: genderFemale,
			LanguageCode: lcMexSpanish, LanguageName: langMexSpanish,
			SupportedEngines: []string{std, neu},
		},
		// Spanish (US)
		{
			ID: "Lupe", Name: "Lupe", Gender: genderFemale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{std, neu},
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
			SupportedEngines: []string{neu},
		},
	}
}

func builtInVoicesOther() []Voice {
	std := defaultEngine
	neu := engineNeural

	return []Voice{
		// Catalan
		{
			ID: "Arlet", Name: "Arlet", Gender: genderFemale,
			LanguageCode: lcCatalan, LanguageName: "Catalan",
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
			ID: "Takumi", Name: "Takumi", Gender: genderMale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{std, neu},
		},
		// Korean
		{
			ID: "Seoyeon", Name: "Seoyeon", Gender: genderFemale,
			LanguageCode: lcKorean, LanguageName: langKorean,
			SupportedEngines: []string{std, neu},
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
			SupportedEngines: []string{neu},
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
