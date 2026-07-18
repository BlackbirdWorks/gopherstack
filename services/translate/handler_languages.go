package translate

// --- Languages ---

func (h *Handler) listLanguages(input map[string]any) (map[string]any, error) {
	const defaultMaxLanguages = 500

	maxResults := maxResultsField(input)
	if maxResults <= 0 {
		maxResults = defaultMaxLanguages
	}

	nextTokenIn, _ := input["NextToken"].(string)
	displayLang, _ := input["DisplayLanguageCode"].(string)
	if displayLang == "" {
		displayLang = "en"
	}

	languages := knownLanguages()

	// Apply cursor-based pagination using LanguageCode as token.
	start := 0
	if nextTokenIn != "" {
		for i, lang := range languages {
			if code, _ := lang[keyLanguageCode].(string); code == nextTokenIn {
				start = i

				break
			}
		}
	}

	end := start + maxResults
	var nextTokenOut string

	if end < len(languages) {
		if code, _ := languages[end][keyLanguageCode].(string); code != "" {
			nextTokenOut = code
		}
	} else {
		end = len(languages)
	}

	page := languages[start:end]
	result := map[string]any{
		"Languages":           page,
		"DisplayLanguageCode": displayLang,
	}

	if nextTokenOut != "" {
		result["NextToken"] = nextTokenOut
	}

	return result, nil
}

func knownLanguages() []map[string]any {
	return []map[string]any{
		{keyLanguageCode: "af", keyLanguageName: "Afrikaans"},
		{keyLanguageCode: "sq", keyLanguageName: "Albanian"},
		{keyLanguageCode: "am", keyLanguageName: "Amharic"},
		{keyLanguageCode: "ar", keyLanguageName: "Arabic"},
		{keyLanguageCode: "hy", keyLanguageName: "Armenian"},
		{keyLanguageCode: "az", keyLanguageName: "Azerbaijani"},
		{keyLanguageCode: "bn", keyLanguageName: "Bengali"},
		{keyLanguageCode: "bs", keyLanguageName: "Bosnian"},
		{keyLanguageCode: "bg", keyLanguageName: "Bulgarian"},
		{keyLanguageCode: "ca", keyLanguageName: "Catalan"},
		{keyLanguageCode: "zh", keyLanguageName: "Chinese (Simplified)"},
		{keyLanguageCode: "zh-TW", keyLanguageName: "Chinese (Traditional)"},
		{keyLanguageCode: "hr", keyLanguageName: "Croatian"},
		{keyLanguageCode: "cs", keyLanguageName: "Czech"},
		{keyLanguageCode: "da", keyLanguageName: "Danish"},
		{keyLanguageCode: "fa-AF", keyLanguageName: "Dari"},
		{keyLanguageCode: "nl", keyLanguageName: "Dutch"},
		{keyLanguageCode: "en", keyLanguageName: "English"},
		{keyLanguageCode: "et", keyLanguageName: "Estonian"},
		{keyLanguageCode: "fa", keyLanguageName: "Farsi (Persian)"},
		{keyLanguageCode: "tl", keyLanguageName: "Filipino, Tagalog"},
		{keyLanguageCode: "fi", keyLanguageName: "Finnish"},
		{keyLanguageCode: "fr", keyLanguageName: "French"},
		{keyLanguageCode: "fr-CA", keyLanguageName: "French (Canada)"},
		{keyLanguageCode: "ka", keyLanguageName: "Georgian"},
		{keyLanguageCode: "de", keyLanguageName: "German"},
		{keyLanguageCode: "el", keyLanguageName: "Greek"},
		{keyLanguageCode: "gu", keyLanguageName: "Gujarati"},
		{keyLanguageCode: "ht", keyLanguageName: "Haitian Creole"},
		{keyLanguageCode: "ha", keyLanguageName: "Hausa"},
		{keyLanguageCode: "he", keyLanguageName: "Hebrew"},
		{keyLanguageCode: "hi", keyLanguageName: "Hindi"},
		{keyLanguageCode: "hu", keyLanguageName: "Hungarian"},
		{keyLanguageCode: "id", keyLanguageName: "Indonesian"},
		{keyLanguageCode: "ga", keyLanguageName: "Irish"},
		{keyLanguageCode: "it", keyLanguageName: "Italian"},
		{keyLanguageCode: "ja", keyLanguageName: "Japanese"},
		{keyLanguageCode: "kn", keyLanguageName: "Kannada"},
		{keyLanguageCode: "kk", keyLanguageName: "Kazakh"},
		{keyLanguageCode: "ko", keyLanguageName: "Korean"},
		{keyLanguageCode: "lv", keyLanguageName: "Latvian"},
		{keyLanguageCode: "lt", keyLanguageName: "Lithuanian"},
		{keyLanguageCode: "mk", keyLanguageName: "Macedonian"},
		{keyLanguageCode: "ms", keyLanguageName: "Malay"},
		{keyLanguageCode: "ml", keyLanguageName: "Malayalam"},
		{keyLanguageCode: "mt", keyLanguageName: "Maltese"},
		{keyLanguageCode: "mr", keyLanguageName: "Marathi"},
		{keyLanguageCode: "mn", keyLanguageName: "Mongolian"},
		{keyLanguageCode: "no", keyLanguageName: "Norwegian"},
		{keyLanguageCode: "ps", keyLanguageName: "Pashto"},
		{keyLanguageCode: "pl", keyLanguageName: "Polish"},
		{keyLanguageCode: "pt", keyLanguageName: "Portuguese (Brazil)"},
		{keyLanguageCode: "pt-PT", keyLanguageName: "Portuguese (Portugal)"},
		{keyLanguageCode: "pa", keyLanguageName: "Punjabi"},
		{keyLanguageCode: "ro", keyLanguageName: "Romanian"},
		{keyLanguageCode: "ru", keyLanguageName: "Russian"},
		{keyLanguageCode: "sr", keyLanguageName: "Serbian"},
		{keyLanguageCode: "si", keyLanguageName: "Sinhala"},
		{keyLanguageCode: "sk", keyLanguageName: "Slovak"},
		{keyLanguageCode: "sl", keyLanguageName: "Slovenian"},
		{keyLanguageCode: "so", keyLanguageName: "Somali"},
		{keyLanguageCode: "es", keyLanguageName: "Spanish"},
		{keyLanguageCode: "es-MX", keyLanguageName: "Spanish (Mexico)"},
		{keyLanguageCode: "sw", keyLanguageName: "Swahili"},
		{keyLanguageCode: "sv", keyLanguageName: "Swedish"},
		{keyLanguageCode: "ta", keyLanguageName: "Tamil"},
		{keyLanguageCode: "te", keyLanguageName: "Telugu"},
		{keyLanguageCode: "th", keyLanguageName: "Thai"},
		{keyLanguageCode: "tr", keyLanguageName: "Turkish"},
		{keyLanguageCode: "uk", keyLanguageName: "Ukrainian"},
		{keyLanguageCode: "ur", keyLanguageName: "Urdu"},
		{keyLanguageCode: "uz", keyLanguageName: "Uzbek"},
		{keyLanguageCode: "vi", keyLanguageName: "Vietnamese"},
		{keyLanguageCode: "cy", keyLanguageName: "Welsh"},
	}
}
