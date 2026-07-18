package support

// DescribeSupportedLanguages returns languages supported for the given parameters.
// The account-and-billing issue type is handled exclusively in English.
func (b *InMemoryBackend) DescribeSupportedLanguages(issueType, _, _ string) []SupportedLanguage {
	all := []SupportedLanguage{
		{Code: "en", Display: "ENGLISH", Language: "English"},
		{Code: "zh", Display: "CHINESE", Language: "Chinese"},
		{Code: "ja", Display: "JAPANESE", Language: "Japanese"},
		{Code: "ko", Display: "KOREAN", Language: "Korean"},
	}

	if issueType == "account-and-billing" {
		return all[:1]
	}

	return all
}

func validLanguage(value string) bool {
	return value == "en" || value == "ja" || value == "zh" || value == "ko"
}
