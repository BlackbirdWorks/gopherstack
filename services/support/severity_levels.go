package support

const (
	severityLow      = "low"
	severityNormal   = "normal"
	severityHigh     = "high"
	severityUrgent   = "urgent"
	severityCritical = "critical"
)

// DescribeSeverityLevels returns the available severity levels.
func (b *InMemoryBackend) DescribeSeverityLevels(language string) []SeverityLevel {
	if language == "ja" {
		return []SeverityLevel{
			{Code: severityLow, Name: japaneseGeneralGuidance},
			{Code: severityNormal, Name: "システム障害"},
			{Code: severityHigh, Name: "本番システム障害"},
			{Code: severityUrgent, Name: "本番システム停止"},
			{Code: severityCritical, Name: "ビジネスクリティカルシステム停止"},
		}
	}

	return []SeverityLevel{
		{Code: severityLow, Name: "General guidance"},
		{Code: severityNormal, Name: "System impaired"},
		{Code: severityHigh, Name: "Production system impaired"},
		{Code: severityUrgent, Name: "Production system down"},
		{Code: severityCritical, Name: "Business-critical system down"},
	}
}

func validSeverity(value string) bool {
	return value == severityLow || value == severityNormal || value == severityHigh ||
		value == severityUrgent || value == severityCritical
}
