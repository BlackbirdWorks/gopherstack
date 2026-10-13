package cleanrooms

import "slices"

const analysisTypeDirect = "DIRECT_ANALYSIS"

type directAnalysisDetails struct {
	ReceiverAccountIDs []string `json:"receiverAccountIds"`
}

type receiverConfigurationDetails struct {
	DirectAnalysis directAnalysisDetails `json:"directAnalysisConfigurationDetails"`
}

type receiverConfiguration struct {
	AnalysisType         string                       `json:"analysisType"`
	ConfigurationDetails receiverConfigurationDetails `json:"configurationDetails"`
}

// receiverConfigurations derives the accounts that receive results from a result
// configuration; S3 and intermediate-table output land in the running member's account.
func receiverConfigurations(resultConfig map[string]any, selfAccount string) []receiverConfiguration {
	var accounts []string
	output, _ := resultConfig["outputConfiguration"].(map[string]any)
	collectOutputReceivers(output, selfAccount, &accounts)
	if len(accounts) == 0 {
		accounts = []string{selfAccount}
	}

	return []receiverConfiguration{{
		AnalysisType: analysisTypeDirect,
		ConfigurationDetails: receiverConfigurationDetails{
			DirectAnalysis: directAnalysisDetails{ReceiverAccountIDs: accounts},
		},
	}}
}

func collectOutputReceivers(output map[string]any, selfAccount string, accounts *[]string) {
	addAccount := func(id string) {
		if id != "" && !slices.Contains(*accounts, id) {
			*accounts = append(*accounts, id)
		}
	}
	if member, ok := output["member"].(map[string]any); ok {
		id, _ := member["accountId"].(string)
		addAccount(id)
	}
	if _, ok := output["s3"]; ok {
		addAccount(selfAccount)
	}
	if _, ok := output["intermediateTable"]; ok {
		addAccount(selfAccount)
	}
	distribute, _ := output["distribute"].(map[string]any)
	locations, _ := distribute["locations"].([]any)
	for _, loc := range locations {
		if m, ok := loc.(map[string]any); ok {
			collectOutputReceivers(m, selfAccount, accounts)
		}
	}
}
