package amplify

import "encoding/json"

// sentKeys reports which top-level JSON members a request body carried, so an
// update can tell an explicit empty value or false from an omitted member.
func sentKeys(body []byte) map[string]bool {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}

	out := make(map[string]bool, len(raw))

	for k, v := range raw {
		if string(v) != "null" {
			out[k] = true
		}
	}

	return out
}

func sentStr(sent map[string]bool, key, v string) *string {
	if sent[key] {
		return &v
	}

	return nil
}

func sentBool(sent map[string]bool, key string, v bool) *bool {
	if sent[key] {
		return &v
	}

	return nil
}

// toAppUpdateOptions builds UpdateApp's options: only members the body carried are applied.
func (r createAppRequest) toAppUpdateOptions(sent map[string]bool) AppOptions {
	opts := AppOptions{
		EnvironmentVariables:       r.EnvironmentVariables,
		AutoBranchCreationConfig:   r.AutoBranchCreationConfig,
		CacheConfig:                r.CacheConfig,
		AutoBranchCreationPatterns: r.AutoBranchCreationPatterns,
		CustomRules:                r.CustomRules,
		Description:                sentStr(sent, "description", r.Description),
		Repository:                 sentStr(sent, "repository", r.Repository),
		EnableBranchAutoBuild:      r.EnableBranchAutoBuild,
		BasicAuthCredentials:       sentStr(sent, "basicAuthCredentials", r.BasicAuthCredentials),
		BuildSpec:                  sentStr(sent, "buildSpec", r.BuildSpec),
		CustomHeaders:              sentStr(sent, "customHeaders", r.CustomHeaders),
		IAMServiceRoleArn:          sentStr(sent, "iamServiceRoleArn", r.IAMServiceRoleArn),
		ComputeRoleARN:             sentStr(sent, "computeRoleArn", r.ComputeRoleArn),
		EnableBasicAuth:            sentBool(sent, "enableBasicAuth", r.EnableBasicAuth),
		EnableAutoBranchCreation:   sentBool(sent, "enableAutoBranchCreation", r.EnableAutoBranchCreation),
		EnableBranchAutoDeletion:   sentBool(sent, "enableBranchAutoDeletion", r.EnableBranchAutoDeletion),
	}

	if r.JobConfig != nil {
		opts.JobConfigBuildComputeType = &r.JobConfig.BuildComputeType
	}

	return opts
}

// toBranchUpdateOptions builds UpdateBranch's options: only members the body carried are applied.
func (r createBranchRequest) toBranchUpdateOptions(sent map[string]bool) BranchOptions {
	opts := BranchOptions{
		EnvironmentVariables:       r.EnvironmentVariables,
		Description:                sentStr(sent, "description", r.Description),
		DisplayName:                sentStr(sent, "displayName", r.DisplayName),
		Framework:                  sentStr(sent, "framework", r.Framework),
		TTL:                        sentStr(sent, "ttl", r.TTL),
		BasicAuthCredentials:       sentStr(sent, "basicAuthCredentials", r.BasicAuthCredentials),
		BuildSpec:                  sentStr(sent, "buildSpec", r.BuildSpec),
		BackendEnvironmentARN:      sentStr(sent, "backendEnvironmentArn", r.BackendEnvironmentARN),
		PullRequestEnvironmentName: sentStr(sent, "pullRequestEnvironmentName", r.PullRequestEnvironmentName),
		SourceBranch:               sentStr(sent, "sourceBranch", r.SourceBranch),
		ComputeRoleARN:             sentStr(sent, "computeRoleArn", r.ComputeRoleARN),
		EnableBasicAuth:            sentBool(sent, "enableBasicAuth", r.EnableBasicAuth),
		EnableNotification:         sentBool(sent, "enableNotification", r.EnableNotification),
		EnablePullRequestPreview:   sentBool(sent, "enablePullRequestPreview", r.EnablePullRequestPreview),
		EnablePerformanceMode:      sentBool(sent, "enablePerformanceMode", r.EnablePerformanceMode),
		EnableSkewProtection:       sentBool(sent, "enableSkewProtection", r.EnableSkewProtection),
	}

	if r.Backend != nil {
		stack := r.Backend.StackARN
		opts.BackendStackARN = &stack
	}

	return opts
}
