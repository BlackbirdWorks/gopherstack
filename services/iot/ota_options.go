package iot

import (
	"encoding/json"
	"fmt"
	"slices"
)

const (
	otaTargetSnapshot   = "SNAPSHOT"
	otaTargetContinuous = "CONTINUOUS"
)

func firstNonEmptyString(v, dflt string) string {
	if v == "" {
		return dflt
	}

	return v
}

func validateOTAUpdateOptions(opts OTAUpdateOptions) error {
	if opts.TargetSelection != "" && opts.TargetSelection != otaTargetSnapshot &&
		opts.TargetSelection != otaTargetContinuous {
		return fmt.Errorf("%w: targetSelection must be CONTINUOUS or SNAPSHOT", ErrValidation)
	}

	for _, p := range opts.Protocols {
		if !slices.Contains([]string{"MQTT", "HTTP"}, p) {
			return fmt.Errorf("%w: protocol %q must be MQTT or HTTP", ErrValidation, p)
		}
	}

	return nil
}

// otaJobInput maps the OTA update's AWS job settings onto the job it creates.
func otaJobInput(opts OTAUpdateOptions, roleARN string) *CreateJobInput {
	in := &CreateJobInput{}

	if opts.AWSJobAbortConfig != nil {
		in.AbortConfig = &AbortConfig{}
		remapJSON(map[string]any{"criteriaList": opts.AWSJobAbortConfig["abortCriteriaList"]}, in.AbortConfig)
	}

	if opts.AWSJobTimeoutConfig != nil {
		in.TimeoutConfig = &TimeoutConfig{}
		remapJSON(opts.AWSJobTimeoutConfig, in.TimeoutConfig)
	}

	if opts.AWSJobExecutionsRolloutConfig != nil {
		in.JobExecutionsRolloutConfig = &JobExecutionsRolloutConfig{}
		remapJSON(opts.AWSJobExecutionsRolloutConfig, in.JobExecutionsRolloutConfig)
	}

	if opts.AWSJobPresignedURLConfig != nil {
		in.PresignedURLConfig = &PresignedURLConfig{RoleARN: roleARN}
		remapJSON(opts.AWSJobPresignedURLConfig, in.PresignedURLConfig)
	}

	return in
}

// remapJSON copies the keys src shares with dst's JSON tags; an unmarshal failure leaves dst as is.
func remapJSON(src map[string]any, dst any) {
	raw, err := json.Marshal(src)
	if err != nil {
		return
	}

	_ = json.Unmarshal(raw, dst)
}
