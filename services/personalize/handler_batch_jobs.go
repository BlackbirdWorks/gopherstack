package personalize

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- BatchInferenceJob ---

func (h *Handler) createBatchInferenceJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	jobInput, _ := input["jobInput"].(map[string]any)
	jobOutput, _ := input["jobOutput"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateBatchInferenceJob(jobName, solutionVersionArn, roleArn, jobInput, jobOutput, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyBatchInferenceJobArn: job.BatchInferenceJobArn}, nil
}

func (h *Handler) describeBatchInferenceJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input[keyBatchInferenceJobArn].(string)

	job, err := h.Backend.DescribeBatchInferenceJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"batchInferenceJob": batchInferenceJobToMap(job)}, nil
}

func (h *Handler) listBatchInferenceJobs(input map[string]any) (map[string]any, error) {
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListBatchInferenceJobs(solutionVersionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, batchInferenceJobSummaryToMap(job))
	}

	result := map[string]any{"batchInferenceJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func batchInferenceJobToMap(job *BatchInferenceJob) map[string]any {
	return map[string]any{
		keyBatchInferenceJobArn: job.BatchInferenceJobArn,
		keyJobName:              job.JobName,
		keySolutionVersionArn:   job.SolutionVersionArn,
		keyRoleArn:              job.RoleArn,
		"jobInput":              job.JobInput,
		keyJobOutput:            job.JobOutput,
		keyStatus:               job.Status,
		keyCreationDateTime:     awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime:  awstime.Epoch(job.LastUpdatedDateTime),
	}
}

// batchInferenceJobSummaryToMap builds the types.BatchInferenceJobSummary
// shape (types.go:223) -- no roleArn, jobInput, or jobOutput.
// batchInferenceJobMode and failureReason are real Summary members, but the
// backend's BatchInferenceJob model has no source for either (this backend
// has no theme-generation mode and never fails a job asynchronously), so
// both stay absent rather than being fabricated.
func batchInferenceJobSummaryToMap(job *BatchInferenceJob) map[string]any {
	return map[string]any{
		keyBatchInferenceJobArn: job.BatchInferenceJobArn,
		keyJobName:              job.JobName,
		keySolutionVersionArn:   job.SolutionVersionArn,
		keyStatus:               job.Status,
		keyCreationDateTime:     awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime:  awstime.Epoch(job.LastUpdatedDateTime),
	}
}

// --- BatchSegmentJob ---

func (h *Handler) createBatchSegmentJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	jobInput, _ := input["jobInput"].(map[string]any)
	jobOutput, _ := input["jobOutput"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateBatchSegmentJob(jobName, solutionVersionArn, roleArn, jobInput, jobOutput, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyBatchSegmentJobArn: job.BatchSegmentJobArn}, nil
}

func (h *Handler) describeBatchSegmentJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input[keyBatchSegmentJobArn].(string)

	job, err := h.Backend.DescribeBatchSegmentJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"batchSegmentJob": batchSegmentJobToMap(job)}, nil
}

func (h *Handler) listBatchSegmentJobs(input map[string]any) (map[string]any, error) {
	solutionVersionArn, _ := input["solutionVersionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListBatchSegmentJobs(solutionVersionArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, batchSegmentJobSummaryToMap(job))
	}

	result := map[string]any{"batchSegmentJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func batchSegmentJobToMap(job *BatchSegmentJob) map[string]any {
	return map[string]any{
		keyBatchSegmentJobArn:  job.BatchSegmentJobArn,
		keyJobName:             job.JobName,
		keySolutionVersionArn:  job.SolutionVersionArn,
		keyRoleArn:             job.RoleArn,
		"jobInput":             job.JobInput,
		keyJobOutput:           job.JobOutput,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

// batchSegmentJobSummaryToMap builds the types.BatchSegmentJobSummary shape
// (types.go:343) -- no roleArn, jobInput, or jobOutput. failureReason is a
// real member but the backend's BatchSegmentJob model has no source for it,
// so it stays absent rather than being fabricated.
func batchSegmentJobSummaryToMap(job *BatchSegmentJob) map[string]any {
	return map[string]any{
		keyBatchSegmentJobArn:  job.BatchSegmentJobArn,
		keyJobName:             job.JobName,
		keySolutionVersionArn:  job.SolutionVersionArn,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}
