package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- DataDeletionJob ---

func (h *Handler) createDataDeletionJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	dataSource, _ := input["dataSource"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateDataDeletionJob(jobName, datasetGroupArn, roleArn, dataSource, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDataDeletionJobArn: job.DataDeletionJobArn}, nil
}

func (h *Handler) describeDataDeletionJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input[keyDataDeletionJobArn].(string)

	job, err := h.Backend.DescribeDataDeletionJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"dataDeletionJob": dataDeletionJobToMap(job)}, nil
}

func (h *Handler) listDataDeletionJobs(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDataDeletionJobs(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, dataDeletionJobSummaryToMap(job))
	}

	result := map[string]any{"dataDeletionJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func dataDeletionJobToMap(job *DataDeletionJob) map[string]any {
	return map[string]any{
		keyDataDeletionJobArn:  job.DataDeletionJobArn,
		keyJobName:             job.JobName,
		keyDatasetGroupArn:     job.DatasetGroupArn,
		keyRoleArn:             job.RoleArn,
		"dataSource":           job.DataSource,
		keyStatus:              job.Status,
		"numDeleted":           job.NumDeleted,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

// dataDeletionJobSummaryToMap builds the types.DataDeletionJobSummary shape
// (types.go:625) -- no roleArn, dataSource, or numDeleted. failureReason is a
// real member but the backend's DataDeletionJob model has no source for it,
// so it stays absent rather than being fabricated.
func dataDeletionJobSummaryToMap(job *DataDeletionJob) map[string]any {
	return map[string]any{
		keyDataDeletionJobArn:  job.DataDeletionJobArn,
		keyJobName:             job.JobName,
		keyDatasetGroupArn:     job.DatasetGroupArn,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}
