package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- Dataset ---

func (h *Handler) createDataset(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	datasetType, _ := input["datasetType"].(string)
	schemaArn, _ := input["schemaArn"].(string)
	tags := extractTags(input)

	ds, err := h.Backend.CreateDataset(name, datasetGroupArn, datasetType, schemaArn, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDatasetArn: ds.DatasetArn}, nil
}

func (h *Handler) describeDataset(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetArn"].(string)

	ds, err := h.Backend.DescribeDataset(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"dataset": datasetToMap(ds)}, nil
}

func (h *Handler) updateDataset(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetArn"].(string)
	schemaArn, _ := input["schemaArn"].(string)

	ds, err := h.Backend.UpdateDataset(nameOrArn, schemaArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDatasetArn: ds.DatasetArn}, nil
}

func (h *Handler) deleteDataset(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["datasetArn"].(string)

	return map[string]any{}, h.Backend.DeleteDataset(nameOrArn)
}

func (h *Handler) listDatasets(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasets(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, ds := range list {
		summaries = append(summaries, datasetSummaryToMap(ds))
	}

	result := map[string]any{"datasets": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- DatasetImportJob ---

func (h *Handler) createDatasetImportJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	datasetArn, _ := input["datasetArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	dataSource, _ := input["dataSource"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateDatasetImportJob(jobName, datasetArn, roleArn, dataSource, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDatasetImportJobArn: job.DatasetImportJobArn}, nil
}

func (h *Handler) describeDatasetImportJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input[keyDatasetImportJobArn].(string)

	job, err := h.Backend.DescribeDatasetImportJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"datasetImportJob": datasetImportJobToMap(job)}, nil
}

func (h *Handler) listDatasetImportJobs(input map[string]any) (map[string]any, error) {
	datasetArn, _ := input["datasetArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasetImportJobs(datasetArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, datasetImportJobSummaryToMap(job))
	}

	result := map[string]any{"datasetImportJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

// --- DatasetExportJob ---

func (h *Handler) createDatasetExportJob(input map[string]any) (map[string]any, error) {
	jobName, _ := input["jobName"].(string)
	datasetArn, _ := input["datasetArn"].(string)
	roleArn, _ := input["roleArn"].(string)
	jobOutput, _ := input["jobOutput"].(map[string]any)
	tags := extractTags(input)

	job, err := h.Backend.CreateDatasetExportJob(jobName, datasetArn, roleArn, jobOutput, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDatasetExportJobArn: job.DatasetExportJobArn}, nil
}

func (h *Handler) describeDatasetExportJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input[keyDatasetExportJobArn].(string)

	job, err := h.Backend.DescribeDatasetExportJob(jobArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"datasetExportJob": datasetExportJobToMap(job)}, nil
}

func (h *Handler) listDatasetExportJobs(input map[string]any) (map[string]any, error) {
	datasetArn, _ := input["datasetArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListDatasetExportJobs(datasetArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, job := range list {
		summaries = append(summaries, datasetExportJobSummaryToMap(job))
	}

	result := map[string]any{"datasetExportJobs": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func datasetToMap(ds *Dataset) map[string]any {
	return map[string]any{
		keyDatasetArn:          ds.DatasetArn,
		keyDatasetGroupArn:     ds.DatasetGroupArn,
		keySchemaArn:           ds.SchemaArn,
		keyName:                ds.Name,
		"datasetType":          ds.DatasetType,
		keyStatus:              ds.Status,
		keyCreationDateTime:    awstime.Epoch(ds.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(ds.LastUpdatedDateTime),
	}
}

func datasetImportJobToMap(job *DatasetImportJob) map[string]any {
	return map[string]any{
		keyDatasetImportJobArn: job.DatasetImportJobArn,
		keyJobName:             job.JobName,
		keyDatasetArn:          job.DatasetArn,
		keyRoleArn:             job.RoleArn,
		"dataSource":           job.DataSource,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

func datasetExportJobToMap(job *DatasetExportJob) map[string]any {
	return map[string]any{
		keyDatasetExportJobArn: job.DatasetExportJobArn,
		keyJobName:             job.JobName,
		keyDatasetArn:          job.DatasetArn,
		keyRoleArn:             job.RoleArn,
		keyJobOutput:           job.JobOutput,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

// datasetSummaryToMap builds the types.DatasetSummary shape (types.go:1040)
// -- no datasetGroupArn or schemaArn.
func datasetSummaryToMap(ds *Dataset) map[string]any {
	return map[string]any{
		keyDatasetArn:          ds.DatasetArn,
		keyName:                ds.Name,
		"datasetType":          ds.DatasetType,
		keyStatus:              ds.Status,
		keyCreationDateTime:    awstime.Epoch(ds.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(ds.LastUpdatedDateTime),
	}
}

// datasetImportJobSummaryToMap builds the types.DatasetImportJobSummary
// shape (types.go:952) -- no datasetArn, roleArn, or dataSource. importMode
// and failureReason are real Summary members, but the backend's
// DatasetImportJob model has no source for either, so both stay absent
// rather than being fabricated.
func datasetImportJobSummaryToMap(job *DatasetImportJob) map[string]any {
	return map[string]any{
		keyDatasetImportJobArn: job.DatasetImportJobArn,
		keyJobName:             job.JobName,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}

// datasetExportJobSummaryToMap builds the types.DatasetExportJobSummary
// shape (types.go:780) -- no datasetArn, roleArn, or jobOutput.
// failureReason is a real member but the backend's DatasetExportJob model
// has no source for it, so it stays absent rather than being fabricated.
func datasetExportJobSummaryToMap(job *DatasetExportJob) map[string]any {
	return map[string]any{
		keyDatasetExportJobArn: job.DatasetExportJobArn,
		keyJobName:             job.JobName,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}
