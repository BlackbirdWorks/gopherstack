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
		summaries = append(summaries, datasetToMap(ds))
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

	return map[string]any{"datasetImportJobArn": job.DatasetImportJobArn}, nil
}

func (h *Handler) describeDatasetImportJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input["datasetImportJobArn"].(string)

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
		summaries = append(summaries, datasetImportJobToMap(job))
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

	return map[string]any{"datasetExportJobArn": job.DatasetExportJobArn}, nil
}

func (h *Handler) describeDatasetExportJob(input map[string]any) (map[string]any, error) {
	jobArn, _ := input["datasetExportJobArn"].(string)

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
		summaries = append(summaries, datasetExportJobToMap(job))
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
		"datasetImportJobArn":  job.DatasetImportJobArn,
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
		"datasetExportJobArn":  job.DatasetExportJobArn,
		keyJobName:             job.JobName,
		keyDatasetArn:          job.DatasetArn,
		keyRoleArn:             job.RoleArn,
		keyJobOutput:           job.JobOutput,
		keyStatus:              job.Status,
		keyCreationDateTime:    awstime.Epoch(job.CreationDateTime),
		keyLastUpdatedDateTime: awstime.Epoch(job.LastUpdatedDateTime),
	}
}
