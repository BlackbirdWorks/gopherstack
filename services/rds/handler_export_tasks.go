package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleStartExportTask(vals url.Values) (any, error) {
	taskID := vals.Get("ExportTaskIdentifier")
	sourceARN := vals.Get("SourceArn")
	s3Bucket := vals.Get("S3BucketName")
	iamRoleARN := vals.Get("IamRoleArn")
	kmsKeyID := vals.Get("KmsKeyId")
	task, err := h.Backend.StartExportTask(taskID, sourceARN, s3Bucket, iamRoleARN, kmsKeyID)
	if err != nil {
		return nil, err
	}

	return &startExportTaskResponse{
		Xmlns:  rdsXMLNS,
		Result: startExportTaskResult{toXMLExportTask(task)},
	}, nil
}

func (h *Handler) handleDescribeExportTasks(vals url.Values) (any, error) {
	taskID := vals.Get("ExportTaskIdentifier")
	tasks, err := h.Backend.DescribeExportTasks(taskID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlExportTask, 0, len(tasks))
	for _, task := range tasks {
		cp := task
		members = append(members, toXMLExportTask(&cp))
	}

	return &describeExportTasksResponse{
		Xmlns:       rdsXMLNS,
		ExportTasks: xmlExportTaskList{Members: members},
	}, nil
}

func (h *Handler) handleCancelExportTask(vals url.Values) (any, error) {
	taskID := vals.Get("ExportTaskIdentifier")
	task, err := h.Backend.CancelExportTask(taskID)
	if err != nil {
		return nil, err
	}

	return &cancelExportTaskResponse{
		Xmlns:  rdsXMLNS,
		Result: cancelExportTaskResult{toXMLExportTask(task)},
	}, nil
}

func toXMLExportTask(task *ExportTask) xmlExportTask {
	return xmlExportTask{
		ExportTaskIdentifier: task.ExportTaskIdentifier,
		SourceArn:            task.SourceArn,
		Status:               task.Status,
		S3Bucket:             task.S3Bucket,
		IamRoleArn:           task.IamRoleArn,
		KmsKeyID:             task.KmsKeyID,
	}
}

type xmlExportTask struct {
	ExportTaskIdentifier string `xml:"ExportTaskIdentifier"`
	SourceArn            string `xml:"SourceArn"`
	Status               string `xml:"Status"`
	S3Bucket             string `xml:"S3Bucket,omitempty"`
	IamRoleArn           string `xml:"IamRoleArn,omitempty"`
	KmsKeyID             string `xml:"KmsKeyId,omitempty"`
}

type xmlExportTaskList struct {
	Members []xmlExportTask `xml:"ExportTask"`
}

// startExportTaskResult inlines export task fields directly inside StartExportTaskResult.
// The SDK deserializes fields directly (no nested ExportTask element).
type startExportTaskResult struct {
	xmlExportTask
}

type startExportTaskResponse struct {
	XMLName xml.Name              `xml:"StartExportTaskResponse"`
	Xmlns   string                `xml:"xmlns,attr"`
	Result  startExportTaskResult `xml:"StartExportTaskResult"`
}

type describeExportTasksResponse struct {
	XMLName     xml.Name          `xml:"DescribeExportTasksResponse"`
	Xmlns       string            `xml:"xmlns,attr"`
	ExportTasks xmlExportTaskList `xml:"DescribeExportTasksResult>ExportTasks"`
}

// cancelExportTaskResult inlines export task fields directly inside CancelExportTaskResult.
type cancelExportTaskResult struct {
	xmlExportTask
}

type cancelExportTaskResponse struct {
	XMLName xml.Name               `xml:"CancelExportTaskResponse"`
	Xmlns   string                 `xml:"xmlns,attr"`
	Result  cancelExportTaskResult `xml:"CancelExportTaskResult"`
}
