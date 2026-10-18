package backup

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	colResourceArn    = "Resource ARN"
	colCreationTime   = "Creation Time"
	colResourceType   = "Resource Type"
	colJobStatus      = "Job Status"
	colStatusMessage  = "Status Message"
	colAccountID      = "Account ID"
	colCompletionTime = "Completion Time"
	colIAMRoleArn     = "IAM Role ARN"

	reportFormatCSV  = "CSV"
	reportFormatJSON = "JSON"

	reportBackupJobs     = "BACKUP_JOB_REPORT"
	reportCopyJobs       = "COPY_JOB_REPORT"
	reportRestoreJobs    = "RESTORE_JOB_REPORT"
	reportResourceStatus = "RESOURCE_COMPLIANCE_REPORT"
	reportControlStatus  = "CONTROL_COMPLIANCE_REPORT"
)

type reportTable struct {
	header []string
	rows   [][]string
}

// deliverReportLocked writes the plan's report in each requested format and returns the destination.
func (b *InMemoryBackend) deliverReportLocked(
	plan *ReportPlan, job *ReportJob, now time.Time,
) (*ReportDestination, error) {
	ch := plan.ReportDeliveryChannel
	dest := &ReportDestination{S3BucketName: ch.S3BucketName}

	formats := ch.Formats
	if len(formats) == 0 {
		formats = []string{reportFormatCSV}
	}

	table := b.buildReportLocked(job.ReportTemplate, plan.ReportSetting)
	base := path.Join(
		strings.Trim(ch.S3KeyPrefix, "/"), "Backup", b.region, now.Format("2006"), now.Format("01"), now.Format("02"),
		plan.ReportPlanName+"_"+job.ReportJobID,
	)

	for _, format := range formats {
		body, ext, err := renderReport(format, table)
		if err != nil {
			return dest, err
		}

		key := strings.TrimPrefix(base+ext, "/")
		if _, err = b.objects.PutObject(context.Background(), &sdk_s3.PutObjectInput{
			Bucket: aws.String(ch.S3BucketName), Key: aws.String(key), Body: bytes.NewReader(body),
		}); err != nil {
			return dest, fmt.Errorf("deliver report to s3://%s/%s: %w", ch.S3BucketName, key, err)
		}

		dest.S3Keys = append(dest.S3Keys, key)
	}

	return dest, nil
}

func renderReport(format string, t reportTable) ([]byte, string, error) {
	if format == reportFormatJSON {
		recs := make([]map[string]string, 0, len(t.rows))
		for _, row := range t.rows {
			rec := make(map[string]string, len(row))
			for i, v := range row {
				rec[t.header[i]] = v
			}
			recs = append(recs, rec)
		}

		body, err := json.Marshal(recs)
		if err != nil {
			return nil, "", fmt.Errorf("encode JSON report: %w", err)
		}

		return body, ".json", nil
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(t.header)
	_ = w.WriteAll(t.rows)
	w.Flush()

	return buf.Bytes(), ".csv", w.Error()
}

func (b *InMemoryBackend) buildReportLocked(template string, setting *ReportSetting) reportTable {
	switch template {
	case reportCopyJobs:
		return b.copyJobReportLocked()
	case reportRestoreJobs:
		return b.restoreJobReportLocked()
	case reportResourceStatus:
		return b.resourceReportLocked()
	case reportControlStatus:
		return controlReport(setting)
	default:
		return b.backupJobReportLocked()
	}
}

func reportTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func reportTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}

	return reportTime(*t)
}

func (b *InMemoryBackend) backupJobReportLocked() reportTable {
	t := reportTable{header: []string{
		colAccountID, "Region", "Backup Job ID", colResourceArn, colResourceType, "Backup Vault Name",
		colJobStatus, colStatusMessage, colCreationTime, colCompletionTime, "Backup Size In Bytes",
		"Recovery Point ARN", colIAMRoleArn,
	}}

	for _, j := range b.jobs.All() {
		t.rows = append(t.rows, []string{
			j.AccountID, j.Region, j.BackupJobID, j.ResourceArn, j.ResourceType, j.BackupVaultName,
			j.State, j.StatusMessage, reportTime(j.CreationTime), reportTimePtr(j.CompletionTime),
			strconv.FormatInt(j.BackupSizeInBytes, 10), j.RecoveryPointArn, j.IAMRoleArn,
		})
	}

	return sortedReport(t)
}

func (b *InMemoryBackend) copyJobReportLocked() reportTable {
	t := reportTable{header: []string{
		colAccountID, "Region", "Copy Job ID", "Source Recovery Point ARN", "Source Vault ARN",
		"Destination Vault ARN", "Destination Recovery Point ARN", colResourceArn, colResourceType,
		colJobStatus, colStatusMessage, colCreationTime, colCompletionTime, colIAMRoleArn,
	}}

	for _, j := range b.copyJobs.All() {
		t.rows = append(t.rows, []string{
			j.AccountID, j.Region, j.CopyJobID, j.SourceRecoveryPointArn, j.SourceBackupVaultArn,
			j.DestinationBackupVaultArn, j.DestinationRecoveryPointArn, j.ResourceArn, j.ResourceType,
			j.State, j.StatusMessage, reportTime(j.CreationDate), reportTimePtr(j.CompletionDate), j.IAMRoleArn,
		})
	}

	return sortedReport(t)
}

func (b *InMemoryBackend) restoreJobReportLocked() reportTable {
	t := reportTable{header: []string{
		colAccountID, "Restore Job ID", "Recovery Point ARN", colResourceType, colJobStatus, colStatusMessage,
		colCreationTime, colCompletionTime, "Created Resource ARN", colIAMRoleArn,
	}}

	for _, j := range b.restoreJobs.All() {
		t.rows = append(t.rows, []string{
			j.AccountID, j.RestoreJobID, j.RecoveryPointArn, j.ResourceType, j.Status, j.StatusMessage,
			reportTime(j.StartTime), reportTimePtr(j.CompletionDate), j.CreatedResourceArn, j.IAMRoleArn,
		})
	}

	return sortedReport(t)
}

func (b *InMemoryBackend) resourceReportLocked() reportTable {
	t := reportTable{header: []string{
		colResourceArn, colResourceType, "Last Backup Time", "Last Backup Vault ARN", "Last Recovery Point ARN",
	}}

	for _, r := range b.protectedResources.All() {
		t.rows = append(t.rows, []string{
			r.ResourceArn, r.ResourceType, reportTime(r.LastBackupTime), r.LastBackupVaultArn, r.LastRecoveryPointArn,
		})
	}

	return sortedReport(t)
}

func controlReport(setting *ReportSetting) reportTable {
	t := reportTable{header: []string{"Framework ARN"}}
	if setting != nil {
		for _, arn := range setting.FrameworkArns {
			t.rows = append(t.rows, []string{arn})
		}
	}

	return t
}

// sortedReport orders rows by their identifying leading columns so output is deterministic.
func sortedReport(t reportTable) reportTable {
	sort.Slice(t.rows, func(i, j int) bool {
		return strings.Join(t.rows[i], "\x00") < strings.Join(t.rows[j], "\x00")
	})

	return t
}
