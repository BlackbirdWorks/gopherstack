package workmail

import (
	"context"
)

type getMailboxDetailsReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
}

type getMailboxDetailsResp struct {
	MailboxQuota int32   `json:"MailboxQuota"`
	MailboxSize  float64 `json:"MailboxSize"`
}

func (h *Handler) handleGetMailboxDetails(
	_ context.Context,
	req *getMailboxDetailsReq,
) (*getMailboxDetailsResp, error) {
	details, err := h.Backend.GetMailboxDetails(req.OrganizationID, req.UserID)
	if err != nil {
		return nil, err
	}

	return &getMailboxDetailsResp{MailboxQuota: details.MailboxQuota, MailboxSize: details.MailboxSize}, nil
}

type updateMailboxQuotaReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserID         string `json:"UserId"`
	MailboxQuota   int32  `json:"MailboxQuota"`
}

func (h *Handler) handleUpdateMailboxQuota(_ context.Context, req *updateMailboxQuotaReq) (*emptyResp, error) {
	if err := h.Backend.UpdateMailboxQuota(req.OrganizationID, req.UserID, req.MailboxQuota); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

// ---- Mailbox Permissions ----

type putMailboxPermsReq struct {
	OrganizationID   string   `json:"OrganizationId"`
	EntityID         string   `json:"EntityId"`
	GranteeID        string   `json:"GranteeId"`
	PermissionValues []string `json:"PermissionValues"`
}

func (h *Handler) handlePutMailboxPermissions(_ context.Context, req *putMailboxPermsReq) (*emptyResp, error) {
	if err := h.Backend.PutMailboxPermissions(
		req.OrganizationID, req.EntityID, req.GranteeID, req.PermissionValues,
	); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteMailboxPermsReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	GranteeID      string `json:"GranteeId"`
}

func (h *Handler) handleDeleteMailboxPermissions(_ context.Context, req *deleteMailboxPermsReq) (*emptyResp, error) {
	if err := h.Backend.DeleteMailboxPermissions(req.OrganizationID, req.EntityID, req.GranteeID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listMailboxPermsReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type permissionResp struct {
	GranteeID        string   `json:"GranteeId"`
	GranteeType      string   `json:"GranteeType"`
	PermissionValues []string `json:"PermissionValues"`
}

type listMailboxPermsResp struct {
	NextToken   string           `json:"NextToken,omitempty"`
	Permissions []permissionResp `json:"Permissions"`
}

func (h *Handler) handleListMailboxPermissions(
	_ context.Context,
	req *listMailboxPermsReq,
) (*listMailboxPermsResp, error) {
	perms, next, err := h.Backend.ListMailboxPermissions(
		req.OrganizationID,
		req.EntityID,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	presps := make([]permissionResp, 0, len(perms))
	for _, p := range perms {
		presps = append(presps, permissionResp{
			GranteeID:        p.GranteeID,
			GranteeType:      p.GranteeType,
			PermissionValues: p.Permissions,
		})
	}

	return &listMailboxPermsResp{Permissions: presps, NextToken: next}, nil
}

// ---- Mailbox Export Jobs ----

type startMailboxExportJobReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityId       string `json:"EntityId"` //nolint:revive,staticcheck // existing issue.
	Description    string `json:"Description"`
	RoleArn        string `json:"RoleArn"`
	KmsKeyArn      string `json:"KmsKeyArn"`
	S3BucketName   string `json:"S3BucketName"`
	S3Prefix       string `json:"S3Prefix"`
}

type startMailboxExportJobResp struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleStartMailboxExportJob(
	_ context.Context, req *startMailboxExportJobReq,
) (*startMailboxExportJobResp, error) {
	job, err := h.Backend.StartMailboxExportJob(
		req.OrganizationID, req.EntityId, req.Description, req.RoleArn, req.KmsKeyArn,
		req.S3BucketName, req.S3Prefix,
	)
	if err != nil {
		return nil, err
	}

	return &startMailboxExportJobResp{JobId: job.JobID}, nil
}

type cancelMailboxExportJobReq struct {
	OrganizationID string `json:"OrganizationId"`
	JobId          string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCancelMailboxExportJob(
	_ context.Context, req *cancelMailboxExportJobReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.CancelMailboxExportJob(req.OrganizationID, req.JobId)
}

type describeMailboxExportJobReq struct {
	OrganizationID string `json:"OrganizationId"`
	JobId          string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

type describeMailboxExportJobResp struct {
	S3Prefix          string `json:"S3Prefix,omitempty"`
	EntityId          string `json:"EntityId,omitempty"` //nolint:revive,staticcheck // existing issue.
	Description       string `json:"Description,omitempty"`
	RoleArn           string `json:"RoleArn,omitempty"`
	KmsKeyArn         string `json:"KmsKeyArn,omitempty"`
	S3BucketName      string `json:"S3BucketName,omitempty"`
	JobId             string `json:"JobId,omitempty"` //nolint:revive,staticcheck // existing issue.
	S3Path            string `json:"S3Path,omitempty"`
	State             string `json:"State,omitempty"`
	ErrorInfo         string `json:"ErrorInfo,omitempty"`
	StartTime         int64  `json:"StartTime,omitempty"`
	EndTime           int64  `json:"EndTime,omitempty"`
	EstimatedProgress int32  `json:"EstimatedProgress"`
}

func (h *Handler) handleDescribeMailboxExportJob(
	_ context.Context, req *describeMailboxExportJobReq,
) (*describeMailboxExportJobResp, error) {
	job, err := h.Backend.DescribeMailboxExportJob(req.OrganizationID, req.JobId)
	if err != nil {
		return nil, err
	}
	resp := &describeMailboxExportJobResp{
		JobId:             job.JobID,
		EntityId:          job.EntityID,
		Description:       job.Description,
		RoleArn:           job.RoleARN,
		KmsKeyArn:         job.KmsKeyARN,
		S3BucketName:      job.S3BucketName,
		S3Prefix:          job.S3Prefix,
		S3Path:            job.S3Path,
		EstimatedProgress: job.EstimatedProgress,
		State:             job.State,
		ErrorInfo:         job.ErrorInfo,
		StartTime:         job.StartTime.Unix(),
	}
	if !job.EndTime.IsZero() {
		resp.EndTime = job.EndTime.Unix()
	}

	return resp, nil
}

type listMailboxExportJobsReq struct {
	OrganizationID string `json:"OrganizationId"`
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

// mailboxExportJobSummaryJSON mirrors aws-sdk-go-v2/service/workmail/types.
// MailboxExportJob (the ListMailboxExportJobs item shape) -- a genuinely
// NARROWER type than DescribeMailboxExportJobOutput. Confirmed against
// types.MailboxExportJob (aws-sdk-go-v2/service/workmail@v1.39.4/types.go):
// it has no RoleArn/KmsKeyArn/S3Prefix/ErrorInfo members at all. A prior
// version of this struct carried those four fields anyway (an invented
// shape, not a summarized-away omission) -- the raw wire body leaked the IAM
// role ARN and KMS key ARN of every export job on every List call, fields no
// real client can even decode since its own typed item has no such field.
type mailboxExportJobSummaryJSON struct {
	JobId             string `json:"JobId"`    //nolint:revive,staticcheck // existing issue.
	EntityId          string `json:"EntityId"` //nolint:revive,staticcheck // existing issue.
	Description       string `json:"Description,omitempty"`
	S3BucketName      string `json:"S3BucketName,omitempty"`
	S3Path            string `json:"S3Path,omitempty"`
	State             string `json:"State"`
	StartTime         int64  `json:"StartTime,omitempty"`
	EndTime           int64  `json:"EndTime,omitempty"`
	EstimatedProgress int32  `json:"EstimatedProgress"`
}

type listMailboxExportJobsResp struct {
	NextToken string                        `json:"NextToken,omitempty"`
	Jobs      []mailboxExportJobSummaryJSON `json:"Jobs"`
}

func (h *Handler) handleListMailboxExportJobs(
	_ context.Context, req *listMailboxExportJobsReq,
) (*listMailboxExportJobsResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	jobs, next, err := h.Backend.ListMailboxExportJobs(req.OrganizationID, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}
	result := make([]mailboxExportJobSummaryJSON, 0, len(jobs))
	for _, j := range jobs {
		item := mailboxExportJobSummaryJSON{
			JobId:             j.JobID,
			EntityId:          j.EntityID,
			Description:       j.Description,
			S3BucketName:      j.S3BucketName,
			S3Path:            j.S3Path,
			State:             j.State,
			StartTime:         j.StartTime.Unix(),
			EstimatedProgress: j.EstimatedProgress,
		}
		if !j.EndTime.IsZero() {
			item.EndTime = j.EndTime.Unix()
		}
		result = append(result, item)
	}

	return &listMailboxExportJobsResp{Jobs: result, NextToken: next}, nil
}
