package appstream

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- User handlers ---

type createUserInput struct {
	UserName           string `json:"UserName"`
	Email              string `json:"Email"`
	FirstName          string `json:"FirstName"`
	LastName           string `json:"LastName"`
	AuthenticationType string `json:"AuthenticationType"`
}

func (h *Handler) opCreateUser(_ context.Context, body []byte) (any, error) {
	var req createUserInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if _, err := h.Backend.CreateUser(
		req.UserName,
		req.Email,
		req.FirstName,
		req.LastName,
		req.AuthenticationType,
	); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type deleteUserInput struct {
	UserName           string `json:"UserName"`
	AuthenticationType string `json:"AuthenticationType"`
}

func (h *Handler) opDeleteUser(_ context.Context, body []byte) (any, error) {
	var req deleteUserInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteUser(req.UserName, req.AuthenticationType); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeUsersInput struct {
	AuthenticationType string `json:"AuthenticationType"`
}

func (h *Handler) opDescribeUsers(_ context.Context, body []byte) (any, error) {
	var req describeUsersInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	users, err := h.Backend.DescribeUsers(req.AuthenticationType)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(users))
	for _, u := range users {
		resp = append(resp, userToResponse(u))
	}

	return map[string]any{"Users": resp}, nil
}

type userAuthInput struct {
	UserName           string `json:"UserName"`
	AuthenticationType string `json:"AuthenticationType"`
}

func (h *Handler) opDisableUser(_ context.Context, body []byte) (any, error) {
	var req userAuthInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DisableUser(req.UserName, req.AuthenticationType); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opEnableUser(_ context.Context, body []byte) (any, error) {
	var req userAuthInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.EnableUser(req.UserName, req.AuthenticationType); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// --- UserStack association handlers ---

type userStackAssocJSON struct {
	UserName              string `json:"UserName"`
	StackName             string `json:"StackName"`
	AuthenticationType    string `json:"AuthenticationType"`
	SendEmailNotification bool   `json:"SendEmailNotification"`
}

type batchAssociateUserStackInput struct {
	UserStackAssociations []userStackAssocJSON `json:"UserStackAssociations"`
}

func jsonAssocToModel(a userStackAssocJSON) UserStackAssociation {
	return UserStackAssociation(a)
}

func (h *Handler) opBatchAssociateUserStack( //nolint:dupl // existing issue.
	_ context.Context,
	body []byte,
) (any, error) {
	var req batchAssociateUserStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	assocs := make([]UserStackAssociation, len(req.UserStackAssociations))
	for i, a := range req.UserStackAssociations {
		assocs[i] = jsonAssocToModel(a)
	}

	errs, err := h.Backend.BatchAssociateUserStack(assocs)
	if err != nil {
		return nil, err
	}

	errResp := make([]any, 0, len(errs))
	for _, e := range errs {
		var assoc any
		if e.UserStackAssociation != nil {
			assoc = map[string]any{
				"UserName":           e.UserStackAssociation.UserName,           //nolint:goconst // existing issue.
				"StackName":          e.UserStackAssociation.StackName,          //nolint:goconst // existing issue.
				"AuthenticationType": e.UserStackAssociation.AuthenticationType, //nolint:goconst // existing issue.
			}
		}

		errResp = append(errResp, map[string]any{
			"UserStackAssociation": assoc,
			"ErrorCode":            e.ErrorCode,
			"ErrorMessage":         e.ErrorMessage,
		})
	}

	return map[string]any{"Errors": errResp}, nil
}

func (h *Handler) opBatchDisassociateUserStack( //nolint:dupl // existing issue.
	_ context.Context,
	body []byte,
) (any, error) {
	var req batchAssociateUserStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	assocs := make([]UserStackAssociation, len(req.UserStackAssociations))
	for i, a := range req.UserStackAssociations {
		assocs[i] = jsonAssocToModel(a)
	}

	errs, err := h.Backend.BatchDisassociateUserStack(assocs)
	if err != nil {
		return nil, err
	}

	errResp := make([]any, 0, len(errs))
	for _, e := range errs {
		var assoc any
		if e.UserStackAssociation != nil {
			assoc = map[string]any{
				"UserName":           e.UserStackAssociation.UserName,
				"StackName":          e.UserStackAssociation.StackName,
				"AuthenticationType": e.UserStackAssociation.AuthenticationType,
			}
		}

		errResp = append(errResp, map[string]any{
			"UserStackAssociation": assoc,
			"ErrorCode":            e.ErrorCode,
			"ErrorMessage":         e.ErrorMessage,
		})
	}

	return map[string]any{"Errors": errResp}, nil
}

type describeUserStackAssociationsInput struct {
	StackName          string `json:"StackName"`
	UserName           string `json:"UserName"`
	AuthenticationType string `json:"AuthenticationType"`
}

func (h *Handler) opDescribeUserStackAssociations(_ context.Context, body []byte) (any, error) {
	var req describeUserStackAssociationsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	assocs, err := h.Backend.DescribeUserStackAssociations(req.StackName, req.UserName, req.AuthenticationType)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(assocs))
	for _, a := range assocs {
		resp = append(resp, map[string]any{
			"UserName":           a.UserName,
			"StackName":          a.StackName,
			"AuthenticationType": a.AuthenticationType,
		})
	}

	return map[string]any{"UserStackAssociations": resp}, nil
}

// --- Session handlers ---

type describeSessionsInput struct {
	StackName string `json:"StackName"`
	FleetName string `json:"FleetName"`
	UserId    string `json:"UserId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) opDescribeSessions(_ context.Context, body []byte) (any, error) {
	var req describeSessionsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	sessions, err := h.Backend.DescribeSessions(req.StackName, req.FleetName, req.UserId)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(sessions))
	for _, s := range sessions {
		resp = append(resp, sessionToResponse(s))
	}

	return map[string]any{"Sessions": resp}, nil
}

type sessionIDInput struct {
	SessionId string `json:"SessionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) opDrainSessionInstance(_ context.Context, body []byte) (any, error) {
	var req sessionIDInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DrainSessionInstance(req.SessionId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opExpireSession(_ context.Context, body []byte) (any, error) {
	var req sessionIDInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.ExpireSession(req.SessionId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type createStreamingURLInput struct {
	StackName string `json:"StackName"`
	FleetName string `json:"FleetName"`
	UserId    string `json:"UserId"` //nolint:revive,staticcheck // existing issue.
	Validity  int64  `json:"Validity"`
}

func (h *Handler) opCreateStreamingURL(_ context.Context, body []byte) (any, error) {
	var req createStreamingURLInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	url, expires, err := h.Backend.CreateStreamingURL(req.StackName, req.FleetName, req.UserId, req.Validity)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyStreamingURL: url,
		keyExpires:      awstime.Epoch(expires),
	}, nil
}

// --- UsageReport handlers ---

type createUsageReportSubscriptionInput struct {
	S3BucketName string `json:"S3BucketName"`
	Schedule     string `json:"Schedule"`
}

func (h *Handler) opCreateUsageReportSubscription(_ context.Context, body []byte) (any, error) {
	var req createUsageReportSubscriptionInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	sub, err := h.Backend.CreateUsageReportSubscription(req.Schedule, req.S3BucketName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"S3BucketName": sub.S3BucketName,
		"Schedule":     sub.Schedule,
	}, nil
}

func (h *Handler) opDeleteUsageReportSubscription(_ context.Context, _ []byte) (any, error) {
	if err := h.Backend.DeleteUsageReportSubscription(); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opDescribeUsageReportSubscriptions(_ context.Context, _ []byte) (any, error) {
	subs, err := h.Backend.DescribeUsageReportSubscriptions()
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(subs))
	for _, s := range subs {
		resp = append(resp, map[string]any{
			"S3BucketName": s.S3BucketName,
			"Schedule":     s.Schedule,
		})
	}

	return map[string]any{"UsageReportSubscriptions": resp}, nil
}

// --- Theme handlers ---

type themeStackInput struct {
	StackName string `json:"StackName"`
}

func (h *Handler) opCreateThemeForStack(_ context.Context, body []byte) (any, error) {
	var req themeStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	th, err := h.Backend.CreateThemeForStack(req.StackName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Theme": themeToResponse(th)}, nil //nolint:goconst // existing issue.
}

func (h *Handler) opDeleteThemeForStack(_ context.Context, body []byte) (any, error) {
	var req themeStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteThemeForStack(req.StackName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opDescribeThemeForStack(_ context.Context, body []byte) (any, error) {
	var req themeStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	th, err := h.Backend.DescribeThemeForStack(req.StackName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Theme": themeToResponse(th)}, nil
}

func (h *Handler) opUpdateThemeForStack(_ context.Context, body []byte) (any, error) {
	var req themeStackInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	th, err := h.Backend.UpdateThemeForStack(req.StackName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Theme": themeToResponse(th)}, nil
}

// --- Response helpers ---

func userToResponse(u *User) map[string]any {
	return map[string]any{
		"UserName":           u.UserName,
		"Arn":                u.Arn, //nolint:goconst // existing issue.
		"Email":              u.Email,
		"FirstName":          u.FirstName,
		"LastName":           u.LastName,
		"AuthenticationType": u.AuthenticationType,
		"Status":             u.Status,
		"Enabled":            u.Enabled,
		"CreatedTime":        awstime.Epoch(u.CreatedTime), //nolint:goconst // existing issue.
	}
}

func sessionToResponse(s *Session) map[string]any {
	return map[string]any{
		"Id":                 s.ID,
		"FleetName":          s.FleetName,
		"StackName":          s.StackName,
		"UserId":             s.UserID,
		"State":              s.State, //nolint:goconst // existing issue.
		"ConnectionState":    s.ConnectionState,
		"AuthenticationType": s.AuthenticationType,
		"StartTime":          awstime.Epoch(s.StartTime),
	}
}

func themeToResponse(th *Theme) map[string]any {
	return map[string]any{
		"StackName":   th.StackName,
		"State":       th.State,
		"CreatedTime": awstime.Epoch(th.CreatedTime),
	}
}
