package codedeploy

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// s3LocationEntry is the wire format for an S3 deployment revision.
type s3LocationEntry struct {
	Bucket     string `json:"bucket,omitempty"`
	Key        string `json:"key,omitempty"`
	BundleType string `json:"bundleType,omitempty"`
	ETag       string `json:"eTag,omitempty"`
	Version    string `json:"version,omitempty"`
}

// gitHubLocationEntry is the wire format for a GitHub deployment revision.
type gitHubLocationEntry struct {
	Repository string `json:"repository,omitempty"`
	CommitID   string `json:"commitId,omitempty"`
}

// appSpecContentEntry is the wire format for an AppSpecContent revision.
type appSpecContentEntry struct {
	Content string `json:"content,omitempty"`
	Sha256  string `json:"sha256,omitempty"`
}

type revisionLocationInput struct {
	S3Location     *s3LocationEntry     `json:"s3Location,omitempty"`
	GitHubLocation *gitHubLocationEntry `json:"gitHubLocation,omitempty"`
	AppSpecContent *appSpecContentEntry `json:"appSpecContent,omitempty"`
	RawString      *appSpecContentEntry `json:"string,omitempty"`
	RevisionType   string               `json:"revisionType"`
}

func appSpecFromWire(e *appSpecContentEntry) *RevisionAppSpecContent {
	if e == nil {
		return nil
	}

	return &RevisionAppSpecContent{Content: e.Content, Sha256: e.Sha256}
}

func appSpecToWire(c *RevisionAppSpecContent) *appSpecContentEntry {
	if c == nil {
		return nil
	}

	return &appSpecContentEntry{Content: c.Content, Sha256: c.Sha256}
}

// revisionFromWire converts a wire revisionLocationInput to a backend RevisionLocation.
func revisionFromWire(r *revisionLocationInput) *RevisionLocation {
	if r == nil {
		return nil
	}

	out := &RevisionLocation{RevisionType: r.RevisionType}

	if r.S3Location != nil {
		out.S3Location = &RevisionS3Location{
			Bucket:     r.S3Location.Bucket,
			Key:        r.S3Location.Key,
			BundleType: r.S3Location.BundleType,
			ETag:       r.S3Location.ETag,
			Version:    r.S3Location.Version,
		}
	}

	if r.GitHubLocation != nil {
		out.GitHubLocation = &RevisionGitHubLocation{
			Repository: r.GitHubLocation.Repository,
			CommitID:   r.GitHubLocation.CommitID,
		}
	}

	out.AppSpecContent = appSpecFromWire(r.AppSpecContent)
	out.RawString = appSpecFromWire(r.RawString)

	return out
}

// revisionToWire converts a backend RevisionLocation to the wire revisionLocationInput.
func revisionToWire(r *RevisionLocation) *revisionLocationInput {
	if r == nil {
		return nil
	}

	out := &revisionLocationInput{RevisionType: r.RevisionType}

	if r.S3Location != nil {
		out.S3Location = &s3LocationEntry{
			Bucket:     r.S3Location.Bucket,
			Key:        r.S3Location.Key,
			BundleType: r.S3Location.BundleType,
			ETag:       r.S3Location.ETag,
			Version:    r.S3Location.Version,
		}
	}

	if r.GitHubLocation != nil {
		out.GitHubLocation = &gitHubLocationEntry{
			Repository: r.GitHubLocation.Repository,
			CommitID:   r.GitHubLocation.CommitID,
		}
	}

	out.AppSpecContent = appSpecToWire(r.AppSpecContent)
	out.RawString = appSpecToWire(r.RawString)

	return out
}

type createDeploymentInput struct {
	Revision                      *revisionLocationInput   `json:"revision"`
	AutoRollbackConfiguration     *autoRollbackConfigEntry `json:"autoRollbackConfiguration,omitempty"`
	OverrideAlarmConfiguration    *alarmConfigEntry        `json:"overrideAlarmConfiguration,omitempty"`
	TargetInstances               *targetInstancesEntry    `json:"targetInstances,omitempty"`
	ApplicationName               string                   `json:"applicationName"`
	DeploymentGroupName           string                   `json:"deploymentGroupName"`
	Description                   string                   `json:"description"`
	FileExistsBehavior            string                   `json:"fileExistsBehavior"`
	DeploymentConfigName          string                   `json:"deploymentConfigName"`
	UpdateOutdatedInstancesOnly   bool                     `json:"updateOutdatedInstancesOnly"`
	IgnoreApplicationStopFailures bool                     `json:"ignoreApplicationStopFailures"`
}

// targetInstancesEntry is the wire format for a blue/green replacement environment.
type targetInstancesEntry struct {
	Ec2TagSet         *ec2TagSetEntry  `json:"ec2TagSet,omitempty"`
	AutoScalingGroups []string         `json:"autoScalingGroups,omitempty"`
	TagFilters        []tagFilterEntry `json:"tagFilters,omitempty"`
}

func targetInstancesFromWire(t *targetInstancesEntry) *TargetInstances {
	if t == nil {
		return nil
	}

	out := &TargetInstances{
		AutoScalingGroups: t.AutoScalingGroups,
		Ec2TagSet:         dgEc2TagSetFromWire(t.Ec2TagSet),
	}
	for _, f := range t.TagFilters {
		out.TagFilters = append(out.TagFilters, TagFilter(f))
	}

	return out
}

func targetInstancesToWire(t *TargetInstances) *targetInstancesEntry {
	if t == nil {
		return nil
	}

	out := &targetInstancesEntry{
		AutoScalingGroups: t.AutoScalingGroups,
		Ec2TagSet:         dgEc2TagSetToOutput(t.Ec2TagSet),
	}
	for _, f := range t.TagFilters {
		out.TagFilters = append(out.TagFilters, tagFilterEntry(f))
	}

	return out
}

// toDeploymentInfo renders d as the DeploymentInfo shared by GetDeployment and BatchGetDeployments.
func toDeploymentInfo(d *Deployment) deploymentInfo {
	info := deploymentInfo{
		DeploymentID:                  d.DeploymentID,
		ApplicationName:               d.ApplicationName,
		DeploymentGroupName:           d.DeploymentGroupName,
		DeploymentConfigName:          d.DeploymentConfigName,
		Status:                        d.Status,
		Creator:                       d.Creator,
		CreateTime:                    awstime.Epoch(d.CreateTime),
		Description:                   d.Description,
		FileExistsBehavior:            d.FileExistsBehavior,
		UpdateOutdatedInstancesOnly:   d.UpdateOutdatedInstancesOnly,
		IgnoreApplicationStopFailures: d.IgnoreApplicationStopFailures,
		Revision:                      revisionToWire(d.Revision),
		DeploymentOverview:            deploymentOverviewForStatus(d.Status),
		AutoRollbackConfiguration:     dgAutoRollbackConfigToOutput(d.AutoRollbackConfiguration),
		OverrideAlarmConfiguration:    dgAlarmConfigToOutput(d.OverrideAlarmConfiguration),
		TargetInstances:               targetInstancesToWire(d.TargetInstances),
		DeploymentStyle:               dgDeploymentStyleToOutput(d.DeploymentStyle),
		ComputePlatform:               d.ComputePlatform,
	}

	if d.CompleteTime != nil {
		ct := awstime.Epoch(*d.CompleteTime)
		info.CompleteTime = &ct
	}

	st := awstime.Epoch(d.CreateTime)
	info.StartTime = &st

	return info
}

type createDeploymentOutput struct {
	DeploymentID string `json:"deploymentId"`
}

func (h *Handler) handleCreateDeployment(
	_ context.Context,
	in *createDeploymentInput,
) (*createDeploymentOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", ErrApplicationNameRequired)
	}

	if in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: deploymentGroupName is required", ErrDeploymentGroupNameRequired)
	}

	opts := DeploymentOptions{
		Description:                   in.Description,
		FileExistsBehavior:            in.FileExistsBehavior,
		DeploymentConfigName:          in.DeploymentConfigName,
		UpdateOutdatedInstancesOnly:   in.UpdateOutdatedInstancesOnly,
		IgnoreApplicationStopFailures: in.IgnoreApplicationStopFailures,
		Revision:                      revisionFromWire(in.Revision),
		AutoRollbackConfiguration:     dgAutoRollbackConfigFromWire(in.AutoRollbackConfiguration),
		OverrideAlarmConfiguration:    dgAlarmConfigFromWire(in.OverrideAlarmConfiguration),
		TargetInstances:               targetInstancesFromWire(in.TargetInstances),
	}

	d, err := h.Backend.CreateDeployment(in.ApplicationName, in.DeploymentGroupName, opts)
	if err != nil {
		return nil, err
	}

	return &createDeploymentOutput{DeploymentID: d.DeploymentID}, nil
}

type getDeploymentInput struct {
	DeploymentID string `json:"deploymentId"`
}

// deploymentOverview holds a summary of instance counts for a deployment.
type deploymentOverview struct {
	Pending    int64 `json:"Pending"`
	InProgress int64 `json:"InProgress"`
	Succeeded  int64 `json:"Succeeded"`
	Failed     int64 `json:"Failed"`
	Skipped    int64 `json:"Skipped"`
	Ready      int64 `json:"Ready"`
}

type deploymentInfo struct {
	DeploymentOverview            *deploymentOverview      `json:"deploymentOverview,omitempty"`
	Revision                      *revisionLocationInput   `json:"revision,omitempty"`
	AutoRollbackConfiguration     *autoRollbackConfigEntry `json:"autoRollbackConfiguration,omitempty"`
	OverrideAlarmConfiguration    *alarmConfigEntry        `json:"overrideAlarmConfiguration,omitempty"`
	TargetInstances               *targetInstancesEntry    `json:"targetInstances,omitempty"`
	DeploymentStyle               *deploymentStyleEntry    `json:"deploymentStyle,omitempty"`
	CompleteTime                  *float64                 `json:"completeTime,omitempty"`
	StartTime                     *float64                 `json:"startTime,omitempty"`
	ComputePlatform               string                   `json:"computePlatform,omitempty"`
	DeploymentID                  string                   `json:"deploymentId"`
	ApplicationName               string                   `json:"applicationName"`
	DeploymentGroupName           string                   `json:"deploymentGroupName"`
	DeploymentConfigName          string                   `json:"deploymentConfigName,omitempty"`
	Status                        string                   `json:"status"`
	Creator                       string                   `json:"creator"`
	Description                   string                   `json:"description,omitempty"`
	FileExistsBehavior            string                   `json:"fileExistsBehavior,omitempty"`
	CreateTime                    float64                  `json:"createTime"`
	UpdateOutdatedInstancesOnly   bool                     `json:"updateOutdatedInstancesOnly,omitempty"`
	IgnoreApplicationStopFailures bool                     `json:"ignoreApplicationStopFailures,omitempty"`
}

type getDeploymentOutput struct {
	DeploymentInfo deploymentInfo `json:"deploymentInfo"`
}

func (h *Handler) handleGetDeployment(
	_ context.Context,
	in *getDeploymentInput,
) (*getDeploymentOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", ErrDeploymentIDRequired)
	}

	d, err := h.Backend.GetDeployment(in.DeploymentID)
	if err != nil {
		return nil, err
	}

	info := toDeploymentInfo(d)

	return &getDeploymentOutput{DeploymentInfo: info}, nil
}

// timeRangeEntry is the wire format for a create-time range filter. The AWS
// json-1.1 protocol serializes Timestamp shapes as epoch-seconds JSON numbers
// (see smithytime.FormatEpochSeconds in the real SDK's serializers), not
// epoch milliseconds, so these must be float64 to preserve sub-second
// precision the same way awstime.Epoch does on the response side.
type timeRangeEntry struct {
	Start *float64 `json:"start,omitempty"`
	End   *float64 `json:"end,omitempty"`
}

// epochSecondsToTime converts a wire-format epoch-seconds value (as produced
// by smithytime.FormatEpochSeconds / awstime.Epoch) back into a time.Time.
func epochSecondsToTime(sec float64) time.Time {
	return time.Unix(0, int64(sec*float64(time.Second))).UTC()
}

type listDeploymentsInput struct {
	CreateTimeRange     *timeRangeEntry `json:"createTimeRange"`
	ApplicationName     string          `json:"applicationName"`
	DeploymentGroupName string          `json:"deploymentGroupName"`
	ExternalID          string          `json:"externalId"`
	NextToken           string          `json:"nextToken"`
	IncludeOnlyStatuses []string        `json:"includeOnlyStatuses"`
}

type listDeploymentsOutput struct {
	Deployments []string `json:"deployments"`
}

func (h *Handler) handleListDeployments(
	_ context.Context,
	in *listDeploymentsInput,
) (*listDeploymentsOutput, error) {
	if err := rejectNextToken(in.NextToken); err != nil {
		return nil, err
	}

	if err := h.validateListDeploymentsScope(in); err != nil {
		return nil, err
	}

	filter := DeploymentFilter{
		ApplicationName:     in.ApplicationName,
		DeploymentGroupName: in.DeploymentGroupName,
		ExternalID:          in.ExternalID,
		Statuses:            in.IncludeOnlyStatuses,
	}

	if in.CreateTimeRange != nil {
		if in.CreateTimeRange.Start != nil {
			t := epochSecondsToTime(*in.CreateTimeRange.Start)
			filter.CreateTimeStart = &t
		}
		if in.CreateTimeRange.End != nil {
			t := epochSecondsToTime(*in.CreateTimeRange.End)
			filter.CreateTimeEnd = &t
		}
	}

	return &listDeploymentsOutput{
		Deployments: h.Backend.ListDeployments(filter),
	}, nil
}

// validateListDeploymentsScope enforces ListDeploymentsInput's rule that
// applicationName and deploymentGroupName are specified together or not at all.
func (h *Handler) validateListDeploymentsScope(in *listDeploymentsInput) error {
	switch {
	case in.ApplicationName == "" && in.DeploymentGroupName == "":
		return nil
	case in.DeploymentGroupName == "":
		return fmt.Errorf("%w: deploymentGroupName is required with applicationName", ErrDeploymentGroupNameRequired)
	case in.ApplicationName == "":
		return fmt.Errorf("%w: applicationName is required with deploymentGroupName", ErrApplicationNameRequired)
	}

	if _, err := h.Backend.GetApplication(in.ApplicationName); err != nil {
		return err
	}

	_, err := h.Backend.GetDeploymentGroup(in.ApplicationName, in.DeploymentGroupName)

	return err
}

// deploymentOverviewForStatus returns a synthetic DeploymentOverview based on deployment status.
func deploymentOverviewForStatus(status string) *deploymentOverview {
	switch status {
	case statusSucceeded:
		return &deploymentOverview{Succeeded: 1}
	case "Failed":
		return &deploymentOverview{Failed: 1}
	case statusStopped:
		return &deploymentOverview{Skipped: 1}
	default:
		return &deploymentOverview{InProgress: 1}
	}
}

type stopDeploymentInput struct {
	DeploymentID        string `json:"deploymentId"`
	AutoRollbackEnabled bool   `json:"autoRollbackEnabled"`
}

type stopDeploymentOutput struct {
	Status        string `json:"status"`
	StatusMessage string `json:"statusMessage,omitempty"`
}

func (h *Handler) handleStopDeployment(
	_ context.Context,
	in *stopDeploymentInput,
) (*stopDeploymentOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", ErrDeploymentIDRequired)
	}

	if err := h.Backend.StopDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &stopDeploymentOutput{Status: stopStatusSucceeded, StatusMessage: stopStatusSucceededMessage}, nil
}

type skipWaitTimeInput struct {
	DeploymentID string `json:"deploymentId"`
}

type skipWaitTimeOutput struct{}

func (h *Handler) handleSkipWaitTimeForInstanceTermination(
	_ context.Context,
	in *skipWaitTimeInput,
) (*skipWaitTimeOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", ErrDeploymentIDRequired)
	}

	if _, err := h.Backend.GetDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &skipWaitTimeOutput{}, nil
}

type continueDeploymentInput struct {
	DeploymentID       string `json:"deploymentId"`
	DeploymentWaitType string `json:"deploymentWaitType"`
}

type continueDeploymentOutput struct{}

func (h *Handler) handleContinueDeployment(
	_ context.Context,
	in *continueDeploymentInput,
) (*continueDeploymentOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", ErrDeploymentIDRequired)
	}

	if in.DeploymentWaitType != "" &&
		in.DeploymentWaitType != waitTypeReadyWait &&
		in.DeploymentWaitType != waitTypeTerminationWait {
		return nil, fmt.Errorf(
			"%w: deploymentWaitType must be READY_WAIT or TERMINATION_WAIT", ErrInvalidDeploymentWaitType,
		)
	}

	if err := h.Backend.ContinueDeployment(in.DeploymentID); err != nil {
		return nil, err
	}

	return &continueDeploymentOutput{}, nil
}

type batchGetDeploymentsInput struct {
	DeploymentIDs []string `json:"deploymentIds"`
}

type batchGetDeploymentsOutput struct {
	DeploymentsInfo []deploymentInfo `json:"deploymentsInfo"`
}

func (h *Handler) handleBatchGetDeployments(
	_ context.Context,
	in *batchGetDeploymentsInput,
) (*batchGetDeploymentsOutput, error) {
	if len(in.DeploymentIDs) == 0 {
		return nil, fmt.Errorf("%w: deploymentIds is required", ErrDeploymentIDRequired)
	}

	deployments := h.Backend.BatchGetDeployments(in.DeploymentIDs)

	infos := make([]deploymentInfo, 0, len(deployments))
	for _, d := range deployments {
		info := toDeploymentInfo(d)

		infos = append(infos, info)
	}

	return &batchGetDeploymentsOutput{DeploymentsInfo: infos}, nil
}
