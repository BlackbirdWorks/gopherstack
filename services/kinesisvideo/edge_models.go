package kinesisvideo

import "time"

// EdgeSchedule mirrors types.ScheduleConfig.
type EdgeSchedule struct {
	ScheduleExpression string
	DurationInSeconds  int32
}

// EdgeMediaSource mirrors types.MediaSourceConfig.
type EdgeMediaSource struct {
	MediaURISecretARN string
	MediaURIType      string
}

// EdgeRecorder mirrors types.RecorderConfig.
type EdgeRecorder struct {
	MediaSource *EdgeMediaSource
	Schedule    *EdgeSchedule
}

// EdgeLocalSize mirrors types.LocalSizeConfig.
type EdgeLocalSize struct {
	StrategyOnFullSize    string
	MaxLocalMediaSizeInMB int32
}

// EdgeDeletion mirrors types.DeletionConfig.
type EdgeDeletion struct {
	DeleteAfterUpload    *bool
	EdgeRetentionInHours *int32
	LocalSize            *EdgeLocalSize
}

// EdgeUploader mirrors types.UploaderConfig.
type EdgeUploader struct {
	Schedule *EdgeSchedule
}

// EdgeConfig mirrors types.EdgeConfig.
type EdgeConfig struct {
	Recorder     *EdgeRecorder
	Deletion     *EdgeDeletion
	Uploader     *EdgeUploader
	HubDeviceARN string
}

// EdgeState is a stream's edge agent configuration plus its sync bookkeeping.
// StreamARN and StreamName are filled in on return, not stored.
type EdgeState struct {
	CreationTime    time.Time
	LastUpdatedTime time.Time
	StreamARN       string
	StreamName      string
	SyncStatus      string
	Config          EdgeConfig
}

// MediaStorage is a signaling channel's media storage configuration.
type MediaStorage struct {
	Status    string
	StreamARN string
}

// ChannelEndpoint is one protocol endpoint of a signaling channel.
type ChannelEndpoint struct {
	Protocol string
	Endpoint string
}

func ptrCopy[T any](p *T) *T {
	if p == nil {
		return nil
	}

	cp := *p

	return &cp
}

func (c EdgeConfig) clone() EdgeConfig {
	out := EdgeConfig{HubDeviceARN: c.HubDeviceARN}

	if c.Recorder != nil {
		out.Recorder = &EdgeRecorder{
			MediaSource: ptrCopy(c.Recorder.MediaSource),
			Schedule:    ptrCopy(c.Recorder.Schedule),
		}
	}

	if c.Deletion != nil {
		out.Deletion = &EdgeDeletion{
			DeleteAfterUpload:    ptrCopy(c.Deletion.DeleteAfterUpload),
			EdgeRetentionInHours: ptrCopy(c.Deletion.EdgeRetentionInHours),
			LocalSize:            ptrCopy(c.Deletion.LocalSize),
		}
	}

	if c.Uploader != nil {
		out.Uploader = &EdgeUploader{Schedule: ptrCopy(c.Uploader.Schedule)}
	}

	return out
}

func (e *EdgeState) clone() *EdgeState {
	if e == nil {
		return nil
	}

	cp := *e
	cp.Config = e.Config.clone()

	return &cp
}
