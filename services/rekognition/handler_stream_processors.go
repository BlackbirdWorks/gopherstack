package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) streamProcessorOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateStreamProcessor":   service.WrapOp(h.handleCreateStreamProcessor),
		"DeleteStreamProcessor":   service.WrapOp(h.handleDeleteStreamProcessor),
		"DescribeStreamProcessor": service.WrapOp(h.handleDescribeStreamProcessor),
		"ListStreamProcessors":    service.WrapOp(h.handleListStreamProcessors),
		"StartStreamProcessor":    service.WrapOp(h.handleStartStreamProcessor),
		"StopStreamProcessor":     service.WrapOp(h.handleStopStreamProcessor),
		"UpdateStreamProcessor":   service.WrapOp(h.handleUpdateStreamProcessor),
	}
}

// --- Wire shapes shared by Create/DescribeStreamProcessor. Field names and
// nesting mirror aws-sdk-go-v2/service/rekognition/types.{StreamProcessorInput,
// StreamProcessorOutput,StreamProcessorSettings,StreamProcessorSettingsForUpdate,
// RegionOfInterest,BoundingBox,Point,StreamProcessorNotificationChannel,
// StreamProcessorDataSharingPreference} exactly (verified against that
// package's awsAwsjson11 serializers/deserializers). ---

type kinesisVideoStreamWire struct {
	Arn string `json:"Arn"`
}

type kinesisDataStreamWire struct {
	Arn string `json:"Arn"`
}

type s3DestinationWire struct {
	Bucket    string `json:"Bucket"`
	KeyPrefix string `json:"KeyPrefix"`
}

type streamProcessorInputWire struct {
	KinesisVideoStream *kinesisVideoStreamWire `json:"KinesisVideoStream"`
}

type streamProcessorOutputWire struct {
	KinesisDataStream *kinesisDataStreamWire `json:"KinesisDataStream"`
	S3Destination     *s3DestinationWire     `json:"S3Destination"`
}

type connectedHomeSettingsWire struct {
	MinConfidence *float32 `json:"MinConfidence,omitempty"`
	Labels        []string `json:"Labels"`
}

type faceSearchSettingsWire struct {
	FaceMatchThreshold *float32 `json:"FaceMatchThreshold,omitempty"`
	CollectionID       string   `json:"CollectionId"`
}

type streamProcessorSettingsWire struct {
	ConnectedHome *connectedHomeSettingsWire `json:"ConnectedHome"`
	FaceSearch    *faceSearchSettingsWire    `json:"FaceSearch"`
}

type pointWire struct {
	X *float32 `json:"X,omitempty"`
	Y *float32 `json:"Y,omitempty"`
}

type boundingBoxWire struct {
	Height *float32 `json:"Height,omitempty"`
	Left   *float32 `json:"Left,omitempty"`
	Top    *float32 `json:"Top,omitempty"`
	Width  *float32 `json:"Width,omitempty"`
}

type regionOfInterestWire struct {
	BoundingBox *boundingBoxWire `json:"BoundingBox"`
	Polygon     []pointWire      `json:"Polygon"`
}

type notificationChannelWire struct {
	SNSTopicArn string `json:"SNSTopicArn"`
}

type dataSharingPreferenceWire struct {
	OptIn bool `json:"OptIn"`
}

func (w *streamProcessorInputWire) toDomain() *StreamProcessorInput {
	if w == nil {
		return nil
	}

	out := &StreamProcessorInput{}
	if w.KinesisVideoStream != nil {
		out.KinesisVideoStreamARN = w.KinesisVideoStream.Arn
	}

	return out
}

func streamProcessorInputFromDomain(v *StreamProcessorInput) *streamProcessorInputWire {
	if v == nil {
		return nil
	}

	return &streamProcessorInputWire{KinesisVideoStream: &kinesisVideoStreamWire{Arn: v.KinesisVideoStreamARN}}
}

func (w *streamProcessorOutputWire) toDomain() *StreamProcessorOutput {
	if w == nil {
		return nil
	}

	out := &StreamProcessorOutput{}
	if w.KinesisDataStream != nil {
		out.KinesisDataStreamARN = w.KinesisDataStream.Arn
	}

	if w.S3Destination != nil {
		out.S3Bucket = w.S3Destination.Bucket
		out.S3KeyPrefix = w.S3Destination.KeyPrefix
	}

	return out
}

func streamProcessorOutputFromDomain(v *StreamProcessorOutput) *streamProcessorOutputWire {
	if v == nil {
		return nil
	}

	out := &streamProcessorOutputWire{}
	if v.KinesisDataStreamARN != "" {
		out.KinesisDataStream = &kinesisDataStreamWire{Arn: v.KinesisDataStreamARN}
	}

	if v.S3Bucket != "" || v.S3KeyPrefix != "" {
		out.S3Destination = &s3DestinationWire{Bucket: v.S3Bucket, KeyPrefix: v.S3KeyPrefix}
	}

	return out
}

func (w *streamProcessorSettingsWire) toDomain() *StreamProcessorSettings {
	if w == nil {
		return nil
	}

	out := &StreamProcessorSettings{}
	if w.ConnectedHome != nil {
		out.ConnectedHomeLabels = w.ConnectedHome.Labels
		out.ConnectedHomeMinConfidence = w.ConnectedHome.MinConfidence
	}

	if w.FaceSearch != nil {
		out.FaceSearchCollectionID = w.FaceSearch.CollectionID
		out.FaceSearchFaceMatchThreshold = w.FaceSearch.FaceMatchThreshold
	}

	return out
}

func streamProcessorSettingsFromDomain(v *StreamProcessorSettings) *streamProcessorSettingsWire {
	if v == nil {
		return nil
	}

	out := &streamProcessorSettingsWire{}
	if v.ConnectedHomeLabels != nil || v.ConnectedHomeMinConfidence != nil {
		out.ConnectedHome = &connectedHomeSettingsWire{
			Labels:        v.ConnectedHomeLabels,
			MinConfidence: v.ConnectedHomeMinConfidence,
		}
	}

	if v.FaceSearchCollectionID != "" || v.FaceSearchFaceMatchThreshold != nil {
		out.FaceSearch = &faceSearchSettingsWire{
			CollectionID:       v.FaceSearchCollectionID,
			FaceMatchThreshold: v.FaceSearchFaceMatchThreshold,
		}
	}

	return out
}

func regionsOfInterestToDomain(w []regionOfInterestWire) []RegionOfInterest {
	if w == nil {
		return nil
	}

	out := make([]RegionOfInterest, 0, len(w))
	for _, r := range w {
		roi := RegionOfInterest{}
		if r.BoundingBox != nil {
			roi.BoundingBox = &BoundingBox{
				Height: r.BoundingBox.Height,
				Left:   r.BoundingBox.Left,
				Top:    r.BoundingBox.Top,
				Width:  r.BoundingBox.Width,
			}
		}

		for _, p := range r.Polygon {
			roi.Polygon = append(roi.Polygon, Point(p))
		}

		out = append(out, roi)
	}

	return out
}

func regionsOfInterestFromDomain(v []RegionOfInterest) []regionOfInterestWire {
	if v == nil {
		return nil
	}

	out := make([]regionOfInterestWire, 0, len(v))
	for _, roi := range v {
		w := regionOfInterestWire{}
		if roi.BoundingBox != nil {
			w.BoundingBox = &boundingBoxWire{
				Height: roi.BoundingBox.Height,
				Left:   roi.BoundingBox.Left,
				Top:    roi.BoundingBox.Top,
				Width:  roi.BoundingBox.Width,
			}
		}

		for _, p := range roi.Polygon {
			w.Polygon = append(w.Polygon, pointWire(p))
		}

		out = append(out, w)
	}

	return out
}

func (w *notificationChannelWire) toDomain() *StreamProcessorNotificationChannel {
	if w == nil {
		return nil
	}

	return &StreamProcessorNotificationChannel{SNSTopicARN: w.SNSTopicArn}
}

func notificationChannelFromDomain(v *StreamProcessorNotificationChannel) *notificationChannelWire {
	if v == nil {
		return nil
	}

	return &notificationChannelWire{SNSTopicArn: v.SNSTopicARN}
}

func (w *dataSharingPreferenceWire) toDomain() *StreamProcessorDataSharingPreference {
	if w == nil {
		return nil
	}

	return &StreamProcessorDataSharingPreference{OptIn: w.OptIn}
}

func dataSharingPreferenceFromDomain(v *StreamProcessorDataSharingPreference) *dataSharingPreferenceWire {
	if v == nil {
		return nil
	}

	return &dataSharingPreferenceWire{OptIn: v.OptIn}
}

// --- Stream processor requests ---

type createStreamProcessorReq struct {
	DataSharingPreference *dataSharingPreferenceWire   `json:"DataSharingPreference"`
	Input                 *streamProcessorInputWire    `json:"Input"`
	Output                *streamProcessorOutputWire   `json:"Output"`
	NotificationChannel   *notificationChannelWire     `json:"NotificationChannel"`
	Settings              *streamProcessorSettingsWire `json:"Settings"`
	Tags                  map[string]string            `json:"Tags"`
	Name                  string                       `json:"Name"`
	RoleArn               string                       `json:"RoleArn"`
	KmsKeyID              string                       `json:"KmsKeyId"`
	RegionsOfInterest     []regionOfInterestWire       `json:"RegionsOfInterest"`
}

type createStreamProcessorResp struct {
	StreamProcessorArn string `json:"StreamProcessorArn"`
}

func (h *Handler) handleCreateStreamProcessor(
	_ context.Context,
	req *createStreamProcessorReq,
) (*createStreamProcessorResp, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	params := CreateStreamProcessorParams{
		Input:                 req.Input.toDomain(),
		Output:                req.Output.toDomain(),
		Settings:              req.Settings.toDomain(),
		NotificationChannel:   req.NotificationChannel.toDomain(),
		DataSharingPreference: req.DataSharingPreference.toDomain(),
		RegionsOfInterest:     regionsOfInterestToDomain(req.RegionsOfInterest),
		KmsKeyID:              req.KmsKeyID,
	}

	proc, err := h.Backend.CreateStreamProcessor(req.Name, req.RoleArn, params, req.Tags)
	if err != nil {
		return nil, err
	}

	return &createStreamProcessorResp{StreamProcessorArn: proc.StreamProcessorARN}, nil
}

type deleteStreamProcessorReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteStreamProcessor(_ context.Context, req *deleteStreamProcessorReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.DeleteStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeStreamProcessorReq struct {
	Name string `json:"Name"`
}

type describeStreamProcessorResp struct {
	DataSharingPreference *dataSharingPreferenceWire   `json:"DataSharingPreference,omitempty"`
	Input                 *streamProcessorInputWire    `json:"Input,omitempty"`
	NotificationChannel   *notificationChannelWire     `json:"NotificationChannel,omitempty"`
	Output                *streamProcessorOutputWire   `json:"Output,omitempty"`
	Settings              *streamProcessorSettingsWire `json:"Settings,omitempty"`
	Name                  string                       `json:"Name"`
	RoleArn               string                       `json:"RoleArn"`
	Status                string                       `json:"Status"`
	StatusMessage         string                       `json:"StatusMessage,omitempty"`
	StreamProcessorArn    string                       `json:"StreamProcessorArn"`
	KmsKeyID              string                       `json:"KmsKeyId,omitempty"`
	RegionsOfInterest     []regionOfInterestWire       `json:"RegionsOfInterest,omitempty"`
	CreationTimestamp     float64                      `json:"CreationTimestamp"`
	LastUpdateTimestamp   float64                      `json:"LastUpdateTimestamp"`
}

func (h *Handler) handleDescribeStreamProcessor(
	_ context.Context,
	req *describeStreamProcessorReq,
) (*describeStreamProcessorResp, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	proc, err := h.Backend.DescribeStreamProcessor(req.Name)
	if err != nil {
		return nil, err
	}

	return &describeStreamProcessorResp{
		CreationTimestamp:     epochSeconds(proc.CreationTimestamp),
		LastUpdateTimestamp:   epochSeconds(proc.LastUpdateTimestamp),
		Name:                  proc.Name,
		RoleArn:               proc.RoleARN,
		Status:                proc.Status,
		StatusMessage:         proc.StatusMessage,
		StreamProcessorArn:    proc.StreamProcessorARN,
		KmsKeyID:              proc.KmsKeyID,
		Input:                 streamProcessorInputFromDomain(proc.Input),
		Output:                streamProcessorOutputFromDomain(proc.Output),
		Settings:              streamProcessorSettingsFromDomain(proc.Settings),
		RegionsOfInterest:     regionsOfInterestFromDomain(proc.RegionsOfInterest),
		NotificationChannel:   notificationChannelFromDomain(proc.NotificationChannel),
		DataSharingPreference: dataSharingPreferenceFromDomain(proc.DataSharingPreference),
	}, nil
}

type listStreamProcessorsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type streamProcessorEntry struct {
	Name   string `json:"Name"`
	Status string `json:"Status"`
}

type listStreamProcessorsResp struct {
	NextToken        string                 `json:"NextToken,omitempty"`
	StreamProcessors []streamProcessorEntry `json:"StreamProcessors"`
}

func (h *Handler) handleListStreamProcessors(
	_ context.Context,
	req *listStreamProcessorsReq,
) (*listStreamProcessorsResp, error) {
	procs, nextToken, err := h.Backend.ListStreamProcessors(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]streamProcessorEntry, 0, len(procs))

	for _, p := range procs {
		entries = append(entries, streamProcessorEntry{
			Name:   p.Name,
			Status: p.Status,
		})
	}

	return &listStreamProcessorsResp{
		NextToken:        nextToken,
		StreamProcessors: entries,
	}, nil
}

type streamProcessorNameReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartStreamProcessor(_ context.Context, req *streamProcessorNameReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.StartStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopStreamProcessor(_ context.Context, req *streamProcessorNameReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.StopStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type connectedHomeForUpdateWire struct {
	MinConfidence *float32 `json:"MinConfidence"`
	Labels        []string `json:"Labels"`
}

type settingsForUpdateWire struct {
	ConnectedHomeForUpdate *connectedHomeForUpdateWire `json:"ConnectedHomeForUpdate"`
}

type updateStreamProcessorReq struct {
	DataSharingPreferenceForUpdate *dataSharingPreferenceWire `json:"DataSharingPreferenceForUpdate"`
	SettingsForUpdate              *settingsForUpdateWire     `json:"SettingsForUpdate"`
	Name                           string                     `json:"Name"`
	ParametersToDelete             []string                   `json:"ParametersToDelete"`
	RegionsOfInterestForUpdate     []regionOfInterestWire     `json:"RegionsOfInterestForUpdate"`
}

func (h *Handler) handleUpdateStreamProcessor(
	_ context.Context,
	req *updateStreamProcessorReq,
) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	params := UpdateStreamProcessorParams{
		DataSharingPreference: req.DataSharingPreferenceForUpdate.toDomain(),
		ParametersToDelete:    req.ParametersToDelete,
		RegionsOfInterest:     regionsOfInterestToDomain(req.RegionsOfInterestForUpdate),
	}

	if req.SettingsForUpdate != nil && req.SettingsForUpdate.ConnectedHomeForUpdate != nil {
		params.ConnectedHomeLabels = req.SettingsForUpdate.ConnectedHomeForUpdate.Labels
		params.ConnectedHomeMinConfidence = req.SettingsForUpdate.ConnectedHomeForUpdate.MinConfidence
	}

	if err := h.Backend.UpdateStreamProcessor(req.Name, params); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
