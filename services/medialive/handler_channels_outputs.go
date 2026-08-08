package medialive

// --- OutputSettings / OutputGroupSettings wire (de)serialization ---
//
// Mirrors aws-sdk-go-v2/service/medialive types.* field-for-field for the
// EncoderOutput.OutputSettings and OutputGroup.OutputGroupSettings unions --
// see interfaces.go's per-type doc comments for SDK file:line citations.
// Verified against v1.101.4, this repo's currently pinned version (go.mod).

// -- OutputLocationRef / shared leaf types --

type outputLocationRefOutput struct {
	DestinationRefID string `json:"destinationRefId,omitempty"`
}

func toOutputLocationRefOutput(l *OutputLocationRef) *outputLocationRefOutput {
	if l == nil {
		return nil
	}

	return &outputLocationRefOutput{DestinationRefID: l.DestinationRefID}
}

func extractOutputLocationRef(m map[string]any) *OutputLocationRef {
	raw, ok := m["destination"].(map[string]any)
	if !ok {
		return nil
	}

	return &OutputLocationRef{DestinationRefID: stringFromAny(raw["destinationRefId"])}
}

type outputAdditionalDestinationOutput struct {
	Destination *outputLocationRefOutput `json:"destination,omitempty"`
}

func toOutputAdditionalDestinationsOutput(ds []OutputAdditionalDestination) []outputAdditionalDestinationOutput {
	if len(ds) == 0 {
		return nil
	}

	out := make([]outputAdditionalDestinationOutput, 0, len(ds))
	for _, d := range ds {
		out = append(out, outputAdditionalDestinationOutput{Destination: toOutputLocationRefOutput(d.Destination)})
	}

	return out
}

func extractOutputAdditionalDestinations(raw []any) []OutputAdditionalDestination {
	out := make([]OutputAdditionalDestination, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, OutputAdditionalDestination{Destination: extractOutputLocationRef(m)})
	}

	return out
}

type captionLanguageMappingOutput struct {
	LanguageCode        string `json:"languageCode,omitempty"`
	LanguageDescription string `json:"languageDescription,omitempty"`
	CaptionChannel      int32  `json:"captionChannel,omitempty"`
}

func toCaptionLanguageMappingsOutput(ms []CaptionLanguageMapping) []captionLanguageMappingOutput {
	if len(ms) == 0 {
		return nil
	}

	out := make([]captionLanguageMappingOutput, 0, len(ms))
	for _, m := range ms {
		out = append(out, captionLanguageMappingOutput(m))
	}

	return out
}

func extractCaptionLanguageMappings(raw []any) []CaptionLanguageMapping {
	out := make([]CaptionLanguageMapping, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, CaptionLanguageMapping{
			CaptionChannel:      int32FromAny(m["captionChannel"]),
			LanguageCode:        stringFromAny(m["languageCode"]),
			LanguageDescription: stringFromAny(m["languageDescription"]),
		})
	}

	return out
}

type cmafIngestCaptionLanguageMappingOutput struct {
	LanguageCode   string `json:"languageCode,omitempty"`
	CaptionChannel int32  `json:"captionChannel,omitempty"`
}

func toCmafIngestCaptionLanguageMappingsOutput(
	ms []CmafIngestCaptionLanguageMapping,
) []cmafIngestCaptionLanguageMappingOutput {
	if len(ms) == 0 {
		return nil
	}

	out := make([]cmafIngestCaptionLanguageMappingOutput, 0, len(ms))
	for _, m := range ms {
		out = append(out, cmafIngestCaptionLanguageMappingOutput(m))
	}

	return out
}

func extractCmafIngestCaptionLanguageMappings(raw []any) []CmafIngestCaptionLanguageMapping {
	out := make([]CmafIngestCaptionLanguageMapping, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, CmafIngestCaptionLanguageMapping{
			CaptionChannel: int32FromAny(m["captionChannel"]),
			LanguageCode:   stringFromAny(m["languageCode"]),
		})
	}

	return out
}

// -- M2tsSettings (Archive/MediaConnectRouter/UDP container settings) --

type dvbNitSettingsOutput struct {
	NetworkName string `json:"networkName,omitempty"`
	NetworkID   int32  `json:"networkId,omitempty"`
	RepInterval int32  `json:"repInterval,omitempty"`
}

func toDvbNitSettingsOutput(d *DvbNitSettings) *dvbNitSettingsOutput {
	if d == nil {
		return nil
	}

	out := dvbNitSettingsOutput(*d)

	return &out
}

func extractDvbNitSettings(m map[string]any) *DvbNitSettings {
	raw, ok := m["dvbNitSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &DvbNitSettings{
		NetworkName: stringFromAny(raw["networkName"]),
		NetworkID:   int32FromAny(raw["networkId"]),
		RepInterval: int32FromAny(raw["repInterval"]),
	}
}

type dvbSdtSettingsOutput struct {
	OutputSdt           string `json:"outputSdt,omitempty"`
	ServiceName         string `json:"serviceName,omitempty"`
	ServiceProviderName string `json:"serviceProviderName,omitempty"`
	RepInterval         int32  `json:"repInterval,omitempty"`
}

func toDvbSdtSettingsOutput(d *DvbSdtSettings) *dvbSdtSettingsOutput {
	if d == nil {
		return nil
	}

	out := dvbSdtSettingsOutput(*d)

	return &out
}

func extractDvbSdtSettings(m map[string]any) *DvbSdtSettings {
	raw, ok := m["dvbSdtSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &DvbSdtSettings{
		OutputSdt:           stringFromAny(raw["outputSdt"]),
		ServiceName:         stringFromAny(raw["serviceName"]),
		ServiceProviderName: stringFromAny(raw["serviceProviderName"]),
		RepInterval:         int32FromAny(raw["repInterval"]),
	}
}

type dvbTdtSettingsOutput struct {
	RepInterval int32 `json:"repInterval,omitempty"`
}

func toDvbTdtSettingsOutput(d *DvbTdtSettings) *dvbTdtSettingsOutput {
	if d == nil {
		return nil
	}

	return &dvbTdtSettingsOutput{RepInterval: d.RepInterval}
}

func extractDvbTdtSettings(m map[string]any) *DvbTdtSettings {
	raw, ok := m["dvbTdtSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &DvbTdtSettings{RepInterval: int32FromAny(raw["repInterval"])}
}

//nolint:dupl // mirrors M2tsSettings field-for-field by design; see its doc comment
type m2tsSettingsOutput struct {
	DvbNitSettings                  *dvbNitSettingsOutput `json:"dvbNitSettings,omitempty"`
	DvbSdtSettings                  *dvbSdtSettingsOutput `json:"dvbSdtSettings,omitempty"`
	DvbTdtSettings                  *dvbTdtSettingsOutput `json:"dvbTdtSettings,omitempty"`
	AbsentInputAudioBehavior        string                `json:"absentInputAudioBehavior,omitempty"`
	Arib                            string                `json:"arib,omitempty"`
	AribCaptionsPid                 string                `json:"aribCaptionsPid,omitempty"`
	AribCaptionsPidControl          string                `json:"aribCaptionsPidControl,omitempty"`
	AudioBufferModel                string                `json:"audioBufferModel,omitempty"`
	AudioPids                       string                `json:"audioPids,omitempty"`
	AudioStreamType                 string                `json:"audioStreamType,omitempty"`
	BufferModel                     string                `json:"bufferModel,omitempty"`
	CcDescriptor                    string                `json:"ccDescriptor,omitempty"`
	DvbSubPids                      string                `json:"dvbSubPids,omitempty"`
	DvbTeletextPid                  string                `json:"dvbTeletextPid,omitempty"`
	Ebif                            string                `json:"ebif,omitempty"`
	EbpAudioInterval                string                `json:"ebpAudioInterval,omitempty"`
	EbpPlacement                    string                `json:"ebpPlacement,omitempty"`
	EcmPid                          string                `json:"ecmPid,omitempty"`
	EsRateInPes                     string                `json:"esRateInPes,omitempty"`
	EtvPlatformPid                  string                `json:"etvPlatformPid,omitempty"`
	EtvSignalPid                    string                `json:"etvSignalPid,omitempty"`
	Klv                             string                `json:"klv,omitempty"`
	KlvDataPids                     string                `json:"klvDataPids,omitempty"`
	NielsenID3Behavior              string                `json:"nielsenId3Behavior,omitempty"`
	PcrControl                      string                `json:"pcrControl,omitempty"`
	PcrPid                          string                `json:"pcrPid,omitempty"`
	PmtPid                          string                `json:"pmtPid,omitempty"`
	RateMode                        string                `json:"rateMode,omitempty"`
	Scte27Pids                      string                `json:"scte27Pids,omitempty"`
	Scte35Control                   string                `json:"scte35Control,omitempty"`
	Scte35Pid                       string                `json:"scte35Pid,omitempty"`
	SegmentationMarkers             string                `json:"segmentationMarkers,omitempty"`
	SegmentationStyle               string                `json:"segmentationStyle,omitempty"`
	TimedMetadataBehavior           string                `json:"timedMetadataBehavior,omitempty"`
	TimedMetadataPid                string                `json:"timedMetadataPid,omitempty"`
	VideoPid                        string                `json:"videoPid,omitempty"`
	FragmentTime                    float64               `json:"fragmentTime,omitempty"`
	NullPacketBitrate               float64               `json:"nullPacketBitrate,omitempty"`
	Scte35PrerollPullupMilliseconds float64               `json:"scte35PrerollPullupMilliseconds,omitempty"`
	SegmentationTime                float64               `json:"segmentationTime,omitempty"`
	AudioFramesPerPes               int32                 `json:"audioFramesPerPes,omitempty"`
	Bitrate                         int32                 `json:"bitrate,omitempty"`
	EbpLookaheadMs                  int32                 `json:"ebpLookaheadMs,omitempty"`
	PatInterval                     int32                 `json:"patInterval,omitempty"`
	PcrPeriod                       int32                 `json:"pcrPeriod,omitempty"`
	PmtInterval                     int32                 `json:"pmtInterval,omitempty"`
	ProgramNum                      int32                 `json:"programNum,omitempty"`
	TransportStreamID               int32                 `json:"transportStreamId,omitempty"`
}

func toM2tsSettingsOutput(s *M2tsSettings) *m2tsSettingsOutput {
	if s == nil {
		return nil
	}

	out := m2tsSettingsOutput{
		AbsentInputAudioBehavior: s.AbsentInputAudioBehavior, Arib: s.Arib,
		AribCaptionsPid: s.AribCaptionsPid, AribCaptionsPidControl: s.AribCaptionsPidControl,
		AudioBufferModel: s.AudioBufferModel, AudioPids: s.AudioPids, AudioStreamType: s.AudioStreamType,
		BufferModel: s.BufferModel, CcDescriptor: s.CcDescriptor, DvbSubPids: s.DvbSubPids,
		DvbTeletextPid: s.DvbTeletextPid, Ebif: s.Ebif, EbpAudioInterval: s.EbpAudioInterval,
		EbpPlacement: s.EbpPlacement, EcmPid: s.EcmPid, EsRateInPes: s.EsRateInPes,
		EtvPlatformPid: s.EtvPlatformPid, EtvSignalPid: s.EtvSignalPid, Klv: s.Klv,
		KlvDataPids: s.KlvDataPids, NielsenID3Behavior: s.NielsenID3Behavior, PcrControl: s.PcrControl,
		PcrPid: s.PcrPid, PmtPid: s.PmtPid, RateMode: s.RateMode, Scte27Pids: s.Scte27Pids,
		Scte35Control: s.Scte35Control, Scte35Pid: s.Scte35Pid, SegmentationMarkers: s.SegmentationMarkers,
		SegmentationStyle: s.SegmentationStyle, TimedMetadataBehavior: s.TimedMetadataBehavior,
		TimedMetadataPid: s.TimedMetadataPid, VideoPid: s.VideoPid, FragmentTime: s.FragmentTime,
		NullPacketBitrate: s.NullPacketBitrate, Scte35PrerollPullupMilliseconds: s.Scte35PrerollPullupMilliseconds,
		SegmentationTime: s.SegmentationTime, AudioFramesPerPes: s.AudioFramesPerPes, Bitrate: s.Bitrate,
		EbpLookaheadMs: s.EbpLookaheadMs, PatInterval: s.PatInterval, PcrPeriod: s.PcrPeriod,
		PmtInterval: s.PmtInterval, ProgramNum: s.ProgramNum, TransportStreamID: s.TransportStreamID,
		DvbNitSettings: toDvbNitSettingsOutput(s.DvbNitSettings),
		DvbSdtSettings: toDvbSdtSettingsOutput(s.DvbSdtSettings),
		DvbTdtSettings: toDvbTdtSettingsOutput(s.DvbTdtSettings),
	}

	return &out
}

func extractM2tsSettings(m map[string]any) *M2tsSettings {
	raw, ok := m["m2tsSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &M2tsSettings{
		DvbNitSettings:                  extractDvbNitSettings(raw),
		DvbSdtSettings:                  extractDvbSdtSettings(raw),
		DvbTdtSettings:                  extractDvbTdtSettings(raw),
		AbsentInputAudioBehavior:        stringFromAny(raw["absentInputAudioBehavior"]),
		Arib:                            stringFromAny(raw["arib"]),
		AribCaptionsPid:                 stringFromAny(raw["aribCaptionsPid"]),
		AribCaptionsPidControl:          stringFromAny(raw["aribCaptionsPidControl"]),
		AudioBufferModel:                stringFromAny(raw["audioBufferModel"]),
		AudioPids:                       stringFromAny(raw["audioPids"]),
		AudioStreamType:                 stringFromAny(raw["audioStreamType"]),
		BufferModel:                     stringFromAny(raw["bufferModel"]),
		CcDescriptor:                    stringFromAny(raw["ccDescriptor"]),
		DvbSubPids:                      stringFromAny(raw["dvbSubPids"]),
		DvbTeletextPid:                  stringFromAny(raw["dvbTeletextPid"]),
		Ebif:                            stringFromAny(raw["ebif"]),
		EbpAudioInterval:                stringFromAny(raw["ebpAudioInterval"]),
		EbpPlacement:                    stringFromAny(raw["ebpPlacement"]),
		EcmPid:                          stringFromAny(raw["ecmPid"]),
		EsRateInPes:                     stringFromAny(raw["esRateInPes"]),
		EtvPlatformPid:                  stringFromAny(raw["etvPlatformPid"]),
		EtvSignalPid:                    stringFromAny(raw["etvSignalPid"]),
		Klv:                             stringFromAny(raw["klv"]),
		KlvDataPids:                     stringFromAny(raw["klvDataPids"]),
		NielsenID3Behavior:              stringFromAny(raw["nielsenId3Behavior"]),
		PcrControl:                      stringFromAny(raw["pcrControl"]),
		PcrPid:                          stringFromAny(raw["pcrPid"]),
		PmtPid:                          stringFromAny(raw["pmtPid"]),
		RateMode:                        stringFromAny(raw["rateMode"]),
		Scte27Pids:                      stringFromAny(raw["scte27Pids"]),
		Scte35Control:                   stringFromAny(raw["scte35Control"]),
		Scte35Pid:                       stringFromAny(raw["scte35Pid"]),
		SegmentationMarkers:             stringFromAny(raw["segmentationMarkers"]),
		SegmentationStyle:               stringFromAny(raw["segmentationStyle"]),
		TimedMetadataBehavior:           stringFromAny(raw["timedMetadataBehavior"]),
		TimedMetadataPid:                stringFromAny(raw["timedMetadataPid"]),
		VideoPid:                        stringFromAny(raw["videoPid"]),
		FragmentTime:                    float64FromAny(raw["fragmentTime"]),
		NullPacketBitrate:               float64FromAny(raw["nullPacketBitrate"]),
		Scte35PrerollPullupMilliseconds: float64FromAny(raw["scte35PrerollPullupMilliseconds"]),
		SegmentationTime:                float64FromAny(raw["segmentationTime"]),
		AudioFramesPerPes:               int32FromAny(raw["audioFramesPerPes"]),
		Bitrate:                         int32FromAny(raw["bitrate"]),
		EbpLookaheadMs:                  int32FromAny(raw["ebpLookaheadMs"]),
		PatInterval:                     int32FromAny(raw["patInterval"]),
		PcrPeriod:                       int32FromAny(raw["pcrPeriod"]),
		PmtInterval:                     int32FromAny(raw["pmtInterval"]),
		ProgramNum:                      int32FromAny(raw["programNum"]),
		TransportStreamID:               int32FromAny(raw["transportStreamId"]),
	}
}

// -- ArchiveContainerSettings / ArchiveOutputSettings --

type archiveContainerSettingsOutput struct {
	M2tsSettings *m2tsSettingsOutput `json:"m2tsSettings,omitempty"`
	RawSettings  *emptyMarker        `json:"rawSettings,omitempty"`
}

func toArchiveContainerSettingsOutput(s *ArchiveContainerSettings) *archiveContainerSettingsOutput {
	if s == nil {
		return nil
	}

	out := &archiveContainerSettingsOutput{M2tsSettings: toM2tsSettingsOutput(s.M2tsSettings)}
	if s.RawSettings {
		out.RawSettings = &emptyMarker{}
	}

	return out
}

func extractArchiveContainerSettings(m map[string]any) *ArchiveContainerSettings {
	raw, ok := m["containerSettings"].(map[string]any)
	if !ok {
		return nil
	}

	_, hasRaw := raw["rawSettings"]

	return &ArchiveContainerSettings{M2tsSettings: extractM2tsSettings(raw), RawSettings: hasRaw}
}

type archiveOutputSettingsOutput struct {
	ContainerSettings *archiveContainerSettingsOutput `json:"containerSettings,omitempty"`
	Extension         string                          `json:"extension,omitempty"`
	NameModifier      string                          `json:"nameModifier,omitempty"`
}

func toArchiveOutputSettingsOutput(s *ArchiveOutputSettings) *archiveOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &archiveOutputSettingsOutput{
		ContainerSettings: toArchiveContainerSettingsOutput(s.ContainerSettings),
		Extension:         s.Extension,
		NameModifier:      s.NameModifier,
	}
}

func extractArchiveOutputSettings(m map[string]any) *ArchiveOutputSettings {
	raw, ok := m["archiveOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &ArchiveOutputSettings{
		ContainerSettings: extractArchiveContainerSettings(raw),
		Extension:         stringFromAny(raw["extension"]),
		NameModifier:      stringFromAny(raw["nameModifier"]),
	}
}

// -- FrameCapture (S3/CDN/output) --

type frameCaptureS3SettingsOutput struct {
	CannedACL string `json:"cannedAcl,omitempty"`
}

func toFrameCaptureCdnSettingsOutput(s *FrameCaptureCdnSettings) *frameCaptureCdnSettingsOutput {
	if s == nil {
		return nil
	}

	if s.FrameCaptureS3Settings == nil {
		return &frameCaptureCdnSettingsOutput{}
	}

	return &frameCaptureCdnSettingsOutput{
		FrameCaptureS3Settings: &frameCaptureS3SettingsOutput{CannedACL: s.FrameCaptureS3Settings.CannedACL},
	}
}

type frameCaptureCdnSettingsOutput struct {
	FrameCaptureS3Settings *frameCaptureS3SettingsOutput `json:"frameCaptureS3Settings,omitempty"`
}

func extractFrameCaptureCdnSettings(m map[string]any) *FrameCaptureCdnSettings {
	raw, ok := m["frameCaptureCdnSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &FrameCaptureCdnSettings{}

	if s3, hasS3 := raw["frameCaptureS3Settings"].(map[string]any); hasS3 {
		out.FrameCaptureS3Settings = &FrameCaptureS3Settings{CannedACL: stringFromAny(s3["cannedAcl"])}
	}

	return out
}

type frameCaptureOutputSettingsOutput struct {
	NameModifier string `json:"nameModifier,omitempty"`
}

func toFrameCaptureOutputSettingsOutput(s *FrameCaptureOutputSettings) *frameCaptureOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &frameCaptureOutputSettingsOutput{NameModifier: s.NameModifier}
}

func extractFrameCaptureOutputSettings(m map[string]any) *FrameCaptureOutputSettings {
	raw, ok := m["frameCaptureOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &FrameCaptureOutputSettings{NameModifier: stringFromAny(raw["nameModifier"])}
}

type cmafIngestOutputSettingsOutput struct {
	NameModifier string `json:"nameModifier,omitempty"`
}

func toCmafIngestOutputSettingsOutput(s *CmafIngestOutputSettings) *cmafIngestOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &cmafIngestOutputSettingsOutput{NameModifier: s.NameModifier}
}

func extractCmafIngestOutputSettings(m map[string]any) *CmafIngestOutputSettings {
	raw, ok := m["cmafIngestOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &CmafIngestOutputSettings{NameModifier: stringFromAny(raw["nameModifier"])}
}

// -- HLS stream settings (AudioOnly/Fmp4/FrameCapture/Standard + M3u8) --

type m3u8SettingsOutput struct {
	AudioPids             string `json:"audioPids,omitempty"`
	EcmPid                string `json:"ecmPid,omitempty"`
	KlvBehavior           string `json:"klvBehavior,omitempty"`
	KlvDataPids           string `json:"klvDataPids,omitempty"`
	NielsenID3Behavior    string `json:"nielsenId3Behavior,omitempty"`
	PcrControl            string `json:"pcrControl,omitempty"`
	PcrPid                string `json:"pcrPid,omitempty"`
	PmtPid                string `json:"pmtPid,omitempty"`
	Scte35Behavior        string `json:"scte35Behavior,omitempty"`
	Scte35Pid             string `json:"scte35Pid,omitempty"`
	TimedMetadataBehavior string `json:"timedMetadataBehavior,omitempty"`
	TimedMetadataPid      string `json:"timedMetadataPid,omitempty"`
	AudioFramesPerPes     int32  `json:"audioFramesPerPes,omitempty"`
	PatInterval           int32  `json:"patInterval,omitempty"`
	PcrPeriod             int32  `json:"pcrPeriod,omitempty"`
	PmtInterval           int32  `json:"pmtInterval,omitempty"`
	ProgramNum            int32  `json:"programNum,omitempty"`
	TransportStreamID     int32  `json:"transportStreamId,omitempty"`
}

func toM3u8SettingsOutput(s *M3u8Settings) *m3u8SettingsOutput {
	if s == nil {
		return nil
	}

	out := m3u8SettingsOutput(*s)

	return &out
}

func extractM3u8Settings(m map[string]any) *M3u8Settings {
	raw, ok := m["m3u8Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &M3u8Settings{
		AudioPids:             stringFromAny(raw["audioPids"]),
		EcmPid:                stringFromAny(raw["ecmPid"]),
		KlvBehavior:           stringFromAny(raw["klvBehavior"]),
		KlvDataPids:           stringFromAny(raw["klvDataPids"]),
		NielsenID3Behavior:    stringFromAny(raw["nielsenId3Behavior"]),
		PcrControl:            stringFromAny(raw["pcrControl"]),
		PcrPid:                stringFromAny(raw["pcrPid"]),
		PmtPid:                stringFromAny(raw["pmtPid"]),
		Scte35Behavior:        stringFromAny(raw["scte35Behavior"]),
		Scte35Pid:             stringFromAny(raw["scte35Pid"]),
		TimedMetadataBehavior: stringFromAny(raw["timedMetadataBehavior"]),
		TimedMetadataPid:      stringFromAny(raw["timedMetadataPid"]),
		AudioFramesPerPes:     int32FromAny(raw["audioFramesPerPes"]),
		PatInterval:           int32FromAny(raw["patInterval"]),
		PcrPeriod:             int32FromAny(raw["pcrPeriod"]),
		PmtInterval:           int32FromAny(raw["pmtInterval"]),
		ProgramNum:            int32FromAny(raw["programNum"]),
		TransportStreamID:     int32FromAny(raw["transportStreamId"]),
	}
}

type audioOnlyHlsSettingsOutput struct {
	AudioOnlyImage *inputLocationOutput `json:"audioOnlyImage,omitempty"`
	AudioGroupID   string               `json:"audioGroupId,omitempty"`
	AudioTrackType string               `json:"audioTrackType,omitempty"`
	SegmentType    string               `json:"segmentType,omitempty"`
}

func toAudioOnlyHlsSettingsOutput(s *AudioOnlyHlsSettings) *audioOnlyHlsSettingsOutput {
	if s == nil {
		return nil
	}

	return &audioOnlyHlsSettingsOutput{
		AudioOnlyImage: toInputLocationOutput(s.AudioOnlyImage),
		AudioGroupID:   s.AudioGroupID,
		AudioTrackType: s.AudioTrackType,
		SegmentType:    s.SegmentType,
	}
}

func extractAudioOnlyHlsSettings(m map[string]any) *AudioOnlyHlsSettings {
	raw, ok := m["audioOnlyHlsSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &AudioOnlyHlsSettings{
		AudioGroupID:   stringFromAny(raw["audioGroupId"]),
		AudioTrackType: stringFromAny(raw["audioTrackType"]),
		SegmentType:    stringFromAny(raw["segmentType"]),
		AudioOnlyImage: extractInputLocation(raw, "audioOnlyImage"),
	}
}

type fmp4HlsSettingsOutput struct {
	AudioRenditionSets    string `json:"audioRenditionSets,omitempty"`
	NielsenID3Behavior    string `json:"nielsenId3Behavior,omitempty"`
	TimedMetadataBehavior string `json:"timedMetadataBehavior,omitempty"`
}

func toFmp4HlsSettingsOutput(s *Fmp4HlsSettings) *fmp4HlsSettingsOutput {
	if s == nil {
		return nil
	}

	out := fmp4HlsSettingsOutput(*s)

	return &out
}

func extractFmp4HlsSettings(m map[string]any) *Fmp4HlsSettings {
	raw, ok := m["fmp4HlsSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Fmp4HlsSettings{
		AudioRenditionSets:    stringFromAny(raw["audioRenditionSets"]),
		NielsenID3Behavior:    stringFromAny(raw["nielsenId3Behavior"]),
		TimedMetadataBehavior: stringFromAny(raw["timedMetadataBehavior"]),
	}
}

type standardHlsSettingsOutput struct {
	M3u8Settings       *m3u8SettingsOutput `json:"m3u8Settings,omitempty"`
	AudioRenditionSets string              `json:"audioRenditionSets,omitempty"`
}

func toStandardHlsSettingsOutput(s *StandardHlsSettings) *standardHlsSettingsOutput {
	if s == nil {
		return nil
	}

	return &standardHlsSettingsOutput{
		M3u8Settings:       toM3u8SettingsOutput(s.M3u8Settings),
		AudioRenditionSets: s.AudioRenditionSets,
	}
}

func extractStandardHlsSettings(m map[string]any) *StandardHlsSettings {
	raw, ok := m["standardHlsSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &StandardHlsSettings{
		M3u8Settings:       extractM3u8Settings(raw),
		AudioRenditionSets: stringFromAny(raw["audioRenditionSets"]),
	}
}

type hlsSettingsOutput struct {
	AudioOnlyHlsSettings    *audioOnlyHlsSettingsOutput `json:"audioOnlyHlsSettings,omitempty"`
	Fmp4HlsSettings         *fmp4HlsSettingsOutput      `json:"fmp4HlsSettings,omitempty"`
	StandardHlsSettings     *standardHlsSettingsOutput  `json:"standardHlsSettings,omitempty"`
	FrameCaptureHlsSettings *emptyMarker                `json:"frameCaptureHlsSettings,omitempty"`
}

func toHlsSettingsOutput(s *HlsSettings) *hlsSettingsOutput {
	if s == nil {
		return nil
	}

	out := &hlsSettingsOutput{
		AudioOnlyHlsSettings: toAudioOnlyHlsSettingsOutput(s.AudioOnlyHlsSettings),
		Fmp4HlsSettings:      toFmp4HlsSettingsOutput(s.Fmp4HlsSettings),
		StandardHlsSettings:  toStandardHlsSettingsOutput(s.StandardHlsSettings),
	}
	if s.FrameCaptureHlsSettings {
		out.FrameCaptureHlsSettings = &emptyMarker{}
	}

	return out
}

func extractHlsSettings(m map[string]any) *HlsSettings {
	raw, ok := m["hlsSettings"].(map[string]any)
	if !ok {
		return nil
	}

	_, hasFrameCapture := raw["frameCaptureHlsSettings"]

	return &HlsSettings{
		AudioOnlyHlsSettings:    extractAudioOnlyHlsSettings(raw),
		Fmp4HlsSettings:         extractFmp4HlsSettings(raw),
		StandardHlsSettings:     extractStandardHlsSettings(raw),
		FrameCaptureHlsSettings: hasFrameCapture,
	}
}

type hlsOutputSettingsOutput struct {
	HlsSettings       *hlsSettingsOutput `json:"hlsSettings,omitempty"`
	H265PackagingType string             `json:"h265PackagingType,omitempty"`
	NameModifier      string             `json:"nameModifier,omitempty"`
	SegmentModifier   string             `json:"segmentModifier,omitempty"`
}

func toHlsOutputSettingsOutput(s *HlsOutputSettings) *hlsOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &hlsOutputSettingsOutput{
		HlsSettings:       toHlsSettingsOutput(s.HlsSettings),
		H265PackagingType: s.H265PackagingType,
		NameModifier:      s.NameModifier,
		SegmentModifier:   s.SegmentModifier,
	}
}

func extractHlsOutputSettings(m map[string]any) *HlsOutputSettings {
	raw, ok := m["hlsOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsOutputSettings{
		HlsSettings:       extractHlsSettings(raw),
		H265PackagingType: stringFromAny(raw["h265PackagingType"]),
		NameModifier:      stringFromAny(raw["nameModifier"]),
		SegmentModifier:   stringFromAny(raw["segmentModifier"]),
	}
}

// -- MediaConnectRouter / MediaPackage / MsSmooth output settings --

type mediaConnectRouterContainerSettingsOutput struct {
	M2tsSettings *m2tsSettingsOutput `json:"m2tsSettings,omitempty"`
}

func toMediaConnectRouterOutputSettingsOutput(
	s *MediaConnectRouterOutputSettings,
) *mcRouterOutputSettingsOutput {
	if s == nil {
		return nil
	}

	out := &mcRouterOutputSettingsOutput{Destination: toOutputLocationRefOutput(s.Destination)}
	if s.ContainerSettings != nil {
		out.ContainerSettings = &mediaConnectRouterContainerSettingsOutput{
			M2tsSettings: toM2tsSettingsOutput(s.ContainerSettings.M2tsSettings),
		}
	}

	return out
}

type mcRouterOutputSettingsOutput struct {
	ContainerSettings *mediaConnectRouterContainerSettingsOutput `json:"containerSettings,omitempty"`
	Destination       *outputLocationRefOutput                   `json:"destination,omitempty"`
}

func extractMediaConnectRouterOutputSettings(m map[string]any) *MediaConnectRouterOutputSettings {
	raw, ok := m["mediaConnectRouterOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &MediaConnectRouterOutputSettings{Destination: extractOutputLocationRef(raw)}

	if cs, hasCS := raw["containerSettings"].(map[string]any); hasCS {
		out.ContainerSettings = &MediaConnectRouterContainerSettings{M2tsSettings: extractM2tsSettings(cs)}
	}

	return out
}

type mpV2DestSettingsOutput struct {
	AudioGroupID       string `json:"audioGroupId,omitempty"`
	AudioRenditionSets string `json:"audioRenditionSets,omitempty"`
	HlsAutoSelect      string `json:"hlsAutoSelect,omitempty"`
	HlsDefault         string `json:"hlsDefault,omitempty"`
}

func toMediaPackageV2DestinationSettingsOutput(
	s *MediaPackageV2DestinationSettings,
) *mpV2DestSettingsOutput {
	if s == nil {
		return nil
	}

	out := mpV2DestSettingsOutput(*s)

	return &out
}

func extractMediaPackageV2DestinationSettings(m map[string]any) *MediaPackageV2DestinationSettings {
	raw, ok := m["mediaPackageV2DestinationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &MediaPackageV2DestinationSettings{
		AudioGroupID:       stringFromAny(raw["audioGroupId"]),
		AudioRenditionSets: stringFromAny(raw["audioRenditionSets"]),
		HlsAutoSelect:      stringFromAny(raw["hlsAutoSelect"]),
		HlsDefault:         stringFromAny(raw["hlsDefault"]),
	}
}

type mediaPackageOutputSettingsOutput struct {
	MediaPackageV2DestinationSettings *mpV2DestSettingsOutput `json:"mediaPackageV2DestinationSettings,omitempty"`
}

func toMediaPackageOutputSettingsOutput(s *MediaPackageOutputSettings) *mediaPackageOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &mediaPackageOutputSettingsOutput{
		MediaPackageV2DestinationSettings: toMediaPackageV2DestinationSettingsOutput(
			s.MediaPackageV2DestinationSettings,
		),
	}
}

func extractMediaPackageOutputSettings(m map[string]any) *MediaPackageOutputSettings {
	raw, ok := m["mediaPackageOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &MediaPackageOutputSettings{
		MediaPackageV2DestinationSettings: extractMediaPackageV2DestinationSettings(raw),
	}
}

type msSmoothOutputSettingsOutput struct {
	H265PackagingType string `json:"h265PackagingType,omitempty"`
	NameModifier      string `json:"nameModifier,omitempty"`
}

func toMsSmoothOutputSettingsOutput(s *MsSmoothOutputSettings) *msSmoothOutputSettingsOutput {
	if s == nil {
		return nil
	}

	out := msSmoothOutputSettingsOutput(*s)

	return &out
}

func extractMsSmoothOutputSettings(m map[string]any) *MsSmoothOutputSettings {
	raw, ok := m["msSmoothOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &MsSmoothOutputSettings{
		H265PackagingType: stringFromAny(raw["h265PackagingType"]),
		NameModifier:      stringFromAny(raw["nameModifier"]),
	}
}

// -- Multiplex / Rtmp / Srt / UDP output settings --

type multiplexM2tsSettingsOutput struct {
	AbsentInputAudioBehavior        string  `json:"absentInputAudioBehavior,omitempty"`
	Arib                            string  `json:"arib,omitempty"`
	AudioBufferModel                string  `json:"audioBufferModel,omitempty"`
	AudioStreamType                 string  `json:"audioStreamType,omitempty"`
	CcDescriptor                    string  `json:"ccDescriptor,omitempty"`
	Ebif                            string  `json:"ebif,omitempty"`
	EsRateInPes                     string  `json:"esRateInPes,omitempty"`
	Klv                             string  `json:"klv,omitempty"`
	NielsenID3Behavior              string  `json:"nielsenId3Behavior,omitempty"`
	PcrControl                      string  `json:"pcrControl,omitempty"`
	Scte35Control                   string  `json:"scte35Control,omitempty"`
	AudioFramesPerPes               int32   `json:"audioFramesPerPes,omitempty"`
	PcrPeriod                       int32   `json:"pcrPeriod,omitempty"`
	Scte35PrerollPullupMilliseconds float64 `json:"scte35PrerollPullupMilliseconds,omitempty"`
}

func toMultiplexM2tsSettingsOutput(s *MultiplexM2tsSettings) *multiplexM2tsSettingsOutput {
	if s == nil {
		return nil
	}

	out := multiplexM2tsSettingsOutput(*s)

	return &out
}

func extractMultiplexM2tsSettings(m map[string]any) *MultiplexM2tsSettings {
	raw, ok := m["multiplexM2tsSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &MultiplexM2tsSettings{
		AbsentInputAudioBehavior:        stringFromAny(raw["absentInputAudioBehavior"]),
		Arib:                            stringFromAny(raw["arib"]),
		AudioBufferModel:                stringFromAny(raw["audioBufferModel"]),
		AudioStreamType:                 stringFromAny(raw["audioStreamType"]),
		CcDescriptor:                    stringFromAny(raw["ccDescriptor"]),
		Ebif:                            stringFromAny(raw["ebif"]),
		EsRateInPes:                     stringFromAny(raw["esRateInPes"]),
		Klv:                             stringFromAny(raw["klv"]),
		NielsenID3Behavior:              stringFromAny(raw["nielsenId3Behavior"]),
		PcrControl:                      stringFromAny(raw["pcrControl"]),
		Scte35Control:                   stringFromAny(raw["scte35Control"]),
		AudioFramesPerPes:               int32FromAny(raw["audioFramesPerPes"]),
		PcrPeriod:                       int32FromAny(raw["pcrPeriod"]),
		Scte35PrerollPullupMilliseconds: float64FromAny(raw["scte35PrerollPullupMilliseconds"]),
	}
}

type multiplexContainerSettingsOutput struct {
	MultiplexM2tsSettings *multiplexM2tsSettingsOutput `json:"multiplexM2tsSettings,omitempty"`
}

func toMultiplexOutputSettingsOutput(s *MultiplexOutputSettings) *multiplexOutputSettingsOutput {
	if s == nil {
		return nil
	}

	out := &multiplexOutputSettingsOutput{Destination: toOutputLocationRefOutput(s.Destination)}
	if s.ContainerSettings != nil {
		out.ContainerSettings = &multiplexContainerSettingsOutput{
			MultiplexM2tsSettings: toMultiplexM2tsSettingsOutput(s.ContainerSettings.MultiplexM2tsSettings),
		}
	}

	return out
}

type multiplexOutputSettingsOutput struct {
	Destination       *outputLocationRefOutput          `json:"destination,omitempty"`
	ContainerSettings *multiplexContainerSettingsOutput `json:"containerSettings,omitempty"`
}

func extractMultiplexOutputSettings(m map[string]any) *MultiplexOutputSettings {
	raw, ok := m["multiplexOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &MultiplexOutputSettings{Destination: extractOutputLocationRef(raw)}

	if cs, hasCS := raw["containerSettings"].(map[string]any); hasCS {
		out.ContainerSettings = &MultiplexContainerSettings{MultiplexM2tsSettings: extractMultiplexM2tsSettings(cs)}
	}

	return out
}

type rtmpOutputSettingsOutput struct {
	Destination             *outputLocationRefOutput `json:"destination,omitempty"`
	CertificateMode         string                   `json:"certificateMode,omitempty"`
	ConnectionRetryInterval int32                    `json:"connectionRetryInterval,omitempty"`
	NumRetries              int32                    `json:"numRetries,omitempty"`
}

func toRtmpOutputSettingsOutput(s *RtmpOutputSettings) *rtmpOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &rtmpOutputSettingsOutput{
		Destination:             toOutputLocationRefOutput(s.Destination),
		CertificateMode:         s.CertificateMode,
		ConnectionRetryInterval: s.ConnectionRetryInterval,
		NumRetries:              s.NumRetries,
	}
}

func extractRtmpOutputSettings(m map[string]any) *RtmpOutputSettings {
	raw, ok := m["rtmpOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &RtmpOutputSettings{
		Destination:             extractOutputLocationRef(raw),
		CertificateMode:         stringFromAny(raw["certificateMode"]),
		ConnectionRetryInterval: int32FromAny(raw["connectionRetryInterval"]),
		NumRetries:              int32FromAny(raw["numRetries"]),
	}
}

type udpContainerSettingsOutput struct {
	M2tsSettings *m2tsSettingsOutput `json:"m2tsSettings,omitempty"`
}

func toUDPContainerSettingsOutput(s *UDPContainerSettings) *udpContainerSettingsOutput {
	if s == nil {
		return nil
	}

	return &udpContainerSettingsOutput{M2tsSettings: toM2tsSettingsOutput(s.M2tsSettings)}
}

func extractUDPContainerSettings(m map[string]any) *UDPContainerSettings {
	raw, ok := m["containerSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &UDPContainerSettings{M2tsSettings: extractM2tsSettings(raw)}
}

type fecOutputSettingsOutput struct {
	IncludeFec  string `json:"includeFec,omitempty"`
	ColumnDepth int32  `json:"columnDepth,omitempty"`
	RowLength   int32  `json:"rowLength,omitempty"`
}

func toFecOutputSettingsOutput(s *FecOutputSettings) *fecOutputSettingsOutput {
	if s == nil {
		return nil
	}

	out := fecOutputSettingsOutput(*s)

	return &out
}

func extractFecOutputSettings(m map[string]any) *FecOutputSettings {
	raw, ok := m["fecOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &FecOutputSettings{
		IncludeFec:  stringFromAny(raw["includeFec"]),
		ColumnDepth: int32FromAny(raw["columnDepth"]),
		RowLength:   int32FromAny(raw["rowLength"]),
	}
}

type srtOutputSettingsOutput struct {
	ContainerSettings *udpContainerSettingsOutput `json:"containerSettings,omitempty"`
	Destination       *outputLocationRefOutput    `json:"destination,omitempty"`
	EncryptionType    string                      `json:"encryptionType,omitempty"`
	BufferMsec        int32                       `json:"bufferMsec,omitempty"`
	Latency           int32                       `json:"latency,omitempty"`
}

func toSrtOutputSettingsOutput(s *SrtOutputSettings) *srtOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &srtOutputSettingsOutput{
		ContainerSettings: toUDPContainerSettingsOutput(s.ContainerSettings),
		Destination:       toOutputLocationRefOutput(s.Destination),
		EncryptionType:    s.EncryptionType,
		BufferMsec:        s.BufferMsec,
		Latency:           s.Latency,
	}
}

func extractSrtOutputSettings(m map[string]any) *SrtOutputSettings {
	raw, ok := m["srtOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &SrtOutputSettings{
		ContainerSettings: extractUDPContainerSettings(raw),
		Destination:       extractOutputLocationRef(raw),
		EncryptionType:    stringFromAny(raw["encryptionType"]),
		BufferMsec:        int32FromAny(raw["bufferMsec"]),
		Latency:           int32FromAny(raw["latency"]),
	}
}

type udpOutputSettingsOutput struct {
	ContainerSettings *udpContainerSettingsOutput `json:"containerSettings,omitempty"`
	Destination       *outputLocationRefOutput    `json:"destination,omitempty"`
	FecOutputSettings *fecOutputSettingsOutput    `json:"fecOutputSettings,omitempty"`
	BufferMsec        int32                       `json:"bufferMsec,omitempty"`
}

func toUDPOutputSettingsOutput(s *UDPOutputSettings) *udpOutputSettingsOutput {
	if s == nil {
		return nil
	}

	return &udpOutputSettingsOutput{
		ContainerSettings: toUDPContainerSettingsOutput(s.ContainerSettings),
		Destination:       toOutputLocationRefOutput(s.Destination),
		FecOutputSettings: toFecOutputSettingsOutput(s.FecOutputSettings),
		BufferMsec:        s.BufferMsec,
	}
}

func extractUDPOutputSettings(m map[string]any) *UDPOutputSettings {
	raw, ok := m["udpOutputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &UDPOutputSettings{
		ContainerSettings: extractUDPContainerSettings(raw),
		Destination:       extractOutputLocationRef(raw),
		FecOutputSettings: extractFecOutputSettings(raw),
		BufferMsec:        int32FromAny(raw["bufferMsec"]),
	}
}

// -- OutputSettings (top-level union) --

type outputSettingsOutput struct {
	ArchiveOutputSettings            *archiveOutputSettingsOutput      `json:"archiveOutputSettings,omitempty"`
	CmafIngestOutputSettings         *cmafIngestOutputSettingsOutput   `json:"cmafIngestOutputSettings,omitempty"`
	FrameCaptureOutputSettings       *frameCaptureOutputSettingsOutput `json:"frameCaptureOutputSettings,omitempty"`
	HlsOutputSettings                *hlsOutputSettingsOutput          `json:"hlsOutputSettings,omitempty"`
	MediaConnectRouterOutputSettings *mcRouterOutputSettingsOutput     `json:"mediaConnectRouterOutputSettings,omitempty"`
	MediaPackageOutputSettings       *mediaPackageOutputSettingsOutput `json:"mediaPackageOutputSettings,omitempty"`
	MsSmoothOutputSettings           *msSmoothOutputSettingsOutput     `json:"msSmoothOutputSettings,omitempty"`
	MultiplexOutputSettings          *multiplexOutputSettingsOutput    `json:"multiplexOutputSettings,omitempty"`
	RtmpOutputSettings               *rtmpOutputSettingsOutput         `json:"rtmpOutputSettings,omitempty"`
	SrtOutputSettings                *srtOutputSettingsOutput          `json:"srtOutputSettings,omitempty"`
	UDPOutputSettings                *udpOutputSettingsOutput          `json:"udpOutputSettings,omitempty"`
}

func toOutputSettingsOutput(s *OutputSettings) *outputSettingsOutput {
	if !s.hasOutputSettings() {
		return nil
	}

	return &outputSettingsOutput{
		ArchiveOutputSettings:            toArchiveOutputSettingsOutput(s.ArchiveOutputSettings),
		CmafIngestOutputSettings:         toCmafIngestOutputSettingsOutput(s.CmafIngestOutputSettings),
		FrameCaptureOutputSettings:       toFrameCaptureOutputSettingsOutput(s.FrameCaptureOutputSettings),
		HlsOutputSettings:                toHlsOutputSettingsOutput(s.HlsOutputSettings),
		MediaConnectRouterOutputSettings: toMediaConnectRouterOutputSettingsOutput(s.MediaConnectRouterOutputSettings),
		MediaPackageOutputSettings:       toMediaPackageOutputSettingsOutput(s.MediaPackageOutputSettings),
		MsSmoothOutputSettings:           toMsSmoothOutputSettingsOutput(s.MsSmoothOutputSettings),
		MultiplexOutputSettings:          toMultiplexOutputSettingsOutput(s.MultiplexOutputSettings),
		RtmpOutputSettings:               toRtmpOutputSettingsOutput(s.RtmpOutputSettings),
		SrtOutputSettings:                toSrtOutputSettingsOutput(s.SrtOutputSettings),
		UDPOutputSettings:                toUDPOutputSettingsOutput(s.UDPOutputSettings),
	}
}

func extractOutputSettings(m map[string]any) *OutputSettings {
	raw, ok := m["outputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &OutputSettings{
		ArchiveOutputSettings:            extractArchiveOutputSettings(raw),
		CmafIngestOutputSettings:         extractCmafIngestOutputSettings(raw),
		FrameCaptureOutputSettings:       extractFrameCaptureOutputSettings(raw),
		HlsOutputSettings:                extractHlsOutputSettings(raw),
		MediaConnectRouterOutputSettings: extractMediaConnectRouterOutputSettings(raw),
		MediaPackageOutputSettings:       extractMediaPackageOutputSettings(raw),
		MsSmoothOutputSettings:           extractMsSmoothOutputSettings(raw),
		MultiplexOutputSettings:          extractMultiplexOutputSettings(raw),
		RtmpOutputSettings:               extractRtmpOutputSettings(raw),
		SrtOutputSettings:                extractSrtOutputSettings(raw),
		UDPOutputSettings:                extractUDPOutputSettings(raw),
	}
}

// -- Archive/FrameCapture group CDN settings --

type archiveS3SettingsOutput struct {
	CannedACL string `json:"cannedAcl,omitempty"`
}

type archiveCdnSettingsOutput struct {
	ArchiveS3Settings *archiveS3SettingsOutput `json:"archiveS3Settings,omitempty"`
}

func toArchiveCdnSettingsOutput(s *ArchiveCdnSettings) *archiveCdnSettingsOutput {
	if s == nil {
		return nil
	}

	if s.ArchiveS3Settings == nil {
		return &archiveCdnSettingsOutput{}
	}

	return &archiveCdnSettingsOutput{
		ArchiveS3Settings: &archiveS3SettingsOutput{CannedACL: s.ArchiveS3Settings.CannedACL},
	}
}

func extractArchiveCdnSettings(m map[string]any) *ArchiveCdnSettings {
	raw, ok := m["archiveCdnSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &ArchiveCdnSettings{}

	if s3, hasS3 := raw["archiveS3Settings"].(map[string]any); hasS3 {
		out.ArchiveS3Settings = &ArchiveS3Settings{CannedACL: stringFromAny(s3["cannedAcl"])}
	}

	return out
}

// -- ArchiveGroupSettings / CmafIngestGroupSettings / FrameCaptureGroupSettings --

type archiveGroupSettingsOutput struct {
	Destination        *outputLocationRefOutput  `json:"destination,omitempty"`
	ArchiveCdnSettings *archiveCdnSettingsOutput `json:"archiveCdnSettings,omitempty"`
	RolloverInterval   int32                     `json:"rolloverInterval,omitempty"`
}

func toArchiveGroupSettingsOutput(s *ArchiveGroupSettings) *archiveGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &archiveGroupSettingsOutput{
		Destination:        toOutputLocationRefOutput(s.Destination),
		ArchiveCdnSettings: toArchiveCdnSettingsOutput(s.ArchiveCdnSettings),
		RolloverInterval:   s.RolloverInterval,
	}
}

func extractArchiveGroupSettings(m map[string]any) *ArchiveGroupSettings {
	raw, ok := m["archiveGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &ArchiveGroupSettings{
		Destination:        extractOutputLocationRef(raw),
		ArchiveCdnSettings: extractArchiveCdnSettings(raw),
		RolloverInterval:   int32FromAny(raw["rolloverInterval"]),
	}
}

type cmafIngestGroupSettingsOutput struct {
	Destination              *outputLocationRefOutput                 `json:"destination,omitempty"`
	NielsenID3NameModifier   string                                   `json:"nielsenId3NameModifier,omitempty"`
	Scte35Type               string                                   `json:"scte35Type,omitempty"`
	ID3Behavior              string                                   `json:"id3Behavior,omitempty"`
	ID3NameModifier          string                                   `json:"id3NameModifier,omitempty"`
	KlvBehavior              string                                   `json:"klvBehavior,omitempty"`
	KlvNameModifier          string                                   `json:"klvNameModifier,omitempty"`
	TimedMetadataPassthrough string                                   `json:"timedMetadataPassthrough,omitempty"`
	NielsenID3Behavior       string                                   `json:"nielsenId3Behavior,omitempty"`
	Scte35NameModifier       string                                   `json:"scte35NameModifier,omitempty"`
	TimedMetadataID3Frame    string                                   `json:"timedMetadataId3Frame,omitempty"`
	SegmentLengthUnits       string                                   `json:"segmentLengthUnits,omitempty"`
	AdditionalDestinations   []outputAdditionalDestinationOutput      `json:"additionalDestinations,omitempty"`
	CaptionLanguageMappings  []cmafIngestCaptionLanguageMappingOutput `json:"captionLanguageMappings,omitempty"`
	SegmentLength            int32                                    `json:"segmentLength,omitempty"`
	SendDelayMs              int32                                    `json:"sendDelayMs,omitempty"`
	TimedMetadataID3Period   int32                                    `json:"timedMetadataId3Period,omitempty"`
}

func toCmafIngestGroupSettingsOutput(s *CmafIngestGroupSettings) *cmafIngestGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &cmafIngestGroupSettingsOutput{
		Destination:              toOutputLocationRefOutput(s.Destination),
		AdditionalDestinations:   toOutputAdditionalDestinationsOutput(s.AdditionalDestinations),
		CaptionLanguageMappings:  toCmafIngestCaptionLanguageMappingsOutput(s.CaptionLanguageMappings),
		ID3Behavior:              s.ID3Behavior,
		ID3NameModifier:          s.ID3NameModifier,
		KlvBehavior:              s.KlvBehavior,
		KlvNameModifier:          s.KlvNameModifier,
		NielsenID3Behavior:       s.NielsenID3Behavior,
		NielsenID3NameModifier:   s.NielsenID3NameModifier,
		Scte35NameModifier:       s.Scte35NameModifier,
		Scte35Type:               s.Scte35Type,
		SegmentLengthUnits:       s.SegmentLengthUnits,
		TimedMetadataID3Frame:    s.TimedMetadataID3Frame,
		TimedMetadataPassthrough: s.TimedMetadataPassthrough,
		SegmentLength:            s.SegmentLength,
		SendDelayMs:              s.SendDelayMs,
		TimedMetadataID3Period:   s.TimedMetadataID3Period,
	}
}

func extractCmafIngestGroupSettings(m map[string]any) *CmafIngestGroupSettings {
	raw, ok := m["cmafIngestGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &CmafIngestGroupSettings{
		Destination:              extractOutputLocationRef(raw),
		ID3Behavior:              stringFromAny(raw["id3Behavior"]),
		ID3NameModifier:          stringFromAny(raw["id3NameModifier"]),
		KlvBehavior:              stringFromAny(raw["klvBehavior"]),
		KlvNameModifier:          stringFromAny(raw["klvNameModifier"]),
		NielsenID3Behavior:       stringFromAny(raw["nielsenId3Behavior"]),
		NielsenID3NameModifier:   stringFromAny(raw["nielsenId3NameModifier"]),
		Scte35NameModifier:       stringFromAny(raw["scte35NameModifier"]),
		Scte35Type:               stringFromAny(raw["scte35Type"]),
		SegmentLengthUnits:       stringFromAny(raw["segmentLengthUnits"]),
		TimedMetadataID3Frame:    stringFromAny(raw["timedMetadataId3Frame"]),
		TimedMetadataPassthrough: stringFromAny(raw["timedMetadataPassthrough"]),
		SegmentLength:            int32FromAny(raw["segmentLength"]),
		SendDelayMs:              int32FromAny(raw["sendDelayMs"]),
		TimedMetadataID3Period:   int32FromAny(raw["timedMetadataId3Period"]),
	}

	if v, hasAdditional := raw["additionalDestinations"].([]any); hasAdditional {
		out.AdditionalDestinations = extractOutputAdditionalDestinations(v)
	}

	if v, hasCaptions := raw["captionLanguageMappings"].([]any); hasCaptions {
		out.CaptionLanguageMappings = extractCmafIngestCaptionLanguageMappings(v)
	}

	return out
}

type frameCaptureGroupSettingsOutput struct {
	Destination             *outputLocationRefOutput       `json:"destination,omitempty"`
	FrameCaptureCdnSettings *frameCaptureCdnSettingsOutput `json:"frameCaptureCdnSettings,omitempty"`
}

func toFrameCaptureGroupSettingsOutput(s *FrameCaptureGroupSettings) *frameCaptureGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &frameCaptureGroupSettingsOutput{
		Destination:             toOutputLocationRefOutput(s.Destination),
		FrameCaptureCdnSettings: toFrameCaptureCdnSettingsOutput(s.FrameCaptureCdnSettings),
	}
}

func extractFrameCaptureGroupSettings(m map[string]any) *FrameCaptureGroupSettings {
	raw, ok := m["frameCaptureGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &FrameCaptureGroupSettings{
		Destination:             extractOutputLocationRef(raw),
		FrameCaptureCdnSettings: extractFrameCaptureCdnSettings(raw),
	}
}

// -- HlsCdnSettings (Akamai/BasicPut/MediaStore/S3/Webdav) --

type hlsAkamaiSettingsOutput struct {
	HTTPTransferMode        string `json:"httpTransferMode,omitempty"`
	Salt                    string `json:"salt,omitempty"`
	Token                   string `json:"token,omitempty"`
	ConnectionRetryInterval int32  `json:"connectionRetryInterval,omitempty"`
	FilecacheDuration       int32  `json:"filecacheDuration,omitempty"`
	NumRetries              int32  `json:"numRetries,omitempty"`
	RestartDelay            int32  `json:"restartDelay,omitempty"`
}

func toHlsAkamaiSettingsOutput(s *HlsAkamaiSettings) *hlsAkamaiSettingsOutput {
	if s == nil {
		return nil
	}

	out := hlsAkamaiSettingsOutput(*s)

	return &out
}

func extractHlsAkamaiSettings(m map[string]any) *HlsAkamaiSettings {
	raw, ok := m["hlsAkamaiSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsAkamaiSettings{
		HTTPTransferMode:        stringFromAny(raw["httpTransferMode"]),
		Salt:                    stringFromAny(raw["salt"]),
		Token:                   stringFromAny(raw["token"]),
		ConnectionRetryInterval: int32FromAny(raw["connectionRetryInterval"]),
		FilecacheDuration:       int32FromAny(raw["filecacheDuration"]),
		NumRetries:              int32FromAny(raw["numRetries"]),
		RestartDelay:            int32FromAny(raw["restartDelay"]),
	}
}

type hlsBasicPutSettingsOutput struct {
	ConnectionRetryInterval int32 `json:"connectionRetryInterval,omitempty"`
	FilecacheDuration       int32 `json:"filecacheDuration,omitempty"`
	NumRetries              int32 `json:"numRetries,omitempty"`
	RestartDelay            int32 `json:"restartDelay,omitempty"`
}

func toHlsBasicPutSettingsOutput(s *HlsBasicPutSettings) *hlsBasicPutSettingsOutput {
	if s == nil {
		return nil
	}

	out := hlsBasicPutSettingsOutput(*s)

	return &out
}

func extractHlsBasicPutSettings(m map[string]any) *HlsBasicPutSettings {
	raw, ok := m["hlsBasicPutSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsBasicPutSettings{
		ConnectionRetryInterval: int32FromAny(raw["connectionRetryInterval"]),
		FilecacheDuration:       int32FromAny(raw["filecacheDuration"]),
		NumRetries:              int32FromAny(raw["numRetries"]),
		RestartDelay:            int32FromAny(raw["restartDelay"]),
	}
}

type hlsMediaStoreSettingsOutput struct {
	MediaStoreStorageClass  string `json:"mediaStoreStorageClass,omitempty"`
	ConnectionRetryInterval int32  `json:"connectionRetryInterval,omitempty"`
	FilecacheDuration       int32  `json:"filecacheDuration,omitempty"`
	NumRetries              int32  `json:"numRetries,omitempty"`
	RestartDelay            int32  `json:"restartDelay,omitempty"`
}

func toHlsMediaStoreSettingsOutput(s *HlsMediaStoreSettings) *hlsMediaStoreSettingsOutput {
	if s == nil {
		return nil
	}

	out := hlsMediaStoreSettingsOutput(*s)

	return &out
}

func extractHlsMediaStoreSettings(m map[string]any) *HlsMediaStoreSettings {
	raw, ok := m["hlsMediaStoreSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsMediaStoreSettings{
		MediaStoreStorageClass:  stringFromAny(raw["mediaStoreStorageClass"]),
		ConnectionRetryInterval: int32FromAny(raw["connectionRetryInterval"]),
		FilecacheDuration:       int32FromAny(raw["filecacheDuration"]),
		NumRetries:              int32FromAny(raw["numRetries"]),
		RestartDelay:            int32FromAny(raw["restartDelay"]),
	}
}

type hlsS3SettingsOutput struct {
	CannedACL string `json:"cannedAcl,omitempty"`
}

func toHlsS3SettingsOutput(s *HlsS3Settings) *hlsS3SettingsOutput {
	if s == nil {
		return nil
	}

	return &hlsS3SettingsOutput{CannedACL: s.CannedACL}
}

func extractHlsS3Settings(m map[string]any) *HlsS3Settings {
	raw, ok := m["hlsS3Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsS3Settings{CannedACL: stringFromAny(raw["cannedAcl"])}
}

type hlsWebdavSettingsOutput struct {
	HTTPTransferMode        string `json:"httpTransferMode,omitempty"`
	ConnectionRetryInterval int32  `json:"connectionRetryInterval,omitempty"`
	FilecacheDuration       int32  `json:"filecacheDuration,omitempty"`
	NumRetries              int32  `json:"numRetries,omitempty"`
	RestartDelay            int32  `json:"restartDelay,omitempty"`
}

func toHlsWebdavSettingsOutput(s *HlsWebdavSettings) *hlsWebdavSettingsOutput {
	if s == nil {
		return nil
	}

	out := hlsWebdavSettingsOutput(*s)

	return &out
}

func extractHlsWebdavSettings(m map[string]any) *HlsWebdavSettings {
	raw, ok := m["hlsWebdavSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsWebdavSettings{
		HTTPTransferMode:        stringFromAny(raw["httpTransferMode"]),
		ConnectionRetryInterval: int32FromAny(raw["connectionRetryInterval"]),
		FilecacheDuration:       int32FromAny(raw["filecacheDuration"]),
		NumRetries:              int32FromAny(raw["numRetries"]),
		RestartDelay:            int32FromAny(raw["restartDelay"]),
	}
}

type hlsCdnSettingsOutput struct {
	HlsAkamaiSettings     *hlsAkamaiSettingsOutput     `json:"hlsAkamaiSettings,omitempty"`
	HlsBasicPutSettings   *hlsBasicPutSettingsOutput   `json:"hlsBasicPutSettings,omitempty"`
	HlsMediaStoreSettings *hlsMediaStoreSettingsOutput `json:"hlsMediaStoreSettings,omitempty"`
	HlsS3Settings         *hlsS3SettingsOutput         `json:"hlsS3Settings,omitempty"`
	HlsWebdavSettings     *hlsWebdavSettingsOutput     `json:"hlsWebdavSettings,omitempty"`
}

func toHlsCdnSettingsOutput(s *HlsCdnSettings) *hlsCdnSettingsOutput {
	if s == nil {
		return nil
	}

	return &hlsCdnSettingsOutput{
		HlsAkamaiSettings:     toHlsAkamaiSettingsOutput(s.HlsAkamaiSettings),
		HlsBasicPutSettings:   toHlsBasicPutSettingsOutput(s.HlsBasicPutSettings),
		HlsMediaStoreSettings: toHlsMediaStoreSettingsOutput(s.HlsMediaStoreSettings),
		HlsS3Settings:         toHlsS3SettingsOutput(s.HlsS3Settings),
		HlsWebdavSettings:     toHlsWebdavSettingsOutput(s.HlsWebdavSettings),
	}
}

func extractHlsCdnSettings(m map[string]any) *HlsCdnSettings {
	raw, ok := m["hlsCdnSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsCdnSettings{
		HlsAkamaiSettings:     extractHlsAkamaiSettings(raw),
		HlsBasicPutSettings:   extractHlsBasicPutSettings(raw),
		HlsMediaStoreSettings: extractHlsMediaStoreSettings(raw),
		HlsS3Settings:         extractHlsS3Settings(raw),
		HlsWebdavSettings:     extractHlsWebdavSettings(raw),
	}
}

// -- KeyProviderSettings / StaticKeySettings --

type staticKeySettingsOutput struct {
	KeyProviderServer *inputLocationOutput `json:"keyProviderServer,omitempty"`
	StaticKeyValue    string               `json:"staticKeyValue,omitempty"`
}

func toStaticKeySettingsOutput(s *StaticKeySettings) *staticKeySettingsOutput {
	if s == nil {
		return nil
	}

	return &staticKeySettingsOutput{
		KeyProviderServer: toInputLocationOutput(s.KeyProviderServer),
		StaticKeyValue:    s.StaticKeyValue,
	}
}

func extractStaticKeySettings(m map[string]any) *StaticKeySettings {
	raw, ok := m["staticKeySettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &StaticKeySettings{
		KeyProviderServer: extractInputLocation(raw, "keyProviderServer"),
		StaticKeyValue:    stringFromAny(raw["staticKeyValue"]),
	}
}

type keyProviderSettingsOutput struct {
	StaticKeySettings *staticKeySettingsOutput `json:"staticKeySettings,omitempty"`
}

func toKeyProviderSettingsOutput(s *KeyProviderSettings) *keyProviderSettingsOutput {
	if s == nil {
		return nil
	}

	return &keyProviderSettingsOutput{StaticKeySettings: toStaticKeySettingsOutput(s.StaticKeySettings)}
}

func extractKeyProviderSettings(m map[string]any) *KeyProviderSettings {
	raw, ok := m["keyProviderSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &KeyProviderSettings{StaticKeySettings: extractStaticKeySettings(raw)}
}

// -- HlsGroupSettings --

type hlsGroupSettingsOutput struct {
	Destination                *outputLocationRefOutput       `json:"destination,omitempty"`
	HlsCdnSettings             *hlsCdnSettingsOutput          `json:"hlsCdnSettings,omitempty"`
	KeyProviderSettings        *keyProviderSettingsOutput     `json:"keyProviderSettings,omitempty"`
	IvInManifest               string                         `json:"ivInManifest,omitempty"`
	IFrameOnlyPlaylists        string                         `json:"iFrameOnlyPlaylists,omitempty"`
	BaseURLContent             string                         `json:"baseUrlContent,omitempty"`
	BaseURLContent1            string                         `json:"baseUrlContent1,omitempty"`
	BaseURLManifest            string                         `json:"baseUrlManifest,omitempty"`
	BaseURLManifest1           string                         `json:"baseUrlManifest1,omitempty"`
	CaptionLanguageSetting     string                         `json:"captionLanguageSetting,omitempty"`
	ClientCache                string                         `json:"clientCache,omitempty"`
	CodecSpecification         string                         `json:"codecSpecification,omitempty"`
	ConstantIv                 string                         `json:"constantIv,omitempty"`
	DirectoryStructure         string                         `json:"directoryStructure,omitempty"`
	DiscontinuityTags          string                         `json:"discontinuityTags,omitempty"`
	EncryptionType             string                         `json:"encryptionType,omitempty"`
	HlsID3SegmentTagging       string                         `json:"hlsId3SegmentTagging,omitempty"`
	KeyFormat                  string                         `json:"keyFormat,omitempty"`
	IncompleteSegmentBehavior  string                         `json:"incompleteSegmentBehavior,omitempty"`
	InputLossAction            string                         `json:"inputLossAction,omitempty"`
	TSFileMode                 string                         `json:"tsFileMode,omitempty"`
	TimedMetadataID3Frame      string                         `json:"timedMetadataId3Frame,omitempty"`
	IvSource                   string                         `json:"ivSource,omitempty"`
	ProgramDateTime            string                         `json:"programDateTime,omitempty"`
	ManifestCompression        string                         `json:"manifestCompression,omitempty"`
	ManifestDurationFormat     string                         `json:"manifestDurationFormat,omitempty"`
	Mode                       string                         `json:"mode,omitempty"`
	OutputSelection            string                         `json:"outputSelection,omitempty"`
	KeyFormatVersions          string                         `json:"keyFormatVersions,omitempty"`
	ProgramDateTimeClock       string                         `json:"programDateTimeClock,omitempty"`
	RedundantManifest          string                         `json:"redundantManifest,omitempty"`
	SegmentationMode           string                         `json:"segmentationMode,omitempty"`
	StreamInfResolution        string                         `json:"streamInfResolution,omitempty"`
	CaptionLanguageMappings    []captionLanguageMappingOutput `json:"captionLanguageMappings,omitempty"`
	AdMarkers                  []string                       `json:"adMarkers,omitempty"`
	IndexNSegments             int32                          `json:"indexNSegments,omitempty"`
	KeepSegments               int32                          `json:"keepSegments,omitempty"`
	MinSegmentLength           int32                          `json:"minSegmentLength,omitempty"`
	ProgramDateTimePeriod      int32                          `json:"programDateTimePeriod,omitempty"`
	SegmentLength              int32                          `json:"segmentLength,omitempty"`
	SegmentsPerSubdirectory    int32                          `json:"segmentsPerSubdirectory,omitempty"`
	TimedMetadataID3Period     int32                          `json:"timedMetadataId3Period,omitempty"`
	TimestampDeltaMilliseconds int32                          `json:"timestampDeltaMilliseconds,omitempty"`
}

func toHlsGroupSettingsOutput(s *HlsGroupSettings) *hlsGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &hlsGroupSettingsOutput{
		Destination:                toOutputLocationRefOutput(s.Destination),
		HlsCdnSettings:             toHlsCdnSettingsOutput(s.HlsCdnSettings),
		KeyProviderSettings:        toKeyProviderSettingsOutput(s.KeyProviderSettings),
		AdMarkers:                  s.AdMarkers,
		CaptionLanguageMappings:    toCaptionLanguageMappingsOutput(s.CaptionLanguageMappings),
		BaseURLContent:             s.BaseURLContent,
		BaseURLContent1:            s.BaseURLContent1,
		BaseURLManifest:            s.BaseURLManifest,
		BaseURLManifest1:           s.BaseURLManifest1,
		CaptionLanguageSetting:     s.CaptionLanguageSetting,
		ClientCache:                s.ClientCache,
		CodecSpecification:         s.CodecSpecification,
		ConstantIv:                 s.ConstantIv,
		DirectoryStructure:         s.DirectoryStructure,
		DiscontinuityTags:          s.DiscontinuityTags,
		EncryptionType:             s.EncryptionType,
		HlsID3SegmentTagging:       s.HlsID3SegmentTagging,
		IFrameOnlyPlaylists:        s.IFrameOnlyPlaylists,
		IncompleteSegmentBehavior:  s.IncompleteSegmentBehavior,
		InputLossAction:            s.InputLossAction,
		IvInManifest:               s.IvInManifest,
		IvSource:                   s.IvSource,
		KeyFormat:                  s.KeyFormat,
		KeyFormatVersions:          s.KeyFormatVersions,
		ManifestCompression:        s.ManifestCompression,
		ManifestDurationFormat:     s.ManifestDurationFormat,
		Mode:                       s.Mode,
		OutputSelection:            s.OutputSelection,
		ProgramDateTime:            s.ProgramDateTime,
		ProgramDateTimeClock:       s.ProgramDateTimeClock,
		RedundantManifest:          s.RedundantManifest,
		SegmentationMode:           s.SegmentationMode,
		StreamInfResolution:        s.StreamInfResolution,
		TimedMetadataID3Frame:      s.TimedMetadataID3Frame,
		TSFileMode:                 s.TSFileMode,
		IndexNSegments:             s.IndexNSegments,
		KeepSegments:               s.KeepSegments,
		MinSegmentLength:           s.MinSegmentLength,
		ProgramDateTimePeriod:      s.ProgramDateTimePeriod,
		SegmentLength:              s.SegmentLength,
		SegmentsPerSubdirectory:    s.SegmentsPerSubdirectory,
		TimedMetadataID3Period:     s.TimedMetadataID3Period,
		TimestampDeltaMilliseconds: s.TimestampDeltaMilliseconds,
	}
}

func extractHlsGroupSettings(m map[string]any) *HlsGroupSettings {
	raw, ok := m["hlsGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &HlsGroupSettings{
		Destination:                extractOutputLocationRef(raw),
		HlsCdnSettings:             extractHlsCdnSettings(raw),
		KeyProviderSettings:        extractKeyProviderSettings(raw),
		BaseURLContent:             stringFromAny(raw["baseUrlContent"]),
		BaseURLContent1:            stringFromAny(raw["baseUrlContent1"]),
		BaseURLManifest:            stringFromAny(raw["baseUrlManifest"]),
		BaseURLManifest1:           stringFromAny(raw["baseUrlManifest1"]),
		CaptionLanguageSetting:     stringFromAny(raw["captionLanguageSetting"]),
		ClientCache:                stringFromAny(raw["clientCache"]),
		CodecSpecification:         stringFromAny(raw["codecSpecification"]),
		ConstantIv:                 stringFromAny(raw["constantIv"]),
		DirectoryStructure:         stringFromAny(raw["directoryStructure"]),
		DiscontinuityTags:          stringFromAny(raw["discontinuityTags"]),
		EncryptionType:             stringFromAny(raw["encryptionType"]),
		HlsID3SegmentTagging:       stringFromAny(raw["hlsId3SegmentTagging"]),
		IFrameOnlyPlaylists:        stringFromAny(raw["iFrameOnlyPlaylists"]),
		IncompleteSegmentBehavior:  stringFromAny(raw["incompleteSegmentBehavior"]),
		InputLossAction:            stringFromAny(raw["inputLossAction"]),
		IvInManifest:               stringFromAny(raw["ivInManifest"]),
		IvSource:                   stringFromAny(raw["ivSource"]),
		KeyFormat:                  stringFromAny(raw["keyFormat"]),
		KeyFormatVersions:          stringFromAny(raw["keyFormatVersions"]),
		ManifestCompression:        stringFromAny(raw["manifestCompression"]),
		ManifestDurationFormat:     stringFromAny(raw["manifestDurationFormat"]),
		Mode:                       stringFromAny(raw["mode"]),
		OutputSelection:            stringFromAny(raw["outputSelection"]),
		ProgramDateTime:            stringFromAny(raw["programDateTime"]),
		ProgramDateTimeClock:       stringFromAny(raw["programDateTimeClock"]),
		RedundantManifest:          stringFromAny(raw["redundantManifest"]),
		SegmentationMode:           stringFromAny(raw["segmentationMode"]),
		StreamInfResolution:        stringFromAny(raw["streamInfResolution"]),
		TimedMetadataID3Frame:      stringFromAny(raw["timedMetadataId3Frame"]),
		TSFileMode:                 stringFromAny(raw["tsFileMode"]),
		IndexNSegments:             int32FromAny(raw["indexNSegments"]),
		KeepSegments:               int32FromAny(raw["keepSegments"]),
		MinSegmentLength:           int32FromAny(raw["minSegmentLength"]),
		ProgramDateTimePeriod:      int32FromAny(raw["programDateTimePeriod"]),
		SegmentLength:              int32FromAny(raw["segmentLength"]),
		SegmentsPerSubdirectory:    int32FromAny(raw["segmentsPerSubdirectory"]),
		TimedMetadataID3Period:     int32FromAny(raw["timedMetadataId3Period"]),
		TimestampDeltaMilliseconds: int32FromAny(raw["timestampDeltaMilliseconds"]),
	}

	if v, hasAdMarkers := raw["adMarkers"].([]any); hasAdMarkers {
		out.AdMarkers = anySliceToStrings(v)
	}

	if v, hasCaptions := raw["captionLanguageMappings"].([]any); hasCaptions {
		out.CaptionLanguageMappings = extractCaptionLanguageMappings(v)
	}

	return out
}

// -- MediaConnectRouterGroupSettings / MediaPackage(V2)GroupSettings --

type mcRouterGroupSettingsOutput struct {
	AvailabilityZones []string `json:"availabilityZones,omitempty"`
}

func toMediaConnectRouterGroupSettingsOutput(
	s *MediaConnectRouterGroupSettings,
) *mcRouterGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &mcRouterGroupSettingsOutput{AvailabilityZones: s.AvailabilityZones}
}

func extractMediaConnectRouterGroupSettings(m map[string]any) *MediaConnectRouterGroupSettings {
	raw, ok := m["mediaConnectRouterGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &MediaConnectRouterGroupSettings{}
	if v, hasZones := raw["availabilityZones"].([]any); hasZones {
		out.AvailabilityZones = anySliceToStrings(v)
	}

	return out
}

type mediaPackageV2GroupSettingsOutput struct {
	ID3Behavior              string                              `json:"id3Behavior,omitempty"`
	KlvBehavior              string                              `json:"klvBehavior,omitempty"`
	NielsenID3Behavior       string                              `json:"nielsenId3Behavior,omitempty"`
	Scte35Type               string                              `json:"scte35Type,omitempty"`
	SegmentLengthUnits       string                              `json:"segmentLengthUnits,omitempty"`
	TimedMetadataID3Frame    string                              `json:"timedMetadataId3Frame,omitempty"`
	TimedMetadataPassthrough string                              `json:"timedMetadataPassthrough,omitempty"`
	AdditionalDestinations   []outputAdditionalDestinationOutput `json:"additionalDestinations,omitempty"`
	CaptionLanguageMappings  []captionLanguageMappingOutput      `json:"captionLanguageMappings,omitempty"`
	SegmentLength            int32                               `json:"segmentLength,omitempty"`
	TimedMetadataID3Period   int32                               `json:"timedMetadataId3Period,omitempty"`
}

func toMediaPackageV2GroupSettingsOutput(s *MediaPackageV2GroupSettings) *mediaPackageV2GroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &mediaPackageV2GroupSettingsOutput{
		AdditionalDestinations:   toOutputAdditionalDestinationsOutput(s.AdditionalDestinations),
		CaptionLanguageMappings:  toCaptionLanguageMappingsOutput(s.CaptionLanguageMappings),
		ID3Behavior:              s.ID3Behavior,
		KlvBehavior:              s.KlvBehavior,
		NielsenID3Behavior:       s.NielsenID3Behavior,
		Scte35Type:               s.Scte35Type,
		SegmentLengthUnits:       s.SegmentLengthUnits,
		TimedMetadataID3Frame:    s.TimedMetadataID3Frame,
		TimedMetadataPassthrough: s.TimedMetadataPassthrough,
		SegmentLength:            s.SegmentLength,
		TimedMetadataID3Period:   s.TimedMetadataID3Period,
	}
}

func extractMediaPackageV2GroupSettings(m map[string]any) *MediaPackageV2GroupSettings {
	raw, ok := m["mediapackageV2GroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &MediaPackageV2GroupSettings{
		ID3Behavior:              stringFromAny(raw["id3Behavior"]),
		KlvBehavior:              stringFromAny(raw["klvBehavior"]),
		NielsenID3Behavior:       stringFromAny(raw["nielsenId3Behavior"]),
		Scte35Type:               stringFromAny(raw["scte35Type"]),
		SegmentLengthUnits:       stringFromAny(raw["segmentLengthUnits"]),
		TimedMetadataID3Frame:    stringFromAny(raw["timedMetadataId3Frame"]),
		TimedMetadataPassthrough: stringFromAny(raw["timedMetadataPassthrough"]),
		SegmentLength:            int32FromAny(raw["segmentLength"]),
		TimedMetadataID3Period:   int32FromAny(raw["timedMetadataId3Period"]),
	}

	if v, hasAdditional := raw["additionalDestinations"].([]any); hasAdditional {
		out.AdditionalDestinations = extractOutputAdditionalDestinations(v)
	}

	if v, hasCaptions := raw["captionLanguageMappings"].([]any); hasCaptions {
		out.CaptionLanguageMappings = extractCaptionLanguageMappings(v)
	}

	return out
}

type mediaPackageGroupSettingsOutput struct {
	Destination                 *outputLocationRefOutput           `json:"destination,omitempty"`
	MediaPackageV2GroupSettings *mediaPackageV2GroupSettingsOutput `json:"mediapackageV2GroupSettings,omitempty"`
}

func toMediaPackageGroupSettingsOutput(s *MediaPackageGroupSettings) *mediaPackageGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &mediaPackageGroupSettingsOutput{
		Destination:                 toOutputLocationRefOutput(s.Destination),
		MediaPackageV2GroupSettings: toMediaPackageV2GroupSettingsOutput(s.MediaPackageV2GroupSettings),
	}
}

func extractMediaPackageGroupSettings(m map[string]any) *MediaPackageGroupSettings {
	raw, ok := m["mediaPackageGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &MediaPackageGroupSettings{
		Destination:                 extractOutputLocationRef(raw),
		MediaPackageV2GroupSettings: extractMediaPackageV2GroupSettings(raw),
	}
}

// -- MsSmoothGroupSettings / MultiplexGroupSettings / RtmpGroupSettings / SrtGroupSettings / UDPGroupSettings --

type msSmoothGroupSettingsOutput struct {
	Destination              *outputLocationRefOutput `json:"destination,omitempty"`
	AcquisitionPointID       string                   `json:"acquisitionPointId,omitempty"`
	AudioOnlyTimecodeControl string                   `json:"audioOnlyTimecodeControl,omitempty"`
	CertificateMode          string                   `json:"certificateMode,omitempty"`
	EventID                  string                   `json:"eventId,omitempty"`
	EventIDMode              string                   `json:"eventIdMode,omitempty"`
	EventStopBehavior        string                   `json:"eventStopBehavior,omitempty"`
	InputLossAction          string                   `json:"inputLossAction,omitempty"`
	SegmentationMode         string                   `json:"segmentationMode,omitempty"`
	SparseTrackType          string                   `json:"sparseTrackType,omitempty"`
	StreamManifestBehavior   string                   `json:"streamManifestBehavior,omitempty"`
	TimestampOffset          string                   `json:"timestampOffset,omitempty"`
	TimestampOffsetMode      string                   `json:"timestampOffsetMode,omitempty"`
	ConnectionRetryInterval  int32                    `json:"connectionRetryInterval,omitempty"`
	FilecacheDuration        int32                    `json:"filecacheDuration,omitempty"`
	FragmentLength           int32                    `json:"fragmentLength,omitempty"`
	NumRetries               int32                    `json:"numRetries,omitempty"`
	RestartDelay             int32                    `json:"restartDelay,omitempty"`
	SendDelayMs              int32                    `json:"sendDelayMs,omitempty"`
}

func toMsSmoothGroupSettingsOutput(s *MsSmoothGroupSettings) *msSmoothGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &msSmoothGroupSettingsOutput{
		Destination:              toOutputLocationRefOutput(s.Destination),
		AcquisitionPointID:       s.AcquisitionPointID,
		AudioOnlyTimecodeControl: s.AudioOnlyTimecodeControl,
		CertificateMode:          s.CertificateMode,
		EventID:                  s.EventID,
		EventIDMode:              s.EventIDMode,
		EventStopBehavior:        s.EventStopBehavior,
		InputLossAction:          s.InputLossAction,
		SegmentationMode:         s.SegmentationMode,
		SparseTrackType:          s.SparseTrackType,
		StreamManifestBehavior:   s.StreamManifestBehavior,
		TimestampOffset:          s.TimestampOffset,
		TimestampOffsetMode:      s.TimestampOffsetMode,
		ConnectionRetryInterval:  s.ConnectionRetryInterval,
		FilecacheDuration:        s.FilecacheDuration,
		FragmentLength:           s.FragmentLength,
		NumRetries:               s.NumRetries,
		RestartDelay:             s.RestartDelay,
		SendDelayMs:              s.SendDelayMs,
	}
}

func extractMsSmoothGroupSettings(m map[string]any) *MsSmoothGroupSettings {
	raw, ok := m["msSmoothGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &MsSmoothGroupSettings{
		Destination:              extractOutputLocationRef(raw),
		AcquisitionPointID:       stringFromAny(raw["acquisitionPointId"]),
		AudioOnlyTimecodeControl: stringFromAny(raw["audioOnlyTimecodeControl"]),
		CertificateMode:          stringFromAny(raw["certificateMode"]),
		EventID:                  stringFromAny(raw["eventId"]),
		EventIDMode:              stringFromAny(raw["eventIdMode"]),
		EventStopBehavior:        stringFromAny(raw["eventStopBehavior"]),
		InputLossAction:          stringFromAny(raw["inputLossAction"]),
		SegmentationMode:         stringFromAny(raw["segmentationMode"]),
		SparseTrackType:          stringFromAny(raw["sparseTrackType"]),
		StreamManifestBehavior:   stringFromAny(raw["streamManifestBehavior"]),
		TimestampOffset:          stringFromAny(raw["timestampOffset"]),
		TimestampOffsetMode:      stringFromAny(raw["timestampOffsetMode"]),
		ConnectionRetryInterval:  int32FromAny(raw["connectionRetryInterval"]),
		FilecacheDuration:        int32FromAny(raw["filecacheDuration"]),
		FragmentLength:           int32FromAny(raw["fragmentLength"]),
		NumRetries:               int32FromAny(raw["numRetries"]),
		RestartDelay:             int32FromAny(raw["restartDelay"]),
		SendDelayMs:              int32FromAny(raw["sendDelayMs"]),
	}
}

type rtmpGroupSettingsOutput struct {
	AuthenticationScheme  string   `json:"authenticationScheme,omitempty"`
	CacheFullBehavior     string   `json:"cacheFullBehavior,omitempty"`
	CaptionData           string   `json:"captionData,omitempty"`
	IncludeFillerNalUnits string   `json:"includeFillerNalUnits,omitempty"`
	InputLossAction       string   `json:"inputLossAction,omitempty"`
	AdMarkers             []string `json:"adMarkers,omitempty"`
	CacheLength           int32    `json:"cacheLength,omitempty"`
	RestartDelay          int32    `json:"restartDelay,omitempty"`
}

func toRtmpGroupSettingsOutput(s *RtmpGroupSettings) *rtmpGroupSettingsOutput {
	if s == nil {
		return nil
	}

	out := rtmpGroupSettingsOutput(*s)

	return &out
}

func extractRtmpGroupSettings(m map[string]any) *RtmpGroupSettings {
	raw, ok := m["rtmpGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &RtmpGroupSettings{
		AuthenticationScheme:  stringFromAny(raw["authenticationScheme"]),
		CacheFullBehavior:     stringFromAny(raw["cacheFullBehavior"]),
		CaptionData:           stringFromAny(raw["captionData"]),
		IncludeFillerNalUnits: stringFromAny(raw["includeFillerNalUnits"]),
		InputLossAction:       stringFromAny(raw["inputLossAction"]),
		CacheLength:           int32FromAny(raw["cacheLength"]),
		RestartDelay:          int32FromAny(raw["restartDelay"]),
	}

	if v, hasAdMarkers := raw["adMarkers"].([]any); hasAdMarkers {
		out.AdMarkers = anySliceToStrings(v)
	}

	return out
}

type srtGroupSettingsOutput struct {
	InputLossAction string `json:"inputLossAction,omitempty"`
}

func toSrtGroupSettingsOutput(s *SrtGroupSettings) *srtGroupSettingsOutput {
	if s == nil {
		return nil
	}

	return &srtGroupSettingsOutput{InputLossAction: s.InputLossAction}
}

func extractSrtGroupSettings(m map[string]any) *SrtGroupSettings {
	raw, ok := m["srtGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &SrtGroupSettings{InputLossAction: stringFromAny(raw["inputLossAction"])}
}

type udpGroupSettingsOutput struct {
	InputLossAction        string `json:"inputLossAction,omitempty"`
	TimedMetadataID3Frame  string `json:"timedMetadataId3Frame,omitempty"`
	TimedMetadataID3Period int32  `json:"timedMetadataId3Period,omitempty"`
}

func toUDPGroupSettingsOutput(s *UDPGroupSettings) *udpGroupSettingsOutput {
	if s == nil {
		return nil
	}

	out := udpGroupSettingsOutput(*s)

	return &out
}

func extractUDPGroupSettings(m map[string]any) *UDPGroupSettings {
	raw, ok := m["udpGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &UDPGroupSettings{
		InputLossAction:        stringFromAny(raw["inputLossAction"]),
		TimedMetadataID3Frame:  stringFromAny(raw["timedMetadataId3Frame"]),
		TimedMetadataID3Period: int32FromAny(raw["timedMetadataId3Period"]),
	}
}

// -- OutputGroupSettings (top-level union) --

type outputGroupSettingsOutput struct {
	ArchiveGroupSettings            *archiveGroupSettingsOutput      `json:"archiveGroupSettings,omitempty"`
	CmafIngestGroupSettings         *cmafIngestGroupSettingsOutput   `json:"cmafIngestGroupSettings,omitempty"`
	FrameCaptureGroupSettings       *frameCaptureGroupSettingsOutput `json:"frameCaptureGroupSettings,omitempty"`
	HlsGroupSettings                *hlsGroupSettingsOutput          `json:"hlsGroupSettings,omitempty"`
	MediaConnectRouterGroupSettings *mcRouterGroupSettingsOutput     `json:"mediaConnectRouterGroupSettings,omitempty"`
	MediaPackageGroupSettings       *mediaPackageGroupSettingsOutput `json:"mediaPackageGroupSettings,omitempty"`
	MsSmoothGroupSettings           *msSmoothGroupSettingsOutput     `json:"msSmoothGroupSettings,omitempty"`
	MultiplexGroupSettings          *emptyMarker                     `json:"multiplexGroupSettings,omitempty"`
	RtmpGroupSettings               *rtmpGroupSettingsOutput         `json:"rtmpGroupSettings,omitempty"`
	SrtGroupSettings                *srtGroupSettingsOutput          `json:"srtGroupSettings,omitempty"`
	UDPGroupSettings                *udpGroupSettingsOutput          `json:"udpGroupSettings,omitempty"`
}

func toOutputGroupSettingsOutput(s *OutputGroupSettings) *outputGroupSettingsOutput {
	if !s.hasOutputGroupSettings() {
		return nil
	}

	out := &outputGroupSettingsOutput{
		ArchiveGroupSettings:            toArchiveGroupSettingsOutput(s.ArchiveGroupSettings),
		CmafIngestGroupSettings:         toCmafIngestGroupSettingsOutput(s.CmafIngestGroupSettings),
		FrameCaptureGroupSettings:       toFrameCaptureGroupSettingsOutput(s.FrameCaptureGroupSettings),
		HlsGroupSettings:                toHlsGroupSettingsOutput(s.HlsGroupSettings),
		MediaConnectRouterGroupSettings: toMediaConnectRouterGroupSettingsOutput(s.MediaConnectRouterGroupSettings),
		MediaPackageGroupSettings:       toMediaPackageGroupSettingsOutput(s.MediaPackageGroupSettings),
		MsSmoothGroupSettings:           toMsSmoothGroupSettingsOutput(s.MsSmoothGroupSettings),
		RtmpGroupSettings:               toRtmpGroupSettingsOutput(s.RtmpGroupSettings),
		SrtGroupSettings:                toSrtGroupSettingsOutput(s.SrtGroupSettings),
		UDPGroupSettings:                toUDPGroupSettingsOutput(s.UDPGroupSettings),
	}

	if s.MultiplexGroupSettings {
		out.MultiplexGroupSettings = &emptyMarker{}
	}

	return out
}

func extractOutputGroupSettings(m map[string]any) *OutputGroupSettings {
	raw, ok := m["outputGroupSettings"].(map[string]any)
	if !ok {
		return nil
	}

	_, hasMultiplex := raw["multiplexGroupSettings"]

	return &OutputGroupSettings{
		ArchiveGroupSettings:            extractArchiveGroupSettings(raw),
		CmafIngestGroupSettings:         extractCmafIngestGroupSettings(raw),
		FrameCaptureGroupSettings:       extractFrameCaptureGroupSettings(raw),
		HlsGroupSettings:                extractHlsGroupSettings(raw),
		MediaConnectRouterGroupSettings: extractMediaConnectRouterGroupSettings(raw),
		MediaPackageGroupSettings:       extractMediaPackageGroupSettings(raw),
		MsSmoothGroupSettings:           extractMsSmoothGroupSettings(raw),
		RtmpGroupSettings:               extractRtmpGroupSettings(raw),
		SrtGroupSettings:                extractSrtGroupSettings(raw),
		UDPGroupSettings:                extractUDPGroupSettings(raw),
		MultiplexGroupSettings:          hasMultiplex,
	}
}
