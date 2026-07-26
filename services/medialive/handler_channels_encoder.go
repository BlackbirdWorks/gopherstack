package medialive

// --- EncoderSettings wire (de)serialization ---
//
// Every shape here mirrors a real aws-sdk-go-v2/service/medialive@v1.97.2
// types.* struct field-for-field for the subset gopherstack models -- see
// the domain type doc comments in interfaces.go (AvailBlanking,
// BlackoutSlate, FeatureActivations, GlobalConfiguration,
// ThumbnailConfiguration, AudioDescription, VideoDescription,
// CaptionDescription, OutputGroup, EncoderOutput, EncoderSettings) for
// exactly what is and is not modeled, and why.

// -- InputLocation --

type inputLocationOutput struct {
	URI           string `json:"uri,omitempty"`
	PasswordParam string `json:"passwordParam,omitempty"`
	Username      string `json:"username,omitempty"`
}

func toInputLocationOutput(l InputLocation) *inputLocationOutput {
	if l.URI == "" {
		return nil
	}

	return &inputLocationOutput{URI: l.URI, PasswordParam: l.PasswordParam, Username: l.Username}
}

func extractInputLocation(m map[string]any, key string) InputLocation {
	raw, ok := m[key].(map[string]any)
	if !ok {
		return InputLocation{}
	}

	return InputLocation{
		URI:           stringFromAny(raw["uri"]),
		PasswordParam: stringFromAny(raw["passwordParam"]),
		Username:      stringFromAny(raw["username"]),
	}
}

// -- TimecodeConfig --

type timecodeConfigOutput struct {
	Source        string `json:"source,omitempty"`
	SyncThreshold int32  `json:"syncThreshold,omitempty"`
}

func toTimecodeConfigOutput(t TimecodeConfig) *timecodeConfigOutput {
	if t.Source == "" {
		return nil
	}

	return &timecodeConfigOutput{Source: t.Source, SyncThreshold: t.SyncThreshold}
}

func extractTimecodeConfig(m map[string]any) TimecodeConfig {
	raw, ok := m["timecodeConfig"].(map[string]any)
	if !ok {
		return TimecodeConfig{}
	}

	return TimecodeConfig{
		Source:        stringFromAny(raw["source"]),
		SyncThreshold: int32FromAny(raw["syncThreshold"]),
	}
}

// -- AvailBlanking --

type availBlankingOutput struct {
	AvailBlankingImage *inputLocationOutput `json:"availBlankingImage,omitempty"`
	State              string               `json:"state,omitempty"`
}

func toAvailBlankingOutput(a AvailBlanking) *availBlankingOutput {
	if a.State == "" && a.AvailBlankingImage.URI == "" {
		return nil
	}

	return &availBlankingOutput{
		State:              a.State,
		AvailBlankingImage: toInputLocationOutput(a.AvailBlankingImage),
	}
}

func extractAvailBlanking(m map[string]any) AvailBlanking {
	raw, ok := m["availBlanking"].(map[string]any)
	if !ok {
		return AvailBlanking{}
	}

	return AvailBlanking{
		State:              stringFromAny(raw["state"]),
		AvailBlankingImage: extractInputLocation(raw, "availBlankingImage"),
	}
}

// -- BlackoutSlate --

type blackoutSlateOutput struct {
	BlackoutSlateImage      *inputLocationOutput `json:"blackoutSlateImage,omitempty"`
	NetworkEndBlackoutImage *inputLocationOutput `json:"networkEndBlackoutImage,omitempty"`
	NetworkEndBlackout      string               `json:"networkEndBlackout,omitempty"`
	NetworkID               string               `json:"networkId,omitempty"`
	State                   string               `json:"state,omitempty"`
}

func toBlackoutSlateOutput(b BlackoutSlate) *blackoutSlateOutput {
	if b.State == "" && b.NetworkEndBlackout == "" && b.NetworkID == "" &&
		b.BlackoutSlateImage.URI == "" && b.NetworkEndBlackoutImage.URI == "" {
		return nil
	}

	return &blackoutSlateOutput{
		State:                   b.State,
		NetworkEndBlackout:      b.NetworkEndBlackout,
		NetworkID:               b.NetworkID,
		BlackoutSlateImage:      toInputLocationOutput(b.BlackoutSlateImage),
		NetworkEndBlackoutImage: toInputLocationOutput(b.NetworkEndBlackoutImage),
	}
}

func extractBlackoutSlate(m map[string]any) BlackoutSlate {
	raw, ok := m["blackoutSlate"].(map[string]any)
	if !ok {
		return BlackoutSlate{}
	}

	return BlackoutSlate{
		State:                   stringFromAny(raw["state"]),
		NetworkEndBlackout:      stringFromAny(raw["networkEndBlackout"]),
		NetworkID:               stringFromAny(raw["networkId"]),
		BlackoutSlateImage:      extractInputLocation(raw, "blackoutSlateImage"),
		NetworkEndBlackoutImage: extractInputLocation(raw, "networkEndBlackoutImage"),
	}
}

// -- FeatureActivations --

type featureActivationsOutput struct {
	InputPrepareScheduleActions             string `json:"inputPrepareScheduleActions,omitempty"`
	OutputStaticImageOverlayScheduleActions string `json:"outputStaticImageOverlayScheduleActions,omitempty"`
}

func toFeatureActivationsOutput(f FeatureActivations) *featureActivationsOutput {
	if f.InputPrepareScheduleActions == "" && f.OutputStaticImageOverlayScheduleActions == "" {
		return nil
	}

	return &featureActivationsOutput{
		InputPrepareScheduleActions:             f.InputPrepareScheduleActions,
		OutputStaticImageOverlayScheduleActions: f.OutputStaticImageOverlayScheduleActions,
	}
}

func extractFeatureActivations(m map[string]any) FeatureActivations {
	raw, ok := m["featureActivations"].(map[string]any)
	if !ok {
		return FeatureActivations{}
	}

	return FeatureActivations{
		InputPrepareScheduleActions:             stringFromAny(raw["inputPrepareScheduleActions"]),
		OutputStaticImageOverlayScheduleActions: stringFromAny(raw["outputStaticImageOverlayScheduleActions"]),
	}
}

// -- GlobalConfiguration (InputLossBehavior / OutputLockingSettings) --

type inputLossBehaviorOutput struct {
	InputLossImageSlate *inputLocationOutput `json:"inputLossImageSlate,omitempty"`
	InputLossImageColor string               `json:"inputLossImageColor,omitempty"`
	InputLossImageType  string               `json:"inputLossImageType,omitempty"`
	BlackFrameMsec      int32                `json:"blackFrameMsec,omitempty"`
	RepeatFrameMsec     int32                `json:"repeatFrameMsec,omitempty"`
}

func toInputLossBehaviorOutput(l InputLossBehavior) *inputLossBehaviorOutput {
	if l.InputLossImageColor == "" && l.InputLossImageType == "" && l.BlackFrameMsec == 0 &&
		l.RepeatFrameMsec == 0 && l.InputLossImageSlate.URI == "" {
		return nil
	}

	return &inputLossBehaviorOutput{
		InputLossImageColor: l.InputLossImageColor,
		InputLossImageType:  l.InputLossImageType,
		BlackFrameMsec:      l.BlackFrameMsec,
		RepeatFrameMsec:     l.RepeatFrameMsec,
		InputLossImageSlate: toInputLocationOutput(l.InputLossImageSlate),
	}
}

func extractInputLossBehavior(m map[string]any) InputLossBehavior {
	raw, ok := m["inputLossBehavior"].(map[string]any)
	if !ok {
		return InputLossBehavior{}
	}

	return InputLossBehavior{
		InputLossImageColor: stringFromAny(raw["inputLossImageColor"]),
		InputLossImageType:  stringFromAny(raw["inputLossImageType"]),
		BlackFrameMsec:      int32FromAny(raw["blackFrameMsec"]),
		RepeatFrameMsec:     int32FromAny(raw["repeatFrameMsec"]),
		InputLossImageSlate: extractInputLocation(raw, "inputLossImageSlate"),
	}
}

type outputLockingSettingsOutput struct {
	DisabledLockingSettings map[string]any `json:"disabledLockingSettings,omitempty"`
	EpochLockingSettings    map[string]any `json:"epochLockingSettings,omitempty"`
	PipelineLockingSettings map[string]any `json:"pipelineLockingSettings,omitempty"`
}

func toOutputLockingSettingsOutput(s OutputLockingSettings) *outputLockingSettingsOutput {
	out := &outputLockingSettingsOutput{}

	if d := s.Disabled; d != nil {
		out.DisabledLockingSettings = map[string]any{}
		if d.CustomEpoch != "" {
			out.DisabledLockingSettings["customEpoch"] = d.CustomEpoch
		}
	}

	if e := s.Epoch; e != nil {
		m := map[string]any{}
		if e.CustomEpoch != "" {
			m["customEpoch"] = e.CustomEpoch
		}

		if e.JamSyncTime != "" {
			m["jamSyncTime"] = e.JamSyncTime
		}

		out.EpochLockingSettings = m
	}

	if p := s.Pipeline; p != nil {
		m := map[string]any{}
		if p.CustomEpoch != "" {
			m["customEpoch"] = p.CustomEpoch
		}

		if p.PipelineLockingMethod != "" {
			m["pipelineLockingMethod"] = p.PipelineLockingMethod
		}

		out.PipelineLockingSettings = m
	}

	if s.Disabled == nil && s.Epoch == nil && s.Pipeline == nil {
		return nil
	}

	return out
}

func extractOutputLockingSettings(m map[string]any) OutputLockingSettings {
	raw, ok := m["outputLockingSettings"].(map[string]any)
	if !ok {
		return OutputLockingSettings{}
	}

	var out OutputLockingSettings

	if d, hasDisabled := raw["disabledLockingSettings"].(map[string]any); hasDisabled {
		out.Disabled = &DisabledLockingSettings{CustomEpoch: stringFromAny(d["customEpoch"])}
	}

	if e, hasEpoch := raw["epochLockingSettings"].(map[string]any); hasEpoch {
		out.Epoch = &EpochLockingSettings{
			CustomEpoch: stringFromAny(e["customEpoch"]), JamSyncTime: stringFromAny(e["jamSyncTime"]),
		}
	}

	if p, hasPipeline := raw["pipelineLockingSettings"].(map[string]any); hasPipeline {
		out.Pipeline = &PipelineLockingSettings{
			CustomEpoch:           stringFromAny(p["customEpoch"]),
			PipelineLockingMethod: stringFromAny(p["pipelineLockingMethod"]),
		}
	}

	return out
}

type globalConfigurationOutput struct {
	InputLossBehavior         *inputLossBehaviorOutput     `json:"inputLossBehavior,omitempty"`
	OutputLockingSettings     *outputLockingSettingsOutput `json:"outputLockingSettings,omitempty"`
	InputEndAction            string                       `json:"inputEndAction,omitempty"`
	OutputLockingMode         string                       `json:"outputLockingMode,omitempty"`
	OutputTimingSource        string                       `json:"outputTimingSource,omitempty"`
	SupportLowFramerateInputs string                       `json:"supportLowFramerateInputs,omitempty"`
	InitialAudioGain          int32                        `json:"initialAudioGain,omitempty"`
}

func hasGlobalConfiguration(g GlobalConfiguration) bool {
	return g.InputEndAction != "" || g.OutputLockingMode != "" || g.OutputTimingSource != "" ||
		g.SupportLowFramerateInputs != "" || g.InitialAudioGain != 0 ||
		toInputLossBehaviorOutput(g.InputLossBehavior) != nil ||
		toOutputLockingSettingsOutput(g.OutputLockingSettings) != nil
}

func toGlobalConfigurationOutput(g GlobalConfiguration) *globalConfigurationOutput {
	if !hasGlobalConfiguration(g) {
		return nil
	}

	return &globalConfigurationOutput{
		InputEndAction:            g.InputEndAction,
		OutputLockingMode:         g.OutputLockingMode,
		OutputTimingSource:        g.OutputTimingSource,
		SupportLowFramerateInputs: g.SupportLowFramerateInputs,
		InitialAudioGain:          g.InitialAudioGain,
		InputLossBehavior:         toInputLossBehaviorOutput(g.InputLossBehavior),
		OutputLockingSettings:     toOutputLockingSettingsOutput(g.OutputLockingSettings),
	}
}

func extractGlobalConfiguration(m map[string]any) GlobalConfiguration {
	raw, ok := m["globalConfiguration"].(map[string]any)
	if !ok {
		return GlobalConfiguration{}
	}

	return GlobalConfiguration{
		InputEndAction:            stringFromAny(raw["inputEndAction"]),
		OutputLockingMode:         stringFromAny(raw["outputLockingMode"]),
		OutputTimingSource:        stringFromAny(raw["outputTimingSource"]),
		SupportLowFramerateInputs: stringFromAny(raw["supportLowFramerateInputs"]),
		InitialAudioGain:          int32FromAny(raw["initialAudioGain"]),
		InputLossBehavior:         extractInputLossBehavior(raw),
		OutputLockingSettings:     extractOutputLockingSettings(raw),
	}
}

// -- ThumbnailConfiguration --

type thumbnailConfigurationOutput struct {
	State string `json:"state,omitempty"`
}

func toThumbnailConfigurationOutput(t ThumbnailConfiguration) *thumbnailConfigurationOutput {
	if t.State == "" {
		return nil
	}

	return &thumbnailConfigurationOutput{State: t.State}
}

func extractThumbnailConfiguration(m map[string]any) ThumbnailConfiguration {
	raw, ok := m["thumbnailConfiguration"].(map[string]any)
	if !ok {
		return ThumbnailConfiguration{}
	}

	return ThumbnailConfiguration{State: stringFromAny(raw["state"])}
}

// -- AudioDescriptions --

type audioDescriptionOutput struct {
	Name                string `json:"name"`
	AudioSelectorName   string `json:"audioSelectorName,omitempty"`
	LanguageCode        string `json:"languageCode,omitempty"`
	LanguageCodeControl string `json:"languageCodeControl,omitempty"`
	AudioType           string `json:"audioType,omitempty"`
	AudioTypeControl    string `json:"audioTypeControl,omitempty"`
	StreamName          string `json:"streamName,omitempty"`
}

func toAudioDescriptionsOutput(descs []AudioDescription) []audioDescriptionOutput {
	if len(descs) == 0 {
		return nil
	}

	out := make([]audioDescriptionOutput, 0, len(descs))
	for _, d := range descs {
		out = append(out, audioDescriptionOutput(d))
	}

	return out
}

func extractAudioDescriptions(raw []any) []AudioDescription {
	out := make([]AudioDescription, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, AudioDescription{
			Name:                stringFromAny(m["name"]),
			AudioSelectorName:   stringFromAny(m["audioSelectorName"]),
			LanguageCode:        stringFromAny(m["languageCode"]),
			LanguageCodeControl: stringFromAny(m["languageCodeControl"]),
			AudioType:           stringFromAny(m["audioType"]),
			AudioTypeControl:    stringFromAny(m["audioTypeControl"]),
			StreamName:          stringFromAny(m["streamName"]),
		})
	}

	return out
}

// -- VideoDescriptions --

type videoDescriptionOutput struct {
	Name            string `json:"name"`
	RespondToAfd    string `json:"respondToAfd,omitempty"`
	ScalingBehavior string `json:"scalingBehavior,omitempty"`
	Height          int32  `json:"height,omitempty"`
	Width           int32  `json:"width,omitempty"`
	Sharpness       int32  `json:"sharpness,omitempty"`
}

func toVideoDescriptionsOutput(descs []VideoDescription) []videoDescriptionOutput {
	if len(descs) == 0 {
		return nil
	}

	out := make([]videoDescriptionOutput, 0, len(descs))
	for _, d := range descs {
		out = append(out, videoDescriptionOutput(d))
	}

	return out
}

func extractVideoDescriptions(raw []any) []VideoDescription {
	out := make([]VideoDescription, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, VideoDescription{
			Name:            stringFromAny(m["name"]),
			RespondToAfd:    stringFromAny(m["respondToAfd"]),
			ScalingBehavior: stringFromAny(m["scalingBehavior"]),
			Height:          int32FromAny(m["height"]),
			Width:           int32FromAny(m["width"]),
			Sharpness:       int32FromAny(m["sharpness"]),
		})
	}

	return out
}

// -- CaptionDescriptions --

type captionDescriptionOutput struct {
	CaptionSelectorName  string `json:"captionSelectorName,omitempty"`
	Name                 string `json:"name"`
	Accessibility        string `json:"accessibility,omitempty"`
	DvbDashAccessibility string `json:"dvbDashAccessibility,omitempty"`
	LanguageCode         string `json:"languageCode,omitempty"`
	LanguageDescription  string `json:"languageDescription,omitempty"`
}

func toCaptionDescriptionsOutput(descs []CaptionDescription) []captionDescriptionOutput {
	if len(descs) == 0 {
		return nil
	}

	out := make([]captionDescriptionOutput, 0, len(descs))
	for _, d := range descs {
		out = append(out, captionDescriptionOutput(d))
	}

	return out
}

func extractCaptionDescriptions(raw []any) []CaptionDescription {
	out := make([]CaptionDescription, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, CaptionDescription{
			CaptionSelectorName:  stringFromAny(m["captionSelectorName"]),
			Name:                 stringFromAny(m["name"]),
			Accessibility:        stringFromAny(m["accessibility"]),
			DvbDashAccessibility: stringFromAny(m["dvbDashAccessibility"]),
			LanguageCode:         stringFromAny(m["languageCode"]),
			LanguageDescription:  stringFromAny(m["languageDescription"]),
		})
	}

	return out
}

// -- OutputGroups / EncoderOutput --

type encoderOutputOutput struct {
	OutputName              string   `json:"outputName,omitempty"`
	VideoDescriptionName    string   `json:"videoDescriptionName,omitempty"`
	AudioDescriptionNames   []string `json:"audioDescriptionNames,omitempty"`
	CaptionDescriptionNames []string `json:"captionDescriptionNames,omitempty"`
}

type outputGroupOutput struct {
	Name    string                `json:"name,omitempty"`
	Outputs []encoderOutputOutput `json:"outputs,omitempty"`
}

func toOutputGroupsOutput(groups []OutputGroup) []outputGroupOutput {
	if len(groups) == 0 {
		return nil
	}

	out := make([]outputGroupOutput, 0, len(groups))

	for _, g := range groups {
		outputs := make([]encoderOutputOutput, 0, len(g.Outputs))
		for _, o := range g.Outputs {
			outputs = append(outputs, encoderOutputOutput(o))
		}

		out = append(out, outputGroupOutput{Name: g.Name, Outputs: outputs})
	}

	return out
}

func extractEncoderOutputs(raw []any) []EncoderOutput {
	out := make([]EncoderOutput, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, EncoderOutput{
			OutputName:              stringFromAny(m["outputName"]),
			VideoDescriptionName:    stringFromAny(m["videoDescriptionName"]),
			AudioDescriptionNames:   anySliceToStrings(mustSlice(m["audioDescriptionNames"])),
			CaptionDescriptionNames: anySliceToStrings(mustSlice(m["captionDescriptionNames"])),
		})
	}

	return out
}

func extractOutputGroups(raw []any) []OutputGroup {
	out := make([]OutputGroup, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		g := OutputGroup{Name: stringFromAny(m["name"])}
		if outputs, hasOutputs := m["outputs"].([]any); hasOutputs {
			g.Outputs = extractEncoderOutputs(outputs)
		}

		out = append(out, g)
	}

	return out
}

// -- EncoderSettings (top level) --

type encoderSettingsOutput struct {
	AvailBlanking          *availBlankingOutput          `json:"availBlanking,omitempty"`
	BlackoutSlate          *blackoutSlateOutput          `json:"blackoutSlate,omitempty"`
	FeatureActivations     *featureActivationsOutput     `json:"featureActivations,omitempty"`
	GlobalConfiguration    *globalConfigurationOutput    `json:"globalConfiguration,omitempty"`
	ThumbnailConfiguration *thumbnailConfigurationOutput `json:"thumbnailConfiguration,omitempty"`
	TimecodeConfig         *timecodeConfigOutput         `json:"timecodeConfig,omitempty"`
	AudioDescriptions      []audioDescriptionOutput      `json:"audioDescriptions,omitempty"`
	VideoDescriptions      []videoDescriptionOutput      `json:"videoDescriptions,omitempty"`
	CaptionDescriptions    []captionDescriptionOutput    `json:"captionDescriptions,omitempty"`
	OutputGroups           []outputGroupOutput           `json:"outputGroups,omitempty"`
}

func toEncoderSettingsOutput(s EncoderSettings) *encoderSettingsOutput {
	if !s.hasEncoderSettings() {
		return nil
	}

	return &encoderSettingsOutput{
		AudioDescriptions:      toAudioDescriptionsOutput(s.AudioDescriptions),
		VideoDescriptions:      toVideoDescriptionsOutput(s.VideoDescriptions),
		CaptionDescriptions:    toCaptionDescriptionsOutput(s.CaptionDescriptions),
		OutputGroups:           toOutputGroupsOutput(s.OutputGroups),
		TimecodeConfig:         toTimecodeConfigOutput(s.TimecodeConfig),
		AvailBlanking:          toAvailBlankingOutput(s.AvailBlanking),
		BlackoutSlate:          toBlackoutSlateOutput(s.BlackoutSlate),
		FeatureActivations:     toFeatureActivationsOutput(s.FeatureActivations),
		GlobalConfiguration:    toGlobalConfigurationOutput(s.GlobalConfiguration),
		ThumbnailConfiguration: toThumbnailConfigurationOutput(s.ThumbnailConfiguration),
	}
}

func extractEncoderSettings(body map[string]any) (EncoderSettings, bool) {
	raw, hasEncoderSettings := body["encoderSettings"].(map[string]any)
	if !hasEncoderSettings {
		return EncoderSettings{}, false
	}

	settings := EncoderSettings{
		TimecodeConfig:         extractTimecodeConfig(raw),
		AvailBlanking:          extractAvailBlanking(raw),
		BlackoutSlate:          extractBlackoutSlate(raw),
		FeatureActivations:     extractFeatureActivations(raw),
		GlobalConfiguration:    extractGlobalConfiguration(raw),
		ThumbnailConfiguration: extractThumbnailConfiguration(raw),
	}

	if v, hasAudio := raw["audioDescriptions"].([]any); hasAudio {
		settings.AudioDescriptions = extractAudioDescriptions(v)
	}

	if v, hasVideo := raw["videoDescriptions"].([]any); hasVideo {
		settings.VideoDescriptions = extractVideoDescriptions(v)
	}

	if v, hasCaptions := raw["captionDescriptions"].([]any); hasCaptions {
		settings.CaptionDescriptions = extractCaptionDescriptions(v)
	}

	if v, hasGroups := raw["outputGroups"].([]any); hasGroups {
		settings.OutputGroups = extractOutputGroups(v)
	}

	return settings, true
}

// --- ChannelCreateExtras / ChannelUpdateExtras aggregation ---

// extractChannelCreateExtras parses every gopherstack-jb9i CreateChannelInput
// member (beyond name/channelClass/roleArn/tags/anywhereSettings, parsed by
// handleCreateChannel directly). Split per-field into the extractX helpers
// above/in handler_channels.go to keep this aggregator, and each individual
// parser, under the project's funlen/gocyclo/gocognit budget.
func extractChannelCreateExtras(body map[string]any) ChannelCreateExtras {
	var extras ChannelCreateExtras

	extras.CdiInputSpecification, _ = extractCdiInputSpecification(body)
	extras.ChannelEngineVersion, _ = extractChannelEngineVersion(body)
	extras.InputSpecification, _ = extractInputSpecification(body)
	extras.Vpc, _ = extractVpc(body)
	extras.Maintenance, _ = extractMaintenance(body)
	extras.InferenceSettings, _ = extractInferenceSettings(body)
	extras.LinkedChannelSettings, _ = extractLinkedChannelSettings(body)
	extras.LogLevel, _ = body["logLevel"].(string)

	if v, ok := extractDestinations(body); ok {
		extras.Destinations = v
	}

	if v, ok := extractInputAttachments(body); ok {
		extras.InputAttachments = v
	}

	if v, ok := body["channelSecurityGroups"].([]any); ok {
		extras.ChannelSecurityGroups = anySliceToStrings(v)
	}

	extras.EncoderSettings, _ = extractEncoderSettings(body)

	return extras
}

// extractChannelUpdateExtras is extractChannelCreateExtras' Update-side
// counterpart -- every field is paired with a HasX flag (see
// ChannelUpdateExtras' doc comment).
func extractChannelUpdateExtras(body map[string]any) ChannelUpdateExtras {
	var extras ChannelUpdateExtras

	extras.CdiInputSpecification, extras.HasCdiInputSpecification = extractCdiInputSpecification(body)
	extras.ChannelEngineVersion, extras.HasChannelEngineVersion = extractChannelEngineVersion(body)
	extras.InputSpecification, extras.HasInputSpecification = extractInputSpecification(body)
	extras.Vpc, extras.HasVpc = extractVpc(body)
	extras.Maintenance, extras.HasMaintenance = extractMaintenance(body)
	extras.InferenceSettings, extras.HasInferenceSettings = extractInferenceSettings(body)
	extras.LinkedChannelSettings, extras.HasLinkedChannelSettings = extractLinkedChannelSettings(body)
	extras.Destinations, extras.HasDestinations = extractDestinations(body)
	extras.InputAttachments, extras.HasInputAttachments = extractInputAttachments(body)
	extras.EncoderSettings, extras.HasEncoderSettings = extractEncoderSettings(body)

	if v, ok := body["logLevel"].(string); ok {
		extras.LogLevel, extras.HasLogLevel = v, true
	}

	if v, ok := body["channelSecurityGroups"].([]any); ok {
		extras.ChannelSecurityGroups, extras.HasChannelSecurityGroups = anySliceToStrings(v), true
	}

	return extras
}
