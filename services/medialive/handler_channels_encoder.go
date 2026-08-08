package medialive

// --- EncoderSettings wire (de)serialization ---
//
// Every shape here mirrors a real aws-sdk-go-v2/service/medialive types.*
// struct field-for-field for the subset gopherstack models -- see the domain
// type doc comments in interfaces.go (AvailBlanking, BlackoutSlate,
// FeatureActivations, GlobalConfiguration, ThumbnailConfiguration,
// AudioDescription, VideoDescription, CaptionDescription, OutputGroup,
// EncoderOutput, EncoderSettings) for exactly what is and is not modeled,
// and why. The pre-gopherstack-sthr shapes below were verified against
// v1.97.2 and not re-verified this pass; the AvailConfiguration/
// ColorCorrectionSettings/MotionGraphicsConfiguration/NielsenConfiguration
// section (gopherstack-sthr) was verified against v1.101.4, this repo's
// currently pinned version (go.mod) -- go.mod had already drifted ahead of
// PARITY.md's audited version before this pass; see PARITY.md's Notes.

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

type aacSettingsOutput struct {
	CodingMode      string  `json:"codingMode,omitempty"`
	InputType       string  `json:"inputType,omitempty"`
	Profile         string  `json:"profile,omitempty"`
	RateControlMode string  `json:"rateControlMode,omitempty"`
	RawFormat       string  `json:"rawFormat,omitempty"`
	Spec            string  `json:"spec,omitempty"`
	VbrQuality      string  `json:"vbrQuality,omitempty"`
	Bitrate         float64 `json:"bitrate,omitempty"`
	SampleRate      float64 `json:"sampleRate,omitempty"`
}

func toAacSettingsOutput(s *AacSettings) *aacSettingsOutput {
	if s == nil {
		return nil
	}

	out := aacSettingsOutput(*s)

	return &out
}

func extractAacSettings(m map[string]any) *AacSettings {
	raw, ok := m["aacSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &AacSettings{
		CodingMode:      stringFromAny(raw["codingMode"]),
		InputType:       stringFromAny(raw["inputType"]),
		Profile:         stringFromAny(raw["profile"]),
		RateControlMode: stringFromAny(raw["rateControlMode"]),
		RawFormat:       stringFromAny(raw["rawFormat"]),
		Spec:            stringFromAny(raw["spec"]),
		VbrQuality:      stringFromAny(raw["vbrQuality"]),
		Bitrate:         float64FromAny(raw["bitrate"]),
		SampleRate:      float64FromAny(raw["sampleRate"]),
	}
}

type ac3SettingsOutput struct {
	AttenuationControl string  `json:"attenuationControl,omitempty"`
	BitstreamMode      string  `json:"bitstreamMode,omitempty"`
	CodingMode         string  `json:"codingMode,omitempty"`
	DrcProfile         string  `json:"drcProfile,omitempty"`
	LfeFilter          string  `json:"lfeFilter,omitempty"`
	MetadataControl    string  `json:"metadataControl,omitempty"`
	Bitrate            float64 `json:"bitrate,omitempty"`
	Dialnorm           int32   `json:"dialnorm,omitempty"`
}

func toAc3SettingsOutput(s *Ac3Settings) *ac3SettingsOutput {
	if s == nil {
		return nil
	}

	out := ac3SettingsOutput(*s)

	return &out
}

func extractAc3Settings(m map[string]any) *Ac3Settings {
	raw, ok := m["ac3Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Ac3Settings{
		AttenuationControl: stringFromAny(raw["attenuationControl"]),
		BitstreamMode:      stringFromAny(raw["bitstreamMode"]),
		CodingMode:         stringFromAny(raw["codingMode"]),
		DrcProfile:         stringFromAny(raw["drcProfile"]),
		LfeFilter:          stringFromAny(raw["lfeFilter"]),
		MetadataControl:    stringFromAny(raw["metadataControl"]),
		Bitrate:            float64FromAny(raw["bitrate"]),
		Dialnorm:           int32FromAny(raw["dialnorm"]),
	}
}

type eac3AtmosSettingsOutput struct {
	CodingMode   string  `json:"codingMode,omitempty"`
	DrcLine      string  `json:"drcLine,omitempty"`
	DrcRf        string  `json:"drcRf,omitempty"`
	Bitrate      float64 `json:"bitrate,omitempty"`
	HeightTrim   float64 `json:"heightTrim,omitempty"`
	SurroundTrim float64 `json:"surroundTrim,omitempty"`
	Dialnorm     int32   `json:"dialnorm,omitempty"`
}

func toEac3AtmosSettingsOutput(s *Eac3AtmosSettings) *eac3AtmosSettingsOutput {
	if s == nil {
		return nil
	}

	out := eac3AtmosSettingsOutput(*s)

	return &out
}

func extractEac3AtmosSettings(m map[string]any) *Eac3AtmosSettings {
	raw, ok := m["eac3AtmosSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Eac3AtmosSettings{
		CodingMode:   stringFromAny(raw["codingMode"]),
		DrcLine:      stringFromAny(raw["drcLine"]),
		DrcRf:        stringFromAny(raw["drcRf"]),
		Bitrate:      float64FromAny(raw["bitrate"]),
		HeightTrim:   float64FromAny(raw["heightTrim"]),
		SurroundTrim: float64FromAny(raw["surroundTrim"]),
		Dialnorm:     int32FromAny(raw["dialnorm"]),
	}
}

type eac3SettingsOutput struct {
	AttenuationControl   string  `json:"attenuationControl,omitempty"`
	BitstreamMode        string  `json:"bitstreamMode,omitempty"`
	CodingMode           string  `json:"codingMode,omitempty"`
	DcFilter             string  `json:"dcFilter,omitempty"`
	DrcLine              string  `json:"drcLine,omitempty"`
	DrcRf                string  `json:"drcRf,omitempty"`
	LfeControl           string  `json:"lfeControl,omitempty"`
	LfeFilter            string  `json:"lfeFilter,omitempty"`
	MetadataControl      string  `json:"metadataControl,omitempty"`
	PassthroughControl   string  `json:"passthroughControl,omitempty"`
	PhaseControl         string  `json:"phaseControl,omitempty"`
	StereoDownmix        string  `json:"stereoDownmix,omitempty"`
	SurroundExMode       string  `json:"surroundExMode,omitempty"`
	SurroundMode         string  `json:"surroundMode,omitempty"`
	Bitrate              float64 `json:"bitrate,omitempty"`
	LoRoCenterMixLevel   float64 `json:"loRoCenterMixLevel,omitempty"`
	LoRoSurroundMixLevel float64 `json:"loRoSurroundMixLevel,omitempty"`
	LtRtCenterMixLevel   float64 `json:"ltRtCenterMixLevel,omitempty"`
	LtRtSurroundMixLevel float64 `json:"ltRtSurroundMixLevel,omitempty"`
	Dialnorm             int32   `json:"dialnorm,omitempty"`
}

func toEac3SettingsOutput(s *Eac3Settings) *eac3SettingsOutput {
	if s == nil {
		return nil
	}

	out := eac3SettingsOutput(*s)

	return &out
}

func extractEac3Settings(m map[string]any) *Eac3Settings {
	raw, ok := m["eac3Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Eac3Settings{
		AttenuationControl:   stringFromAny(raw["attenuationControl"]),
		BitstreamMode:        stringFromAny(raw["bitstreamMode"]),
		CodingMode:           stringFromAny(raw["codingMode"]),
		DcFilter:             stringFromAny(raw["dcFilter"]),
		DrcLine:              stringFromAny(raw["drcLine"]),
		DrcRf:                stringFromAny(raw["drcRf"]),
		LfeControl:           stringFromAny(raw["lfeControl"]),
		LfeFilter:            stringFromAny(raw["lfeFilter"]),
		MetadataControl:      stringFromAny(raw["metadataControl"]),
		PassthroughControl:   stringFromAny(raw["passthroughControl"]),
		PhaseControl:         stringFromAny(raw["phaseControl"]),
		StereoDownmix:        stringFromAny(raw["stereoDownmix"]),
		SurroundExMode:       stringFromAny(raw["surroundExMode"]),
		SurroundMode:         stringFromAny(raw["surroundMode"]),
		Bitrate:              float64FromAny(raw["bitrate"]),
		LoRoCenterMixLevel:   float64FromAny(raw["loRoCenterMixLevel"]),
		LoRoSurroundMixLevel: float64FromAny(raw["loRoSurroundMixLevel"]),
		LtRtCenterMixLevel:   float64FromAny(raw["ltRtCenterMixLevel"]),
		LtRtSurroundMixLevel: float64FromAny(raw["ltRtSurroundMixLevel"]),
		Dialnorm:             int32FromAny(raw["dialnorm"]),
	}
}

type mp2SettingsOutput struct {
	CodingMode string  `json:"codingMode,omitempty"`
	Bitrate    float64 `json:"bitrate,omitempty"`
	SampleRate float64 `json:"sampleRate,omitempty"`
}

func toMp2SettingsOutput(s *Mp2Settings) *mp2SettingsOutput {
	if s == nil {
		return nil
	}

	out := mp2SettingsOutput(*s)

	return &out
}

func extractMp2Settings(m map[string]any) *Mp2Settings {
	raw, ok := m["mp2Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Mp2Settings{
		CodingMode: stringFromAny(raw["codingMode"]),
		Bitrate:    float64FromAny(raw["bitrate"]),
		SampleRate: float64FromAny(raw["sampleRate"]),
	}
}

type wavSettingsOutput struct {
	CodingMode string  `json:"codingMode,omitempty"`
	BitDepth   float64 `json:"bitDepth,omitempty"`
	SampleRate float64 `json:"sampleRate,omitempty"`
}

func toWavSettingsOutput(s *WavSettings) *wavSettingsOutput {
	if s == nil {
		return nil
	}

	out := wavSettingsOutput(*s)

	return &out
}

func extractWavSettings(m map[string]any) *WavSettings {
	raw, ok := m["wavSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &WavSettings{
		CodingMode: stringFromAny(raw["codingMode"]),
		BitDepth:   float64FromAny(raw["bitDepth"]),
		SampleRate: float64FromAny(raw["sampleRate"]),
	}
}

type audioCodecSettingsOutput struct {
	AacSettings         *aacSettingsOutput       `json:"aacSettings,omitempty"`
	Ac3Settings         *ac3SettingsOutput       `json:"ac3Settings,omitempty"`
	Eac3AtmosSettings   *eac3AtmosSettingsOutput `json:"eac3AtmosSettings,omitempty"`
	Eac3Settings        *eac3SettingsOutput      `json:"eac3Settings,omitempty"`
	Mp2Settings         *mp2SettingsOutput       `json:"mp2Settings,omitempty"`
	WavSettings         *wavSettingsOutput       `json:"wavSettings,omitempty"`
	PassThroughSettings *emptyMarker             `json:"passThroughSettings,omitempty"`
}

func toAudioCodecSettingsOutput(s *AudioCodecSettings) *audioCodecSettingsOutput {
	if s == nil {
		return nil
	}

	out := &audioCodecSettingsOutput{
		AacSettings:       toAacSettingsOutput(s.AacSettings),
		Ac3Settings:       toAc3SettingsOutput(s.Ac3Settings),
		Eac3AtmosSettings: toEac3AtmosSettingsOutput(s.Eac3AtmosSettings),
		Eac3Settings:      toEac3SettingsOutput(s.Eac3Settings),
		Mp2Settings:       toMp2SettingsOutput(s.Mp2Settings),
		WavSettings:       toWavSettingsOutput(s.WavSettings),
	}

	if s.PassThroughSettings {
		out.PassThroughSettings = &emptyMarker{}
	}

	return out
}

func extractAudioCodecSettings(m map[string]any) *AudioCodecSettings {
	raw, ok := m["codecSettings"].(map[string]any)
	if !ok {
		return nil
	}

	_, hasPassThrough := raw["passThroughSettings"]

	return &AudioCodecSettings{
		AacSettings:         extractAacSettings(raw),
		Ac3Settings:         extractAc3Settings(raw),
		Eac3AtmosSettings:   extractEac3AtmosSettings(raw),
		Eac3Settings:        extractEac3Settings(raw),
		Mp2Settings:         extractMp2Settings(raw),
		WavSettings:         extractWavSettings(raw),
		PassThroughSettings: hasPassThrough,
	}
}

type nielsenCBETOutput struct {
	CbetCheckDigitString string `json:"cbetCheckDigitString,omitempty"`
	CbetStepaside        string `json:"cbetStepaside,omitempty"`
	Csid                 string `json:"csid,omitempty"`
}

func toNielsenCBETOutput(n *NielsenCBET) *nielsenCBETOutput {
	if n == nil {
		return nil
	}

	out := nielsenCBETOutput(*n)

	return &out
}

func extractNielsenCBET(m map[string]any) *NielsenCBET {
	raw, ok := m["nielsenCbetSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &NielsenCBET{
		CbetCheckDigitString: stringFromAny(raw["cbetCheckDigitString"]),
		CbetStepaside:        stringFromAny(raw["cbetStepaside"]),
		Csid:                 stringFromAny(raw["csid"]),
	}
}

type nielsenNaesIiNwOutput struct {
	CheckDigitString string  `json:"checkDigitString,omitempty"`
	Timezone         string  `json:"timezone,omitempty"`
	Sid              float64 `json:"sid,omitempty"`
}

func toNielsenNaesIiNwOutput(n *NielsenNaesIiNw) *nielsenNaesIiNwOutput {
	if n == nil {
		return nil
	}

	out := nielsenNaesIiNwOutput(*n)

	return &out
}

func extractNielsenNaesIiNw(m map[string]any) *NielsenNaesIiNw {
	raw, ok := m["nielsenNaesIiNwSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &NielsenNaesIiNw{
		CheckDigitString: stringFromAny(raw["checkDigitString"]),
		Timezone:         stringFromAny(raw["timezone"]),
		Sid:              float64FromAny(raw["sid"]),
	}
}

type nielsenWatermarksSettingsOutput struct {
	NielsenCbetSettings     *nielsenCBETOutput     `json:"nielsenCbetSettings,omitempty"`
	NielsenNaesIiNwSettings *nielsenNaesIiNwOutput `json:"nielsenNaesIiNwSettings,omitempty"`
	NielsenDistributionType string                 `json:"nielsenDistributionType,omitempty"`
}

func toNielsenWatermarksSettingsOutput(s *NielsenWatermarksSettings) *nielsenWatermarksSettingsOutput {
	if s == nil {
		return nil
	}

	return &nielsenWatermarksSettingsOutput{
		NielsenCbetSettings:     toNielsenCBETOutput(s.NielsenCbetSettings),
		NielsenNaesIiNwSettings: toNielsenNaesIiNwOutput(s.NielsenNaesIiNwSettings),
		NielsenDistributionType: s.NielsenDistributionType,
	}
}

func extractNielsenWatermarksSettings(m map[string]any) *NielsenWatermarksSettings {
	raw, ok := m["nielsenWatermarksSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &NielsenWatermarksSettings{
		NielsenCbetSettings:     extractNielsenCBET(raw),
		NielsenNaesIiNwSettings: extractNielsenNaesIiNw(raw),
		NielsenDistributionType: stringFromAny(raw["nielsenDistributionType"]),
	}
}

type audioWatermarkSettingsOutput struct {
	NielsenWatermarksSettings *nielsenWatermarksSettingsOutput `json:"nielsenWatermarksSettings,omitempty"`
}

func toAudioWatermarkSettingsOutput(s *AudioWatermarkSettings) *audioWatermarkSettingsOutput {
	if s == nil {
		return nil
	}

	return &audioWatermarkSettingsOutput{
		NielsenWatermarksSettings: toNielsenWatermarksSettingsOutput(s.NielsenWatermarksSettings),
	}
}

func extractAudioWatermarkSettings(m map[string]any) *AudioWatermarkSettings {
	raw, ok := m["audioWatermarkingSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &AudioWatermarkSettings{NielsenWatermarksSettings: extractNielsenWatermarksSettings(raw)}
}

type audioDescriptionOutput struct {
	AudioNormalizationSettings *audioNormalizationSettingsOutput `json:"audioNormalizationSettings,omitempty"`
	CodecSettings              *audioCodecSettingsOutput         `json:"codecSettings,omitempty"`
	AudioWatermarkingSettings  *audioWatermarkSettingsOutput     `json:"audioWatermarkingSettings,omitempty"`
	RemixSettings              *remixSettingsOutput              `json:"remixSettings,omitempty"`
	LanguageCodeControl        string                            `json:"languageCodeControl,omitempty"`
	AudioTypeControl           string                            `json:"audioTypeControl,omitempty"`
	StreamName                 string                            `json:"streamName,omitempty"`
	DvbDashAccessibility       string                            `json:"dvbDashAccessibility,omitempty"`
	AudioType                  string                            `json:"audioType,omitempty"`
	Name                       string                            `json:"name"`
	LanguageCode               string                            `json:"languageCode,omitempty"`
	AudioSelectorName          string                            `json:"audioSelectorName,omitempty"`
	AudioDashRoles             []string                          `json:"audioDashRoles,omitempty"`
}

func toAudioDescriptionsOutput(descs []AudioDescription) []audioDescriptionOutput {
	if len(descs) == 0 {
		return nil
	}

	out := make([]audioDescriptionOutput, 0, len(descs))

	for _, d := range descs {
		out = append(out, audioDescriptionOutput{
			Name:                       d.Name,
			AudioSelectorName:          d.AudioSelectorName,
			LanguageCode:               d.LanguageCode,
			LanguageCodeControl:        d.LanguageCodeControl,
			AudioType:                  d.AudioType,
			AudioTypeControl:           d.AudioTypeControl,
			StreamName:                 d.StreamName,
			DvbDashAccessibility:       d.DvbDashAccessibility,
			AudioDashRoles:             d.AudioDashRoles,
			AudioNormalizationSettings: toAudioNormalizationSettingsOutput(d.AudioNormalizationSettings),
			RemixSettings:              toRemixSettingsOutput(d.RemixSettings),
			AudioWatermarkingSettings:  toAudioWatermarkSettingsOutput(d.AudioWatermarkingSettings),
			CodecSettings:              toAudioCodecSettingsOutput(d.CodecSettings),
		})
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
			Name:                       stringFromAny(m["name"]),
			AudioSelectorName:          stringFromAny(m["audioSelectorName"]),
			LanguageCode:               stringFromAny(m["languageCode"]),
			LanguageCodeControl:        stringFromAny(m["languageCodeControl"]),
			AudioType:                  stringFromAny(m["audioType"]),
			AudioTypeControl:           stringFromAny(m["audioTypeControl"]),
			StreamName:                 stringFromAny(m["streamName"]),
			DvbDashAccessibility:       stringFromAny(m["dvbDashAccessibility"]),
			AudioDashRoles:             anySliceToStrings(mustSlice(m["audioDashRoles"])),
			AudioNormalizationSettings: extractAudioNormalizationSettings(m),
			RemixSettings:              extractRemixSettings(m),
			AudioWatermarkingSettings:  extractAudioWatermarkSettings(m),
			CodecSettings:              extractAudioCodecSettings(m),
		})
	}

	return out
}

// -- VideoDescriptions --
//
// VideoCodecSettings (types.go:8582) is the last EncoderSettings union
// (gopherstack-1szb). Its 5 variants (Av1/H264/H265/Mpeg2/FrameCapture) share
// TimecodeBurninSettings; Av1/H264/H265 each carry their own
// ColorSpaceSettings sub-union (not shared -- their variant sets differ);
// H264/H265's FilterSettings sub-union IS identical between the two and
// shares one wire struct, same convention as burnInAndDvbSubOutput.

type timecodeBurninSettingsOutput struct {
	FontSize string `json:"fontSize,omitempty"`
	Position string `json:"position,omitempty"`
	Prefix   string `json:"prefix,omitempty"`
}

func toTimecodeBurninSettingsOutput(s *TimecodeBurninSettings) *timecodeBurninSettingsOutput {
	if s == nil {
		return nil
	}

	out := timecodeBurninSettingsOutput(*s)

	return &out
}

func extractTimecodeBurninSettings(m map[string]any) *TimecodeBurninSettings {
	raw, ok := m["timecodeBurninSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &TimecodeBurninSettings{
		FontSize: stringFromAny(raw["fontSize"]),
		Position: stringFromAny(raw["position"]),
		Prefix:   stringFromAny(raw["prefix"]),
	}
}

// Hdr10Settings/hdr10SettingsOutput/toHdr10SettingsOutput/
// extractHdr10Settings are shared with VideoSelectorColorSpaceSettings
// (handler_channels_input_settings.go) -- types.Hdr10Settings is the same
// SDK shape in both places.

type av1ColorSpaceSettingsOutput struct {
	Hdr10Settings                 *hdr10SettingsOutput `json:"hdr10Settings,omitempty"`
	ColorSpacePassthroughSettings *emptyMarker         `json:"colorSpacePassthroughSettings,omitempty"`
	Hlg2020Settings               *emptyMarker         `json:"hlg2020Settings,omitempty"`
	Rec601Settings                *emptyMarker         `json:"rec601Settings,omitempty"`
	Rec709Settings                *emptyMarker         `json:"rec709Settings,omitempty"`
}

func toAv1ColorSpaceSettingsOutput(s *Av1ColorSpaceSettings) *av1ColorSpaceSettingsOutput {
	if s == nil {
		return nil
	}

	out := &av1ColorSpaceSettingsOutput{Hdr10Settings: toHdr10SettingsOutput(s.Hdr10Settings)}

	if s.ColorSpacePassthroughSettings {
		out.ColorSpacePassthroughSettings = &emptyMarker{}
	}

	if s.Hlg2020Settings {
		out.Hlg2020Settings = &emptyMarker{}
	}

	if s.Rec601Settings {
		out.Rec601Settings = &emptyMarker{}
	}

	if s.Rec709Settings {
		out.Rec709Settings = &emptyMarker{}
	}

	return out
}

func extractAv1ColorSpaceSettings(m map[string]any) *Av1ColorSpaceSettings {
	raw, ok := m["colorSpaceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &Av1ColorSpaceSettings{Hdr10Settings: extractHdr10Settings(raw)}
	_, out.ColorSpacePassthroughSettings = raw["colorSpacePassthroughSettings"]
	_, out.Hlg2020Settings = raw["hlg2020Settings"]
	_, out.Rec601Settings = raw["rec601Settings"]
	_, out.Rec709Settings = raw["rec709Settings"]

	return out
}

type h264ColorSpaceSettingsOutput struct {
	ColorSpacePassthroughSettings *emptyMarker `json:"colorSpacePassthroughSettings,omitempty"`
	Rec601Settings                *emptyMarker `json:"rec601Settings,omitempty"`
	Rec709Settings                *emptyMarker `json:"rec709Settings,omitempty"`
}

func toH264ColorSpaceSettingsOutput(s *H264ColorSpaceSettings) *h264ColorSpaceSettingsOutput {
	if s == nil {
		return nil
	}

	out := &h264ColorSpaceSettingsOutput{}

	if s.ColorSpacePassthroughSettings {
		out.ColorSpacePassthroughSettings = &emptyMarker{}
	}

	if s.Rec601Settings {
		out.Rec601Settings = &emptyMarker{}
	}

	if s.Rec709Settings {
		out.Rec709Settings = &emptyMarker{}
	}

	return out
}

func extractH264ColorSpaceSettings(m map[string]any) *H264ColorSpaceSettings {
	raw, ok := m["colorSpaceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &H264ColorSpaceSettings{}
	_, out.ColorSpacePassthroughSettings = raw["colorSpacePassthroughSettings"]
	_, out.Rec601Settings = raw["rec601Settings"]
	_, out.Rec709Settings = raw["rec709Settings"]

	return out
}

type h265ColorSpaceSettingsOutput struct {
	Hdr10Settings                 *hdr10SettingsOutput `json:"hdr10Settings,omitempty"`
	ColorSpacePassthroughSettings *emptyMarker         `json:"colorSpacePassthroughSettings,omitempty"`
	DolbyVision81Settings         *emptyMarker         `json:"dolbyVision81Settings,omitempty"`
	Hlg2020Settings               *emptyMarker         `json:"hlg2020Settings,omitempty"`
	Rec601Settings                *emptyMarker         `json:"rec601Settings,omitempty"`
	Rec709Settings                *emptyMarker         `json:"rec709Settings,omitempty"`
}

func toH265ColorSpaceSettingsOutput(s *H265ColorSpaceSettings) *h265ColorSpaceSettingsOutput {
	if s == nil {
		return nil
	}

	out := &h265ColorSpaceSettingsOutput{Hdr10Settings: toHdr10SettingsOutput(s.Hdr10Settings)}

	if s.ColorSpacePassthroughSettings {
		out.ColorSpacePassthroughSettings = &emptyMarker{}
	}

	if s.DolbyVision81Settings {
		out.DolbyVision81Settings = &emptyMarker{}
	}

	if s.Hlg2020Settings {
		out.Hlg2020Settings = &emptyMarker{}
	}

	if s.Rec601Settings {
		out.Rec601Settings = &emptyMarker{}
	}

	if s.Rec709Settings {
		out.Rec709Settings = &emptyMarker{}
	}

	return out
}

func extractH265ColorSpaceSettings(m map[string]any) *H265ColorSpaceSettings {
	raw, ok := m["colorSpaceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &H265ColorSpaceSettings{Hdr10Settings: extractHdr10Settings(raw)}
	_, out.ColorSpacePassthroughSettings = raw["colorSpacePassthroughSettings"]
	_, out.DolbyVision81Settings = raw["dolbyVision81Settings"]
	_, out.Hlg2020Settings = raw["hlg2020Settings"]
	_, out.Rec601Settings = raw["rec601Settings"]
	_, out.Rec709Settings = raw["rec709Settings"]

	return out
}

type bandwidthReductionOutput struct {
	PostFilterSharpening string `json:"postFilterSharpening,omitempty"`
	Strength             string `json:"strength,omitempty"`
}

func toBandwidthReductionFilterSettingsOutput(
	s *BandwidthReductionFilterSettings,
) *bandwidthReductionOutput {
	if s == nil {
		return nil
	}

	out := bandwidthReductionOutput(*s)

	return &out
}

func extractBandwidthReductionFilterSettings(raw map[string]any) *BandwidthReductionFilterSettings {
	b, ok := raw["bandwidthReductionFilterSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &BandwidthReductionFilterSettings{
		PostFilterSharpening: stringFromAny(b["postFilterSharpening"]),
		Strength:             stringFromAny(b["strength"]),
	}
}

type temporalFilterSettingsOutput struct {
	PostFilterSharpening string `json:"postFilterSharpening,omitempty"`
	Strength             string `json:"strength,omitempty"`
}

func toTemporalFilterSettingsOutput(s *TemporalFilterSettings) *temporalFilterSettingsOutput {
	if s == nil {
		return nil
	}

	out := temporalFilterSettingsOutput(*s)

	return &out
}

func extractTemporalFilterSettings(raw map[string]any) *TemporalFilterSettings {
	t, ok := raw["temporalFilterSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &TemporalFilterSettings{
		PostFilterSharpening: stringFromAny(t["postFilterSharpening"]),
		Strength:             stringFromAny(t["strength"]),
	}
}

// videoFilterSettingsOutput is the shared wire shape of H264FilterSettings
// and H265FilterSettings (types.go:3020, :3306) -- identical field set.
type videoFilterSettingsOutput struct {
	BandwidthReductionFilterSettings *bandwidthReductionOutput     `json:"bandwidthReductionFilterSettings,omitempty"`
	TemporalFilterSettings           *temporalFilterSettingsOutput `json:"temporalFilterSettings,omitempty"`
}

func videoFilterFieldsToOutput(s H264FilterSettings) *videoFilterSettingsOutput {
	return &videoFilterSettingsOutput{
		BandwidthReductionFilterSettings: toBandwidthReductionFilterSettingsOutput(s.BandwidthReductionFilterSettings),
		TemporalFilterSettings:           toTemporalFilterSettingsOutput(s.TemporalFilterSettings),
	}
}

func toH264FilterSettingsOutput(s *H264FilterSettings) *videoFilterSettingsOutput {
	if s == nil {
		return nil
	}

	return videoFilterFieldsToOutput(*s)
}

func toH265FilterSettingsOutput(s *H265FilterSettings) *videoFilterSettingsOutput {
	if s == nil {
		return nil
	}

	return videoFilterFieldsToOutput(H264FilterSettings(*s))
}

func extractVideoFilterFields(raw map[string]any) H264FilterSettings {
	return H264FilterSettings{
		BandwidthReductionFilterSettings: extractBandwidthReductionFilterSettings(raw),
		TemporalFilterSettings:           extractTemporalFilterSettings(raw),
	}
}

func extractH264FilterSettings(m map[string]any) *H264FilterSettings {
	raw, ok := m["filterSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := extractVideoFilterFields(raw)

	return &out
}

func extractH265FilterSettings(m map[string]any) *H265FilterSettings {
	raw, ok := m["filterSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := H265FilterSettings(extractVideoFilterFields(raw))

	return &out
}

type mpeg2FilterSettingsOutput struct {
	TemporalFilterSettings *temporalFilterSettingsOutput `json:"temporalFilterSettings,omitempty"`
}

func toMpeg2FilterSettingsOutput(s *Mpeg2FilterSettings) *mpeg2FilterSettingsOutput {
	if s == nil {
		return nil
	}

	return &mpeg2FilterSettingsOutput{TemporalFilterSettings: toTemporalFilterSettingsOutput(s.TemporalFilterSettings)}
}

func extractMpeg2FilterSettings(m map[string]any) *Mpeg2FilterSettings {
	raw, ok := m["filterSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Mpeg2FilterSettings{TemporalFilterSettings: extractTemporalFilterSettings(raw)}
}

type av1SettingsOutput struct {
	ColorSpaceSettings     *av1ColorSpaceSettingsOutput  `json:"colorSpaceSettings,omitempty"`
	TimecodeBurninSettings *timecodeBurninSettingsOutput `json:"timecodeBurninSettings,omitempty"`
	AfdSignaling           string                        `json:"afdSignaling,omitempty"`
	BitDepth               string                        `json:"bitDepth,omitempty"`
	FixedAfd               string                        `json:"fixedAfd,omitempty"`
	GopSizeUnits           string                        `json:"gopSizeUnits,omitempty"`
	Level                  string                        `json:"level,omitempty"`
	LookAheadRateControl   string                        `json:"lookAheadRateControl,omitempty"`
	RateControlMode        string                        `json:"rateControlMode,omitempty"`
	SceneChangeDetect      string                        `json:"sceneChangeDetect,omitempty"`
	SpatialAq              string                        `json:"spatialAq,omitempty"`
	TemporalAq             string                        `json:"temporalAq,omitempty"`
	TimecodeInsertion      string                        `json:"timecodeInsertion,omitempty"`
	GopSize                float64                       `json:"gopSize,omitempty"`
	Bitrate                int32                         `json:"bitrate,omitempty"`
	BufSize                int32                         `json:"bufSize,omitempty"`
	FramerateDenominator   int32                         `json:"framerateDenominator,omitempty"`
	FramerateNumerator     int32                         `json:"framerateNumerator,omitempty"`
	MaxBitrate             int32                         `json:"maxBitrate,omitempty"`
	MinBitrate             int32                         `json:"minBitrate,omitempty"`
	MinIInterval           int32                         `json:"minIInterval,omitempty"`
	ParDenominator         int32                         `json:"parDenominator,omitempty"`
	ParNumerator           int32                         `json:"parNumerator,omitempty"`
	QvbrQualityLevel       int32                         `json:"qvbrQualityLevel,omitempty"`
}

func toAv1SettingsOutput(s *Av1Settings) *av1SettingsOutput {
	if s == nil {
		return nil
	}

	return &av1SettingsOutput{
		ColorSpaceSettings:     toAv1ColorSpaceSettingsOutput(s.ColorSpaceSettings),
		TimecodeBurninSettings: toTimecodeBurninSettingsOutput(s.TimecodeBurninSettings),
		AfdSignaling:           s.AfdSignaling,
		BitDepth:               s.BitDepth,
		FixedAfd:               s.FixedAfd,
		GopSizeUnits:           s.GopSizeUnits,
		Level:                  s.Level,
		LookAheadRateControl:   s.LookAheadRateControl,
		RateControlMode:        s.RateControlMode,
		SceneChangeDetect:      s.SceneChangeDetect,
		SpatialAq:              s.SpatialAq,
		TemporalAq:             s.TemporalAq,
		TimecodeInsertion:      s.TimecodeInsertion,
		GopSize:                s.GopSize,
		Bitrate:                s.Bitrate,
		BufSize:                s.BufSize,
		FramerateDenominator:   s.FramerateDenominator,
		FramerateNumerator:     s.FramerateNumerator,
		MaxBitrate:             s.MaxBitrate,
		MinBitrate:             s.MinBitrate,
		MinIInterval:           s.MinIInterval,
		ParDenominator:         s.ParDenominator,
		ParNumerator:           s.ParNumerator,
		QvbrQualityLevel:       s.QvbrQualityLevel,
	}
}

func extractAv1Settings(m map[string]any) *Av1Settings {
	raw, ok := m["av1Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Av1Settings{
		ColorSpaceSettings:     extractAv1ColorSpaceSettings(raw),
		TimecodeBurninSettings: extractTimecodeBurninSettings(raw),
		AfdSignaling:           stringFromAny(raw["afdSignaling"]),
		BitDepth:               stringFromAny(raw["bitDepth"]),
		FixedAfd:               stringFromAny(raw["fixedAfd"]),
		GopSizeUnits:           stringFromAny(raw["gopSizeUnits"]),
		Level:                  stringFromAny(raw["level"]),
		LookAheadRateControl:   stringFromAny(raw["lookAheadRateControl"]),
		RateControlMode:        stringFromAny(raw["rateControlMode"]),
		SceneChangeDetect:      stringFromAny(raw["sceneChangeDetect"]),
		SpatialAq:              stringFromAny(raw["spatialAq"]),
		TemporalAq:             stringFromAny(raw["temporalAq"]),
		TimecodeInsertion:      stringFromAny(raw["timecodeInsertion"]),
		GopSize:                float64FromAny(raw["gopSize"]),
		Bitrate:                int32FromAny(raw["bitrate"]),
		BufSize:                int32FromAny(raw["bufSize"]),
		FramerateDenominator:   int32FromAny(raw["framerateDenominator"]),
		FramerateNumerator:     int32FromAny(raw["framerateNumerator"]),
		MaxBitrate:             int32FromAny(raw["maxBitrate"]),
		MinBitrate:             int32FromAny(raw["minBitrate"]),
		MinIInterval:           int32FromAny(raw["minIInterval"]),
		ParDenominator:         int32FromAny(raw["parDenominator"]),
		ParNumerator:           int32FromAny(raw["parNumerator"]),
		QvbrQualityLevel:       int32FromAny(raw["qvbrQualityLevel"]),
	}
}

type h264SettingsOutput struct {
	ColorSpaceSettings     *h264ColorSpaceSettingsOutput `json:"colorSpaceSettings,omitempty"`
	FilterSettings         *videoFilterSettingsOutput    `json:"filterSettings,omitempty"`
	TimecodeBurninSettings *timecodeBurninSettingsOutput `json:"timecodeBurninSettings,omitempty"`
	AdaptiveQuantization   string                        `json:"adaptiveQuantization,omitempty"`
	AfdSignaling           string                        `json:"afdSignaling,omitempty"`
	ColorMetadata          string                        `json:"colorMetadata,omitempty"`
	EntropyEncoding        string                        `json:"entropyEncoding,omitempty"`
	FixedAfd               string                        `json:"fixedAfd,omitempty"`
	FlickerAq              string                        `json:"flickerAq,omitempty"`
	ForceFieldPictures     string                        `json:"forceFieldPictures,omitempty"`
	FramerateControl       string                        `json:"framerateControl,omitempty"`
	GopBReference          string                        `json:"gopBReference,omitempty"`
	GopSizeUnits           string                        `json:"gopSizeUnits,omitempty"`
	Level                  string                        `json:"level,omitempty"`
	LookAheadRateControl   string                        `json:"lookAheadRateControl,omitempty"`
	ParControl             string                        `json:"parControl,omitempty"`
	Profile                string                        `json:"profile,omitempty"`
	QualityLevel           string                        `json:"qualityLevel,omitempty"`
	RateControlMode        string                        `json:"rateControlMode,omitempty"`
	ScanType               string                        `json:"scanType,omitempty"`
	SceneChangeDetect      string                        `json:"sceneChangeDetect,omitempty"`
	SpatialAq              string                        `json:"spatialAq,omitempty"`
	SubgopLength           string                        `json:"subgopLength,omitempty"`
	Syntax                 string                        `json:"syntax,omitempty"`
	TemporalAq             string                        `json:"temporalAq,omitempty"`
	TimecodeInsertion      string                        `json:"timecodeInsertion,omitempty"`
	GopSize                float64                       `json:"gopSize,omitempty"`
	Bitrate                int32                         `json:"bitrate,omitempty"`
	BufFillPct             int32                         `json:"bufFillPct,omitempty"`
	BufSize                int32                         `json:"bufSize,omitempty"`
	FramerateDenominator   int32                         `json:"framerateDenominator,omitempty"`
	FramerateNumerator     int32                         `json:"framerateNumerator,omitempty"`
	GopClosedCadence       int32                         `json:"gopClosedCadence,omitempty"`
	GopNumBFrames          int32                         `json:"gopNumBFrames,omitempty"`
	MaxBitrate             int32                         `json:"maxBitrate,omitempty"`
	MinBitrate             int32                         `json:"minBitrate,omitempty"`
	MinIInterval           int32                         `json:"minIInterval,omitempty"`
	MinQp                  int32                         `json:"minQp,omitempty"`
	NumRefFrames           int32                         `json:"numRefFrames,omitempty"`
	ParDenominator         int32                         `json:"parDenominator,omitempty"`
	ParNumerator           int32                         `json:"parNumerator,omitempty"`
	QvbrQualityLevel       int32                         `json:"qvbrQualityLevel,omitempty"`
	Slices                 int32                         `json:"slices,omitempty"`
	Softness               int32                         `json:"softness,omitempty"`
}

func toH264SettingsOutput(s *H264Settings) *h264SettingsOutput {
	if s == nil {
		return nil
	}

	return &h264SettingsOutput{
		ColorSpaceSettings:     toH264ColorSpaceSettingsOutput(s.ColorSpaceSettings),
		FilterSettings:         toH264FilterSettingsOutput(s.FilterSettings),
		TimecodeBurninSettings: toTimecodeBurninSettingsOutput(s.TimecodeBurninSettings),
		AdaptiveQuantization:   s.AdaptiveQuantization,
		AfdSignaling:           s.AfdSignaling,
		ColorMetadata:          s.ColorMetadata,
		EntropyEncoding:        s.EntropyEncoding,
		FixedAfd:               s.FixedAfd,
		FlickerAq:              s.FlickerAq,
		ForceFieldPictures:     s.ForceFieldPictures,
		FramerateControl:       s.FramerateControl,
		GopBReference:          s.GopBReference,
		GopSizeUnits:           s.GopSizeUnits,
		Level:                  s.Level,
		LookAheadRateControl:   s.LookAheadRateControl,
		ParControl:             s.ParControl,
		Profile:                s.Profile,
		QualityLevel:           s.QualityLevel,
		RateControlMode:        s.RateControlMode,
		ScanType:               s.ScanType,
		SceneChangeDetect:      s.SceneChangeDetect,
		SpatialAq:              s.SpatialAq,
		SubgopLength:           s.SubgopLength,
		Syntax:                 s.Syntax,
		TemporalAq:             s.TemporalAq,
		TimecodeInsertion:      s.TimecodeInsertion,
		GopSize:                s.GopSize,
		Bitrate:                s.Bitrate,
		BufFillPct:             s.BufFillPct,
		BufSize:                s.BufSize,
		FramerateDenominator:   s.FramerateDenominator,
		FramerateNumerator:     s.FramerateNumerator,
		GopClosedCadence:       s.GopClosedCadence,
		GopNumBFrames:          s.GopNumBFrames,
		MaxBitrate:             s.MaxBitrate,
		MinBitrate:             s.MinBitrate,
		MinIInterval:           s.MinIInterval,
		MinQp:                  s.MinQp,
		NumRefFrames:           s.NumRefFrames,
		ParDenominator:         s.ParDenominator,
		ParNumerator:           s.ParNumerator,
		QvbrQualityLevel:       s.QvbrQualityLevel,
		Slices:                 s.Slices,
		Softness:               s.Softness,
	}
}

func extractH264Settings(m map[string]any) *H264Settings {
	raw, ok := m["h264Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &H264Settings{
		ColorSpaceSettings:     extractH264ColorSpaceSettings(raw),
		FilterSettings:         extractH264FilterSettings(raw),
		TimecodeBurninSettings: extractTimecodeBurninSettings(raw),
		AdaptiveQuantization:   stringFromAny(raw["adaptiveQuantization"]),
		AfdSignaling:           stringFromAny(raw["afdSignaling"]),
		ColorMetadata:          stringFromAny(raw["colorMetadata"]),
		EntropyEncoding:        stringFromAny(raw["entropyEncoding"]),
		FixedAfd:               stringFromAny(raw["fixedAfd"]),
		FlickerAq:              stringFromAny(raw["flickerAq"]),
		ForceFieldPictures:     stringFromAny(raw["forceFieldPictures"]),
		FramerateControl:       stringFromAny(raw["framerateControl"]),
		GopBReference:          stringFromAny(raw["gopBReference"]),
		GopSizeUnits:           stringFromAny(raw["gopSizeUnits"]),
		Level:                  stringFromAny(raw["level"]),
		LookAheadRateControl:   stringFromAny(raw["lookAheadRateControl"]),
		ParControl:             stringFromAny(raw["parControl"]),
		Profile:                stringFromAny(raw["profile"]),
		QualityLevel:           stringFromAny(raw["qualityLevel"]),
		RateControlMode:        stringFromAny(raw["rateControlMode"]),
		ScanType:               stringFromAny(raw["scanType"]),
		SceneChangeDetect:      stringFromAny(raw["sceneChangeDetect"]),
		SpatialAq:              stringFromAny(raw["spatialAq"]),
		SubgopLength:           stringFromAny(raw["subgopLength"]),
		Syntax:                 stringFromAny(raw["syntax"]),
		TemporalAq:             stringFromAny(raw["temporalAq"]),
		TimecodeInsertion:      stringFromAny(raw["timecodeInsertion"]),
		GopSize:                float64FromAny(raw["gopSize"]),
		Bitrate:                int32FromAny(raw["bitrate"]),
		BufFillPct:             int32FromAny(raw["bufFillPct"]),
		BufSize:                int32FromAny(raw["bufSize"]),
		FramerateDenominator:   int32FromAny(raw["framerateDenominator"]),
		FramerateNumerator:     int32FromAny(raw["framerateNumerator"]),
		GopClosedCadence:       int32FromAny(raw["gopClosedCadence"]),
		GopNumBFrames:          int32FromAny(raw["gopNumBFrames"]),
		MaxBitrate:             int32FromAny(raw["maxBitrate"]),
		MinBitrate:             int32FromAny(raw["minBitrate"]),
		MinIInterval:           int32FromAny(raw["minIInterval"]),
		MinQp:                  int32FromAny(raw["minQp"]),
		NumRefFrames:           int32FromAny(raw["numRefFrames"]),
		ParDenominator:         int32FromAny(raw["parDenominator"]),
		ParNumerator:           int32FromAny(raw["parNumerator"]),
		QvbrQualityLevel:       int32FromAny(raw["qvbrQualityLevel"]),
		Slices:                 int32FromAny(raw["slices"]),
		Softness:               int32FromAny(raw["softness"]),
	}
}

type h265SettingsOutput struct {
	ColorSpaceSettings          *h265ColorSpaceSettingsOutput `json:"colorSpaceSettings,omitempty"`
	FilterSettings              *videoFilterSettingsOutput    `json:"filterSettings,omitempty"`
	TimecodeBurninSettings      *timecodeBurninSettingsOutput `json:"timecodeBurninSettings,omitempty"`
	AdaptiveQuantization        string                        `json:"adaptiveQuantization,omitempty"`
	AfdSignaling                string                        `json:"afdSignaling,omitempty"`
	AlternativeTransferFunction string                        `json:"alternativeTransferFunction,omitempty"`
	ColorMetadata               string                        `json:"colorMetadata,omitempty"`
	Deblocking                  string                        `json:"deblocking,omitempty"`
	FixedAfd                    string                        `json:"fixedAfd,omitempty"`
	FlickerAq                   string                        `json:"flickerAq,omitempty"`
	GopBReference               string                        `json:"gopBReference,omitempty"`
	GopSizeUnits                string                        `json:"gopSizeUnits,omitempty"`
	Level                       string                        `json:"level,omitempty"`
	LookAheadRateControl        string                        `json:"lookAheadRateControl,omitempty"`
	MvOverPictureBoundaries     string                        `json:"mvOverPictureBoundaries,omitempty"`
	MvTemporalPredictor         string                        `json:"mvTemporalPredictor,omitempty"`
	Profile                     string                        `json:"profile,omitempty"`
	RateControlMode             string                        `json:"rateControlMode,omitempty"`
	ScanType                    string                        `json:"scanType,omitempty"`
	SceneChangeDetect           string                        `json:"sceneChangeDetect,omitempty"`
	SubgopLength                string                        `json:"subgopLength,omitempty"`
	Tier                        string                        `json:"tier,omitempty"`
	TilePadding                 string                        `json:"tilePadding,omitempty"`
	TimecodeInsertion           string                        `json:"timecodeInsertion,omitempty"`
	TreeblockSize               string                        `json:"treeblockSize,omitempty"`
	GopSize                     float64                       `json:"gopSize,omitempty"`
	Bitrate                     int32                         `json:"bitrate,omitempty"`
	BufSize                     int32                         `json:"bufSize,omitempty"`
	FramerateDenominator        int32                         `json:"framerateDenominator,omitempty"`
	FramerateNumerator          int32                         `json:"framerateNumerator,omitempty"`
	GopClosedCadence            int32                         `json:"gopClosedCadence,omitempty"`
	GopNumBFrames               int32                         `json:"gopNumBFrames,omitempty"`
	MaxBitrate                  int32                         `json:"maxBitrate,omitempty"`
	MinBitrate                  int32                         `json:"minBitrate,omitempty"`
	MinIInterval                int32                         `json:"minIInterval,omitempty"`
	MinQp                       int32                         `json:"minQp,omitempty"`
	ParDenominator              int32                         `json:"parDenominator,omitempty"`
	ParNumerator                int32                         `json:"parNumerator,omitempty"`
	QvbrQualityLevel            int32                         `json:"qvbrQualityLevel,omitempty"`
	Slices                      int32                         `json:"slices,omitempty"`
	TileHeight                  int32                         `json:"tileHeight,omitempty"`
	TileWidth                   int32                         `json:"tileWidth,omitempty"`
}

func toH265SettingsOutput(s *H265Settings) *h265SettingsOutput {
	if s == nil {
		return nil
	}

	return &h265SettingsOutput{
		ColorSpaceSettings:          toH265ColorSpaceSettingsOutput(s.ColorSpaceSettings),
		FilterSettings:              toH265FilterSettingsOutput(s.FilterSettings),
		TimecodeBurninSettings:      toTimecodeBurninSettingsOutput(s.TimecodeBurninSettings),
		AdaptiveQuantization:        s.AdaptiveQuantization,
		AfdSignaling:                s.AfdSignaling,
		AlternativeTransferFunction: s.AlternativeTransferFunction,
		ColorMetadata:               s.ColorMetadata,
		Deblocking:                  s.Deblocking,
		FixedAfd:                    s.FixedAfd,
		FlickerAq:                   s.FlickerAq,
		GopBReference:               s.GopBReference,
		GopSizeUnits:                s.GopSizeUnits,
		Level:                       s.Level,
		LookAheadRateControl:        s.LookAheadRateControl,
		MvOverPictureBoundaries:     s.MvOverPictureBoundaries,
		MvTemporalPredictor:         s.MvTemporalPredictor,
		Profile:                     s.Profile,
		RateControlMode:             s.RateControlMode,
		ScanType:                    s.ScanType,
		SceneChangeDetect:           s.SceneChangeDetect,
		SubgopLength:                s.SubgopLength,
		Tier:                        s.Tier,
		TilePadding:                 s.TilePadding,
		TimecodeInsertion:           s.TimecodeInsertion,
		TreeblockSize:               s.TreeblockSize,
		GopSize:                     s.GopSize,
		Bitrate:                     s.Bitrate,
		BufSize:                     s.BufSize,
		FramerateDenominator:        s.FramerateDenominator,
		FramerateNumerator:          s.FramerateNumerator,
		GopClosedCadence:            s.GopClosedCadence,
		GopNumBFrames:               s.GopNumBFrames,
		MaxBitrate:                  s.MaxBitrate,
		MinBitrate:                  s.MinBitrate,
		MinIInterval:                s.MinIInterval,
		MinQp:                       s.MinQp,
		ParDenominator:              s.ParDenominator,
		ParNumerator:                s.ParNumerator,
		QvbrQualityLevel:            s.QvbrQualityLevel,
		Slices:                      s.Slices,
		TileHeight:                  s.TileHeight,
		TileWidth:                   s.TileWidth,
	}
}

func extractH265Settings(m map[string]any) *H265Settings {
	raw, ok := m["h265Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &H265Settings{
		ColorSpaceSettings:          extractH265ColorSpaceSettings(raw),
		FilterSettings:              extractH265FilterSettings(raw),
		TimecodeBurninSettings:      extractTimecodeBurninSettings(raw),
		AdaptiveQuantization:        stringFromAny(raw["adaptiveQuantization"]),
		AfdSignaling:                stringFromAny(raw["afdSignaling"]),
		AlternativeTransferFunction: stringFromAny(raw["alternativeTransferFunction"]),
		ColorMetadata:               stringFromAny(raw["colorMetadata"]),
		Deblocking:                  stringFromAny(raw["deblocking"]),
		FixedAfd:                    stringFromAny(raw["fixedAfd"]),
		FlickerAq:                   stringFromAny(raw["flickerAq"]),
		GopBReference:               stringFromAny(raw["gopBReference"]),
		GopSizeUnits:                stringFromAny(raw["gopSizeUnits"]),
		Level:                       stringFromAny(raw["level"]),
		LookAheadRateControl:        stringFromAny(raw["lookAheadRateControl"]),
		MvOverPictureBoundaries:     stringFromAny(raw["mvOverPictureBoundaries"]),
		MvTemporalPredictor:         stringFromAny(raw["mvTemporalPredictor"]),
		Profile:                     stringFromAny(raw["profile"]),
		RateControlMode:             stringFromAny(raw["rateControlMode"]),
		ScanType:                    stringFromAny(raw["scanType"]),
		SceneChangeDetect:           stringFromAny(raw["sceneChangeDetect"]),
		SubgopLength:                stringFromAny(raw["subgopLength"]),
		Tier:                        stringFromAny(raw["tier"]),
		TilePadding:                 stringFromAny(raw["tilePadding"]),
		TimecodeInsertion:           stringFromAny(raw["timecodeInsertion"]),
		TreeblockSize:               stringFromAny(raw["treeblockSize"]),
		GopSize:                     float64FromAny(raw["gopSize"]),
		Bitrate:                     int32FromAny(raw["bitrate"]),
		BufSize:                     int32FromAny(raw["bufSize"]),
		FramerateDenominator:        int32FromAny(raw["framerateDenominator"]),
		FramerateNumerator:          int32FromAny(raw["framerateNumerator"]),
		GopClosedCadence:            int32FromAny(raw["gopClosedCadence"]),
		GopNumBFrames:               int32FromAny(raw["gopNumBFrames"]),
		MaxBitrate:                  int32FromAny(raw["maxBitrate"]),
		MinBitrate:                  int32FromAny(raw["minBitrate"]),
		MinIInterval:                int32FromAny(raw["minIInterval"]),
		MinQp:                       int32FromAny(raw["minQp"]),
		ParDenominator:              int32FromAny(raw["parDenominator"]),
		ParNumerator:                int32FromAny(raw["parNumerator"]),
		QvbrQualityLevel:            int32FromAny(raw["qvbrQualityLevel"]),
		Slices:                      int32FromAny(raw["slices"]),
		TileHeight:                  int32FromAny(raw["tileHeight"]),
		TileWidth:                   int32FromAny(raw["tileWidth"]),
	}
}

type mpeg2SettingsOutput struct {
	FilterSettings         *mpeg2FilterSettingsOutput    `json:"filterSettings,omitempty"`
	TimecodeBurninSettings *timecodeBurninSettingsOutput `json:"timecodeBurninSettings,omitempty"`
	AdaptiveQuantization   string                        `json:"adaptiveQuantization,omitempty"`
	AfdSignaling           string                        `json:"afdSignaling,omitempty"`
	ColorMetadata          string                        `json:"colorMetadata,omitempty"`
	ColorSpace             string                        `json:"colorSpace,omitempty"`
	DisplayAspectRatio     string                        `json:"displayAspectRatio,omitempty"`
	FixedAfd               string                        `json:"fixedAfd,omitempty"`
	GopSizeUnits           string                        `json:"gopSizeUnits,omitempty"`
	ScanType               string                        `json:"scanType,omitempty"`
	SubgopLength           string                        `json:"subgopLength,omitempty"`
	TimecodeInsertion      string                        `json:"timecodeInsertion,omitempty"`
	GopSize                float64                       `json:"gopSize,omitempty"`
	FramerateDenominator   int32                         `json:"framerateDenominator,omitempty"`
	FramerateNumerator     int32                         `json:"framerateNumerator,omitempty"`
	GopClosedCadence       int32                         `json:"gopClosedCadence,omitempty"`
	GopNumBFrames          int32                         `json:"gopNumBFrames,omitempty"`
}

func toMpeg2SettingsOutput(s *Mpeg2Settings) *mpeg2SettingsOutput {
	if s == nil {
		return nil
	}

	return &mpeg2SettingsOutput{
		FilterSettings:         toMpeg2FilterSettingsOutput(s.FilterSettings),
		TimecodeBurninSettings: toTimecodeBurninSettingsOutput(s.TimecodeBurninSettings),
		AdaptiveQuantization:   s.AdaptiveQuantization,
		AfdSignaling:           s.AfdSignaling,
		ColorMetadata:          s.ColorMetadata,
		ColorSpace:             s.ColorSpace,
		DisplayAspectRatio:     s.DisplayAspectRatio,
		FixedAfd:               s.FixedAfd,
		GopSizeUnits:           s.GopSizeUnits,
		ScanType:               s.ScanType,
		SubgopLength:           s.SubgopLength,
		TimecodeInsertion:      s.TimecodeInsertion,
		GopSize:                s.GopSize,
		FramerateDenominator:   s.FramerateDenominator,
		FramerateNumerator:     s.FramerateNumerator,
		GopClosedCadence:       s.GopClosedCadence,
		GopNumBFrames:          s.GopNumBFrames,
	}
}

func extractMpeg2Settings(m map[string]any) *Mpeg2Settings {
	raw, ok := m["mpeg2Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Mpeg2Settings{
		FilterSettings:         extractMpeg2FilterSettings(raw),
		TimecodeBurninSettings: extractTimecodeBurninSettings(raw),
		AdaptiveQuantization:   stringFromAny(raw["adaptiveQuantization"]),
		AfdSignaling:           stringFromAny(raw["afdSignaling"]),
		ColorMetadata:          stringFromAny(raw["colorMetadata"]),
		ColorSpace:             stringFromAny(raw["colorSpace"]),
		DisplayAspectRatio:     stringFromAny(raw["displayAspectRatio"]),
		FixedAfd:               stringFromAny(raw["fixedAfd"]),
		GopSizeUnits:           stringFromAny(raw["gopSizeUnits"]),
		ScanType:               stringFromAny(raw["scanType"]),
		SubgopLength:           stringFromAny(raw["subgopLength"]),
		TimecodeInsertion:      stringFromAny(raw["timecodeInsertion"]),
		GopSize:                float64FromAny(raw["gopSize"]),
		FramerateDenominator:   int32FromAny(raw["framerateDenominator"]),
		FramerateNumerator:     int32FromAny(raw["framerateNumerator"]),
		GopClosedCadence:       int32FromAny(raw["gopClosedCadence"]),
		GopNumBFrames:          int32FromAny(raw["gopNumBFrames"]),
	}
}

type frameCaptureSettingsOutput struct {
	TimecodeBurninSettings *timecodeBurninSettingsOutput `json:"timecodeBurninSettings,omitempty"`
	CaptureIntervalUnits   string                        `json:"captureIntervalUnits,omitempty"`
	CaptureInterval        int32                         `json:"captureInterval,omitempty"`
}

func toFrameCaptureSettingsOutput(s *FrameCaptureSettings) *frameCaptureSettingsOutput {
	if s == nil {
		return nil
	}

	return &frameCaptureSettingsOutput{
		TimecodeBurninSettings: toTimecodeBurninSettingsOutput(s.TimecodeBurninSettings),
		CaptureIntervalUnits:   s.CaptureIntervalUnits,
		CaptureInterval:        s.CaptureInterval,
	}
}

func extractFrameCaptureSettings(m map[string]any) *FrameCaptureSettings {
	raw, ok := m["frameCaptureSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &FrameCaptureSettings{
		TimecodeBurninSettings: extractTimecodeBurninSettings(raw),
		CaptureIntervalUnits:   stringFromAny(raw["captureIntervalUnits"]),
		CaptureInterval:        int32FromAny(raw["captureInterval"]),
	}
}

type videoCodecSettingsOutput struct {
	Av1Settings          *av1SettingsOutput          `json:"av1Settings,omitempty"`
	FrameCaptureSettings *frameCaptureSettingsOutput `json:"frameCaptureSettings,omitempty"`
	H264Settings         *h264SettingsOutput         `json:"h264Settings,omitempty"`
	H265Settings         *h265SettingsOutput         `json:"h265Settings,omitempty"`
	Mpeg2Settings        *mpeg2SettingsOutput        `json:"mpeg2Settings,omitempty"`
}

func toVideoCodecSettingsOutput(s *VideoCodecSettings) *videoCodecSettingsOutput {
	if s == nil {
		return nil
	}

	return &videoCodecSettingsOutput{
		Av1Settings:          toAv1SettingsOutput(s.Av1Settings),
		FrameCaptureSettings: toFrameCaptureSettingsOutput(s.FrameCaptureSettings),
		H264Settings:         toH264SettingsOutput(s.H264Settings),
		H265Settings:         toH265SettingsOutput(s.H265Settings),
		Mpeg2Settings:        toMpeg2SettingsOutput(s.Mpeg2Settings),
	}
}

func extractVideoCodecSettings(m map[string]any) *VideoCodecSettings {
	raw, ok := m["codecSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &VideoCodecSettings{
		Av1Settings:          extractAv1Settings(raw),
		FrameCaptureSettings: extractFrameCaptureSettings(raw),
		H264Settings:         extractH264Settings(raw),
		H265Settings:         extractH265Settings(raw),
		Mpeg2Settings:        extractMpeg2Settings(raw),
	}
}

type videoDescriptionOutput struct {
	CodecSettings   *videoCodecSettingsOutput `json:"codecSettings,omitempty"`
	Name            string                    `json:"name"`
	RespondToAfd    string                    `json:"respondToAfd,omitempty"`
	ScalingBehavior string                    `json:"scalingBehavior,omitempty"`
	Height          int32                     `json:"height,omitempty"`
	Width           int32                     `json:"width,omitempty"`
	Sharpness       int32                     `json:"sharpness,omitempty"`
}

func toVideoDescriptionsOutput(descs []VideoDescription) []videoDescriptionOutput {
	if len(descs) == 0 {
		return nil
	}

	out := make([]videoDescriptionOutput, 0, len(descs))
	for _, d := range descs {
		out = append(out, videoDescriptionOutput{
			CodecSettings:   toVideoCodecSettingsOutput(d.CodecSettings),
			Name:            d.Name,
			RespondToAfd:    d.RespondToAfd,
			ScalingBehavior: d.ScalingBehavior,
			Height:          d.Height,
			Width:           d.Width,
			Sharpness:       d.Sharpness,
		})
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
			CodecSettings:   extractVideoCodecSettings(m),
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

// burnInAndDvbSubOutput is the shared wire shape of BurnInDestinationSettings
// and DvbSubDestinationSettings (types.go:998, :2216) -- identical field set,
// separate DVB-Sub-scoped enums on the SDK side that both collapse to plain
// strings here.
type burnInAndDvbSubOutput struct {
	Font                *inputLocationOutput `json:"font,omitempty"`
	Alignment           string               `json:"alignment,omitempty"`
	BackgroundColor     string               `json:"backgroundColor,omitempty"`
	FontColor           string               `json:"fontColor,omitempty"`
	FontSize            string               `json:"fontSize,omitempty"`
	OutlineColor        string               `json:"outlineColor,omitempty"`
	ShadowColor         string               `json:"shadowColor,omitempty"`
	SubtitleRows        string               `json:"subtitleRows,omitempty"`
	TeletextGridControl string               `json:"teletextGridControl,omitempty"`
	BackgroundOpacity   int32                `json:"backgroundOpacity,omitempty"`
	FontOpacity         int32                `json:"fontOpacity,omitempty"`
	FontResolution      int32                `json:"fontResolution,omitempty"`
	OutlineSize         int32                `json:"outlineSize,omitempty"`
	ShadowOpacity       int32                `json:"shadowOpacity,omitempty"`
	ShadowXOffset       int32                `json:"shadowXOffset,omitempty"`
	ShadowYOffset       int32                `json:"shadowYOffset,omitempty"`
	XPosition           int32                `json:"xPosition,omitempty"`
	YPosition           int32                `json:"yPosition,omitempty"`
}

// burnInFieldsToOutput builds the shared wire shape from
// BurnInDestinationSettings' field set; DvbSubDestinationSettings is
// structurally identical (same field names/types/order), so callers convert
// to it first rather than duplicating this body.
func burnInFieldsToOutput(s BurnInDestinationSettings) *burnInAndDvbSubOutput {
	return &burnInAndDvbSubOutput{
		Font:                toInputLocationOutput(s.Font),
		Alignment:           s.Alignment,
		BackgroundColor:     s.BackgroundColor,
		FontColor:           s.FontColor,
		FontSize:            s.FontSize,
		OutlineColor:        s.OutlineColor,
		ShadowColor:         s.ShadowColor,
		SubtitleRows:        s.SubtitleRows,
		TeletextGridControl: s.TeletextGridControl,
		BackgroundOpacity:   s.BackgroundOpacity,
		FontOpacity:         s.FontOpacity,
		FontResolution:      s.FontResolution,
		OutlineSize:         s.OutlineSize,
		ShadowOpacity:       s.ShadowOpacity,
		ShadowXOffset:       s.ShadowXOffset,
		ShadowYOffset:       s.ShadowYOffset,
		XPosition:           s.XPosition,
		YPosition:           s.YPosition,
	}
}

func toBurnInDestinationSettingsOutput(s *BurnInDestinationSettings) *burnInAndDvbSubOutput {
	if s == nil {
		return nil
	}

	return burnInFieldsToOutput(*s)
}

func toDvbSubDestinationSettingsOutput(s *DvbSubDestinationSettings) *burnInAndDvbSubOutput {
	if s == nil {
		return nil
	}

	return burnInFieldsToOutput(BurnInDestinationSettings(*s))
}

// extractBurnInFields is extractBurnInDestinationSettings/
// extractDvbSubDestinationSettings' shared body -- see burnInFieldsToOutput.
func extractBurnInFields(raw map[string]any) BurnInDestinationSettings {
	return BurnInDestinationSettings{
		Font:                extractInputLocation(raw, "font"),
		Alignment:           stringFromAny(raw["alignment"]),
		BackgroundColor:     stringFromAny(raw["backgroundColor"]),
		FontColor:           stringFromAny(raw["fontColor"]),
		FontSize:            stringFromAny(raw["fontSize"]),
		OutlineColor:        stringFromAny(raw["outlineColor"]),
		ShadowColor:         stringFromAny(raw["shadowColor"]),
		SubtitleRows:        stringFromAny(raw["subtitleRows"]),
		TeletextGridControl: stringFromAny(raw["teletextGridControl"]),
		BackgroundOpacity:   int32FromAny(raw["backgroundOpacity"]),
		FontOpacity:         int32FromAny(raw["fontOpacity"]),
		FontResolution:      int32FromAny(raw["fontResolution"]),
		OutlineSize:         int32FromAny(raw["outlineSize"]),
		ShadowOpacity:       int32FromAny(raw["shadowOpacity"]),
		ShadowXOffset:       int32FromAny(raw["shadowXOffset"]),
		ShadowYOffset:       int32FromAny(raw["shadowYOffset"]),
		XPosition:           int32FromAny(raw["xPosition"]),
		YPosition:           int32FromAny(raw["yPosition"]),
	}
}

func extractBurnInDestinationSettings(m map[string]any) *BurnInDestinationSettings {
	raw, ok := m["burnInDestinationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := extractBurnInFields(raw)

	return &out
}

func extractDvbSubDestinationSettings(m map[string]any) *DvbSubDestinationSettings {
	raw, ok := m["dvbSubDestinationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := DvbSubDestinationSettings(extractBurnInFields(raw))

	return &out
}

type ebuTtDDestinationSettingsOutput struct {
	CopyrightHolder   string `json:"copyrightHolder,omitempty"`
	FontFamily        string `json:"fontFamily,omitempty"`
	FillLineGap       string `json:"fillLineGap,omitempty"`
	StyleControl      string `json:"styleControl,omitempty"`
	DefaultFontSize   int32  `json:"defaultFontSize,omitempty"`
	DefaultLineHeight int32  `json:"defaultLineHeight,omitempty"`
}

func toEbuTtDDestinationSettingsOutput(s *EbuTtDDestinationSettings) *ebuTtDDestinationSettingsOutput {
	if s == nil {
		return nil
	}

	out := ebuTtDDestinationSettingsOutput(*s)

	return &out
}

func extractEbuTtDDestinationSettings(m map[string]any) *EbuTtDDestinationSettings {
	raw, ok := m["ebuTtDDestinationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &EbuTtDDestinationSettings{
		CopyrightHolder:   stringFromAny(raw["copyrightHolder"]),
		FontFamily:        stringFromAny(raw["fontFamily"]),
		FillLineGap:       stringFromAny(raw["fillLineGap"]),
		StyleControl:      stringFromAny(raw["styleControl"]),
		DefaultFontSize:   int32FromAny(raw["defaultFontSize"]),
		DefaultLineHeight: int32FromAny(raw["defaultLineHeight"]),
	}
}

type ttmlDestinationSettingsOutput struct {
	StyleControl string `json:"styleControl,omitempty"`
}

func toTtmlDestinationSettingsOutput(s *TtmlDestinationSettings) *ttmlDestinationSettingsOutput {
	if s == nil {
		return nil
	}

	out := ttmlDestinationSettingsOutput(*s)

	return &out
}

func extractTtmlDestinationSettings(m map[string]any) *TtmlDestinationSettings {
	raw, ok := m["ttmlDestinationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &TtmlDestinationSettings{StyleControl: stringFromAny(raw["styleControl"])}
}

type webvttDestinationSettingsOutput struct {
	StyleControl string `json:"styleControl,omitempty"`
}

func toWebvttDestinationSettingsOutput(s *WebvttDestinationSettings) *webvttDestinationSettingsOutput {
	if s == nil {
		return nil
	}

	out := webvttDestinationSettingsOutput(*s)

	return &out
}

func extractWebvttDestinationSettings(m map[string]any) *WebvttDestinationSettings {
	raw, ok := m["webvttDestinationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &WebvttDestinationSettings{StyleControl: stringFromAny(raw["styleControl"])}
}

type captionDestinationSettingsOutput struct {
	BurnInDestinationSettings *burnInAndDvbSubOutput           `json:"burnInDestinationSettings,omitempty"`
	DvbSubDestinationSettings *burnInAndDvbSubOutput           `json:"dvbSubDestinationSettings,omitempty"`
	EbuTtDDestinationSettings *ebuTtDDestinationSettingsOutput `json:"ebuTtDDestinationSettings,omitempty"`
	TtmlDestinationSettings   *ttmlDestinationSettingsOutput   `json:"ttmlDestinationSettings,omitempty"`
	WebvttDestinationSettings *webvttDestinationSettingsOutput `json:"webvttDestinationSettings,omitempty"`

	AribDestinationSettings               *emptyMarker `json:"aribDestinationSettings,omitempty"`
	EmbeddedDestinationSettings           *emptyMarker `json:"embeddedDestinationSettings,omitempty"`
	EmbeddedPlusScte20DestinationSettings *emptyMarker `json:"embeddedPlusScte20DestinationSettings,omitempty"`
	RtmpCaptionInfoDestinationSettings    *emptyMarker `json:"rtmpCaptionInfoDestinationSettings,omitempty"`
	Scte20PlusEmbeddedDestinationSettings *emptyMarker `json:"scte20PlusEmbeddedDestinationSettings,omitempty"`
	Scte27DestinationSettings             *emptyMarker `json:"scte27DestinationSettings,omitempty"`
	SmpteTtDestinationSettings            *emptyMarker `json:"smpteTtDestinationSettings,omitempty"`
	TeletextDestinationSettings           *emptyMarker `json:"teletextDestinationSettings,omitempty"`
}

func toCaptionDestinationSettingsOutput(s *CaptionDestinationSettings) *captionDestinationSettingsOutput {
	if s == nil {
		return nil
	}

	out := &captionDestinationSettingsOutput{
		BurnInDestinationSettings: toBurnInDestinationSettingsOutput(s.BurnInDestinationSettings),
		DvbSubDestinationSettings: toDvbSubDestinationSettingsOutput(s.DvbSubDestinationSettings),
		EbuTtDDestinationSettings: toEbuTtDDestinationSettingsOutput(s.EbuTtDDestinationSettings),
		TtmlDestinationSettings:   toTtmlDestinationSettingsOutput(s.TtmlDestinationSettings),
		WebvttDestinationSettings: toWebvttDestinationSettingsOutput(s.WebvttDestinationSettings),
	}

	if s.AribDestinationSettings {
		out.AribDestinationSettings = &emptyMarker{}
	}

	if s.EmbeddedDestinationSettings {
		out.EmbeddedDestinationSettings = &emptyMarker{}
	}

	if s.EmbeddedPlusScte20DestinationSettings {
		out.EmbeddedPlusScte20DestinationSettings = &emptyMarker{}
	}

	if s.RtmpCaptionInfoDestinationSettings {
		out.RtmpCaptionInfoDestinationSettings = &emptyMarker{}
	}

	if s.Scte20PlusEmbeddedDestinationSettings {
		out.Scte20PlusEmbeddedDestinationSettings = &emptyMarker{}
	}

	if s.Scte27DestinationSettings {
		out.Scte27DestinationSettings = &emptyMarker{}
	}

	if s.SmpteTtDestinationSettings {
		out.SmpteTtDestinationSettings = &emptyMarker{}
	}

	if s.TeletextDestinationSettings {
		out.TeletextDestinationSettings = &emptyMarker{}
	}

	return out
}

func extractCaptionDestinationSettings(m map[string]any) *CaptionDestinationSettings {
	raw, ok := m["destinationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &CaptionDestinationSettings{
		BurnInDestinationSettings: extractBurnInDestinationSettings(raw),
		DvbSubDestinationSettings: extractDvbSubDestinationSettings(raw),
		EbuTtDDestinationSettings: extractEbuTtDDestinationSettings(raw),
		TtmlDestinationSettings:   extractTtmlDestinationSettings(raw),
		WebvttDestinationSettings: extractWebvttDestinationSettings(raw),
	}

	_, out.AribDestinationSettings = raw["aribDestinationSettings"]
	_, out.EmbeddedDestinationSettings = raw["embeddedDestinationSettings"]
	_, out.EmbeddedPlusScte20DestinationSettings = raw["embeddedPlusScte20DestinationSettings"]
	_, out.RtmpCaptionInfoDestinationSettings = raw["rtmpCaptionInfoDestinationSettings"]
	_, out.Scte20PlusEmbeddedDestinationSettings = raw["scte20PlusEmbeddedDestinationSettings"]
	_, out.Scte27DestinationSettings = raw["scte27DestinationSettings"]
	_, out.SmpteTtDestinationSettings = raw["smpteTtDestinationSettings"]
	_, out.TeletextDestinationSettings = raw["teletextDestinationSettings"]

	return out
}

type captionDescriptionOutput struct {
	DestinationSettings  *captionDestinationSettingsOutput `json:"destinationSettings,omitempty"`
	CaptionSelectorName  string                            `json:"captionSelectorName,omitempty"`
	Name                 string                            `json:"name"`
	Accessibility        string                            `json:"accessibility,omitempty"`
	DvbDashAccessibility string                            `json:"dvbDashAccessibility,omitempty"`
	LanguageCode         string                            `json:"languageCode,omitempty"`
	LanguageDescription  string                            `json:"languageDescription,omitempty"`
	CaptionDashRoles     []string                          `json:"captionDashRoles,omitempty"`
}

func toCaptionDescriptionsOutput(descs []CaptionDescription) []captionDescriptionOutput {
	if len(descs) == 0 {
		return nil
	}

	out := make([]captionDescriptionOutput, 0, len(descs))
	for _, d := range descs {
		out = append(out, captionDescriptionOutput{
			CaptionSelectorName:  d.CaptionSelectorName,
			Name:                 d.Name,
			Accessibility:        d.Accessibility,
			DvbDashAccessibility: d.DvbDashAccessibility,
			LanguageCode:         d.LanguageCode,
			LanguageDescription:  d.LanguageDescription,
			CaptionDashRoles:     d.CaptionDashRoles,
			DestinationSettings:  toCaptionDestinationSettingsOutput(d.DestinationSettings),
		})
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
			CaptionDashRoles:     anySliceToStrings(mustSlice(m["captionDashRoles"])),
			DestinationSettings:  extractCaptionDestinationSettings(m),
		})
	}

	return out
}

// -- OutputGroups / EncoderOutput --

type encoderOutputOutput struct {
	OutputSettings          *outputSettingsOutput `json:"outputSettings,omitempty"`
	OutputName              string                `json:"outputName,omitempty"`
	VideoDescriptionName    string                `json:"videoDescriptionName,omitempty"`
	AudioDescriptionNames   []string              `json:"audioDescriptionNames,omitempty"`
	CaptionDescriptionNames []string              `json:"captionDescriptionNames,omitempty"`
}

type outputGroupOutput struct {
	OutputGroupSettings *outputGroupSettingsOutput `json:"outputGroupSettings,omitempty"`
	Name                string                     `json:"name,omitempty"`
	Outputs             []encoderOutputOutput      `json:"outputs,omitempty"`
}

func toEncoderOutputOutput(o EncoderOutput) encoderOutputOutput {
	return encoderOutputOutput{
		OutputName:              o.OutputName,
		VideoDescriptionName:    o.VideoDescriptionName,
		AudioDescriptionNames:   o.AudioDescriptionNames,
		CaptionDescriptionNames: o.CaptionDescriptionNames,
		OutputSettings:          toOutputSettingsOutput(o.OutputSettings),
	}
}

func toOutputGroupsOutput(groups []OutputGroup) []outputGroupOutput {
	if len(groups) == 0 {
		return nil
	}

	out := make([]outputGroupOutput, 0, len(groups))

	for _, g := range groups {
		outputs := make([]encoderOutputOutput, 0, len(g.Outputs))
		for _, o := range g.Outputs {
			outputs = append(outputs, toEncoderOutputOutput(o))
		}

		out = append(out, outputGroupOutput{
			Name:                g.Name,
			Outputs:             outputs,
			OutputGroupSettings: toOutputGroupSettingsOutput(g.OutputGroupSettings),
		})
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
			OutputSettings:          extractOutputSettings(m),
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

		g := OutputGroup{Name: stringFromAny(m["name"]), OutputGroupSettings: extractOutputGroupSettings(m)}
		if outputs, hasOutputs := m["outputs"].([]any); hasOutputs {
			g.Outputs = extractEncoderOutputs(outputs)
		}

		out = append(out, g)
	}

	return out
}

// -- AvailConfiguration (gopherstack-sthr) --

type esamOutput struct {
	AcquisitionPointID string `json:"acquisitionPointId,omitempty"`
	PoisEndpoint       string `json:"poisEndpoint,omitempty"`
	PasswordParam      string `json:"passwordParam,omitempty"`
	Username           string `json:"username,omitempty"`
	ZoneIdentity       string `json:"zoneIdentity,omitempty"`
	AdAvailOffset      int32  `json:"adAvailOffset,omitempty"`
}

type scte35SpliceInsertOutput struct {
	NoRegionalBlackoutFlag string `json:"noRegionalBlackoutFlag,omitempty"`
	WebDeliveryAllowedFlag string `json:"webDeliveryAllowedFlag,omitempty"`
	AdAvailOffset          int32  `json:"adAvailOffset,omitempty"`
}

type scte35TimeSignalAposOutput struct {
	NoRegionalBlackoutFlag string `json:"noRegionalBlackoutFlag,omitempty"`
	WebDeliveryAllowedFlag string `json:"webDeliveryAllowedFlag,omitempty"`
	AdAvailOffset          int32  `json:"adAvailOffset,omitempty"`
}

type availSettingsOutput struct {
	Esam                 *esamOutput                 `json:"esam,omitempty"`
	Scte35SpliceInsert   *scte35SpliceInsertOutput   `json:"scte35SpliceInsert,omitempty"`
	Scte35TimeSignalApos *scte35TimeSignalAposOutput `json:"scte35TimeSignalApos,omitempty"`
}

type availConfigurationOutput struct {
	AvailSettings           *availSettingsOutput `json:"availSettings,omitempty"`
	Scte35SegmentationScope string               `json:"scte35SegmentationScope,omitempty"`
}

func toAvailSettingsOutput(s ChannelAvailSettings) *availSettingsOutput {
	out := &availSettingsOutput{}

	if e := s.Esam; e != nil {
		out.Esam = &esamOutput{
			AcquisitionPointID: e.AcquisitionPointID,
			PoisEndpoint:       e.PoisEndpoint,
			PasswordParam:      e.PasswordParam,
			Username:           e.Username,
			ZoneIdentity:       e.ZoneIdentity,
			AdAvailOffset:      e.AdAvailOffset,
		}
	}

	if sp := s.Scte35SpliceInsert; sp != nil {
		out.Scte35SpliceInsert = &scte35SpliceInsertOutput{
			NoRegionalBlackoutFlag: sp.NoRegionalBlackoutFlag,
			WebDeliveryAllowedFlag: sp.WebDeliveryAllowedFlag,
			AdAvailOffset:          sp.AdAvailOffset,
		}
	}

	if t := s.Scte35TimeSignalApos; t != nil {
		out.Scte35TimeSignalApos = &scte35TimeSignalAposOutput{
			NoRegionalBlackoutFlag: t.NoRegionalBlackoutFlag,
			WebDeliveryAllowedFlag: t.WebDeliveryAllowedFlag,
			AdAvailOffset:          t.AdAvailOffset,
		}
	}

	if out.Esam == nil && out.Scte35SpliceInsert == nil && out.Scte35TimeSignalApos == nil {
		return nil
	}

	return out
}

func toAvailConfigurationOutput(a ChannelAvailConfiguration) *availConfigurationOutput {
	if !a.hasAvailConfiguration() {
		return nil
	}

	return &availConfigurationOutput{
		AvailSettings:           toAvailSettingsOutput(a.AvailSettings),
		Scte35SegmentationScope: a.Scte35SegmentationScope,
	}
}

func extractAvailSettings(raw map[string]any) ChannelAvailSettings {
	m, ok := raw["availSettings"].(map[string]any)
	if !ok {
		return ChannelAvailSettings{}
	}

	var out ChannelAvailSettings

	if e, hasEsam := m["esam"].(map[string]any); hasEsam {
		out.Esam = &EsamSettings{
			AcquisitionPointID: stringFromAny(e["acquisitionPointId"]),
			PoisEndpoint:       stringFromAny(e["poisEndpoint"]),
			PasswordParam:      stringFromAny(e["passwordParam"]),
			Username:           stringFromAny(e["username"]),
			ZoneIdentity:       stringFromAny(e["zoneIdentity"]),
			AdAvailOffset:      int32FromAny(e["adAvailOffset"]),
		}
	}

	if sp, hasSplice := m["scte35SpliceInsert"].(map[string]any); hasSplice {
		out.Scte35SpliceInsert = &Scte35SpliceInsertSettings{
			NoRegionalBlackoutFlag: stringFromAny(sp["noRegionalBlackoutFlag"]),
			WebDeliveryAllowedFlag: stringFromAny(sp["webDeliveryAllowedFlag"]),
			AdAvailOffset:          int32FromAny(sp["adAvailOffset"]),
		}
	}

	if t, hasApos := m["scte35TimeSignalApos"].(map[string]any); hasApos {
		out.Scte35TimeSignalApos = &Scte35TimeSignalAposSettings{
			NoRegionalBlackoutFlag: stringFromAny(t["noRegionalBlackoutFlag"]),
			WebDeliveryAllowedFlag: stringFromAny(t["webDeliveryAllowedFlag"]),
			AdAvailOffset:          int32FromAny(t["adAvailOffset"]),
		}
	}

	return out
}

func extractAvailConfiguration(raw map[string]any) ChannelAvailConfiguration {
	m, ok := raw["availConfiguration"].(map[string]any)
	if !ok {
		return ChannelAvailConfiguration{}
	}

	return ChannelAvailConfiguration{
		AvailSettings:           extractAvailSettings(m),
		Scte35SegmentationScope: stringFromAny(m["scte35SegmentationScope"]),
	}
}

// -- ColorCorrectionSettings (gopherstack-sthr) --

type colorCorrectionOutput struct {
	InputColorSpace  string `json:"inputColorSpace,omitempty"`
	OutputColorSpace string `json:"outputColorSpace,omitempty"`
	URI              string `json:"uri,omitempty"`
}

type colorCorrectionSettingsOutput struct {
	GlobalColorCorrections []colorCorrectionOutput `json:"globalColorCorrections,omitempty"`
}

func toColorCorrectionSettingsOutput(s ChannelColorCorrectionSettings) *colorCorrectionSettingsOutput {
	if !s.hasColorCorrectionSettings() {
		return nil
	}

	out := make([]colorCorrectionOutput, 0, len(s.GlobalColorCorrections))
	for _, c := range s.GlobalColorCorrections {
		out = append(out, colorCorrectionOutput(c))
	}

	return &colorCorrectionSettingsOutput{GlobalColorCorrections: out}
}

func extractColorCorrectionSettings(raw map[string]any) ChannelColorCorrectionSettings {
	m, ok := raw["colorCorrectionSettings"].(map[string]any)
	if !ok {
		return ChannelColorCorrectionSettings{}
	}

	items, ok := m["globalColorCorrections"].([]any)
	if !ok {
		return ChannelColorCorrectionSettings{}
	}

	out := make([]ChannelColorCorrection, 0, len(items))

	for _, item := range items {
		cm, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, ChannelColorCorrection{
			InputColorSpace:  stringFromAny(cm["inputColorSpace"]),
			OutputColorSpace: stringFromAny(cm["outputColorSpace"]),
			URI:              stringFromAny(cm["uri"]),
		})
	}

	return ChannelColorCorrectionSettings{GlobalColorCorrections: out}
}

// -- MotionGraphicsConfiguration (gopherstack-sthr) --

// htmlMotionGraphicsSettingsOutput mirrors types.HtmlMotionGraphicsSettings,
// which has no fields on the real wire -- an empty JSON object marks the
// variant as set.
type htmlMotionGraphicsSettingsOutput struct{}

type motionGraphicsSettingsOutput struct {
	HTMLMotionGraphicsSettings *htmlMotionGraphicsSettingsOutput `json:"htmlMotionGraphicsSettings,omitempty"`
}

type motionGraphicsConfigurationOutput struct {
	MotionGraphicsSettings  *motionGraphicsSettingsOutput `json:"motionGraphicsSettings,omitempty"`
	MotionGraphicsInsertion string                        `json:"motionGraphicsInsertion,omitempty"`
}

func toMotionGraphicsConfigurationOutput(m ChannelMotionGraphicsConfiguration) *motionGraphicsConfigurationOutput {
	if !m.hasMotionGraphicsConfiguration() {
		return nil
	}

	out := &motionGraphicsConfigurationOutput{MotionGraphicsInsertion: m.MotionGraphicsInsertion}
	if m.MotionGraphicsSettings.HTMLMotionGraphicsSettings {
		out.MotionGraphicsSettings = &motionGraphicsSettingsOutput{
			HTMLMotionGraphicsSettings: &htmlMotionGraphicsSettingsOutput{},
		}
	}

	return out
}

func extractMotionGraphicsConfiguration(raw map[string]any) ChannelMotionGraphicsConfiguration {
	m, ok := raw["motionGraphicsConfiguration"].(map[string]any)
	if !ok {
		return ChannelMotionGraphicsConfiguration{}
	}

	out := ChannelMotionGraphicsConfiguration{
		MotionGraphicsInsertion: stringFromAny(m["motionGraphicsInsertion"]),
	}

	if s, hasSettings := m["motionGraphicsSettings"].(map[string]any); hasSettings {
		_, hasHTML := s["htmlMotionGraphicsSettings"]
		out.MotionGraphicsSettings.HTMLMotionGraphicsSettings = hasHTML
	}

	return out
}

// -- NielsenConfiguration (gopherstack-sthr) --

type nielsenConfigurationOutput struct {
	DistributorID          string `json:"distributorId,omitempty"`
	NielsenPcmToID3Tagging string `json:"nielsenPcmToId3Tagging,omitempty"`
}

func toNielsenConfigurationOutput(n ChannelNielsenConfiguration) *nielsenConfigurationOutput {
	if !n.hasNielsenConfiguration() {
		return nil
	}

	return &nielsenConfigurationOutput{
		DistributorID:          n.DistributorID,
		NielsenPcmToID3Tagging: n.NielsenPcmToID3Tagging,
	}
}

func extractNielsenConfiguration(raw map[string]any) ChannelNielsenConfiguration {
	m, ok := raw["nielsenConfiguration"].(map[string]any)
	if !ok {
		return ChannelNielsenConfiguration{}
	}

	return ChannelNielsenConfiguration{
		DistributorID:          stringFromAny(m["distributorId"]),
		NielsenPcmToID3Tagging: stringFromAny(m["nielsenPcmToId3Tagging"]),
	}
}

// -- EncoderSettings (top level) --

type encoderSettingsOutput struct {
	AvailBlanking               *availBlankingOutput               `json:"availBlanking,omitempty"`
	AvailConfiguration          *availConfigurationOutput          `json:"availConfiguration,omitempty"`
	BlackoutSlate               *blackoutSlateOutput               `json:"blackoutSlate,omitempty"`
	ColorCorrectionSettings     *colorCorrectionSettingsOutput     `json:"colorCorrectionSettings,omitempty"`
	FeatureActivations          *featureActivationsOutput          `json:"featureActivations,omitempty"`
	GlobalConfiguration         *globalConfigurationOutput         `json:"globalConfiguration,omitempty"`
	MotionGraphicsConfiguration *motionGraphicsConfigurationOutput `json:"motionGraphicsConfiguration,omitempty"`
	NielsenConfiguration        *nielsenConfigurationOutput        `json:"nielsenConfiguration,omitempty"`
	ThumbnailConfiguration      *thumbnailConfigurationOutput      `json:"thumbnailConfiguration,omitempty"`
	TimecodeConfig              *timecodeConfigOutput              `json:"timecodeConfig,omitempty"`
	AudioDescriptions           []audioDescriptionOutput           `json:"audioDescriptions,omitempty"`
	VideoDescriptions           []videoDescriptionOutput           `json:"videoDescriptions,omitempty"`
	CaptionDescriptions         []captionDescriptionOutput         `json:"captionDescriptions,omitempty"`
	OutputGroups                []outputGroupOutput                `json:"outputGroups,omitempty"`
}

func toEncoderSettingsOutput(s EncoderSettings) *encoderSettingsOutput {
	if !s.hasEncoderSettings() {
		return nil
	}

	return &encoderSettingsOutput{
		AudioDescriptions:           toAudioDescriptionsOutput(s.AudioDescriptions),
		VideoDescriptions:           toVideoDescriptionsOutput(s.VideoDescriptions),
		CaptionDescriptions:         toCaptionDescriptionsOutput(s.CaptionDescriptions),
		OutputGroups:                toOutputGroupsOutput(s.OutputGroups),
		TimecodeConfig:              toTimecodeConfigOutput(s.TimecodeConfig),
		AvailBlanking:               toAvailBlankingOutput(s.AvailBlanking),
		BlackoutSlate:               toBlackoutSlateOutput(s.BlackoutSlate),
		FeatureActivations:          toFeatureActivationsOutput(s.FeatureActivations),
		GlobalConfiguration:         toGlobalConfigurationOutput(s.GlobalConfiguration),
		ThumbnailConfiguration:      toThumbnailConfigurationOutput(s.ThumbnailConfiguration),
		AvailConfiguration:          toAvailConfigurationOutput(s.AvailConfiguration),
		ColorCorrectionSettings:     toColorCorrectionSettingsOutput(s.ColorCorrectionSettings),
		MotionGraphicsConfiguration: toMotionGraphicsConfigurationOutput(s.MotionGraphicsConfiguration),
		NielsenConfiguration:        toNielsenConfigurationOutput(s.NielsenConfiguration),
	}
}

func extractEncoderSettings(body map[string]any) (EncoderSettings, bool) {
	raw, hasEncoderSettings := body["encoderSettings"].(map[string]any)
	if !hasEncoderSettings {
		return EncoderSettings{}, false
	}

	settings := EncoderSettings{
		TimecodeConfig:              extractTimecodeConfig(raw),
		AvailBlanking:               extractAvailBlanking(raw),
		BlackoutSlate:               extractBlackoutSlate(raw),
		FeatureActivations:          extractFeatureActivations(raw),
		GlobalConfiguration:         extractGlobalConfiguration(raw),
		ThumbnailConfiguration:      extractThumbnailConfiguration(raw),
		AvailConfiguration:          extractAvailConfiguration(raw),
		ColorCorrectionSettings:     extractColorCorrectionSettings(raw),
		MotionGraphicsConfiguration: extractMotionGraphicsConfiguration(raw),
		NielsenConfiguration:        extractNielsenConfiguration(raw),
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
