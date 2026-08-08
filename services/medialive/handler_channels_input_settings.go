package medialive

// --- ChannelInputAttachment.InputSettings wire (de)serialization
// (gopherstack-sthr) ---
//
// Every shape here mirrors a real aws-sdk-go-v2/service/medialive types.*
// struct field-for-field -- see the domain type doc comments in
// interfaces.go for exactly what's modeled and its SDK source. Verified
// against v1.101.4 (this repo's pinned go.mod version).

// -- AudioSelector / AudioSelectorSettings --

type inputChannelLevelOutput struct {
	Gain         int32 `json:"gain,omitempty"`
	InputChannel int32 `json:"inputChannel,omitempty"`
}

func toInputChannelLevelsOutput(levels []InputChannelLevel) []inputChannelLevelOutput {
	if len(levels) == 0 {
		return nil
	}

	out := make([]inputChannelLevelOutput, 0, len(levels))
	for _, l := range levels {
		out = append(out, inputChannelLevelOutput(l))
	}

	return out
}

func extractInputChannelLevels(raw []any) []InputChannelLevel {
	out := make([]InputChannelLevel, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, InputChannelLevel{
			Gain:         int32FromAny(m["gain"]),
			InputChannel: int32FromAny(m["inputChannel"]),
		})
	}

	return out
}

type audioChannelMappingOutput struct {
	InputChannelLevels []inputChannelLevelOutput `json:"inputChannelLevels,omitempty"`
	OutputChannel      int32                     `json:"outputChannel,omitempty"`
}

func toAudioChannelMappingsOutput(mappings []AudioChannelMapping) []audioChannelMappingOutput {
	if len(mappings) == 0 {
		return nil
	}

	out := make([]audioChannelMappingOutput, 0, len(mappings))
	for _, m := range mappings {
		out = append(out, audioChannelMappingOutput{
			InputChannelLevels: toInputChannelLevelsOutput(m.InputChannelLevels),
			OutputChannel:      m.OutputChannel,
		})
	}

	return out
}

func extractAudioChannelMappings(raw []any) []AudioChannelMapping {
	out := make([]AudioChannelMapping, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		mapping := AudioChannelMapping{OutputChannel: int32FromAny(m["outputChannel"])}
		if levels, hasLevels := m["inputChannelLevels"].([]any); hasLevels {
			mapping.InputChannelLevels = extractInputChannelLevels(levels)
		}

		out = append(out, mapping)
	}

	return out
}

type remixSettingsOutput struct {
	ChannelMappings []audioChannelMappingOutput `json:"channelMappings,omitempty"`
	ChannelsIn      int32                       `json:"channelsIn,omitempty"`
	ChannelsOut     int32                       `json:"channelsOut,omitempty"`
}

func toRemixSettingsOutput(s *RemixSettings) *remixSettingsOutput {
	if s == nil {
		return nil
	}

	return &remixSettingsOutput{
		ChannelMappings: toAudioChannelMappingsOutput(s.ChannelMappings),
		ChannelsIn:      s.ChannelsIn,
		ChannelsOut:     s.ChannelsOut,
	}
}

func extractRemixSettings(m map[string]any) *RemixSettings {
	raw, ok := m["remixSettings"].(map[string]any)
	if !ok {
		return nil
	}

	out := &RemixSettings{ChannelsIn: int32FromAny(raw["channelsIn"]), ChannelsOut: int32FromAny(raw["channelsOut"])}
	if mappings, hasMappings := raw["channelMappings"].([]any); hasMappings {
		out.ChannelMappings = extractAudioChannelMappings(mappings)
	}

	return out
}

type audioNormalizationSettingsOutput struct {
	Algorithm            string  `json:"algorithm,omitempty"`
	AlgorithmControl     string  `json:"algorithmControl,omitempty"`
	PeakCalculation      string  `json:"peakCalculation,omitempty"`
	PeakLimiterThreshold float64 `json:"peakLimiterThreshold,omitempty"`
	TargetLkfs           float64 `json:"targetLkfs,omitempty"`
}

func toAudioNormalizationSettingsOutput(s *AudioNormalizationSettings) *audioNormalizationSettingsOutput {
	if s == nil {
		return nil
	}

	out := audioNormalizationSettingsOutput(*s)

	return &out
}

func extractAudioNormalizationSettings(m map[string]any) *AudioNormalizationSettings {
	raw, ok := m["audioNormalizationSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &AudioNormalizationSettings{
		Algorithm:            stringFromAny(raw["algorithm"]),
		AlgorithmControl:     stringFromAny(raw["algorithmControl"]),
		PeakCalculation:      stringFromAny(raw["peakCalculation"]),
		PeakLimiterThreshold: float64FromAny(raw["peakLimiterThreshold"]),
		TargetLkfs:           float64FromAny(raw["targetLkfs"]),
	}
}

type audioPreMixerSettingsOutput struct {
	AudioNormalizationSettings *audioNormalizationSettingsOutput `json:"audioNormalizationSettings,omitempty"`
	RemixSettings              *remixSettingsOutput              `json:"remixSettings,omitempty"`
	GainDB                     float64                           `json:"gainDb,omitempty"`
	Channels                   int32                             `json:"channels,omitempty"`
}

func toAudioPreMixerSettingsOutput(s *AudioPreMixerSettings) *audioPreMixerSettingsOutput {
	if s == nil {
		return nil
	}

	return &audioPreMixerSettingsOutput{
		AudioNormalizationSettings: toAudioNormalizationSettingsOutput(s.AudioNormalizationSettings),
		RemixSettings:              toRemixSettingsOutput(s.RemixSettings),
		GainDB:                     s.GainDB,
		Channels:                   s.Channels,
	}
}

func extractAudioPreMixerSettings(m map[string]any) *AudioPreMixerSettings {
	raw, ok := m["premixSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &AudioPreMixerSettings{
		AudioNormalizationSettings: extractAudioNormalizationSettings(raw),
		RemixSettings:              extractRemixSettings(raw),
		GainDB:                     float64FromAny(raw["gainDb"]),
		Channels:                   int32FromAny(raw["channels"]),
	}
}

type audioDolbyEDecodeOutput struct {
	ProgramSelection string `json:"programSelection,omitempty"`
}

func toAudioDolbyEDecodeOutput(d *AudioDolbyEDecode) *audioDolbyEDecodeOutput {
	if d == nil {
		return nil
	}

	return &audioDolbyEDecodeOutput{ProgramSelection: d.ProgramSelection}
}

func extractAudioDolbyEDecode(m map[string]any) *AudioDolbyEDecode {
	raw, ok := m["dolbyEDecode"].(map[string]any)
	if !ok {
		return nil
	}

	return &AudioDolbyEDecode{ProgramSelection: stringFromAny(raw["programSelection"])}
}

type audioPidOutput struct {
	DolbyEDecode   *audioDolbyEDecodeOutput     `json:"dolbyEDecode,omitempty"`
	PremixSettings *audioPreMixerSettingsOutput `json:"premixSettings,omitempty"`
	Pid            int32                        `json:"pid,omitempty"`
}

func toAudioPidsOutput(pids []AudioPid) []audioPidOutput {
	if len(pids) == 0 {
		return nil
	}

	out := make([]audioPidOutput, 0, len(pids))
	for _, p := range pids {
		out = append(out, audioPidOutput{
			Pid:            p.Pid,
			DolbyEDecode:   toAudioDolbyEDecodeOutput(p.DolbyEDecode),
			PremixSettings: toAudioPreMixerSettingsOutput(p.PremixSettings),
		})
	}

	return out
}

func extractAudioPids(raw []any) []AudioPid {
	out := make([]AudioPid, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, AudioPid{
			Pid:            int32FromAny(m["pid"]),
			DolbyEDecode:   extractAudioDolbyEDecode(m),
			PremixSettings: extractAudioPreMixerSettings(m),
		})
	}

	return out
}

type audioPidSelectionOutput struct {
	Pids []audioPidOutput `json:"pids,omitempty"`
	Pid  int32            `json:"pid,omitempty"`
}

func toAudioPidSelectionOutput(s *AudioPidSelection) *audioPidSelectionOutput {
	if s == nil {
		return nil
	}

	return &audioPidSelectionOutput{Pid: s.Pid, Pids: toAudioPidsOutput(s.Pids)}
}

func extractAudioPidSelection(m map[string]any) *AudioPidSelection {
	raw, ok := m["audioPidSelection"].(map[string]any)
	if !ok {
		return nil
	}

	out := &AudioPidSelection{Pid: int32FromAny(raw["pid"])}
	if pids, hasPids := raw["pids"].([]any); hasPids {
		out.Pids = extractAudioPids(pids)
	}

	return out
}

type audioTrackOutput struct {
	PremixSettings *audioPreMixerSettingsOutput `json:"premixSettings,omitempty"`
	Track          int32                        `json:"track,omitempty"`
}

func toAudioTracksOutput(tracks []AudioTrack) []audioTrackOutput {
	if len(tracks) == 0 {
		return nil
	}

	out := make([]audioTrackOutput, 0, len(tracks))
	for _, t := range tracks {
		out = append(out, audioTrackOutput{
			Track:          t.Track,
			PremixSettings: toAudioPreMixerSettingsOutput(t.PremixSettings),
		})
	}

	return out
}

func extractAudioTracks(raw []any) []AudioTrack {
	out := make([]AudioTrack, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, AudioTrack{Track: int32FromAny(m["track"]), PremixSettings: extractAudioPreMixerSettings(m)})
	}

	return out
}

type audioTrackSelectionOutput struct {
	DolbyEDecode *audioDolbyEDecodeOutput `json:"dolbyEDecode,omitempty"`
	Tracks       []audioTrackOutput       `json:"tracks,omitempty"`
}

func toAudioTrackSelectionOutput(s *AudioTrackSelection) *audioTrackSelectionOutput {
	if s == nil {
		return nil
	}

	return &audioTrackSelectionOutput{
		Tracks:       toAudioTracksOutput(s.Tracks),
		DolbyEDecode: toAudioDolbyEDecodeOutput(s.DolbyEDecode),
	}
}

func extractAudioTrackSelection(m map[string]any) *AudioTrackSelection {
	raw, ok := m["audioTrackSelection"].(map[string]any)
	if !ok {
		return nil
	}

	out := &AudioTrackSelection{DolbyEDecode: extractAudioDolbyEDecode(raw)}
	if tracks, hasTracks := raw["tracks"].([]any); hasTracks {
		out.Tracks = extractAudioTracks(tracks)
	}

	return out
}

type audioHlsRenditionSelectionOutput struct {
	GroupID string `json:"groupId,omitempty"`
	Name    string `json:"name,omitempty"`
}

func toAudioHlsRenditionSelectionOutput(s *AudioHlsRenditionSelection) *audioHlsRenditionSelectionOutput {
	if s == nil {
		return nil
	}

	out := audioHlsRenditionSelectionOutput(*s)

	return &out
}

func extractAudioHlsRenditionSelection(m map[string]any) *AudioHlsRenditionSelection {
	raw, ok := m["audioHlsRenditionSelection"].(map[string]any)
	if !ok {
		return nil
	}

	return &AudioHlsRenditionSelection{GroupID: stringFromAny(raw["groupId"]), Name: stringFromAny(raw["name"])}
}

type audioLanguageSelectionOutput struct {
	LanguageCode            string `json:"languageCode,omitempty"`
	LanguageSelectionPolicy string `json:"languageSelectionPolicy,omitempty"`
}

func toAudioLanguageSelectionOutput(s *AudioLanguageSelection) *audioLanguageSelectionOutput {
	if s == nil {
		return nil
	}

	out := audioLanguageSelectionOutput(*s)

	return &out
}

func extractAudioLanguageSelection(m map[string]any) *AudioLanguageSelection {
	raw, ok := m["audioLanguageSelection"].(map[string]any)
	if !ok {
		return nil
	}

	return &AudioLanguageSelection{
		LanguageCode:            stringFromAny(raw["languageCode"]),
		LanguageSelectionPolicy: stringFromAny(raw["languageSelectionPolicy"]),
	}
}

type audioSelectorSettingsOutput struct {
	AudioHlsRenditionSelection *audioHlsRenditionSelectionOutput `json:"audioHlsRenditionSelection,omitempty"`
	AudioLanguageSelection     *audioLanguageSelectionOutput     `json:"audioLanguageSelection,omitempty"`
	AudioPidSelection          *audioPidSelectionOutput          `json:"audioPidSelection,omitempty"`
	AudioTrackSelection        *audioTrackSelectionOutput        `json:"audioTrackSelection,omitempty"`
}

func toAudioSelectorSettingsOutput(s AudioSelectorSettings) *audioSelectorSettingsOutput {
	out := &audioSelectorSettingsOutput{
		AudioHlsRenditionSelection: toAudioHlsRenditionSelectionOutput(s.AudioHlsRenditionSelection),
		AudioLanguageSelection:     toAudioLanguageSelectionOutput(s.AudioLanguageSelection),
		AudioPidSelection:          toAudioPidSelectionOutput(s.AudioPidSelection),
		AudioTrackSelection:        toAudioTrackSelectionOutput(s.AudioTrackSelection),
	}

	if out.AudioHlsRenditionSelection == nil && out.AudioLanguageSelection == nil &&
		out.AudioPidSelection == nil && out.AudioTrackSelection == nil {
		return nil
	}

	return out
}

func extractAudioSelectorSettings(m map[string]any) AudioSelectorSettings {
	raw, ok := m["selectorSettings"].(map[string]any)
	if !ok {
		return AudioSelectorSettings{}
	}

	return AudioSelectorSettings{
		AudioHlsRenditionSelection: extractAudioHlsRenditionSelection(raw),
		AudioLanguageSelection:     extractAudioLanguageSelection(raw),
		AudioPidSelection:          extractAudioPidSelection(raw),
		AudioTrackSelection:        extractAudioTrackSelection(raw),
	}
}

type audioSelectorOutput struct {
	SelectorSettings *audioSelectorSettingsOutput `json:"selectorSettings,omitempty"`
	Name             string                       `json:"name"`
}

func toAudioSelectorsOutput(selectors []AudioSelector) []audioSelectorOutput {
	if len(selectors) == 0 {
		return nil
	}

	out := make([]audioSelectorOutput, 0, len(selectors))
	for _, s := range selectors {
		out = append(out, audioSelectorOutput{
			Name:             s.Name,
			SelectorSettings: toAudioSelectorSettingsOutput(s.SelectorSettings),
		})
	}

	return out
}

func extractAudioSelectors(raw []any) []AudioSelector {
	out := make([]AudioSelector, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(
			out,
			AudioSelector{Name: stringFromAny(m["name"]), SelectorSettings: extractAudioSelectorSettings(m)},
		)
	}

	return out
}

// -- CaptionSelector / CaptionSelectorSettings --

type ancillarySourceSettingsOutput struct {
	SourceAncillaryChannelNumber int32 `json:"sourceAncillaryChannelNumber,omitempty"`
}

func toAncillarySourceSettingsOutput(s *AncillarySourceSettings) *ancillarySourceSettingsOutput {
	if s == nil {
		return nil
	}

	out := ancillarySourceSettingsOutput(*s)

	return &out
}

func extractAncillarySourceSettings(m map[string]any) *AncillarySourceSettings {
	raw, ok := m["ancillarySourceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &AncillarySourceSettings{SourceAncillaryChannelNumber: int32FromAny(raw["sourceAncillaryChannelNumber"])}
}

type dvbSubSourceSettingsOutput struct {
	OcrLanguage string `json:"ocrLanguage,omitempty"`
	Pid         int32  `json:"pid,omitempty"`
}

func toDvbSubSourceSettingsOutput(s *DvbSubSourceSettings) *dvbSubSourceSettingsOutput {
	if s == nil {
		return nil
	}

	out := dvbSubSourceSettingsOutput(*s)

	return &out
}

func extractDvbSubSourceSettings(m map[string]any) *DvbSubSourceSettings {
	raw, ok := m["dvbSubSourceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &DvbSubSourceSettings{OcrLanguage: stringFromAny(raw["ocrLanguage"]), Pid: int32FromAny(raw["pid"])}
}

type embeddedSourceSettingsOutput struct {
	Convert608To708        string `json:"convert608To708,omitempty"`
	Scte20Detection        string `json:"scte20Detection,omitempty"`
	Source608ChannelNumber int32  `json:"source608ChannelNumber,omitempty"`
	Source608TrackNumber   int32  `json:"source608TrackNumber,omitempty"`
}

func toEmbeddedSourceSettingsOutput(s *EmbeddedSourceSettings) *embeddedSourceSettingsOutput {
	if s == nil {
		return nil
	}

	out := embeddedSourceSettingsOutput(*s)

	return &out
}

func extractEmbeddedSourceSettings(m map[string]any) *EmbeddedSourceSettings {
	raw, ok := m["embeddedSourceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &EmbeddedSourceSettings{
		Convert608To708:        stringFromAny(raw["convert608To708"]),
		Scte20Detection:        stringFromAny(raw["scte20Detection"]),
		Source608ChannelNumber: int32FromAny(raw["source608ChannelNumber"]),
		Source608TrackNumber:   int32FromAny(raw["source608TrackNumber"]),
	}
}

type scte20SourceSettingsOutput struct {
	Convert608To708        string `json:"convert608To708,omitempty"`
	Source608ChannelNumber int32  `json:"source608ChannelNumber,omitempty"`
}

func toScte20SourceSettingsOutput(s *Scte20SourceSettings) *scte20SourceSettingsOutput {
	if s == nil {
		return nil
	}

	out := scte20SourceSettingsOutput(*s)

	return &out
}

func extractScte20SourceSettings(m map[string]any) *Scte20SourceSettings {
	raw, ok := m["scte20SourceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Scte20SourceSettings{
		Convert608To708:        stringFromAny(raw["convert608To708"]),
		Source608ChannelNumber: int32FromAny(raw["source608ChannelNumber"]),
	}
}

type scte27SourceSettingsOutput struct {
	OcrLanguage string `json:"ocrLanguage,omitempty"`
	Pid         int32  `json:"pid,omitempty"`
}

func toScte27SourceSettingsOutput(s *Scte27SourceSettings) *scte27SourceSettingsOutput {
	if s == nil {
		return nil
	}

	out := scte27SourceSettingsOutput(*s)

	return &out
}

func extractScte27SourceSettings(m map[string]any) *Scte27SourceSettings {
	raw, ok := m["scte27SourceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Scte27SourceSettings{OcrLanguage: stringFromAny(raw["ocrLanguage"]), Pid: int32FromAny(raw["pid"])}
}

type smartSubtitleSourceSettingsOutput struct {
	CaptionSynchronizationMode string `json:"captionSynchronizationMode,omitempty"`
	InferenceFeedOutput        string `json:"inferenceFeedOutput,omitempty"`
}

func toSmartSubtitleSourceSettingsOutput(s *SmartSubtitleSourceSettings) *smartSubtitleSourceSettingsOutput {
	if s == nil {
		return nil
	}

	out := smartSubtitleSourceSettingsOutput(*s)

	return &out
}

func extractSmartSubtitleSourceSettings(m map[string]any) *SmartSubtitleSourceSettings {
	raw, ok := m["smartSubtitleSourceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &SmartSubtitleSourceSettings{
		CaptionSynchronizationMode: stringFromAny(raw["captionSynchronizationMode"]),
		InferenceFeedOutput:        stringFromAny(raw["inferenceFeedOutput"]),
	}
}

type captionRectangleOutput struct {
	Height     float64 `json:"height,omitempty"`
	LeftOffset float64 `json:"leftOffset,omitempty"`
	TopOffset  float64 `json:"topOffset,omitempty"`
	Width      float64 `json:"width,omitempty"`
}

func toCaptionRectangleOutput(r *CaptionRectangle) *captionRectangleOutput {
	if r == nil {
		return nil
	}

	out := captionRectangleOutput(*r)

	return &out
}

func extractCaptionRectangle(m map[string]any) *CaptionRectangle {
	raw, ok := m["outputRectangle"].(map[string]any)
	if !ok {
		return nil
	}

	return &CaptionRectangle{
		Height:     float64FromAny(raw["height"]),
		LeftOffset: float64FromAny(raw["leftOffset"]),
		TopOffset:  float64FromAny(raw["topOffset"]),
		Width:      float64FromAny(raw["width"]),
	}
}

type teletextSourceSettingsOutput struct {
	OutputRectangle *captionRectangleOutput `json:"outputRectangle,omitempty"`
	PageNumber      string                  `json:"pageNumber,omitempty"`
}

func toTeletextSourceSettingsOutput(s *TeletextSourceSettings) *teletextSourceSettingsOutput {
	if s == nil {
		return nil
	}

	return &teletextSourceSettingsOutput{
		OutputRectangle: toCaptionRectangleOutput(s.OutputRectangle),
		PageNumber:      s.PageNumber,
	}
}

func extractTeletextSourceSettings(m map[string]any) *TeletextSourceSettings {
	raw, ok := m["teletextSourceSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &TeletextSourceSettings{
		OutputRectangle: extractCaptionRectangle(raw),
		PageNumber:      stringFromAny(raw["pageNumber"]),
	}
}

// emptyMarker renders as "{}" on the wire -- a *struct{} rather than a bare
// map so a non-nil-but-empty value isn't dropped by "omitempty" (an empty
// map IS the zero value encoding/json checks for, so map[string]any{} with
// omitempty never serializes; a non-nil *struct{} always does).
type emptyMarker struct{}

type captionSelectorSettingsOutput struct {
	AncillarySourceSettings     *ancillarySourceSettingsOutput     `json:"ancillarySourceSettings,omitempty"`
	AribSourceSettings          *emptyMarker                       `json:"aribSourceSettings,omitempty"`
	DvbSubSourceSettings        *dvbSubSourceSettingsOutput        `json:"dvbSubSourceSettings,omitempty"`
	EmbeddedSourceSettings      *embeddedSourceSettingsOutput      `json:"embeddedSourceSettings,omitempty"`
	Scte20SourceSettings        *scte20SourceSettingsOutput        `json:"scte20SourceSettings,omitempty"`
	Scte27SourceSettings        *scte27SourceSettingsOutput        `json:"scte27SourceSettings,omitempty"`
	SmartSubtitleSourceSettings *smartSubtitleSourceSettingsOutput `json:"smartSubtitleSourceSettings,omitempty"`
	TeletextSourceSettings      *teletextSourceSettingsOutput      `json:"teletextSourceSettings,omitempty"`
}

func toCaptionSelectorSettingsOutput(s CaptionSelectorSettings) *captionSelectorSettingsOutput {
	out := &captionSelectorSettingsOutput{
		AncillarySourceSettings:     toAncillarySourceSettingsOutput(s.AncillarySourceSettings),
		DvbSubSourceSettings:        toDvbSubSourceSettingsOutput(s.DvbSubSourceSettings),
		EmbeddedSourceSettings:      toEmbeddedSourceSettingsOutput(s.EmbeddedSourceSettings),
		Scte20SourceSettings:        toScte20SourceSettingsOutput(s.Scte20SourceSettings),
		Scte27SourceSettings:        toScte27SourceSettingsOutput(s.Scte27SourceSettings),
		SmartSubtitleSourceSettings: toSmartSubtitleSourceSettingsOutput(s.SmartSubtitleSourceSettings),
		TeletextSourceSettings:      toTeletextSourceSettingsOutput(s.TeletextSourceSettings),
	}

	if s.AribSourceSettings {
		out.AribSourceSettings = &emptyMarker{}
	}

	if out.AncillarySourceSettings == nil && out.AribSourceSettings == nil && out.DvbSubSourceSettings == nil &&
		out.EmbeddedSourceSettings == nil && out.Scte20SourceSettings == nil && out.Scte27SourceSettings == nil &&
		out.SmartSubtitleSourceSettings == nil && out.TeletextSourceSettings == nil {
		return nil
	}

	return out
}

func extractCaptionSelectorSettings(m map[string]any) CaptionSelectorSettings {
	raw, ok := m["selectorSettings"].(map[string]any)
	if !ok {
		return CaptionSelectorSettings{}
	}

	_, hasArib := raw["aribSourceSettings"]

	return CaptionSelectorSettings{
		AncillarySourceSettings:     extractAncillarySourceSettings(raw),
		AribSourceSettings:          hasArib,
		DvbSubSourceSettings:        extractDvbSubSourceSettings(raw),
		EmbeddedSourceSettings:      extractEmbeddedSourceSettings(raw),
		Scte20SourceSettings:        extractScte20SourceSettings(raw),
		Scte27SourceSettings:        extractScte27SourceSettings(raw),
		SmartSubtitleSourceSettings: extractSmartSubtitleSourceSettings(raw),
		TeletextSourceSettings:      extractTeletextSourceSettings(raw),
	}
}

type captionSelectorOutput struct {
	SelectorSettings *captionSelectorSettingsOutput `json:"selectorSettings,omitempty"`
	Name             string                         `json:"name"`
	LanguageCode     string                         `json:"languageCode,omitempty"`
}

func toCaptionSelectorsOutput(selectors []CaptionSelector) []captionSelectorOutput {
	if len(selectors) == 0 {
		return nil
	}

	out := make([]captionSelectorOutput, 0, len(selectors))
	for _, s := range selectors {
		out = append(out, captionSelectorOutput{
			Name:             s.Name,
			LanguageCode:     s.LanguageCode,
			SelectorSettings: toCaptionSelectorSettingsOutput(s.SelectorSettings),
		})
	}

	return out
}

func extractCaptionSelectors(raw []any) []CaptionSelector {
	out := make([]CaptionSelector, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, CaptionSelector{
			Name:             stringFromAny(m["name"]),
			LanguageCode:     stringFromAny(m["languageCode"]),
			SelectorSettings: extractCaptionSelectorSettings(m),
		})
	}

	return out
}

// -- VideoSelector --

type hdr10SettingsOutput struct {
	MaxCll  int32 `json:"maxCll,omitempty"`
	MaxFall int32 `json:"maxFall,omitempty"`
}

func toHdr10SettingsOutput(s *Hdr10Settings) *hdr10SettingsOutput {
	if s == nil {
		return nil
	}

	out := hdr10SettingsOutput(*s)

	return &out
}

func extractHdr10Settings(m map[string]any) *Hdr10Settings {
	raw, ok := m["hdr10Settings"].(map[string]any)
	if !ok {
		return nil
	}

	return &Hdr10Settings{MaxCll: int32FromAny(raw["maxCll"]), MaxFall: int32FromAny(raw["maxFall"])}
}

type videoSelectorColorSpaceSettingsOutput struct {
	Hdr10Settings *hdr10SettingsOutput `json:"hdr10Settings,omitempty"`
}

func toVideoSelectorColorSpaceSettingsOutput(s VideoSelectorColorSpaceSettings) *videoSelectorColorSpaceSettingsOutput {
	hdr := toHdr10SettingsOutput(s.Hdr10Settings)
	if hdr == nil {
		return nil
	}

	return &videoSelectorColorSpaceSettingsOutput{Hdr10Settings: hdr}
}

func extractVideoSelectorColorSpaceSettings(m map[string]any) VideoSelectorColorSpaceSettings {
	raw, ok := m["colorSpaceSettings"].(map[string]any)
	if !ok {
		return VideoSelectorColorSpaceSettings{}
	}

	return VideoSelectorColorSpaceSettings{Hdr10Settings: extractHdr10Settings(raw)}
}

type videoSelectorPidOutput struct {
	Pid int32 `json:"pid,omitempty"`
}

func toVideoSelectorPidOutput(s *VideoSelectorPid) *videoSelectorPidOutput {
	if s == nil {
		return nil
	}

	out := videoSelectorPidOutput(*s)

	return &out
}

func extractVideoSelectorPid(m map[string]any) *VideoSelectorPid {
	raw, ok := m["videoSelectorPid"].(map[string]any)
	if !ok {
		return nil
	}

	return &VideoSelectorPid{Pid: int32FromAny(raw["pid"])}
}

type videoSelectorProgramIDOutput struct {
	ProgramID int32 `json:"programId,omitempty"`
}

func toVideoSelectorProgramIDOutput(s *VideoSelectorProgramID) *videoSelectorProgramIDOutput {
	if s == nil {
		return nil
	}

	out := videoSelectorProgramIDOutput(*s)

	return &out
}

func extractVideoSelectorProgramID(m map[string]any) *VideoSelectorProgramID {
	raw, ok := m["videoSelectorProgramId"].(map[string]any)
	if !ok {
		return nil
	}

	return &VideoSelectorProgramID{ProgramID: int32FromAny(raw["programId"])}
}

type videoSelectorSettingsOutput struct {
	VideoSelectorPid       *videoSelectorPidOutput       `json:"videoSelectorPid,omitempty"`
	VideoSelectorProgramID *videoSelectorProgramIDOutput `json:"videoSelectorProgramId,omitempty"`
}

func toVideoSelectorSettingsOutput(s VideoSelectorSettings) *videoSelectorSettingsOutput {
	out := &videoSelectorSettingsOutput{
		VideoSelectorPid:       toVideoSelectorPidOutput(s.VideoSelectorPid),
		VideoSelectorProgramID: toVideoSelectorProgramIDOutput(s.VideoSelectorProgramID),
	}

	if out.VideoSelectorPid == nil && out.VideoSelectorProgramID == nil {
		return nil
	}

	return out
}

func extractVideoSelectorSettings(m map[string]any) VideoSelectorSettings {
	raw, ok := m["selectorSettings"].(map[string]any)
	if !ok {
		return VideoSelectorSettings{}
	}

	return VideoSelectorSettings{
		VideoSelectorPid:       extractVideoSelectorPid(raw),
		VideoSelectorProgramID: extractVideoSelectorProgramID(raw),
	}
}

type videoSelectorOutput struct {
	ColorSpaceSettings *videoSelectorColorSpaceSettingsOutput `json:"colorSpaceSettings,omitempty"`
	SelectorSettings   *videoSelectorSettingsOutput           `json:"selectorSettings,omitempty"`
	ColorSpace         string                                 `json:"colorSpace,omitempty"`
	ColorSpaceUsage    string                                 `json:"colorSpaceUsage,omitempty"`
}

func hasVideoSelector(v VideoSelector) bool {
	return v.ColorSpace != "" || v.ColorSpaceUsage != "" ||
		toVideoSelectorColorSpaceSettingsOutput(v.ColorSpaceSettings) != nil ||
		toVideoSelectorSettingsOutput(v.SelectorSettings) != nil
}

func toVideoSelectorOutput(v VideoSelector) *videoSelectorOutput {
	if !hasVideoSelector(v) {
		return nil
	}

	return &videoSelectorOutput{
		ColorSpace:         v.ColorSpace,
		ColorSpaceUsage:    v.ColorSpaceUsage,
		ColorSpaceSettings: toVideoSelectorColorSpaceSettingsOutput(v.ColorSpaceSettings),
		SelectorSettings:   toVideoSelectorSettingsOutput(v.SelectorSettings),
	}
}

func extractVideoSelector(m map[string]any) VideoSelector {
	raw, ok := m["videoSelector"].(map[string]any)
	if !ok {
		return VideoSelector{}
	}

	return VideoSelector{
		ColorSpace:         stringFromAny(raw["colorSpace"]),
		ColorSpaceUsage:    stringFromAny(raw["colorSpaceUsage"]),
		ColorSpaceSettings: extractVideoSelectorColorSpaceSettings(raw),
		SelectorSettings:   extractVideoSelectorSettings(raw),
	}
}

// -- NetworkInputSettings --

type hlsInputSettingsOutput struct {
	Scte35Source   string `json:"scte35Source,omitempty"`
	Bandwidth      int32  `json:"bandwidth,omitempty"`
	BufferSegments int32  `json:"bufferSegments,omitempty"`
	Retries        int32  `json:"retries,omitempty"`
	RetryInterval  int32  `json:"retryInterval,omitempty"`
}

func toHlsInputSettingsOutput(s *HlsInputSettings) *hlsInputSettingsOutput {
	if s == nil {
		return nil
	}

	out := hlsInputSettingsOutput(*s)

	return &out
}

func extractHlsInputSettings(m map[string]any) *HlsInputSettings {
	raw, ok := m["hlsInputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &HlsInputSettings{
		Bandwidth:      int32FromAny(raw["bandwidth"]),
		BufferSegments: int32FromAny(raw["bufferSegments"]),
		Retries:        int32FromAny(raw["retries"]),
		RetryInterval:  int32FromAny(raw["retryInterval"]),
		Scte35Source:   stringFromAny(raw["scte35Source"]),
	}
}

type multicastInputSettingsOutput struct {
	SourceIPAddress string `json:"sourceIpAddress,omitempty"`
}

func toMulticastInputSettingsOutput(s *MulticastInputSettings) *multicastInputSettingsOutput {
	if s == nil {
		return nil
	}

	out := multicastInputSettingsOutput(*s)

	return &out
}

func extractMulticastInputSettings(m map[string]any) *MulticastInputSettings {
	raw, ok := m["multicastInputSettings"].(map[string]any)
	if !ok {
		return nil
	}

	return &MulticastInputSettings{SourceIPAddress: stringFromAny(raw["sourceIpAddress"])}
}

type networkInputSettingsOutput struct {
	HlsInputSettings       *hlsInputSettingsOutput       `json:"hlsInputSettings,omitempty"`
	MulticastInputSettings *multicastInputSettingsOutput `json:"multicastInputSettings,omitempty"`
	ServerValidation       string                        `json:"serverValidation,omitempty"`
}

func toNetworkInputSettingsOutput(s NetworkInputSettings) *networkInputSettingsOutput {
	out := &networkInputSettingsOutput{
		HlsInputSettings:       toHlsInputSettingsOutput(s.HlsInputSettings),
		MulticastInputSettings: toMulticastInputSettingsOutput(s.MulticastInputSettings),
		ServerValidation:       s.ServerValidation,
	}

	if out.HlsInputSettings == nil && out.MulticastInputSettings == nil && out.ServerValidation == "" {
		return nil
	}

	return out
}

func extractNetworkInputSettings(m map[string]any) NetworkInputSettings {
	raw, ok := m["networkInputSettings"].(map[string]any)
	if !ok {
		return NetworkInputSettings{}
	}

	return NetworkInputSettings{
		HlsInputSettings:       extractHlsInputSettings(raw),
		MulticastInputSettings: extractMulticastInputSettings(raw),
		ServerValidation:       stringFromAny(raw["serverValidation"]),
	}
}

// -- InputSettings (top level) --

type inputSettingsOutput struct {
	NetworkInputSettings    *networkInputSettingsOutput `json:"networkInputSettings,omitempty"`
	VideoSelector           *videoSelectorOutput        `json:"videoSelector,omitempty"`
	DeblockFilter           string                      `json:"deblockFilter,omitempty"`
	DenoiseFilter           string                      `json:"denoiseFilter,omitempty"`
	InputFilter             string                      `json:"inputFilter,omitempty"`
	Smpte2038DataPreference string                      `json:"smpte2038DataPreference,omitempty"`
	SourceEndBehavior       string                      `json:"sourceEndBehavior,omitempty"`
	AudioSelectors          []audioSelectorOutput       `json:"audioSelectors,omitempty"`
	CaptionSelectors        []captionSelectorOutput     `json:"captionSelectors,omitempty"`
	FilterStrength          int32                       `json:"filterStrength,omitempty"`
	Scte35Pid               int32                       `json:"scte35Pid,omitempty"`
}

func hasInputSettings(s InputSettings) bool {
	return len(s.AudioSelectors) > 0 || len(s.CaptionSelectors) > 0 || s.DeblockFilter != "" ||
		s.DenoiseFilter != "" || s.FilterStrength != 0 || s.InputFilter != "" || s.Scte35Pid != 0 ||
		s.Smpte2038DataPreference != "" || s.SourceEndBehavior != "" ||
		toNetworkInputSettingsOutput(s.NetworkInputSettings) != nil || hasVideoSelector(s.VideoSelector)
}

func toInputSettingsOutput(s InputSettings) *inputSettingsOutput {
	if !hasInputSettings(s) {
		return nil
	}

	return &inputSettingsOutput{
		AudioSelectors:          toAudioSelectorsOutput(s.AudioSelectors),
		CaptionSelectors:        toCaptionSelectorsOutput(s.CaptionSelectors),
		DeblockFilter:           s.DeblockFilter,
		DenoiseFilter:           s.DenoiseFilter,
		FilterStrength:          s.FilterStrength,
		InputFilter:             s.InputFilter,
		NetworkInputSettings:    toNetworkInputSettingsOutput(s.NetworkInputSettings),
		Scte35Pid:               s.Scte35Pid,
		Smpte2038DataPreference: s.Smpte2038DataPreference,
		SourceEndBehavior:       s.SourceEndBehavior,
		VideoSelector:           toVideoSelectorOutput(s.VideoSelector),
	}
}

func extractInputSettings(m map[string]any) InputSettings {
	raw, ok := m["inputSettings"].(map[string]any)
	if !ok {
		return InputSettings{}
	}

	out := InputSettings{
		DeblockFilter:           stringFromAny(raw["deblockFilter"]),
		DenoiseFilter:           stringFromAny(raw["denoiseFilter"]),
		FilterStrength:          int32FromAny(raw["filterStrength"]),
		InputFilter:             stringFromAny(raw["inputFilter"]),
		Scte35Pid:               int32FromAny(raw["scte35Pid"]),
		Smpte2038DataPreference: stringFromAny(raw["smpte2038DataPreference"]),
		SourceEndBehavior:       stringFromAny(raw["sourceEndBehavior"]),
		NetworkInputSettings:    extractNetworkInputSettings(raw),
		VideoSelector:           extractVideoSelector(raw),
	}

	if v, hasAudio := raw["audioSelectors"].([]any); hasAudio {
		out.AudioSelectors = extractAudioSelectors(v)
	}

	if v, hasCaptions := raw["captionSelectors"].([]any); hasCaptions {
		out.CaptionSelectors = extractCaptionSelectors(v)
	}

	return out
}
