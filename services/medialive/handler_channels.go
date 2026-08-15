package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Channel handlers ---

// Tags first: reduces GC pointer scan from 104 to 96 bytes.
//
// JSON tags are lowerCamelCase to match the real DescribeChannelOutput wire
// shape (aws-sdk-go-v2/service/medialive deserializers.go switches on
// exact-case keys "arn"/"id"/"name"/... -- a PascalCase key like "Arn" is
// silently ignored by the real SDK client's decoder, leaving every field at
// its zero value).
// anywhereSettingsOutput mirrors types.DescribeAnywhereSettings's wire shape
// (lowerCamel "clusterId"/"channelPlacementGroupId").
type anywhereSettingsOutput struct {
	ClusterID               string `json:"clusterId,omitempty"`
	ChannelPlacementGroupID string `json:"channelPlacementGroupId,omitempty"`
}

func toAnywhereSettingsOutput(s ChannelAnywhereSettings) *anywhereSettingsOutput {
	if !s.hasAnywhereSettings() {
		return nil
	}

	return &anywhereSettingsOutput{
		ClusterID:               s.ClusterID,
		ChannelPlacementGroupID: s.ChannelPlacementGroupID,
	}
}

// extractAnywhereSettings parses the "anywhereSettings" request-body object
// shared by CreateChannelInput/UpdateChannelInput (lowerCamel "clusterId"/
// "channelPlacementGroupId" -- verified against
// awsRestjson1_serializeOpDocumentCreateChannelInput). The second return
// reports whether the key was present, so UpdateChannel can distinguish
// "omitted" (leave unchanged) from an explicit object.
func extractAnywhereSettings(body map[string]any) (ChannelAnywhereSettings, bool) {
	raw, ok := body["anywhereSettings"].(map[string]any)
	if !ok {
		return ChannelAnywhereSettings{}, false
	}

	clusterID, _ := raw["clusterId"].(string)
	cpgID, _ := raw["channelPlacementGroupId"].(string)

	return ChannelAnywhereSettings{ClusterID: clusterID, ChannelPlacementGroupID: cpgID}, true
}

// --- CdiInputSpecification / InputSpecification / ChannelEngineVersion ---

type cdiInputSpecificationOutput struct {
	Resolution string `json:"resolution,omitempty"`
}

func toCdiInputSpecificationOutput(s CdiInputSpecification) *cdiInputSpecificationOutput {
	if !s.hasCdiInputSpecification() {
		return nil
	}

	return &cdiInputSpecificationOutput{Resolution: s.Resolution}
}

func extractCdiInputSpecification(body map[string]any) (CdiInputSpecification, bool) {
	raw, ok := body["cdiInputSpecification"].(map[string]any)
	if !ok {
		return CdiInputSpecification{}, false
	}

	resolution, _ := raw["resolution"].(string)

	return CdiInputSpecification{Resolution: resolution}, true
}

type inputSpecificationOutput struct {
	Codec          string `json:"codec,omitempty"`
	MaximumBitrate string `json:"maximumBitrate,omitempty"`
	Resolution     string `json:"resolution,omitempty"`
}

func toInputSpecificationOutput(s InputSpecification) *inputSpecificationOutput {
	if !s.hasInputSpecification() {
		return nil
	}

	return &inputSpecificationOutput{Codec: s.Codec, MaximumBitrate: s.MaximumBitrate, Resolution: s.Resolution}
}

func extractInputSpecification(body map[string]any) (InputSpecification, bool) {
	raw, ok := body["inputSpecification"].(map[string]any)
	if !ok {
		return InputSpecification{}, false
	}

	codec, _ := raw["codec"].(string)
	maxBitrate, _ := raw["maximumBitrate"].(string)
	resolution, _ := raw["resolution"].(string)

	return InputSpecification{Codec: codec, MaximumBitrate: maxBitrate, Resolution: resolution}, true
}

// channelEngineVersionOutput mirrors types.ChannelEngineVersionResponse.
// ExpirationDate is a real wire field (__timestampIso8601) that gopherstack
// cannot compute without a real version-lifecycle engine -- always omitted,
// same convention as ListVersions' ExpirationDate.
type channelEngineVersionOutput struct {
	Version string `json:"version,omitempty"`
}

func toChannelEngineVersionOutput(v ChannelEngineVersion) *channelEngineVersionOutput {
	if v.Version == "" {
		return nil
	}

	return &channelEngineVersionOutput{Version: v.Version}
}

func extractChannelEngineVersion(body map[string]any) (ChannelEngineVersion, bool) {
	raw, ok := body["channelEngineVersion"].(map[string]any)
	if !ok {
		return ChannelEngineVersion{}, false
	}

	version, _ := raw["version"].(string)

	return ChannelEngineVersion{Version: version}, true
}

// --- Vpc / Maintenance ---

// channelVpcOutput mirrors types.VpcOutputSettingsDescription.
// AvailabilityZones/NetworkInterfaceIds are real wire fields MediaLive
// computes from a live VPC/ENI integration gopherstack does not have --
// always omitted, never fabricated (see ChannelVpcSettings' doc comment).
type channelVpcOutput struct {
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
	SubnetIDs        []string `json:"subnetIds,omitempty"`
}

func toChannelVpcOutput(v ChannelVpcSettings) *channelVpcOutput {
	if !v.hasVpc() {
		return nil
	}

	return &channelVpcOutput{SubnetIDs: v.SubnetIDs, SecurityGroupIDs: v.SecurityGroupIDs}
}

func extractVpc(body map[string]any) (ChannelVpcSettings, bool) {
	raw, ok := body["vpc"].(map[string]any)
	if !ok {
		return ChannelVpcSettings{}, false
	}

	subnetIDs, _ := raw["subnetIds"].([]any)
	pubIDs, _ := raw["publicAddressAllocationIds"].([]any)
	sgIDs, _ := raw["securityGroupIds"].([]any)

	return ChannelVpcSettings{
		SubnetIDs:                  anySliceToStrings(subnetIDs),
		PublicAddressAllocationIDs: anySliceToStrings(pubIDs),
		SecurityGroupIDs:           anySliceToStrings(sgIDs),
	}, true
}

// maintenanceOutput mirrors types.MaintenanceStatus. MaintenanceDeadline is
// a real wire field MediaLive computes from its own maintenance scheduler --
// gopherstack has none, so it's always omitted (see ChannelMaintenance's doc
// comment).
type maintenanceOutput struct {
	MaintenanceDay           string `json:"maintenanceDay,omitempty"`
	MaintenanceScheduledDate string `json:"maintenanceScheduledDate,omitempty"`
	MaintenanceStartTime     string `json:"maintenanceStartTime,omitempty"`
}

func toMaintenanceOutput(m ChannelMaintenance) *maintenanceOutput {
	if !m.hasMaintenance() {
		return nil
	}

	return &maintenanceOutput{
		MaintenanceDay:           m.Day,
		MaintenanceStartTime:     m.StartTime,
		MaintenanceScheduledDate: m.ScheduledDate,
	}
}

// extractMaintenance parses the "maintenance" request-body object.
// maintenanceScheduledDate is only a real member of UpdateChannelInput's
// MaintenanceUpdateSettings (not CreateChannelInput's
// MaintenanceCreateSettings), but reading it unconditionally on Create is
// harmless: a real CreateChannel caller never sends it.
func extractMaintenance(body map[string]any) (ChannelMaintenance, bool) {
	raw, ok := body["maintenance"].(map[string]any)
	if !ok {
		return ChannelMaintenance{}, false
	}

	day, _ := raw["maintenanceDay"].(string)
	startTime, _ := raw["maintenanceStartTime"].(string)
	scheduledDate, _ := raw["maintenanceScheduledDate"].(string)

	return ChannelMaintenance{Day: day, StartTime: startTime, ScheduledDate: scheduledDate}, true
}

// --- InferenceSettings ---

type audioFeedInputOutput struct {
	AudioSelectorName string `json:"audioSelectorName,omitempty"`
	FeedInput         string `json:"feedInput,omitempty"`
}

type inferenceSettingsOutput struct {
	FeedArn         string                 `json:"feedArn,omitempty"`
	AudioFeedInputs []audioFeedInputOutput `json:"audioFeedInputs,omitempty"`
}

func toInferenceSettingsOutput(s ChannelInferenceSettings) *inferenceSettingsOutput {
	if !s.hasInferenceSettings() {
		return nil
	}

	inputs := make([]audioFeedInputOutput, 0, len(s.AudioFeedInputs))
	for _, a := range s.AudioFeedInputs {
		inputs = append(inputs, audioFeedInputOutput(a))
	}

	return &inferenceSettingsOutput{AudioFeedInputs: inputs, FeedArn: s.FeedArn}
}

func extractAudioFeedInputs(raw []any) []AudioFeedInputMapping {
	inputs := make([]AudioFeedInputMapping, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		selectorName, _ := m["audioSelectorName"].(string)
		feedInput, _ := m["feedInput"].(string)
		inputs = append(inputs, AudioFeedInputMapping{AudioSelectorName: selectorName, FeedInput: feedInput})
	}

	return inputs
}

func extractInferenceSettings(body map[string]any) (ChannelInferenceSettings, bool) {
	raw, ok := body["inferenceSettings"].(map[string]any)
	if !ok {
		return ChannelInferenceSettings{}, false
	}

	feedArn, _ := raw["feedArn"].(string)

	var inputs []AudioFeedInputMapping
	if rawInputs, hasInputs := raw["audioFeedInputs"].([]any); hasInputs {
		inputs = extractAudioFeedInputs(rawInputs)
	}

	return ChannelInferenceSettings{AudioFeedInputs: inputs, FeedArn: feedArn}, true
}

// --- LinkedChannelSettings ---

type followerChannelSettingsOutput struct {
	LinkedChannelType string `json:"linkedChannelType,omitempty"`
	PrimaryChannelArn string `json:"primaryChannelArn,omitempty"`
}

type primaryChannelSettingsOutput struct {
	LinkedChannelType    string   `json:"linkedChannelType,omitempty"`
	FollowingChannelArns []string `json:"followingChannelArns,omitempty"`
}

type linkedChannelSettingsOutput struct {
	FollowerChannelSettings *followerChannelSettingsOutput `json:"followerChannelSettings,omitempty"`
	PrimaryChannelSettings  *primaryChannelSettingsOutput  `json:"primaryChannelSettings,omitempty"`
}

func toLinkedChannelSettingsOutput(s ChannelLinkedChannelSettings) *linkedChannelSettingsOutput {
	if !s.hasLinkedChannelSettings() && len(s.Primary.FollowingChannelArns) == 0 {
		return nil
	}

	out := &linkedChannelSettingsOutput{}
	if s.Follower.LinkedChannelType != "" || s.Follower.PrimaryChannelArn != "" {
		out.FollowerChannelSettings = &followerChannelSettingsOutput{
			LinkedChannelType: s.Follower.LinkedChannelType,
			PrimaryChannelArn: s.Follower.PrimaryChannelArn,
		}
	}

	if s.Primary.LinkedChannelType != "" || len(s.Primary.FollowingChannelArns) > 0 {
		out.PrimaryChannelSettings = &primaryChannelSettingsOutput{
			LinkedChannelType:    s.Primary.LinkedChannelType,
			FollowingChannelArns: s.Primary.FollowingChannelArns,
		}
	}

	return out
}

func extractLinkedChannelSettings(body map[string]any) (ChannelLinkedChannelSettings, bool) {
	raw, ok := body["linkedChannelSettings"].(map[string]any)
	if !ok {
		return ChannelLinkedChannelSettings{}, false
	}

	var out ChannelLinkedChannelSettings

	if f, hasFollower := raw["followerChannelSettings"].(map[string]any); hasFollower {
		linkedType, _ := f["linkedChannelType"].(string)
		primaryArn, _ := f["primaryChannelArn"].(string)
		out.Follower = ChannelFollowerSettings{LinkedChannelType: linkedType, PrimaryChannelArn: primaryArn}
	}

	if p, hasPrimary := raw["primaryChannelSettings"].(map[string]any); hasPrimary {
		linkedType, _ := p["linkedChannelType"].(string)
		out.Primary = ChannelPrimarySettings{LinkedChannelType: linkedType}
	}

	return out, true
}

// --- Destinations ---

type outputDestinationSettingOutput struct {
	PasswordParam string `json:"passwordParam,omitempty"`
	StreamName    string `json:"streamName,omitempty"`
	URL           string `json:"url,omitempty"`
	Username      string `json:"username,omitempty"`
}

type mediaPackageDestinationSettingsOutput struct {
	ChannelEndpointID      string `json:"channelEndpointId,omitempty"`
	ChannelGroup           string `json:"channelGroup,omitempty"`
	ChannelID              string `json:"channelId,omitempty"`
	ChannelName            string `json:"channelName,omitempty"`
	MediaPackageRegionName string `json:"mediaPackageRegionName,omitempty"`
}

type multiplexDestinationSettingsOutput struct {
	MultiplexID string `json:"multiplexId,omitempty"`
	ProgramName string `json:"programName,omitempty"`
}

type mediaConnectRouterDestinationSettingsOutput struct {
	EncryptionType string `json:"encryptionType,omitempty"`
	SecretArn      string `json:"secretArn,omitempty"`
}

type srtDestinationSettingsOutput struct {
	ConnectionMode                string `json:"connectionMode,omitempty"`
	EncryptionPassphraseSecretArn string `json:"encryptionPassphraseSecretArn,omitempty"`
	StreamID                      string `json:"streamId,omitempty"`
	URL                           string `json:"url,omitempty"`
	ListenerPort                  int32  `json:"listenerPort,omitempty"`
}

type outputDestinationOutput struct {
	ID                         string                                        `json:"id,omitempty"`
	LogicalInterfaceNames      []string                                      `json:"logicalInterfaceNames,omitempty"`
	MediaConnectRouterSettings []mediaConnectRouterDestinationSettingsOutput `json:"mediaConnectRouterSettings,omitempty"`
	MediaPackageSettings       []mediaPackageDestinationSettingsOutput       `json:"mediaPackageSettings,omitempty"`
	MultiplexSettings          *multiplexDestinationSettingsOutput           `json:"multiplexSettings,omitempty"`
	Settings                   []outputDestinationSettingOutput              `json:"settings,omitempty"`
	SrtSettings                []srtDestinationSettingsOutput                `json:"srtSettings,omitempty"`
}

func toDestinationsOutput(dests []ChannelOutputDestination) []outputDestinationOutput {
	if len(dests) == 0 {
		return nil
	}

	out := make([]outputDestinationOutput, 0, len(dests))

	for _, d := range dests {
		item := outputDestinationOutput{
			ID:                    d.ID,
			LogicalInterfaceNames: d.LogicalInterfaceNames,
		}

		for _, s := range d.Settings {
			item.Settings = append(item.Settings, outputDestinationSettingOutput(s))
		}

		for _, s := range d.MediaPackageSettings {
			item.MediaPackageSettings = append(item.MediaPackageSettings, mediaPackageDestinationSettingsOutput(s))
		}

		for _, s := range d.MediaConnectRouterSettings {
			item.MediaConnectRouterSettings = append(
				item.MediaConnectRouterSettings, mediaConnectRouterDestinationSettingsOutput(s),
			)
		}

		for _, s := range d.SrtSettings {
			item.SrtSettings = append(item.SrtSettings, srtDestinationSettingsOutput(s))
		}

		if d.MultiplexSettings != nil {
			v := multiplexDestinationSettingsOutput(*d.MultiplexSettings)
			item.MultiplexSettings = &v
		}

		out = append(out, item)
	}

	return out
}

func extractOutputDestinationSettings(raw []any) []OutputDestinationSetting {
	out := make([]OutputDestinationSetting, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		password, _ := m["passwordParam"].(string)
		streamName, _ := m["streamName"].(string)
		url, _ := m["url"].(string)
		username, _ := m["username"].(string)
		out = append(out, OutputDestinationSetting{
			PasswordParam: password, StreamName: streamName, URL: url, Username: username,
		})
	}

	return out
}

func extractMediaPackageSettings(raw []any) []MediaPackageDestinationSettings {
	out := make([]MediaPackageDestinationSettings, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		endpointID, _ := m["channelEndpointId"].(string)
		group, _ := m["channelGroup"].(string)
		channelID, _ := m["channelId"].(string)
		channelName, _ := m["channelName"].(string)
		region, _ := m["mediaPackageRegionName"].(string)
		out = append(out, MediaPackageDestinationSettings{
			ChannelEndpointID: endpointID, ChannelGroup: group, ChannelID: channelID,
			ChannelName: channelName, MediaPackageRegionName: region,
		})
	}

	return out
}

func extractMediaConnectRouterSettings(raw []any) []MediaConnectRouterDestinationSettings {
	out := make([]MediaConnectRouterDestinationSettings, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		encType, _ := m["encryptionType"].(string)
		secretArn, _ := m["secretArn"].(string)
		out = append(out, MediaConnectRouterDestinationSettings{EncryptionType: encType, SecretArn: secretArn})
	}

	return out
}

func extractSrtSettings(raw []any) []SrtDestinationSettings {
	out := make([]SrtDestinationSettings, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		mode, _ := m["connectionMode"].(string)
		secretArn, _ := m["encryptionPassphraseSecretArn"].(string)
		streamID, _ := m["streamId"].(string)
		url, _ := m["url"].(string)
		out = append(out, SrtDestinationSettings{
			ConnectionMode: mode, EncryptionPassphraseSecretArn: secretArn,
			ListenerPort: int32FromAny(m["listenerPort"]), StreamID: streamID, URL: url,
		})
	}

	return out
}

func extractOneDestination(m map[string]any) ChannelOutputDestination {
	dest := ChannelOutputDestination{
		ID:                    stringFromAny(m["id"]),
		LogicalInterfaceNames: anySliceToStrings(mustSlice(m["logicalInterfaceNames"])),
	}

	if raw, ok := m["settings"].([]any); ok {
		dest.Settings = extractOutputDestinationSettings(raw)
	}

	if raw, ok := m["mediaPackageSettings"].([]any); ok {
		dest.MediaPackageSettings = extractMediaPackageSettings(raw)
	}

	if raw, ok := m["mediaConnectRouterSettings"].([]any); ok {
		dest.MediaConnectRouterSettings = extractMediaConnectRouterSettings(raw)
	}

	if raw, ok := m["srtSettings"].([]any); ok {
		dest.SrtSettings = extractSrtSettings(raw)
	}

	if mux, ok := m["multiplexSettings"].(map[string]any); ok {
		dest.MultiplexSettings = &MultiplexDestinationSettings{
			MultiplexID: stringFromAny(mux["multiplexId"]),
			ProgramName: stringFromAny(mux["programName"]),
		}
	}

	return dest
}

func extractDestinations(body map[string]any) ([]ChannelOutputDestination, bool) {
	raw, hasDestinations := body["destinations"].([]any)
	if !hasDestinations {
		return nil, false
	}

	out := make([]ChannelOutputDestination, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, extractOneDestination(m))
	}

	return out, true
}

// --- InputAttachments ---

func toFailoverConditionsOutput(conds []ChannelFailoverCondition) []map[string]any {
	if len(conds) == 0 {
		return nil
	}

	out := make([]map[string]any, 0, len(conds))

	for _, c := range conds {
		settings := map[string]any{}
		if a := c.Settings.AudioSilenceSettings; a != nil {
			settings["audioSilenceSettings"] = map[string]any{
				"audioSelectorName":         a.AudioSelectorName,
				"audioSilenceThresholdMsec": a.AudioSilenceThresholdMsec,
			}
		}

		if l := c.Settings.InputLossSettings; l != nil {
			settings["inputLossSettings"] = map[string]any{"inputLossThresholdMsec": l.InputLossThresholdMsec}
		}

		if v := c.Settings.VideoBlackSettings; v != nil {
			settings["videoBlackSettings"] = map[string]any{
				"blackDetectThreshold":    v.BlackDetectThreshold,
				"videoBlackThresholdMsec": v.VideoBlackThresholdMsec,
			}
		}

		out = append(out, map[string]any{"failoverConditionSettings": settings})
	}

	return out
}

func toAutomaticInputFailoverSettingsOutput(s ChannelAutomaticInputFailoverSettings) map[string]any {
	if !s.hasFailover() {
		return nil
	}

	out := map[string]any{"secondaryInputId": s.SecondaryInputID}
	if s.ErrorClearTimeMsec != 0 {
		out["errorClearTimeMsec"] = s.ErrorClearTimeMsec
	}

	if s.InputPreference != "" {
		out["inputPreference"] = s.InputPreference
	}

	if conds := toFailoverConditionsOutput(s.FailoverConditions); conds != nil {
		out["failoverConditions"] = conds
	}

	return out
}

func toInputAttachmentsOutput(attachments []ChannelInputAttachment) []map[string]any {
	if len(attachments) == 0 {
		return nil
	}

	out := make([]map[string]any, 0, len(attachments))

	for _, a := range attachments {
		item := map[string]any{}
		if a.InputAttachmentName != "" {
			item["inputAttachmentName"] = a.InputAttachmentName
		}

		if a.InputID != "" {
			item["inputId"] = a.InputID
		}

		if len(a.LogicalInterfaceNames) > 0 {
			item["logicalInterfaceNames"] = a.LogicalInterfaceNames
		}

		if failover := toAutomaticInputFailoverSettingsOutput(a.AutomaticInputFailoverSettings); failover != nil {
			item["automaticInputFailoverSettings"] = failover
		}

		if settings := toInputSettingsOutput(a.InputSettings); settings != nil {
			item["inputSettings"] = settings
		}

		out = append(out, item)
	}

	return out
}

func extractFailoverConditions(raw []any) []ChannelFailoverCondition {
	out := make([]ChannelFailoverCondition, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		settings, hasSettings := m["failoverConditionSettings"].(map[string]any)
		if !hasSettings {
			out = append(out, ChannelFailoverCondition{})

			continue
		}

		var cs ChannelFailoverConditionSettings
		if a, hasAudio := settings["audioSilenceSettings"].(map[string]any); hasAudio {
			cs.AudioSilenceSettings = &AudioSilenceFailoverSettings{
				AudioSelectorName:         stringFromAny(a["audioSelectorName"]),
				AudioSilenceThresholdMsec: int32FromAny(a["audioSilenceThresholdMsec"]),
			}
		}

		if l, hasLoss := settings["inputLossSettings"].(map[string]any); hasLoss {
			cs.InputLossSettings = &InputLossFailoverSettings{
				InputLossThresholdMsec: int32FromAny(l["inputLossThresholdMsec"]),
			}
		}

		if v, hasBlack := settings["videoBlackSettings"].(map[string]any); hasBlack {
			threshold, _ := v["blackDetectThreshold"].(float64)
			cs.VideoBlackSettings = &VideoBlackFailoverSettings{
				BlackDetectThreshold:    threshold,
				VideoBlackThresholdMsec: int32FromAny(v["videoBlackThresholdMsec"]),
			}
		}

		out = append(out, ChannelFailoverCondition{Settings: cs})
	}

	return out
}

func extractAutomaticInputFailoverSettings(m map[string]any) ChannelAutomaticInputFailoverSettings {
	raw, ok := m["automaticInputFailoverSettings"].(map[string]any)
	if !ok {
		return ChannelAutomaticInputFailoverSettings{}
	}

	settings := ChannelAutomaticInputFailoverSettings{
		SecondaryInputID:   stringFromAny(raw["secondaryInputId"]),
		InputPreference:    stringFromAny(raw["inputPreference"]),
		ErrorClearTimeMsec: int32FromAny(raw["errorClearTimeMsec"]),
	}

	if conds, hasConditions := raw["failoverConditions"].([]any); hasConditions {
		settings.FailoverConditions = extractFailoverConditions(conds)
	}

	return settings
}

func extractInputAttachments(body map[string]any) ([]ChannelInputAttachment, bool) {
	raw, hasAttachments := body["inputAttachments"].([]any)
	if !hasAttachments {
		return nil, false
	}

	out := make([]ChannelInputAttachment, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, ChannelInputAttachment{
			InputAttachmentName:            stringFromAny(m["inputAttachmentName"]),
			InputID:                        stringFromAny(m["inputId"]),
			LogicalInterfaceNames:          anySliceToStrings(mustSlice(m["logicalInterfaceNames"])),
			AutomaticInputFailoverSettings: extractAutomaticInputFailoverSettings(m),
			InputSettings:                  extractInputSettings(m),
		})
	}

	return out, true
}

// --- small shared helpers ---

func stringFromAny(v any) string {
	s, _ := v.(string)

	return s
}

func mustSlice(v any) []any {
	s, _ := v.([]any)

	return s
}

func anySliceToStrings(raw []any) []string {
	if raw == nil {
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// --- channel response wire shape ---

// pipelinesRunningCount derives the number of currently healthy pipelines
// from a channel's State and ChannelClass, matching AWS: only RUNNING
// channels report running pipelines (2 for STANDARD, 1 for
// SINGLE_PIPELINE); every other state (IDLE, STARTING, STOPPING, etc.)
// reports 0.
func pipelinesRunningCount(state, channelClass string) int32 {
	if state != stateRunning {
		return 0
	}

	if channelClass == channelClassSinglePipeline {
		return pipelinesRunningCountSinglePipeline
	}

	return pipelinesRunningCountStandard
}

// channelOutput mirrors DescribeChannelOutput/CreateChannelOutput/
// UpdateChannelOutput's nested "channel" object (types.Channel). Every
// gopherstack-jb9i addition below is gated by its own toXOutput helper's
// hasX()-style check, so an unconfigured member is omitted entirely --
// matching a real Channel that was never given that setting, not emitted as
// an empty object/array.
type channelOutput struct {
	LinkedChannelSettings *linkedChannelSettingsOutput `json:"linkedChannelSettings,omitempty"`
	Maintenance           *maintenanceOutput           `json:"maintenance,omitempty"`
	Tags                  map[string]string            `json:"tags"`
	Vpc                   *channelVpcOutput            `json:"vpc,omitempty"`
	AnywhereSettings      *anywhereSettingsOutput      `json:"anywhereSettings,omitempty"`
	CdiInputSpecification *cdiInputSpecificationOutput `json:"cdiInputSpecification,omitempty"`
	ChannelEngineVersion  *channelEngineVersionOutput  `json:"channelEngineVersion,omitempty"`
	EncoderSettings       *encoderSettingsOutput       `json:"encoderSettings,omitempty"`
	InferenceSettings     *inferenceSettingsOutput     `json:"inferenceSettings,omitempty"`
	InputSpecification    *inputSpecificationOutput    `json:"inputSpecification,omitempty"`
	Arn                   string                       `json:"arn"`
	ID                    string                       `json:"id"`
	Name                  string                       `json:"name"`
	ChannelClass          string                       `json:"channelClass"`
	RoleArn               string                       `json:"roleArn"`
	State                 string                       `json:"state"`
	LogLevel              string                       `json:"logLevel,omitempty"`
	ChannelSecurityGroups []string                     `json:"channelSecurityGroups,omitempty"`
	InputAttachments      []map[string]any             `json:"inputAttachments,omitempty"`
	Destinations          []outputDestinationOutput    `json:"destinations,omitempty"`
	PipelinesRunningCount int32                        `json:"pipelinesRunningCount"`
}

func toChannelOutput(ch *Channel) channelOutput {
	tags := ch.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return channelOutput{
		Tags:                  tags,
		Arn:                   ch.ARN,
		ID:                    ch.ID,
		Name:                  ch.Name,
		ChannelClass:          ch.ChannelClass,
		RoleArn:               ch.RoleARN,
		State:                 ch.State,
		LogLevel:              ch.LogLevel,
		PipelinesRunningCount: pipelinesRunningCount(ch.State, ch.ChannelClass),
		AnywhereSettings:      toAnywhereSettingsOutput(ch.AnywhereSettings),
		CdiInputSpecification: toCdiInputSpecificationOutput(ch.CdiInputSpecification),
		ChannelEngineVersion:  toChannelEngineVersionOutput(ch.ChannelEngineVersion),
		ChannelSecurityGroups: ch.ChannelSecurityGroups,
		Destinations:          toDestinationsOutput(ch.Destinations),
		EncoderSettings:       toEncoderSettingsOutput(ch.EncoderSettings),
		InferenceSettings:     toInferenceSettingsOutput(ch.InferenceSettings),
		InputAttachments:      toInputAttachmentsOutput(ch.InputAttachments),
		InputSpecification:    toInputSpecificationOutput(ch.InputSpecification),
		LinkedChannelSettings: toLinkedChannelSettingsOutput(ch.LinkedChannelSettings),
		Maintenance:           toMaintenanceOutput(ch.Maintenance),
		Vpc:                   toChannelVpcOutput(ch.Vpc),
	}
}

// channelSummaryToWire renders a ChannelSummary the same way toChannelOutput
// renders a Channel, minus encoderSettings (see ChannelSummary's doc
// comment) and using a plain map (ListChannels' existing wire shape, unlike
// Describe/Create/Update/Delete/Start/Stop's typed channelOutput) so the
// summary's extra "pipelinesRunningCount" placement matches the pre-existing
// handleListChannels behavior exactly.
func channelSummaryToWire(s *ChannelSummary) map[string]any {
	item := map[string]any{
		keyArn:                  s.ARN,
		keyID:                   s.ID,
		keyName:                 s.Name,
		"channelClass":          s.ChannelClass,
		keyState:                s.State,
		"pipelinesRunningCount": pipelinesRunningCount(s.State, s.ChannelClass),
	}

	if s.LogLevel != "" {
		item["logLevel"] = s.LogLevel
	}

	if as := toAnywhereSettingsOutput(s.AnywhereSettings); as != nil {
		item["anywhereSettings"] = as
	}

	if v := toCdiInputSpecificationOutput(s.CdiInputSpecification); v != nil {
		item["cdiInputSpecification"] = v
	}

	if v := toChannelEngineVersionOutput(s.ChannelEngineVersion); v != nil {
		item["channelEngineVersion"] = v
	}

	if len(s.ChannelSecurityGroups) > 0 {
		item["channelSecurityGroups"] = s.ChannelSecurityGroups
	}

	if v := toDestinationsOutput(s.Destinations); v != nil {
		item["destinations"] = v
	}

	if v := toInferenceSettingsOutput(s.InferenceSettings); v != nil {
		item["inferenceSettings"] = v
	}

	if v := toInputAttachmentsOutput(s.InputAttachments); v != nil {
		item["inputAttachments"] = v
	}

	if v := toInputSpecificationOutput(s.InputSpecification); v != nil {
		item["inputSpecification"] = v
	}

	if v := toLinkedChannelSettingsOutput(s.LinkedChannelSettings); v != nil {
		item["linkedChannelSettings"] = v
	}

	if v := toMaintenanceOutput(s.Maintenance); v != nil {
		item["maintenance"] = v
	}

	if v := toChannelVpcOutput(s.Vpc); v != nil {
		item["vpc"] = v
	}

	return item
}

func (h *Handler) handleCreateChannel(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	channelClass, _ := body["channelClass"].(string)
	roleArn, _ := body["roleArn"].(string)
	anywhereSettings, _ := extractAnywhereSettings(body)
	tags := extractTags(body)
	extras := extractChannelCreateExtras(body)

	ch, err := h.Backend.CreateChannel(name, channelClass, roleArn, anywhereSettings, extras, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleDescribeChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DescribeChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	roleArn, _ := body["roleArn"].(string)
	anywhereSettings, hasAnywhereSettings := extractAnywhereSettings(body)
	extras := extractChannelUpdateExtras(body)

	ch, err := h.Backend.UpdateChannel(channelID, name, roleArn, anywhereSettings, hasAnywhereSettings, extras)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleDeleteChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DeleteChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListChannels(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, channelSummaryToWire(s))
	}

	resp := map[string]any{"channels": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StartChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleStopChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StopChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

// --- Alert and version handlers ---

func (h *Handler) handleListAlerts(c *echo.Context, channelID string) error {
	alerts, err := h.Backend.ListAlerts(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAlerts: alerts})
}

func (h *Handler) handleListVersions(c *echo.Context) error {
	versions := h.Backend.ListVersions()

	out := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		item := map[string]any{"version": v.Version}
		// expirationDate is __timestampIso8601 on the real wire
		// (smithytime.ParseDateTime) -- omit rather than emit "", which a
		// real SDK client would fail to parse.
		if v.ExpirationDate != "" {
			item["expirationDate"] = v.ExpirationDate
		}
		out = append(out, item)
	}

	return c.JSON(http.StatusOK, map[string]any{"versions": out})
}

// --- Channel lifecycle extra handlers ---

func (h *Handler) handleUpdateChannelClass(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	channelClass, _ := body["channelClass"].(string)

	ch, err := h.Backend.UpdateChannelClass(channelID, channelClass)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleRestartChannelPipelines(c *echo.Context, channelID string) error {
	pipelineIDs := []string{}

	ch, err := h.Backend.RestartChannelPipelines(channelID, pipelineIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDescribeThumbnails(c *echo.Context, channelID string) error {
	if _, err := h.Backend.DescribeThumbnails(channelID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"thumbnailDetails": []map[string]any{}})
}
