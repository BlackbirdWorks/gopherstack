package appconfig

import "encoding/json"

// featureFlagDocument mirrors the parts of the real AWS.AppConfig.FeatureFlags
// JSON schema this backend needs in order to validate CreateExperimentDefinition's
// FlagKey against actual flag content instead of only checking it is
// non-empty. Confirmed against AWS's published type reference
// (https://docs.aws.amazon.com/appconfig/latest/userguide/appconfig-type-reference-feature-flags.html,
// the "AWS.AppConfig.FeatureFlags" JSON schema): feature flag configuration
// profile content (HostedConfigurationVersion.Content, for a
// ConfigurationProfile whose Type is AWS.AppConfig.FeatureFlags) is a JSON
// document shaped {"version": "1", "flags": {<flagKey>: {...}}, "values":
// {<flagKey>: {...}}}. Only the "flags" object's key set is needed here --
// every other field (name/description/_deprecation/attributes under flags,
// enabled/attribute-values/_variants under values) is per-flag detail this
// backend has no use for. This is not part of the Go SDK (HostedConfigurationVersion.Content
// is opaque []byte there -- AppConfig hosts arbitrary configuration, not just
// feature flags), so it is documented and field-diffed against AWS's public
// user-guide schema page directly rather than SDK-generated code.
type featureFlagDocument struct {
	Flags map[string]json.RawMessage `json:"flags"`
}

// parseFeatureFlagKeys attempts to parse content as a real
// AWS.AppConfig.FeatureFlags document and returns the set of flag keys it
// defines. ok is false when content is empty, is not valid JSON, or has no
// "flags" object at all -- callers must treat that as "content is not
// structured feature-flag JSON, so flag-key validation cannot be performed"
// rather than "the flag doesn't exist": this backend's CreateConfigurationProfile
// treats Type/content as optional (a large share of this package's existing
// test fixtures never set either), so this stays permissive on unparseable/
// absent content, matching the same "unspecified, not wrong" precedent
// already used for an unset ConfigurationProfile.Type in
// experiment_definitions.go.
func parseFeatureFlagKeys(content []byte) (map[string]struct{}, bool) {
	if len(content) == 0 {
		return nil, false
	}

	var doc featureFlagDocument
	if err := json.Unmarshal(content, &doc); err != nil || doc.Flags == nil {
		return nil, false
	}

	keys := make(map[string]struct{}, len(doc.Flags))
	for k := range doc.Flags {
		keys[k] = struct{}{}
	}

	return keys, true
}

// latestFeatureFlagKeysLocked returns the set of flag keys defined in
// profileID's latest hosted configuration version content, parsed as a real
// AWS.AppConfig.FeatureFlags document. ok is false whenever flag-key
// validation cannot be performed (no hosted version has ever been uploaded
// for this profile, or its content is not structured feature-flag JSON) --
// see parseFeatureFlagKeys. "Latest" (not "deployed") is deliberate: an
// experiment definition can reference a flag before it has ever been
// deployed to the target environment, the same way StartDeployment can
// reference a hosted configuration version that has not yet completed a
// deployment. Must be called under lock (read or write).
func (b *InMemoryBackend) latestFeatureFlagKeysLocked(applicationID, profileID string) (map[string]struct{}, bool) {
	latest := b.versionCounters[applicationID][profileID]
	if latest == 0 {
		return nil, false
	}

	v, ok := b.hostedConfigVersions.Get(hcvKey(applicationID, profileID, latest))
	if !ok {
		return nil, false
	}

	return parseFeatureFlagKeys(v.Content)
}
