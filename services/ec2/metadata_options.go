package ec2

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
)

const (
	imdsTokensOptional = "optional"
	imdsTokensRequired = "required"
	imdsEnabled        = "enabled"
	imdsDisabled       = "disabled"
	imdsNoPreference   = "no-preference"
	imdsMinHopLimit    = 1
	imdsMaxHopLimit    = 64
)

// MetadataOptionsInput carries the RunInstances MetadataOptions request fields; empty means unset.
type MetadataOptionsInput struct {
	HTTPTokens           string
	HTTPEndpoint         string
	InstanceMetadataTags string
	HTTPPutHopLimit      int
}

func metadataOptionsFromValues(vals url.Values) (MetadataOptionsInput, error) {
	in := MetadataOptionsInput{
		HTTPTokens:           vals.Get("MetadataOptions.HttpTokens"),
		HTTPEndpoint:         vals.Get("MetadataOptions.HttpEndpoint"),
		InstanceMetadataTags: vals.Get("MetadataOptions.InstanceMetadataTags"),
	}

	if raw := vals.Get("MetadataOptions.HttpPutResponseHopLimit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return in, fmt.Errorf("%w: HttpPutResponseHopLimit %q is not an integer", ErrInvalidParameter, raw)
		}

		in.HTTPPutHopLimit = n
	}

	return in, validateMetadataOptions(in)
}

func validateMetadataOptions(in MetadataOptionsInput) error {
	checks := []struct {
		name, value string
		allowed     []string
	}{
		{"HttpTokens", in.HTTPTokens, []string{imdsTokensOptional, imdsTokensRequired}},
		{"HttpEndpoint", in.HTTPEndpoint, []string{imdsEnabled, imdsDisabled}},
		{"InstanceMetadataTags", in.InstanceMetadataTags, []string{imdsEnabled, imdsDisabled}},
	}

	for _, c := range checks {
		if c.value != "" && !slices.Contains(c.allowed, c.value) {
			return fmt.Errorf("%w: invalid value %q for %s", ErrInvalidParameter, c.value, c.name)
		}
	}

	if in.HTTPPutHopLimit != 0 && (in.HTTPPutHopLimit < imdsMinHopLimit || in.HTTPPutHopLimit > imdsMaxHopLimit) {
		return fmt.Errorf("%w: HttpPutResponseHopLimit must be between %d and %d",
			ErrInvalidParameter, imdsMinHopLimit, imdsMaxHopLimit)
	}

	return nil
}

// metadataOptionsOf returns inst's effective IMDS options, defaulting unset fields.
func metadataOptionsOf(inst *Instance) IMDSOptions {
	opts := IMDSOptions{
		HTTPTokens:              inst.MetadataOptionsTokens,
		HTTPEndpoint:            inst.MetadataOptionsState,
		InstanceMetadataTags:    inst.MetadataTags,
		HTTPPutResponseHopLimit: inst.MetadataHopLimit,
		State:                   "applied",
	}

	if opts.HTTPTokens == "" {
		opts.HTTPTokens = imdsTokensOptional
	}

	if opts.HTTPEndpoint == "" {
		opts.HTTPEndpoint = imdsEnabled
	}

	if opts.InstanceMetadataTags == "" {
		opts.InstanceMetadataTags = imdsDisabled
	}

	if opts.HTTPPutResponseHopLimit == 0 {
		opts.HTTPPutResponseHopLimit = 1
	}

	return opts
}

// ApplyLaunchMetadataOptions sets the IMDS options of freshly launched instances, filling unset
// fields from the account-level defaults.
func (b *InMemoryBackend) ApplyLaunchMetadataOptions(instanceIDs []string, in MetadataOptionsInput) error {
	if err := validateMetadataOptions(in); err != nil {
		return err
	}

	b.mu.Lock("ApplyLaunchMetadataOptions")
	defer b.mu.Unlock()

	def := InstanceMetadataDefaults{}
	if b.instanceMetadataDefaults != nil {
		def = *b.instanceMetadataDefaults
	}

	for _, id := range instanceIDs {
		inst, ok := b.instances.Get(id)
		if !ok {
			return fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		inst.MetadataOptionsTokens = firstSet(in.HTTPTokens, def.HTTPTokens)
		inst.MetadataOptionsState = firstSet(in.HTTPEndpoint, def.HTTPEndpoint)
		inst.MetadataTags = firstSet(in.InstanceMetadataTags, def.InstanceMetadataTags)
		inst.MetadataHopLimit = in.HTTPPutHopLimit

		if inst.MetadataHopLimit == 0 {
			inst.MetadataHopLimit = def.HTTPPutResponseHopLimit
		}
	}

	return nil
}

// firstSet returns the first value that is neither empty nor "no-preference".
func firstSet(values ...string) string {
	for _, v := range values {
		if v != "" && v != imdsNoPreference {
			return v
		}
	}

	return ""
}
