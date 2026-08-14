package ssm

import (
	"context"
	"fmt"
)

// LabelParameterVersion adds labels to a specific parameter version.
// When ParameterVersion is 0, labels are applied to the latest version.
func (b *InMemoryBackend) LabelParameterVersion(
	ctx context.Context,
	input *LabelParameterVersionInput,
) (*LabelParameterVersionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("LabelParameterVersion")
	defer b.mu.Unlock()

	if input.Name == "" {
		return &LabelParameterVersionOutputFull{
			InvalidLabels: []string{},
		}, nil
	}

	paramPtr, exists := b.parametersStore(region).Get(input.Name)
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrParameterNotFound, input.Name)
	}

	param := *paramPtr

	version := input.ParameterVersion
	if version == 0 {
		version = param.Version
	}

	if b.parameterLabels[region] == nil {
		b.parameterLabels[region] = make(map[string]map[int64][]string)
	}
	parameterLabels := b.parameterLabelsStore(region)
	if parameterLabels[input.Name] == nil {
		parameterLabels[input.Name] = make(map[int64][]string)
	}

	// In AWS a label points at exactly one version. Re-applying a label that
	// currently lives on a different version moves it to the target version
	// rather than duplicating it.
	for v, labels := range parameterLabels[input.Name] {
		if v == version {
			continue
		}
		parameterLabels[input.Name][v] = removeLabels(labels, input.Labels)
	}

	updatedLabels, invalidLabels := appendLabelsWithLimit(
		parameterLabels[input.Name][version], input.Labels,
	)
	parameterLabels[input.Name][version] = updatedLabels

	return &LabelParameterVersionOutputFull{
		InvalidLabels:    invalidLabels,
		ParameterVersion: version,
	}, nil
}

// removeLabels returns existing with any entry present in toRemove filtered out.
func removeLabels(existing, toRemove []string) []string {
	if len(existing) == 0 {
		return existing
	}
	remove := make(map[string]bool, len(toRemove))
	for _, l := range toRemove {
		remove[l] = true
	}
	kept := make([]string, 0, len(existing))
	for _, l := range existing {
		if !remove[l] {
			kept = append(kept, l)
		}
	}

	return kept
}

// UnlabelParameterVersion removes labels from a specific parameter version.
// When ParameterVersion is 0, labels are removed from the latest version.
func (b *InMemoryBackend) UnlabelParameterVersion(
	ctx context.Context,
	input *UnlabelParameterVersionInput,
) (*UnlabelParameterVersionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("UnlabelParameterVersion")
	defer b.mu.Unlock()

	if input.Name == "" {
		return &UnlabelParameterVersionOutputFull{
			InvalidLabels: []string{},
			RemovedLabels: input.Labels,
		}, nil
	}

	version := input.ParameterVersion
	if version == 0 {
		if param, exists := b.parametersStore(region).Get(input.Name); exists {
			version = param.Version
		}
	}

	removedSet := make(map[string]bool, len(input.Labels))
	for _, l := range input.Labels {
		removedSet[l] = true
	}

	parameterLabels := b.parameterLabelsStore(region)
	existing := parameterLabels[input.Name][version]
	kept := make([]string, 0, len(existing))

	for _, l := range existing {
		if !removedSet[l] {
			kept = append(kept, l)
		}
	}

	if parameterLabels[input.Name] != nil {
		parameterLabels[input.Name][version] = kept
	}

	return &UnlabelParameterVersionOutputFull{
		InvalidLabels: []string{},
		RemovedLabels: input.Labels,
	}, nil
}

// maxLabelsPerVersion is the AWS-enforced limit on labels per parameter version.
const maxLabelsPerVersion = 10

// appendLabelsWithLimit appends newLabels to existing, skipping duplicates and
// labels that would push the version over the maxLabelsPerVersion limit.
// Returns (updated slice, invalid labels that exceeded the limit).
func appendLabelsWithLimit(existing, newLabels []string) ([]string, []string) {
	var invalid []string

	seen := make(map[string]bool, len(existing))

	for _, l := range existing {
		seen[l] = true
	}

	for _, l := range newLabels {
		if seen[l] {
			// Already present — not an error, not a re-add, not invalid.
			continue
		}

		if len(existing) >= maxLabelsPerVersion {
			invalid = append(invalid, l)

			continue
		}

		existing = append(existing, l)
		seen[l] = true
	}

	if invalid == nil {
		invalid = []string{}
	}

	return existing, invalid
}
