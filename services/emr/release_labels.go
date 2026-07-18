package emr

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// emr6xCoreApps is the base 6.x app set (no MXNet/TF).
var emr6xCoreApps = []string{ //nolint:gochecknoglobals // read-only lookup table
	appFlink, appHadoop, appHBase, appHive, appHue,
	appLivy, appOozie, appPig, appPresto, appSpark, appTez,
}

// emr6xApps is the emr-6.10+ app set (adds MXNet and TensorFlow).
var emr6xApps = []string{ //nolint:gochecknoglobals // read-only lookup table
	appFlink, appHadoop, appHBase, appHive, appHue,
	appLivy, appMXNet, appOozie, appPig, appPresto, appSpark, appTez, appTF,
}

// emr7xApps is the base 7.x app set (adds Trino, drops MXNet/TF).
var emr7xApps = []string{ //nolint:gochecknoglobals // read-only lookup table
	appFlink, appHadoop, appHBase, appHive, appHue,
	appLivy, appOozie, appPig, appPresto, appSpark, appTez, appTrino,
}

// releaseLabelApps maps a release label to its bundled application names.
var releaseLabelApps = map[string][]string{ //nolint:gochecknoglobals // read-only lookup table
	"emr-5.36.2": {
		appHadoop,
		appHive,
		appHue,
		appLivy,
		appMXNet,
		appOozie,
		appPig,
		appPresto,
		appSpark,
		appTez,
	},
	"emr-6.0.0":  emr6xCoreApps,
	"emr-6.1.0":  emr6xCoreApps,
	"emr-6.4.0":  emr6xCoreApps,
	"emr-6.8.0":  emr6xCoreApps,
	"emr-6.10.0": emr6xApps,
	"emr-6.11.0": emr6xApps,
	"emr-6.12.0": emr6xApps,
	"emr-6.13.0": emr6xApps,
	"emr-6.14.0": emr6xApps,
	"emr-6.15.0": emr6xApps,
	"emr-7.0.0":  emr7xApps,
	"emr-7.1.0":  emr7xApps,
	"emr-7.2.0":  emr7xApps,
	"emr-7.3.0":  emr7xApps,
}

// supportedInstanceTypes is a static catalog of EMR-supported EC2 instance types.
var supportedInstanceTypes = []SupportedInstanceType{ //nolint:gochecknoglobals // read-only hardware spec table
	{Type: "m5.xlarge", MemoryGB: gb16, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m5.2xlarge", MemoryGB: gb32, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m5.4xlarge", MemoryGB: gb64, VCPU: vcpu16, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m5.8xlarge", MemoryGB: gb128, VCPU: vcpu32, Architecture: archX86, Is64BitsOnly: true},
	{Type: "m6g.xlarge", MemoryGB: gb16, VCPU: vcpu4, Architecture: archARM64, Is64BitsOnly: true},
	{Type: "m6g.2xlarge", MemoryGB: gb32, VCPU: vcpu8, Architecture: archARM64, Is64BitsOnly: true},
	{Type: "r5.xlarge", MemoryGB: gb32, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "r5.2xlarge", MemoryGB: gb64, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "r5.4xlarge", MemoryGB: gb128, VCPU: vcpu16, Architecture: archX86, Is64BitsOnly: true},
	{Type: "c5.xlarge", MemoryGB: gb8, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "c5.2xlarge", MemoryGB: gb16, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "c5.4xlarge", MemoryGB: gb32, VCPU: vcpu16, Architecture: archX86, Is64BitsOnly: true},
	{Type: "p3.2xlarge", MemoryGB: gb61, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true},
	{Type: "g4dn.xlarge", MemoryGB: gb16, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true},
	{Type: "i3.xlarge", MemoryGB: gb30, VCPU: vcpu4, Architecture: archX86, Is64BitsOnly: true, NumberOfDisks: ndisk1},
	{Type: "i3.2xlarge", MemoryGB: gb61, VCPU: vcpu8, Architecture: archX86, Is64BitsOnly: true, NumberOfDisks: ndisk2},
}

// ListReleaseLabels returns release labels optionally filtered by prefix and application.
func (b *InMemoryBackend) ListReleaseLabels(
	_ context.Context, prefix, application, marker string,
) ([]string, string) {
	var labels []string

	for label := range releaseLabelApps {
		if prefix != "" && !stringHasPrefix(label, prefix) {
			continue
		}

		if application != "" && !labelHasApp(label, application) {
			continue
		}

		labels = append(labels, label)
	}

	sort.Strings(labels)

	p := page.New(labels, marker, listReleaseLabelsPage, listReleaseLabelsPage)

	return p.Data, p.Next
}

func stringHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func labelHasApp(label, application string) bool {
	apps, ok := releaseLabelApps[label]
	if !ok {
		return false
	}

	return slices.Contains(apps, application)
}

// DescribeReleaseLabel returns details about a specific release label.
func (b *InMemoryBackend) DescribeReleaseLabel(
	_ context.Context, releaseLabel string,
) (*ReleaseLabel, error) {
	apps, ok := releaseLabelApps[releaseLabel]
	if !ok {
		return nil, fmt.Errorf("%w: release label %s not found", ErrNotFound, releaseLabel)
	}

	rla := make([]ReleaseLabelApplication, 0, len(apps))
	for _, name := range apps {
		rla = append(rla, ReleaseLabelApplication{Name: name, Version: "latest"})
	}

	return &ReleaseLabel{ReleaseLabel: releaseLabel, Applications: rla}, nil
}

// ListSupportedInstanceTypes returns the static catalog of EMR-supported instance types.
func (b *InMemoryBackend) ListSupportedInstanceTypes(
	_ context.Context, releaseLabel, marker string,
) ([]SupportedInstanceType, string) {
	// Validate release label exists (unknown labels → empty list matches AWS behavior).
	if _, ok := releaseLabelApps[releaseLabel]; !ok {
		return []SupportedInstanceType{}, ""
	}

	p := page.New(supportedInstanceTypes, marker, listInstanceTypesPage, listInstanceTypesPage)

	return p.Data, p.Next
}
