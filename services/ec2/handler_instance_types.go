package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
)

// ---- DescribeInstanceTypes wire shapes ----
//
// Field/element names below are confirmed against aws-sdk-go-v2/service/
// ec2@v1.329.0/deserializers.go: awsEc2query_deserializeDocumentInstanceTypeInfo
// (deserializers.go:121193) and its nested-type deserializers (VCpuInfo:180756,
// MemoryInfo:139906, ProcessorInfo:150545, NetworkInfo:142419,
// InstanceStorageInfo:120850, DiskInfo:99784, GpuInfo:107784,
// GpuDeviceInfo:107663 list wrapper). List wrapper element is "item"
// throughout (confirmed via awsEc2query_deserializeDocumentArchitectureTypeList
// etc.), matching this package's existing `Items []X `xml:"item"`` convention.

type stringListItem struct {
	Items []string `xml:"item"`
}

type vCPUInfoItem struct {
	DefaultVCpus          int32 `xml:"defaultVCpus"`
	DefaultCores          int32 `xml:"defaultCores,omitempty"`
	DefaultThreadsPerCore int32 `xml:"defaultThreadsPerCore,omitempty"`
}

type memoryInfoItem struct {
	SizeInMiB int64 `xml:"sizeInMiB"`
}

type processorInfoItem struct {
	SupportedArchitectures stringListItem `xml:"supportedArchitectures"`
}

type networkInfoItem struct {
	NetworkPerformance        string `xml:"networkPerformance,omitempty"`
	MaximumNetworkInterfaces  int32  `xml:"maximumNetworkInterfaces,omitempty"`
	Ipv4AddressesPerInterface int32  `xml:"ipv4AddressesPerInterface,omitempty"`
}

type diskInfoItem struct {
	Type     string `xml:"type"`
	SizeInGB int64  `xml:"sizeInGB"`
	Count    int32  `xml:"count"`
}

type instanceStorageInfoItem struct {
	Disks struct {
		Items []diskInfoItem `xml:"item"`
	} `xml:"disks"`
	TotalSizeInGB int64 `xml:"totalSizeInGB"`
}

type gpuDeviceMemoryInfoItem struct {
	SizeInMiB int32 `xml:"sizeInMiB"`
}

type gpuDeviceInfoItem struct {
	Name         string                  `xml:"name"`
	Manufacturer string                  `xml:"manufacturer"`
	Count        int32                   `xml:"count"`
	MemoryInfo   gpuDeviceMemoryInfoItem `xml:"memoryInfo"`
}

type gpuInfoItem struct {
	Gpus struct {
		Items []gpuDeviceInfoItem `xml:"item"`
	} `xml:"gpus"`
	TotalGpuMemoryInMiB int32 `xml:"totalGpuMemoryInMiB"`
}

// instanceTypeInfoItem is the rich per-type wire shape DescribeInstanceTypes
// returns. Members with no verified catalog data are left as Go zero values
// and rendered `omitempty` where the XML tag allows it; the always-present
// bool/struct members are still emitted (matching real AWS, which always
// includes them) even when every nested field is zero/absent.
type instanceTypeInfoItem struct {
	GpuInfo                       *gpuInfoItem             `xml:"gpuInfo,omitempty"`
	InstanceStorageInfo           *instanceStorageInfoItem `xml:"instanceStorageInfo,omitempty"`
	Hypervisor                    string                   `xml:"hypervisor,omitempty"`
	InstanceType                  string                   `xml:"instanceType"`
	ProcessorInfo                 processorInfoItem        `xml:"processorInfo"`
	NetworkInfo                   networkInfoItem          `xml:"networkInfo"`
	SupportedRootDeviceTypes      stringListItem           `xml:"supportedRootDeviceTypes"`
	SupportedUsageClasses         stringListItem           `xml:"supportedUsageClasses"`
	SupportedVirtualizationTypes  stringListItem           `xml:"supportedVirtualizationTypes"`
	MemoryInfo                    memoryInfoItem           `xml:"memoryInfo"`
	VCPUInfo                      vCPUInfoItem             `xml:"vCpuInfo"`
	BurstablePerformanceSupported bool                     `xml:"burstablePerformanceSupported"`
	BareMetal                     bool                     `xml:"bareMetal"`
	InstanceStorageSupported      bool                     `xml:"instanceStorageSupported"`
	FreeTierEligible              bool                     `xml:"freeTierEligible"`
	CurrentGeneration             bool                     `xml:"currentGeneration"`
}

type instanceTypeInfoSet struct {
	Items []instanceTypeInfoItem `xml:"item"`
}

type describeInstanceTypesResponse struct {
	XMLName       xml.Name            `xml:"DescribeInstanceTypesResponse"`
	Xmlns         string              `xml:"xmlns,attr"`
	RequestID     string              `xml:"requestId"`
	NextToken     string              `xml:"nextToken,omitempty"`
	InstanceTypes instanceTypeInfoSet `xml:"instanceTypeSet"`
}

// catalogEntryToWire converts one catalog spec to its DescribeInstanceTypes
// wire item. Every catalog member documented as populated for this entry is
// rendered; zero-valued/unset members are simply omitted by the xml tags
// above rather than rendered as fabricated zeros where AWS would omit them
// too (network performance, ENI counts, storage, GPU).
func catalogEntryToWire(instanceType string, spec instanceTypeSpec) instanceTypeInfoItem {
	item := instanceTypeInfoItem{
		InstanceType:                  instanceType,
		CurrentGeneration:             spec.currentGen,
		FreeTierEligible:              spec.freeTier,
		BareMetal:                     false,
		BurstablePerformanceSupported: spec.burstable,
		Hypervisor:                    spec.hypervisor,
		VCPUInfo: vCPUInfoItem{
			DefaultVCpus:          spec.vcpus,
			DefaultCores:          spec.cores,
			DefaultThreadsPerCore: spec.threadsPerCore,
		},
		MemoryInfo: memoryInfoItem{SizeInMiB: spec.memoryMiB},
		ProcessorInfo: processorInfoItem{
			SupportedArchitectures: stringListItem{Items: []string{spec.arch}},
		},
		NetworkInfo: networkInfoItem{
			NetworkPerformance:        spec.networkPerf,
			MaximumNetworkInterfaces:  spec.maxENI,
			Ipv4AddressesPerInterface: spec.ipv4PerENI,
		},
		InstanceStorageSupported:     spec.storage != nil,
		SupportedRootDeviceTypes:     stringListItem{Items: []string{"ebs"}},
		SupportedUsageClasses:        stringListItem{Items: []string{"on-demand", "spot"}},
		SupportedVirtualizationTypes: stringListItem{Items: []string{"hvm"}},
	}

	if spec.storage != nil {
		item.InstanceStorageInfo = &instanceStorageInfoItem{TotalSizeInGB: spec.storage.totalGB}
		item.InstanceStorageInfo.Disks.Items = []diskInfoItem{{
			Count:    spec.storage.diskCount,
			SizeInGB: spec.storage.diskSizeGB,
			Type:     spec.storage.diskType,
		}}
	}

	if spec.gpu != nil {
		item.GpuInfo = &gpuInfoItem{TotalGpuMemoryInMiB: spec.gpu.count * spec.gpu.memoryMiBEach}
		item.GpuInfo.Gpus.Items = []gpuDeviceInfoItem{{
			Count:        spec.gpu.count,
			Name:         spec.gpu.name,
			Manufacturer: spec.gpu.manufacturer,
			MemoryInfo:   gpuDeviceMemoryInfoItem{SizeInMiB: spec.gpu.memoryMiBEach},
		}}
	}

	return item
}

// sortedCatalogTypes returns every cataloged instance type name, sorted for
// deterministic listing/pagination.
func sortedCatalogTypes() []string {
	names := make([]string, 0, len(instanceTypeCatalog))
	for name := range instanceTypeCatalog {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// cpuManufacturerFor derives the CpuManufacturer enum value AWS would report
// for a catalog entry (types.go CpuManufacturer: "intel" | "amd" |
// "amazon-web-services" | "apple"). The catalog doesn't carry this as its own
// field; it is fully determined by architecture plus a small set of known AMD
// families (m5a, t3a, g5's AMD EPYC host CPU), matching the family doc
// citations in instance_type_catalog.go.
func cpuManufacturerFor(instanceType string, spec instanceTypeSpec) string {
	if spec.arch == archArm64 {
		return "amazon-web-services"
	}

	switch {
	case strings.HasPrefix(instanceType, "m5a."),
		strings.HasPrefix(instanceType, "t3a."),
		strings.HasPrefix(instanceType, "g5."):
		return "amd"
	default:
		return "intel"
	}
}

// ---- DescribeInstanceTypes ----

const (
	ec2DescribeInstanceTypesMinPageSize = 5
	ec2DescribeInstanceTypesMaxPageSize = 100
)

// handleDescribeInstanceTypes returns real catalog entries (instance_type_
// catalog.go) for the requested InstanceType.N values, or the full catalog
// when none are requested, honoring the documented Filters and MaxResults/
// NextToken pagination (api_op_DescribeInstanceTypes.go).
func (h *Handler) handleDescribeInstanceTypes(vals url.Values, reqID string) (any, error) {
	requested := parseMemberList(vals, "InstanceType")

	var names []string

	if len(requested) > 0 {
		var unknown []string

		for _, t := range requested {
			if _, ok := instanceTypeCatalog[t]; !ok {
				unknown = append(unknown, t)

				continue
			}

			names = append(names, t)
		}

		if len(unknown) > 0 {
			return nil, fmt.Errorf(
				"%w: The following supplied instance types do not exist: [%s]",
				ErrInvalidInstanceType, strings.Join(unknown, ", "),
			)
		}
	} else {
		names = sortedCatalogTypes()
	}

	names = applyInstanceTypeFilters(names, parseEC2Filters(vals))

	maxResults, offset, err := parseEC2Pagination(
		vals,
		ec2DescribeInstanceTypesMinPageSize,
		ec2DescribeInstanceTypesMaxPageSize,
		ec2DescribeInstanceTypesMaxPageSize,
	)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(names, offset, maxResults)

	items := make([]instanceTypeInfoItem, 0, len(page))
	for _, name := range page {
		items = append(items, catalogEntryToWire(name, instanceTypeCatalog[name]))
	}

	return &describeInstanceTypesResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		NextToken:     nextToken,
		InstanceTypes: instanceTypeInfoSet{Items: items},
	}, nil
}

// applyInstanceTypeFilters implements the DescribeInstanceTypes Filters this
// catalog has verified data for (api_op_DescribeInstanceTypes.go doc comment
// lists ~40 filter names total; the subset below covers every catalog member
// instance_type_catalog.go actually populates -- see PARITY.md for the full
// list of documented-but-unimplemented names, which fall through to this
// package's existing lenient "unknown filters pass through" convention
// (handler_filters.go instanceMatchesFilter)).
func applyInstanceTypeFilters(names []string, filters map[string][]string) []string {
	if len(filters) == 0 {
		return names
	}

	out := make([]string, 0, len(names))

	for _, name := range names {
		spec := instanceTypeCatalog[name]
		if instanceTypeMatchesAllFilters(name, spec, filters) {
			out = append(out, name)
		}
	}

	return out
}

func instanceTypeMatchesAllFilters(name string, spec instanceTypeSpec, filters map[string][]string) bool {
	for filterName, values := range filters {
		if !instanceTypeMatchesFilter(name, spec, filterName, values) {
			return false
		}
	}

	return true
}

// instanceTypeFilterPredicates dispatches each implemented DescribeInstanceTypes
// filter name to its match function, replacing a single large switch
// (analogous to this file's errCodeLookup dispatch-table convention).
// Filter names not present here (dedicated-hosts-supported, ebs-info.*,
// nitro-enclaves-support, nitro-tpm-*, network-info.efa-*, network-info.
// bandwidth-weightings, processor-info.supported-features, processor-info.
// sustained-clock-speed-in-ghz, reboot-migration-support, supported-boot-mode,
// hibernation-supported, auto-recovery-supported, vcpu-info.valid-cores,
// vcpu-info.valid-threads-per-core) are documented-but-unmodeled and fall
// through to instanceTypeMatchesFilter's lenient pass-through, matching this
// package's existing instanceMatchesFilter convention for unknown filters.
//
//nolint:gochecknoglobals // package-level dispatch table, analogous to errCodeLookup
var instanceTypeFilterPredicates = map[string]func(name string, spec instanceTypeSpec, values []string) bool{
	filterKeyInstanceType: func(name string, _ instanceTypeSpec, values []string) bool {
		return anyWildcardMatch(name, values)
	},
	"current-generation": func(_ string, spec instanceTypeSpec, values []string) bool {
		return anyEqual(strconv.FormatBool(spec.currentGen), values)
	},
	"free-tier-eligible": func(_ string, spec instanceTypeSpec, values []string) bool {
		return anyEqual(strconv.FormatBool(spec.freeTier), values)
	},
	"burstable-performance-supported": func(_ string, spec instanceTypeSpec, values []string) bool {
		return anyEqual(strconv.FormatBool(spec.burstable), values)
	},
	"bare-metal": func(_ string, _ instanceTypeSpec, values []string) bool {
		return anyEqual(ec2BooleanFalse, values)
	},
	"hypervisor": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.hypervisor != "" && anyEqual(spec.hypervisor, values)
	},
	"vcpu-info.default-vcpus": func(_ string, spec instanceTypeSpec, values []string) bool {
		return anyEqual(strconv.Itoa(int(spec.vcpus)), values)
	},
	"vcpu-info.default-cores": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.cores != 0 && anyEqual(strconv.Itoa(int(spec.cores)), values)
	},
	"vcpu-info.default-threads-per-core": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.threadsPerCore != 0 && anyEqual(strconv.Itoa(int(spec.threadsPerCore)), values)
	},
	"memory-info.size-in-mib": func(_ string, spec instanceTypeSpec, values []string) bool {
		return anyEqual(strconv.FormatInt(spec.memoryMiB, 10), values)
	},
	"processor-info.supported-architecture": func(_ string, spec instanceTypeSpec, values []string) bool {
		return anyEqual(spec.arch, values)
	},
	"network-info.network-performance": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.networkPerf != "" && anyEqual(spec.networkPerf, values)
	},
	"network-info.maximum-network-interfaces": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.maxENI != 0 && anyEqual(strconv.Itoa(int(spec.maxENI)), values)
	},
	"network-info.ipv4-addresses-per-interface": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.ipv4PerENI != 0 && anyEqual(strconv.Itoa(int(spec.ipv4PerENI)), values)
	},
	"instance-storage-supported": func(_ string, spec instanceTypeSpec, values []string) bool {
		return anyEqual(strconv.FormatBool(spec.storage != nil), values)
	},
	"instance-storage-info.total-size-in-gb": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.storage != nil && anyEqual(strconv.FormatInt(spec.storage.totalGB, 10), values)
	},
	"instance-storage-info.disk.type": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.storage != nil && anyEqual(spec.storage.diskType, values)
	},
	"instance-storage-info.disk.count": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.storage != nil && anyEqual(strconv.Itoa(int(spec.storage.diskCount)), values)
	},
	"instance-storage-info.disk.size-in-gb": func(_ string, spec instanceTypeSpec, values []string) bool {
		return spec.storage != nil && anyEqual(strconv.FormatInt(spec.storage.diskSizeGB, 10), values)
	},
	"supported-usage-class": func(_ string, _ instanceTypeSpec, values []string) bool {
		return anyEqual("on-demand", values) || anyEqual("spot", values)
	},
	"supported-virtualization-type": func(_ string, _ instanceTypeSpec, values []string) bool {
		return anyEqual("hvm", values)
	},
	"supported-root-device-type": func(_ string, _ instanceTypeSpec, values []string) bool {
		return anyEqual("ebs", values)
	},
}

func instanceTypeMatchesFilter(name string, spec instanceTypeSpec, filterName string, values []string) bool {
	fn, ok := instanceTypeFilterPredicates[filterName]
	if !ok {
		return true
	}

	return fn(name, spec, values)
}

// anyWildcardMatch reports whether name matches any of patterns, where a
// pattern may use "*" wildcards (api_op_DescribeInstanceTypes.go's
// instance-type filter doc: "for example c5.2xlarge or c5*").
func anyWildcardMatch(name string, patterns []string) bool {
	for _, p := range patterns {
		if ok, err := path.Match(p, name); err == nil && ok {
			return true
		}
	}

	return false
}

// ---- DescribeInstanceTypeOfferings ----

func (h *Handler) handleDescribeInstanceTypeOfferings(vals url.Values, reqID string) (any, error) {
	resp := &describeInstanceTypeOfferingsResponse{RequestID: reqID}

	locationType := vals.Get("LocationType")
	if locationType == "" {
		locationType = filterKeyAvailabilityZone
	}

	var offerings []InstanceTypeOffering

	switch locationType {
	case filterKeyAvailabilityZone:
		offerings = h.Backend.DescribeInstanceTypeOfferings()
	case "region":
		// h.Region is only populated once the service registry wires a
		// request context; derive the region straight from the backend's own
		// AZ generator instead (DescribeAvailabilityZones always returns
		// "<region>a"/"<region>b"/"<region>c" -- ec2core.go), which is set
		// unconditionally at backend construction.
		azs := h.Backend.DescribeAvailabilityZones("")

		var region string
		if len(azs) > 0 {
			region = strings.TrimSuffix(azs[0], "a")
		}

		for _, name := range sortedCatalogTypes() {
			offerings = append(offerings, InstanceTypeOffering{
				InstanceType: name,
				Location:     region,
				LocationType: "region",
			})
		}
	default:
		// "availability-zone-id" and "outpost": this backend has no AZ-ID or
		// Outpost inventory to offer against, so an honest empty result
		// rather than a fabricated match (matches the pre-existing PARITY.md
		// treatment of DescribeInstanceTypeOfferings' LocationType handling).
		return resp, nil
	}

	offerings = applyInstanceTypeOfferingFilters(offerings, parseEC2Filters(vals))

	for _, o := range offerings {
		resp.InstanceTypeOfferingSet.Items = append(
			resp.InstanceTypeOfferingSet.Items,
			instanceTypeOfferingItem{
				InstanceType: o.InstanceType,
				Location:     o.Location,
				LocationType: o.LocationType,
			},
		)
	}

	return resp, nil
}

type describeInstanceTypeOfferingsResponse struct {
	XMLName                 xml.Name `xml:"DescribeInstanceTypeOfferingsResponse"`
	RequestID               string   `xml:"requestId"`
	InstanceTypeOfferingSet struct {
		Items []instanceTypeOfferingItem `xml:"item"`
	} `xml:"instanceTypeOfferingSet"`
}

// applyInstanceTypeOfferingFilters filters offerings by the real "instance-type"
// and "location" filter names (ec2@v1.329.0 api_op_DescribeInstanceTypeOfferings.go
// DescribeInstanceTypeOfferingsInput.Filters doc comment).
func applyInstanceTypeOfferingFilters(
	offerings []InstanceTypeOffering,
	filters map[string][]string,
) []InstanceTypeOffering {
	if len(filters) == 0 {
		return offerings
	}

	out := make([]InstanceTypeOffering, 0, len(offerings))

	for _, o := range offerings {
		if vals, ok := filters[filterKeyInstanceType]; ok && !anyEqual(o.InstanceType, vals) {
			continue
		}

		if vals, ok := filters["location"]; ok && !anyEqual(o.Location, vals) {
			continue
		}

		out = append(out, o)
	}

	return out
}

// ---- GetInstanceTypesFromInstanceRequirements ----

// instanceRequirementsQuery is the parsed form of InstanceRequirementsRequest
// (aws-sdk-go-v2/service/ec2/types/types.go) plus the two required sibling
// parameters (ArchitectureTypes/VirtualizationTypes) this op's input wraps it
// with (types.go: GetInstanceTypesFromInstanceRequirementsInput). Wire keys
// verified against serializers.go awsEc2query_serializeDocumentInstance
// RequirementsRequest (serializers.go:60628) and
// awsEc2query_serializeDocumentVCpuCountRangeRequest (serializers.go:67863,
// confirming the ".Min"/".Max" range-request wire shape shared by every
// *Request range member below).
type instanceRequirementsQuery struct {
	bareMetal                       string
	localStorage                    string
	burstablePerformance            string
	archTypes                       []string
	acceleratorTypes                []string
	localStorageTypes               []string
	acceleratorNames                []string
	acceleratorManufacturers        []string
	virtTypes                       []string
	instanceGenerations             []string
	allowedTypes                    []string
	excludedTypes                   []string
	cpuManufacturers                []string
	networkInterfaceCountMax        int64
	acceleratorTotalMemoryMiBMin    int64
	memGiBPerVCpuMax                float64
	vcpuMax                         int64
	vcpuMin                         int64
	memGiBPerVCpuMin                float64
	memMax                          int64
	totalLocalStorageGBMin          float64
	totalLocalStorageGBMax          float64
	acceleratorCountMin             int64
	acceleratorCountMax             int64
	memMin                          int64
	networkInterfaceCountMin        int64
	acceleratorTotalMemoryMiBMax    int64
	requireHibernateSupport         bool
	acceleratorTotalMemoryMiBMaxSet bool
	acceleratorCountMaxSet          bool
	memGiBPerVCpuMaxSet             bool
	memGiBPerVCpuMinSet             bool
	totalLocalStorageGBMinSet       bool
	totalLocalStorageGBMaxSet       bool
	memMaxSet                       bool
	vcpuMaxSet                      bool
	networkInterfaceCountMaxSet     bool
	networkBandwidthGbpsGiven       bool
	baselineEbsBandwidthMbpsGiven   bool
}

func parseInt64RangeForm(vals url.Values, prefix string) (int64, int64, bool, error) {
	var minV, maxV int64

	var maxSet bool

	if v := vals.Get(prefix + ".Min"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("%w: %s.Min must be an integer", ErrInvalidParameter, prefix)
		}

		minV = n
	}

	if v := vals.Get(prefix + ".Max"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("%w: %s.Max must be an integer", ErrInvalidParameter, prefix)
		}

		maxV = n
		maxSet = true
	}

	return minV, maxV, maxSet, nil
}

func parseFloat64RangeForm(vals url.Values, prefix string) (float64, float64, bool, bool, error) {
	var minV, maxV float64

	var minSet, maxSet bool

	if v := vals.Get(prefix + ".Min"); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, 0, false, false, fmt.Errorf("%w: %s.Min must be a number", ErrInvalidParameter, prefix)
		}

		minV = n
		minSet = true
	}

	if v := vals.Get(prefix + ".Max"); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, 0, false, false, fmt.Errorf("%w: %s.Max must be a number", ErrInvalidParameter, prefix)
		}

		maxV = n
		maxSet = true
	}

	return minV, maxV, minSet, maxSet, nil
}

func parseInstanceRequirementsQuery(vals url.Values) (*instanceRequirementsQuery, error) {
	q := &instanceRequirementsQuery{
		archTypes: parseMemberList(vals, "ArchitectureType"),
		virtTypes: parseMemberList(vals, "VirtualizationType"),
	}

	if len(q.archTypes) == 0 {
		return nil, fmt.Errorf("%w: ArchitectureTypes is required", ErrInvalidParameter)
	}

	if len(q.virtTypes) == 0 {
		return nil, fmt.Errorf("%w: VirtualizationTypes is required", ErrInvalidParameter)
	}

	const reqPrefix = "InstanceRequirements."

	if vals.Get(reqPrefix+"VCpuCount.Min") == "" || vals.Get(reqPrefix+"MemoryMiB.Min") == "" {
		return nil, fmt.Errorf(
			"%w: InstanceRequirements.VCpuCount and InstanceRequirements.MemoryMiB are required",
			ErrInvalidParameter,
		)
	}

	var err error

	q.vcpuMin, q.vcpuMax, q.vcpuMaxSet, err = parseInt64RangeForm(vals, reqPrefix+"VCpuCount")
	if err != nil {
		return nil, err
	}

	q.memMin, q.memMax, q.memMaxSet, err = parseInt64RangeForm(vals, reqPrefix+"MemoryMiB")
	if err != nil {
		return nil, err
	}

	q.memGiBPerVCpuMin, q.memGiBPerVCpuMax, q.memGiBPerVCpuMinSet, q.memGiBPerVCpuMaxSet, err =
		parseFloat64RangeForm(vals, reqPrefix+"MemoryGiBPerVCpu")
	if err != nil {
		return nil, err
	}

	q.acceleratorCountMin, q.acceleratorCountMax, q.acceleratorCountMaxSet, err =
		parseInt64RangeForm(vals, reqPrefix+"AcceleratorCount")
	if err != nil {
		return nil, err
	}

	q.acceleratorTotalMemoryMiBMin, q.acceleratorTotalMemoryMiBMax, q.acceleratorTotalMemoryMiBMaxSet, err =
		parseInt64RangeForm(vals, reqPrefix+"AcceleratorTotalMemoryMiB")
	if err != nil {
		return nil, err
	}

	q.totalLocalStorageGBMin, q.totalLocalStorageGBMax, q.totalLocalStorageGBMinSet, q.totalLocalStorageGBMaxSet, err =
		parseFloat64RangeForm(vals, reqPrefix+"TotalLocalStorageGB")
	if err != nil {
		return nil, err
	}

	q.networkInterfaceCountMin, q.networkInterfaceCountMax, q.networkInterfaceCountMaxSet, err =
		parseInt64RangeForm(vals, reqPrefix+"NetworkInterfaceCount")
	if err != nil {
		return nil, err
	}

	q.networkBandwidthGbpsGiven = vals.Get(reqPrefix+"NetworkBandwidthGbps.Min") != "" ||
		vals.Get(reqPrefix+"NetworkBandwidthGbps.Max") != ""
	q.baselineEbsBandwidthMbpsGiven = vals.Get(reqPrefix+"BaselineEbsBandwidthMbps.Min") != "" ||
		vals.Get(reqPrefix+"BaselineEbsBandwidthMbps.Max") != ""

	q.cpuManufacturers = parseMemberList(vals, reqPrefix+"CpuManufacturer")
	q.excludedTypes = parseMemberList(vals, reqPrefix+"ExcludedInstanceType")
	q.allowedTypes = parseMemberList(vals, reqPrefix+"AllowedInstanceType")
	q.instanceGenerations = parseMemberList(vals, reqPrefix+"InstanceGeneration")
	q.acceleratorTypes = parseMemberList(vals, reqPrefix+"AcceleratorType")
	q.acceleratorManufacturers = parseMemberList(vals, reqPrefix+"AcceleratorManufacturer")
	q.acceleratorNames = parseMemberList(vals, reqPrefix+"AcceleratorName")
	q.localStorageTypes = parseMemberList(vals, reqPrefix+"LocalStorageType")

	q.bareMetal = vals.Get(reqPrefix + "BareMetal")
	q.burstablePerformance = vals.Get(reqPrefix + "BurstablePerformance")
	q.localStorage = vals.Get(reqPrefix + "LocalStorage")
	q.requireHibernateSupport = vals.Get(reqPrefix+"RequireHibernateSupport") == ec2BooleanTrue

	return q, nil
}

func inRangeInt64(v, minV, maxV int64, maxSet bool) bool {
	if v < minV {
		return false
	}

	return !maxSet || v <= maxV
}

func inRangeFloat64(v, minV float64, minSet bool, maxV float64, maxSet bool) bool {
	if minSet && v < minV {
		return false
	}

	return !maxSet || v <= maxV
}

// matchesInclusionPolicy applies the AWS included/excluded/required
// three-state policy shared by BareMetal, BurstablePerformance, and
// LocalStorage (types.go InstanceRequirementsRequest doc comments).
func matchesInclusionPolicy(policy string, defaultPolicy string, has bool) bool {
	if policy == "" {
		policy = defaultPolicy
	}

	switch policy {
	case "required":
		return has
	case "excluded":
		return !has
	default: // "included"
		return true
	}
}

func instanceTypeMatchesRequirements(name string, spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	return instanceTypeMatchesCoreRequirements(spec, q) &&
		instanceTypeMatchesSelectionRequirements(name, spec, q) &&
		instanceTypeMatchesAcceleratorRequirements(spec, q) &&
		instanceTypeMatchesStorageRequirements(spec, q) &&
		instanceTypeMatchesNetworkRequirements(spec, q)
}

// miBPerGiB converts the catalog's MemoryMiB into GiB for MemoryGiBPerVCpu comparisons.
const miBPerGiB = 1024

// instanceTypeMatchesCoreRequirements checks architecture, virtualization
// type, vCPU count, memory size, and memory-per-vCPU -- the "This member is
// required" pair (VCpuCount/MemoryMiB) plus their nearest siblings.
func instanceTypeMatchesCoreRequirements(spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	if !anyEqual(spec.arch, q.archTypes) {
		return false
	}

	if !anyEqual("hvm", q.virtTypes) {
		return false
	}

	if !inRangeInt64(int64(spec.vcpus), q.vcpuMin, q.vcpuMax, q.vcpuMaxSet) {
		return false
	}

	if !inRangeInt64(spec.memoryMiB, q.memMin, q.memMax, q.memMaxSet) {
		return false
	}

	if spec.vcpus > 0 && (q.memGiBPerVCpuMinSet || q.memGiBPerVCpuMaxSet) {
		perVCPU := (float64(spec.memoryMiB) / miBPerGiB) / float64(spec.vcpus)
		if !inRangeFloat64(
			perVCPU,
			q.memGiBPerVCpuMin,
			q.memGiBPerVCpuMinSet,
			q.memGiBPerVCpuMax,
			q.memGiBPerVCpuMaxSet,
		) {
			return false
		}
	}

	return true
}

// instanceTypeMatchesSelectionRequirements checks the type-selection
// attributes: CpuManufacturers, Allowed/ExcludedInstanceTypes,
// InstanceGenerations, BareMetal, BurstablePerformance, and
// RequireHibernateSupport.
func instanceTypeMatchesSelectionRequirements(name string, spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	if len(q.cpuManufacturers) > 0 && !anyEqual(cpuManufacturerFor(name, spec), q.cpuManufacturers) {
		return false
	}

	if len(q.allowedTypes) > 0 && !anyWildcardMatch(name, q.allowedTypes) {
		return false
	}

	if len(q.excludedTypes) > 0 && anyWildcardMatch(name, q.excludedTypes) {
		return false
	}

	if !instanceTypeMatchesGeneration(spec, q.instanceGenerations) {
		return false
	}

	if !matchesInclusionPolicy(q.bareMetal, "excluded", false) {
		return false
	}

	if !matchesInclusionPolicy(q.burstablePerformance, "excluded", spec.burstable) {
		return false
	}

	// Not modeled: no catalog entry has verified HibernationSupported data.
	return !q.requireHibernateSupport
}

func instanceTypeMatchesGeneration(spec instanceTypeSpec, generations []string) bool {
	if len(generations) == 0 {
		return true
	}

	wantCurrent := anyEqual("current", generations)
	wantPrevious := anyEqual("previous", generations)

	if spec.currentGen {
		return wantCurrent || !wantPrevious
	}

	return wantPrevious || !wantCurrent
}

// instanceTypeMatchesNetworkRequirements checks NetworkInterfaceCount.
// NetworkBandwidthGbps and BaselineEbsBandwidthMbps are deliberately not
// checked here -- see instanceRequirementsQuery's doc comment.
func instanceTypeMatchesNetworkRequirements(spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	if q.networkInterfaceCountMin == 0 && !q.networkInterfaceCountMaxSet {
		return true
	}

	if spec.maxENI == 0 {
		return false // unverified ENI count: conservative exclusion, not a fabricated pass.
	}

	return inRangeInt64(
		int64(spec.maxENI),
		q.networkInterfaceCountMin,
		q.networkInterfaceCountMax,
		q.networkInterfaceCountMaxSet,
	)
}

func instanceTypeMatchesAcceleratorRequirements(spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	return instanceTypeMatchesAcceleratorRanges(spec, q) && instanceTypeMatchesAcceleratorIdentity(spec, q)
}

// instanceTypeMatchesAcceleratorRanges checks AcceleratorCount and
// AcceleratorTotalMemoryMiB. Absent a GPU, both quantities are 0 -- range
// checks with only a Max (including the documented Max=0 "exclude
// accelerator-enabled instance types" case) exclude GPU instances for free.
func instanceTypeMatchesAcceleratorRanges(spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	var acceleratorCount, acceleratorTotalMemMiB int64

	if spec.gpu != nil {
		acceleratorCount = int64(spec.gpu.count)
		acceleratorTotalMemMiB = int64(spec.gpu.count * spec.gpu.memoryMiBEach)
	}

	if (q.acceleratorCountMin != 0 || q.acceleratorCountMaxSet) &&
		!inRangeInt64(acceleratorCount, q.acceleratorCountMin, q.acceleratorCountMax, q.acceleratorCountMaxSet) {
		return false
	}

	if q.acceleratorTotalMemoryMiBMin == 0 && !q.acceleratorTotalMemoryMiBMaxSet {
		return true
	}

	return inRangeInt64(
		acceleratorTotalMemMiB,
		q.acceleratorTotalMemoryMiBMin,
		q.acceleratorTotalMemoryMiBMax,
		q.acceleratorTotalMemoryMiBMaxSet,
	)
}

// instanceTypeMatchesAcceleratorIdentity checks AcceleratorTypes,
// AcceleratorManufacturers, and AcceleratorNames -- every cataloged
// accelerator is a GPU, so any of these being set requires spec.gpu != nil.
func instanceTypeMatchesAcceleratorIdentity(spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	if len(q.acceleratorTypes) > 0 && (spec.gpu == nil || !anyEqual("gpu", q.acceleratorTypes)) {
		return false
	}

	if len(q.acceleratorManufacturers) > 0 &&
		(spec.gpu == nil || !anyEqual(strings.ToLower(spec.gpu.manufacturer), q.acceleratorManufacturers)) {
		return false
	}

	return len(q.acceleratorNames) == 0 ||
		(spec.gpu != nil && anyEqual(strings.ToLower(spec.gpu.name), q.acceleratorNames))
}

func instanceTypeMatchesStorageRequirements(spec instanceTypeSpec, q *instanceRequirementsQuery) bool {
	if !matchesInclusionPolicy(q.localStorage, "included", spec.storage != nil) {
		return false
	}

	if len(q.localStorageTypes) > 0 && (spec.storage == nil || !anyEqual(spec.storage.diskType, q.localStorageTypes)) {
		return false
	}

	if q.totalLocalStorageGBMinSet || q.totalLocalStorageGBMaxSet {
		var totalGB float64
		if spec.storage != nil {
			totalGB = float64(spec.storage.totalGB)
		}

		if !inRangeFloat64(
			totalGB,
			q.totalLocalStorageGBMin,
			q.totalLocalStorageGBMinSet,
			q.totalLocalStorageGBMax,
			q.totalLocalStorageGBMaxSet,
		) {
			return false
		}
	}

	return true
}

type getInstanceTypesFromReqsResponse struct {
	XMLName         xml.Name `xml:"GetInstanceTypesFromInstanceRequirementsResponse"`
	RequestID       string   `xml:"requestId"`
	NextToken       string   `xml:"nextToken,omitempty"`
	InstanceTypeSet struct {
		Items []instanceTypeOfferingItem2 `xml:"item"`
	} `xml:"instanceTypeSet"`
}

type instanceTypeOfferingItem2 struct {
	InstanceType string `xml:"instanceType"`
}

// handleGetInstanceTypesFromInstanceRequirements implements the real
// attribute-based matching engine over instanceTypeCatalog, per
// InstanceRequirementsRequest (types.go) and the EC2 Fleet/ASG
// attribute-based instance type selection doc (docs.aws.amazon.com/AWSEC2/
// latest/UserGuide/ec2-fleet-attribute-based-instance-type-selection.html).
func (h *Handler) handleGetInstanceTypesFromInstanceRequirements(vals url.Values, reqID string) (any, error) {
	q, err := parseInstanceRequirementsQuery(vals)
	if err != nil {
		return nil, err
	}

	matched := h.Backend.GetInstanceTypesFromInstanceRequirements(q)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(matched, offset, maxResults)

	resp := &getInstanceTypesFromReqsResponse{RequestID: reqID, NextToken: nextToken}
	for _, t := range page {
		resp.InstanceTypeSet.Items = append(resp.InstanceTypeSet.Items, instanceTypeOfferingItem2{InstanceType: t})
	}

	return resp, nil
}
