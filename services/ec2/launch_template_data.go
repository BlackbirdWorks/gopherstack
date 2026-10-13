package ec2

import (
	"fmt"
	"maps"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const launchTemplateDataPrefix = "LaunchTemplateData."

// launchTemplateDataFromVals collects the request's LaunchTemplateData.* form keys, prefix stripped.
func launchTemplateDataFromVals(vals url.Values) map[string]string {
	data := make(map[string]string)

	for k, v := range vals {
		if rest, ok := strings.CutPrefix(k, launchTemplateDataPrefix); ok && len(v) > 0 {
			data[rest] = v[0]
		}
	}

	return data
}

func ltDataGroup(key string) string {
	group, _, _ := strings.Cut(key, ".")

	return group
}

// mergeLaunchTemplateData applies over onto base; any top-level member present in over replaces
// the whole member from base.
func mergeLaunchTemplateData(base, over map[string]string) map[string]string {
	replaced := make(map[string]struct{})
	for k := range over {
		replaced[ltDataGroup(k)] = struct{}{}
	}

	out := make(map[string]string, len(base)+len(over))

	for k, v := range base {
		if _, drop := replaced[ltDataGroup(k)]; !drop {
			out[k] = v
		}
	}

	maps.Copy(out, over)

	return out
}

// ltIndexed groups "prefix.N.rest" keys into per-index maps ordered by N.
func ltIndexed(d map[string]string, prefix string) []map[string]string {
	byIdx := make(map[int]map[string]string)

	for k, v := range d {
		rest, ok := strings.CutPrefix(k, prefix+".")
		if !ok {
			continue
		}

		idxStr, sub, _ := strings.Cut(rest, ".")

		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}

		if byIdx[idx] == nil {
			byIdx[idx] = make(map[string]string)
		}

		byIdx[idx][sub] = v
	}

	keys := make([]int, 0, len(byIdx))
	for i := range byIdx {
		keys = append(keys, i)
	}

	sort.Ints(keys)

	out := make([]map[string]string, 0, len(keys))
	for _, i := range keys {
		out = append(out, byIdx[i])
	}

	return out
}

// ltList reads "prefix.N" scalar members in order.
func ltList(d map[string]string, prefix string) []string {
	type kv struct {
		v string
		n int
	}

	var items []kv

	for k, v := range d {
		if rest, ok := strings.CutPrefix(k, prefix+"."); ok {
			if n, err := strconv.Atoi(rest); err == nil {
				items = append(items, kv{n: n, v: v})
			}
		}
	}

	sort.Slice(items, func(i, j int) bool { return items[i].n < items[j].n })

	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.v)
	}

	return out
}

type ltTagXML struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type ltTagSpecXML struct {
	ResourceType string     `xml:"resourceType"`
	Tags         []ltTagXML `xml:"tagSet>item"`
}

type ltEbsXML struct {
	VolumeSize          string `xml:"volumeSize,omitempty"`
	VolumeType          string `xml:"volumeType,omitempty"`
	DeleteOnTermination string `xml:"deleteOnTermination,omitempty"`
	Encrypted           string `xml:"encrypted,omitempty"`
	Iops                string `xml:"iops,omitempty"`
	Throughput          string `xml:"throughput,omitempty"`
	SnapshotID          string `xml:"snapshotId,omitempty"`
	KmsKeyID            string `xml:"kmsKeyId,omitempty"`
}

type ltBlockDeviceXML struct {
	Ebs        *ltEbsXML `xml:"ebs,omitempty"`
	DeviceName string    `xml:"deviceName,omitempty"`
}

type ltNetworkInterfaceXML struct {
	DeviceIndex         string   `xml:"deviceIndex,omitempty"`
	AssociatePublicIP   string   `xml:"associatePublicIpAddress,omitempty"`
	DeleteOnTermination string   `xml:"deleteOnTermination,omitempty"`
	Description         string   `xml:"description,omitempty"`
	SubnetID            string   `xml:"subnetId,omitempty"`
	NetworkInterfaceID  string   `xml:"networkInterfaceId,omitempty"`
	PrivateIPAddress    string   `xml:"privateIpAddress,omitempty"`
	Groups              []string `xml:"groupSet>groupId,omitempty"`
}

type ltIamProfileXML struct {
	Arn  string `xml:"arn,omitempty"`
	Name string `xml:"name,omitempty"`
}

type ltPlacementXML struct {
	AvailabilityZone string `xml:"availabilityZone,omitempty"`
	GroupName        string `xml:"groupName,omitempty"`
	Tenancy          string `xml:"tenancy,omitempty"`
}

type ltMetadataXML struct {
	HTTPTokens              string `xml:"httpTokens,omitempty"`
	HTTPEndpoint            string `xml:"httpEndpoint,omitempty"`
	HTTPPutResponseHopLimit string `xml:"httpPutResponseHopLimit,omitempty"`
	InstanceMetadataTags    string `xml:"instanceMetadataTags,omitempty"`
}

type ltDataXML struct {
	IamInstanceProfile *ltIamProfileXML `xml:"iamInstanceProfile,omitempty"`
	Monitoring         *struct {
		Enabled string `xml:"enabled"`
	} `xml:"monitoring,omitempty"`
	Placement           *ltPlacementXML `xml:"placement,omitempty"`
	MetadataOptions     *ltMetadataXML  `xml:"metadataOptions,omitempty"`
	CreditSpecification *struct {
		CPUCredits string `xml:"cpuCredits"`
	} `xml:"creditSpecification,omitempty"`
	ImageID                           string                  `xml:"imageId,omitempty"`
	InstanceType                      string                  `xml:"instanceType,omitempty"`
	KeyName                           string                  `xml:"keyName,omitempty"`
	UserData                          string                  `xml:"userData,omitempty"`
	EbsOptimized                      string                  `xml:"ebsOptimized,omitempty"`
	DisableAPITermination             string                  `xml:"disableApiTermination,omitempty"`
	DisableAPIStop                    string                  `xml:"disableApiStop,omitempty"`
	InstanceInitiatedShutdownBehavior string                  `xml:"instanceInitiatedShutdownBehavior,omitempty"`
	SecurityGroupIDs                  []string                `xml:"securityGroupIdSet>item,omitempty"`
	SecurityGroups                    []string                `xml:"securityGroupSet>item,omitempty"`
	BlockDevices                      []ltBlockDeviceXML      `xml:"blockDeviceMappingSet>item,omitempty"`
	NetworkInterfaces                 []ltNetworkInterfaceXML `xml:"networkInterfaceSet>item,omitempty"`
	TagSpecifications                 []ltTagSpecXML          `xml:"tagSpecificationSet>item,omitempty"`
}

func renderLaunchTemplateData(d map[string]string) ltDataXML {
	x := ltDataXML{
		ImageID:                           d["ImageId"],
		InstanceType:                      d["InstanceType"],
		KeyName:                           d["KeyName"],
		UserData:                          d["UserData"],
		EbsOptimized:                      d["EbsOptimized"],
		DisableAPITermination:             d["DisableApiTermination"],
		DisableAPIStop:                    d["DisableApiStop"],
		InstanceInitiatedShutdownBehavior: d["InstanceInitiatedShutdownBehavior"],
		SecurityGroupIDs:                  ltList(d, "SecurityGroupId"),
		SecurityGroups:                    ltList(d, "SecurityGroup"),
	}

	if d["IamInstanceProfile.Arn"] != "" || d["IamInstanceProfile.Name"] != "" {
		x.IamInstanceProfile = &ltIamProfileXML{Arn: d["IamInstanceProfile.Arn"], Name: d["IamInstanceProfile.Name"]}
	}

	if v, ok := d["Monitoring.Enabled"]; ok {
		x.Monitoring = &struct {
			Enabled string `xml:"enabled"`
		}{v}
	}

	if v, ok := d["CreditSpecification.CpuCredits"]; ok {
		x.CreditSpecification = &struct {
			CPUCredits string `xml:"cpuCredits"`
		}{v}
	}

	p := ltPlacementXML{d["Placement.AvailabilityZone"], d["Placement.GroupName"], d["Placement.Tenancy"]}
	if p != (ltPlacementXML{}) {
		x.Placement = &p
	}

	if m := (ltMetadataXML{
		d["MetadataOptions.HttpTokens"], d["MetadataOptions.HttpEndpoint"],
		d["MetadataOptions.HttpPutResponseHopLimit"], d["MetadataOptions.InstanceMetadataTags"],
	}); m != (ltMetadataXML{}) {
		x.MetadataOptions = &m
	}

	for _, g := range ltIndexed(d, "BlockDeviceMapping") {
		bd := ltBlockDeviceXML{DeviceName: g["DeviceName"]}

		if e := (ltEbsXML{
			g["Ebs.VolumeSize"], g["Ebs.VolumeType"], g["Ebs.DeleteOnTermination"], g["Ebs.Encrypted"],
			g["Ebs.Iops"], g["Ebs.Throughput"], g["Ebs.SnapshotId"], g["Ebs.KmsKeyId"],
		}); e != (ltEbsXML{}) {
			bd.Ebs = &e
		}

		x.BlockDevices = append(x.BlockDevices, bd)
	}

	for _, g := range ltIndexed(d, "NetworkInterface") {
		x.NetworkInterfaces = append(x.NetworkInterfaces, ltNetworkInterfaceXML{
			DeviceIndex: g["DeviceIndex"], AssociatePublicIP: g["AssociatePublicIpAddress"],
			DeleteOnTermination: g["DeleteOnTermination"], Description: g["Description"],
			SubnetID: g["SubnetId"], NetworkInterfaceID: g["NetworkInterfaceId"],
			PrivateIPAddress: g["PrivateIpAddress"], Groups: ltList(g, "SecurityGroupId"),
		})
	}

	for _, g := range ltIndexed(d, "TagSpecification") {
		ts := ltTagSpecXML{ResourceType: g["ResourceType"]}

		for _, t := range ltIndexed(g, "Tag") {
			ts.Tags = append(ts.Tags, ltTagXML{Key: t["Key"], Value: t["Value"]})
		}

		x.TagSpecifications = append(x.TagSpecifications, ts)
	}

	return x
}

// ResolveLaunchTemplateData returns the data of the given template version for RunInstances to
// overlay; version accepts a number, "$Latest", "$Default" or "".
func (b *InMemoryBackend) ResolveLaunchTemplateData(idOrName, version string) (map[string]string, error) {
	b.mu.RLock("ResolveLaunchTemplateData")
	defer b.mu.RUnlock()

	lt := b.findLaunchTemplateLocked(idOrName)
	if lt == nil {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, idOrName)
	}

	ver, err := resolveLaunchTemplateVersion(lt, version)
	if err != nil {
		return nil, err
	}

	return ver.effectiveData(), nil
}

// effectiveData returns the stored data, falling back to the ImageID/InstanceType scalars of
// versions recorded before full data was kept.
func (v *LaunchTemplateVersion) effectiveData() map[string]string {
	out := make(map[string]string, len(v.Data))
	maps.Copy(out, v.Data)

	if _, ok := out["ImageId"]; !ok && v.ImageID != "" {
		out["ImageId"] = v.ImageID
	}

	if _, ok := out["InstanceType"]; !ok && v.InstanceType != "" {
		out["InstanceType"] = v.InstanceType
	}

	return out
}

// LaunchTemplateVersions returns the template and a copy of every recorded version, oldest first.
func (b *InMemoryBackend) LaunchTemplateVersions(idOrName string) (*LaunchTemplate, []LaunchTemplateVersion, error) {
	b.mu.RLock("LaunchTemplateVersions")
	defer b.mu.RUnlock()

	lt := b.findLaunchTemplateLocked(idOrName)
	if lt == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, idOrName)
	}

	cp := *lt
	versions := make([]LaunchTemplateVersion, len(lt.Versions))

	for i, v := range lt.Versions {
		versions[i] = v
		versions[i].DefaultVersion = v.VersionNumber == lt.DefaultVersionNumber
	}

	return &cp, versions, nil
}

// AllLaunchTemplateVersions returns every template's versions, for describes without an identifier.
func (b *InMemoryBackend) AllLaunchTemplateVersions() []LaunchTemplateVersion {
	b.mu.RLock("AllLaunchTemplateVersions")
	defer b.mu.RUnlock()

	var out []LaunchTemplateVersion

	for _, lt := range b.launchTemplates.All() {
		for _, v := range lt.Versions {
			v.DefaultVersion = v.VersionNumber == lt.DefaultVersionNumber
			out = append(out, v)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].LaunchTemplateID != out[j].LaunchTemplateID {
			return out[i].LaunchTemplateID < out[j].LaunchTemplateID
		}

		return out[i].VersionNumber < out[j].VersionNumber
	})

	return out
}

// CreateLaunchTemplateWithData creates a launch template keeping the full LaunchTemplateData.
func (b *InMemoryBackend) CreateLaunchTemplateWithData(
	name, description string, data map[string]string, tags map[string]string,
) (*LaunchTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateLaunchTemplate")
	defer b.mu.Unlock()

	for _, lt := range b.launchTemplates.All() {
		if lt.Name == name {
			return nil, fmt.Errorf("%w: duplicate launch template name %s", ErrInvalidParameter, name)
		}
	}

	now := time.Now().UTC()
	lt := &LaunchTemplate{
		ID:                   newLaunchTemplateID(),
		Name:                 name,
		ImageID:              data["ImageId"],
		InstanceType:         data["InstanceType"],
		CreatedBy:            b.AccountID,
		CreateTime:           now,
		DefaultVersionNumber: 1,
		LatestVersionNumber:  1,
	}
	lt.Versions = []LaunchTemplateVersion{{
		LaunchTemplateID:   lt.ID,
		LaunchTemplateName: lt.Name,
		CreatedBy:          lt.CreatedBy,
		ImageID:            lt.ImageID,
		InstanceType:       lt.InstanceType,
		Description:        description,
		Data:               data,
		CreateTime:         now,
		VersionNumber:      1,
		DefaultVersion:     true,
	}}
	b.launchTemplates.Put(lt)
	b.setTagsLocked(lt.ID, tags)
	cp := *lt

	return &cp, nil
}

// CreateLaunchTemplateVersionWithData adds a version, inheriting from sourceVersion (when set)
// with the given data members replacing the inherited ones.
func (b *InMemoryBackend) CreateLaunchTemplateVersionWithData(
	idOrName, sourceVersion, description string, data map[string]string,
) (*LaunchTemplateVersion, error) {
	if idOrName == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId or LaunchTemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateLaunchTemplateVersion")
	defer b.mu.Unlock()

	lt := b.findLaunchTemplateLocked(idOrName)
	if lt == nil {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, idOrName)
	}

	merged := data

	if sourceVersion != "" {
		src, err := resolveLaunchTemplateVersion(lt, sourceVersion)
		if err != nil {
			return nil, err
		}

		merged = mergeLaunchTemplateData(src.effectiveData(), data)
	}

	lt.LatestVersionNumber++
	lt.ImageID, lt.InstanceType = merged["ImageId"], merged["InstanceType"]

	ver := LaunchTemplateVersion{
		LaunchTemplateID:   lt.ID,
		LaunchTemplateName: lt.Name,
		CreatedBy:          lt.CreatedBy,
		ImageID:            lt.ImageID,
		InstanceType:       lt.InstanceType,
		Description:        description,
		Data:               merged,
		CreateTime:         time.Now().UTC(),
		VersionNumber:      lt.LatestVersionNumber,
		DefaultVersion:     lt.DefaultVersionNumber == lt.LatestVersionNumber,
	}
	lt.Versions = append(lt.Versions, ver)

	return &ver, nil
}

// DeleteLaunchTemplateVersionsDetailed deletes the given versions, returning those removed and
// those that did not exist. Deleting the default version is rejected.
func (b *InMemoryBackend) DeleteLaunchTemplateVersionsDetailed(
	idOrName string, versions []int64,
) (*LaunchTemplate, []int64, []int64, error) {
	if idOrName == "" {
		return nil, nil, nil, fmt.Errorf("%w: LaunchTemplateId or LaunchTemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLaunchTemplateVersions")
	defer b.mu.Unlock()

	lt := b.findLaunchTemplateLocked(idOrName)
	if lt == nil {
		return nil, nil, nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, idOrName)
	}

	for _, v := range versions {
		if v == lt.DefaultVersionNumber {
			return nil, nil, nil, fmt.Errorf("%w: cannot delete the default version %d", ErrInvalidParameter, v)
		}
	}

	var deleted, missing []int64

	for _, v := range versions {
		idx := -1

		for i := range lt.Versions {
			if lt.Versions[i].VersionNumber == v {
				idx = i

				break
			}
		}

		if idx < 0 {
			missing = append(missing, v)

			continue
		}

		lt.Versions = append(lt.Versions[:idx], lt.Versions[idx+1:]...)
		deleted = append(deleted, v)
	}

	cp := *lt

	return &cp, deleted, missing, nil
}

// ModifyLaunchTemplateByRef sets the default version of a template identified by ID or name.
func (b *InMemoryBackend) ModifyLaunchTemplateByRef(idOrName string, defaultVersion int64) (*LaunchTemplate, error) {
	if idOrName == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateId or LaunchTemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyLaunchTemplate")
	defer b.mu.Unlock()

	lt := b.findLaunchTemplateLocked(idOrName)
	if lt == nil {
		return nil, fmt.Errorf("%w: %s", ErrLaunchTemplateNotFound, idOrName)
	}

	if defaultVersion > 0 {
		found := false

		for _, v := range lt.Versions {
			found = found || v.VersionNumber == defaultVersion
		}

		if !found && len(lt.Versions) > 0 {
			return nil, fmt.Errorf("%w: %s version %d", ErrLaunchTemplateVersionNotFound, lt.ID, defaultVersion)
		}

		lt.DefaultVersionNumber = defaultVersion
	}

	cp := *lt

	return &cp, nil
}
