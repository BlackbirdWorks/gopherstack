package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultLCMaxRecords and maxLCMaxRecords are DescribeLaunchConfigurations's documented
// default/max page size (api_op_DescribeLaunchConfigurations.go: "The default value is 50 and
// the maximum value is 100").
const (
	defaultLCMaxRecords = 50
	maxLCMaxRecords     = 100
)

func (h *Handler) handleCreateLaunchConfiguration(vals url.Values) (any, error) {
	name := vals.Get("LaunchConfigurationName")
	imageID := vals.Get("ImageId")
	instanceType := vals.Get("InstanceType")
	keyName := vals.Get("KeyName")
	iamInstanceProfile := vals.Get("IamInstanceProfile")
	userData := vals.Get("UserData")
	kernelID := vals.Get("KernelId")
	ramdiskID := vals.Get("RamdiskId")
	securityGroups := parseMembers(vals, "SecurityGroups.member")
	classicLinkSGs := parseMembers(vals, "ClassicLinkVPCSecurityGroups.member")
	blockDeviceMappings := parseBlockDeviceMappings(vals)

	var metadata *InstanceMetadataOptions

	if hasMetadataOptions(vals) {
		opts, err := parseMetadataOptions(vals)
		if err != nil {
			return nil, err
		}

		metadata = &opts
	}

	input := CreateLaunchConfigurationInput{
		MetadataOptions:              metadata,
		LaunchConfigurationName:      name,
		ImageID:                      imageID,
		InstanceType:                 instanceType,
		KeyName:                      keyName,
		IAMInstanceProfile:           iamInstanceProfile,
		UserData:                     userData,
		KernelID:                     kernelID,
		RamdiskID:                    ramdiskID,
		SpotPrice:                    vals.Get("SpotPrice"),
		PlacementTenancy:             vals.Get("PlacementTenancy"),
		ClassicLinkVPCID:             vals.Get("ClassicLinkVPCId"),
		SecurityGroups:               securityGroups,
		ClassicLinkVPCSecurityGroups: classicLinkSGs,
		BlockDeviceMappings:          blockDeviceMappings,
		AssociatePublicIPAddress:     vals.Get("AssociatePublicIpAddress") == formValueTrue,
		EbsOptimized:                 vals.Get("EbsOptimized") == formValueTrue,
		InstanceMonitoring:           vals.Get("InstanceMonitoring.Enabled") == formValueTrue,
	}

	_, createErr := h.Backend.CreateLaunchConfiguration(input)
	if createErr != nil {
		return nil, createErr
	}

	return &createLaunchConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-create-lc-" + name},
	}, nil
}

func (h *Handler) handleDescribeLaunchConfigurations(vals url.Values) (any, error) {
	names := parseMembers(vals, "LaunchConfigurationNames.member")

	lcs, err := h.Backend.DescribeLaunchConfigurations(names)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultLCMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), maxLCMaxRecords)
		}
	}

	p := page.New(lcs, vals.Get("NextToken"), maxRecords, defaultLCMaxRecords)

	members := make([]xmlLaunchConfiguration, 0, len(p.Data))
	for i := range p.Data {
		members = append(members, toXMLLaunchConfiguration(&p.Data[i]))
	}

	return &describeLaunchConfigurationsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLaunchConfigurationsResult{
			NextToken:            p.Next,
			LaunchConfigurations: xmlLaunchConfigurationList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lcs"},
	}, nil
}

func (h *Handler) handleDeleteLaunchConfiguration(vals url.Values) (any, error) {
	name := vals.Get("LaunchConfigurationName")

	if err := h.Backend.DeleteLaunchConfiguration(name); err != nil {
		return nil, err
	}

	return &deleteLaunchConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-lc-" + name},
	}, nil
}

// toXMLLaunchConfiguration converts a LaunchConfiguration to the XML response type.
func toXMLLaunchConfiguration(lc *LaunchConfiguration) xmlLaunchConfiguration {
	sgs := make([]xmlStringValue, 0, len(lc.SecurityGroups))
	for _, sg := range lc.SecurityGroups {
		sgs = append(sgs, xmlStringValue{Value: sg})
	}

	clSGs := make([]xmlStringValue, 0, len(lc.ClassicLinkVPCSecurityGroups))
	for _, sg := range lc.ClassicLinkVPCSecurityGroups {
		clSGs = append(clSGs, xmlStringValue{Value: sg})
	}

	bdms := make([]xmlBlockDeviceMapping, 0, len(lc.BlockDeviceMappings))
	for _, bdm := range lc.BlockDeviceMappings {
		xmlBDM := xmlBlockDeviceMapping{
			DeviceName:  bdm.DeviceName,
			VirtualName: bdm.VirtualName,
			NoDevice:    bdm.NoDevice,
		}

		if bdm.Ebs != nil {
			xmlBDM.Ebs = &xmlEbsBlockDevice{
				SnapshotID:          bdm.Ebs.SnapshotID,
				VolumeType:          bdm.Ebs.VolumeType,
				KmsKeyID:            bdm.Ebs.KmsKeyID,
				VolumeSize:          bdm.Ebs.VolumeSize,
				Iops:                bdm.Ebs.Iops,
				Throughput:          bdm.Ebs.Throughput,
				DeleteOnTermination: bdm.Ebs.DeleteOnTermination,
				Encrypted:           bdm.Ebs.Encrypted,
			}
		}

		bdms = append(bdms, xmlBDM)
	}

	return xmlLaunchConfiguration{
		LaunchConfigurationName:      lc.LaunchConfigurationName,
		LaunchConfigurationARN:       lc.LaunchConfigurationARN,
		ImageID:                      lc.ImageID,
		InstanceType:                 lc.InstanceType,
		KeyName:                      lc.KeyName,
		IAMInstanceProfile:           lc.IAMInstanceProfile,
		UserData:                     lc.UserData,
		KernelID:                     lc.KernelID,
		RamdiskID:                    lc.RamdiskID,
		SpotPrice:                    lc.SpotPrice,
		PlacementTenancy:             lc.PlacementTenancy,
		ClassicLinkVPCID:             lc.ClassicLinkVPCID,
		CreatedTime:                  lc.CreatedTime.UTC().Format(time.RFC3339),
		SecurityGroups:               xmlStringValueList{Members: sgs},
		ClassicLinkVPCSecurityGroups: xmlStringValueList{Members: clSGs},
		BlockDeviceMappings:          xmlBlockDeviceMappingList{Members: bdms},
		InstanceMonitoring:           xmlInstanceMonitoring{Enabled: lc.InstanceMonitoring},
		AssociatePublicIPAddress:     lc.AssociatePublicIPAddress,
		EbsOptimized:                 lc.EbsOptimized,
		MetadataOptions:              toXMLMetadataOptions(lc.MetadataOptions),
	}
}

type xmlMetadataOptions struct {
	HTTPEndpoint            string `xml:"HttpEndpoint,omitempty"`
	HTTPTokens              string `xml:"HttpTokens,omitempty"`
	HTTPPutResponseHopLimit int32  `xml:"HttpPutResponseHopLimit,omitempty"`
}

func toXMLMetadataOptions(m *InstanceMetadataOptions) *xmlMetadataOptions {
	if m == nil {
		return nil
	}

	return &xmlMetadataOptions{
		HTTPEndpoint: m.HTTPEndpoint, HTTPTokens: m.HTTPTokens, HTTPPutResponseHopLimit: m.HTTPPutResponseHopLimit,
	}
}

const maxMetadataHopLimit = 64

func hasMetadataOptions(vals url.Values) bool {
	return vals.Get("MetadataOptions.HttpEndpoint") != "" || vals.Get("MetadataOptions.HttpTokens") != "" ||
		vals.Get("MetadataOptions.HttpPutResponseHopLimit") != ""
}

// parseMetadataOptions reads and validates MetadataOptions.* against the SDK enums and 1-64 hop range.
func parseMetadataOptions(vals url.Values) (InstanceMetadataOptions, error) {
	endpoint := vals.Get("MetadataOptions.HttpEndpoint")
	tokens := vals.Get("MetadataOptions.HttpTokens")
	hops := vals.Get("MetadataOptions.HttpPutResponseHopLimit")

	if endpoint != "" && endpoint != "enabled" && endpoint != "disabled" {
		return InstanceMetadataOptions{}, fmt.Errorf(
			"%w: invalid MetadataOptions.HttpEndpoint %q", ErrInvalidParameter, endpoint,
		)
	}

	if tokens != "" && tokens != "optional" && tokens != "required" {
		return InstanceMetadataOptions{}, fmt.Errorf(
			"%w: invalid MetadataOptions.HttpTokens %q", ErrInvalidParameter, tokens,
		)
	}

	n, err := parseIntVal(hops)
	if err != nil || (hops != "" && (n < 1 || n > maxMetadataHopLimit)) {
		return InstanceMetadataOptions{}, fmt.Errorf(
			"%w: invalid MetadataOptions.HttpPutResponseHopLimit", ErrInvalidParameter,
		)
	}

	return InstanceMetadataOptions{HTTPEndpoint: endpoint, HTTPTokens: tokens, HTTPPutResponseHopLimit: n}, nil
}

type xmlEbsBlockDevice struct {
	SnapshotID          string `xml:"SnapshotId,omitempty"`
	VolumeType          string `xml:"VolumeType,omitempty"`
	KmsKeyID            string `xml:"KmsKeyId,omitempty"`
	VolumeSize          int32  `xml:"VolumeSize,omitempty"`
	Iops                int32  `xml:"Iops,omitempty"`
	Throughput          int32  `xml:"Throughput,omitempty"`
	DeleteOnTermination bool   `xml:"DeleteOnTermination,omitempty"`
	Encrypted           bool   `xml:"Encrypted,omitempty"`
}

type xmlBlockDeviceMapping struct {
	Ebs         *xmlEbsBlockDevice `xml:"Ebs,omitempty"`
	DeviceName  string             `xml:"DeviceName"`
	VirtualName string             `xml:"VirtualName,omitempty"`
	NoDevice    string             `xml:"NoDevice,omitempty"`
}

type xmlBlockDeviceMappingList struct {
	Members []xmlBlockDeviceMapping `xml:"member"`
}

type xmlInstanceMonitoring struct {
	Enabled bool `xml:"Enabled"`
}

type xmlLaunchConfiguration struct {
	MetadataOptions              *xmlMetadataOptions       `xml:"MetadataOptions,omitempty"`
	PlacementTenancy             string                    `xml:"PlacementTenancy,omitempty"`
	ClassicLinkVPCID             string                    `xml:"ClassicLinkVPCId,omitempty"`
	InstanceType                 string                    `xml:"InstanceType"`
	LaunchConfigurationName      string                    `xml:"LaunchConfigurationName"`
	IAMInstanceProfile           string                    `xml:"IamInstanceProfile,omitempty"`
	UserData                     string                    `xml:"UserData,omitempty"`
	KernelID                     string                    `xml:"KernelId,omitempty"`
	RamdiskID                    string                    `xml:"RamdiskId,omitempty"`
	ImageID                      string                    `xml:"ImageId"`
	SpotPrice                    string                    `xml:"SpotPrice,omitempty"`
	KeyName                      string                    `xml:"KeyName,omitempty"`
	CreatedTime                  string                    `xml:"CreatedTime"`
	LaunchConfigurationARN       string                    `xml:"LaunchConfigurationARN"`
	ClassicLinkVPCSecurityGroups xmlStringValueList        `xml:"ClassicLinkVPCSecurityGroups,omitempty"`
	BlockDeviceMappings          xmlBlockDeviceMappingList `xml:"BlockDeviceMappings,omitempty"`
	SecurityGroups               xmlStringValueList        `xml:"SecurityGroups"`
	InstanceMonitoring           xmlInstanceMonitoring     `xml:"InstanceMonitoring"`
	AssociatePublicIPAddress     bool                      `xml:"AssociatePublicIpAddress,omitempty"`
	EbsOptimized                 bool                      `xml:"EbsOptimized,omitempty"`
}

type xmlLaunchConfigurationList struct {
	Members []xmlLaunchConfiguration `xml:"member"`
}

type describeLaunchConfigurationsResult struct {
	NextToken            string                     `xml:"NextToken,omitempty"`
	LaunchConfigurations xmlLaunchConfigurationList `xml:"LaunchConfigurations"`
}

type createLaunchConfigurationResponse struct {
	XMLName          xml.Name            `xml:"CreateLaunchConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeLaunchConfigurationsResponse struct {
	XMLName          xml.Name                           `xml:"DescribeLaunchConfigurationsResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeLaunchConfigurationsResult `xml:"DescribeLaunchConfigurationsResult"`
}

type deleteLaunchConfigurationResponse struct {
	XMLName          xml.Name            `xml:"DeleteLaunchConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// parseEbsBlockDevice parses the BlockDeviceMappings.member.<i>.Ebs.* form
// values, returning nil if no Ebs sub-fields were specified for member i.
func parseEbsBlockDevice(vals url.Values, i int) *EbsBlockDevice {
	prefix := fmt.Sprintf("BlockDeviceMappings.member.%d.Ebs.", i)

	snapshotID := vals.Get(prefix + "SnapshotId")
	volumeType := vals.Get(prefix + "VolumeType")
	kmsKeyID := vals.Get(prefix + "KmsKeyId")
	volumeSize := vals.Get(prefix + "VolumeSize")

	if snapshotID == "" && volumeType == "" && kmsKeyID == "" && volumeSize == "" {
		return nil
	}

	ebs := &EbsBlockDevice{
		SnapshotID:          snapshotID,
		VolumeType:          volumeType,
		KmsKeyID:            kmsKeyID,
		DeleteOnTermination: vals.Get(prefix+"DeleteOnTermination") != "false",
		Encrypted:           vals.Get(prefix+"Encrypted") == formValueTrue,
	}

	if n, parseErr := parseIntVal(volumeSize); parseErr == nil {
		ebs.VolumeSize = n
	}

	if v := vals.Get(prefix + "Iops"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil {
			ebs.Iops = n
		}
	}

	if v := vals.Get(prefix + "Throughput"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil {
			ebs.Throughput = n
		}
	}

	return ebs
}

// parseBlockDeviceMappings parses BlockDeviceMappings from form values.
func parseBlockDeviceMappings(vals url.Values) []BlockDeviceMapping {
	result := make([]BlockDeviceMapping, 0)

	for i := 1; ; i++ {
		deviceKey := fmt.Sprintf("BlockDeviceMappings.member.%d.DeviceName", i)
		deviceName := vals.Get(deviceKey)

		if deviceName == "" {
			break
		}

		result = append(result, BlockDeviceMapping{
			DeviceName:  deviceName,
			VirtualName: vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.VirtualName", i)),
			NoDevice:    vals.Get(fmt.Sprintf("BlockDeviceMappings.member.%d.NoDevice", i)),
			Ebs:         parseEbsBlockDevice(vals, i),
		})
	}

	return result
}
