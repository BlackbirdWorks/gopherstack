package ec2

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	imdsMACBytes      = 5
	imdsTimeLayout    = "2006-01-02T15:04:05Z"
	imdsDocumentIndex = "  "
	imdsDocVersion    = "2017-09-30"
	imdsPartition     = "aws"
	imdsDomain        = "amazonaws.com"
	imdsCodeSuccess   = "Success"
	imdsCredType      = "AWS-HMAC"
)

type imdsLeaf func() (string, error)

type imdsTree struct {
	leaves map[string]imdsLeaf
	dirs   map[string]bool
	custom map[string]string
}

func (t *imdsTree) value(path, v string) {
	t.leaves[path] = func() (string, error) { return v, nil }
}

// list renders a directory: children sorted, sub-directories suffixed with "/".
func (t *imdsTree) list(dir string) (string, bool) {
	if custom, ok := t.custom[dir]; ok {
		return custom, true
	}

	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}

	seen := map[string]bool{}

	add := func(path string) {
		rest, ok := strings.CutPrefix(path, prefix)
		if !ok || rest == "" {
			return
		}

		name, deeper, _ := strings.Cut(rest, "/")
		seen[name] = seen[name] || deeper != "" || t.dirs[strings.TrimSuffix(prefix+name, "/")]
	}

	for path := range t.leaves {
		add(path)
	}

	for path := range t.dirs {
		add(path)
	}

	if len(seen) == 0 && !t.dirs[dir] && dir != "" {
		return "", false
	}

	names := make([]string, 0, len(seen))

	for name, isDir := range seen {
		if isDir {
			name += "/"
		}

		names = append(names, name)
	}

	slices.Sort(names)

	return strings.Join(names, "\n"), true
}

func (h *Handler) buildIMDSTree(_ context.Context, v *imdsView, opts IMDSOptions) *imdsTree {
	t := &imdsTree{leaves: map[string]imdsLeaf{}, dirs: map[string]bool{}, custom: map[string]string{}}
	inst := &v.Instance

	t.addInstanceLeaves(v)
	t.addNetworkLeaves(v)
	t.addBlockDevices(v)
	t.addIdentityDocument(v)
	h.addIAMLeaves(t, v)

	if data := decodeUserData(inst.UserData); data != "" {
		t.value("user-data", data)
	}

	if opts.InstanceMetadataTags == imdsEnabled {
		t.dirs["meta-data/tags/instance"] = true

		for k, val := range v.Tags {
			if !strings.HasPrefix(k, "aws:") && !strings.Contains(k, "/") {
				t.value("meta-data/tags/instance/"+k, val)
			}
		}
	}

	return t
}

func decodeUserData(stored string) string {
	if stored == "" {
		return ""
	}

	raw, err := base64.StdEncoding.DecodeString(stored)
	if err != nil {
		return stored
	}

	return string(raw)
}

func (t *imdsTree) addInstanceLeaves(v *imdsView) {
	inst := &v.Instance
	az := inst.Placement.AvailabilityZone

	t.value("meta-data/ami-id", inst.ImageID)
	t.value("meta-data/ami-launch-index", "0")
	t.value("meta-data/ami-manifest-path", "(unknown)")
	t.value("meta-data/instance-action", "none")
	t.value("meta-data/instance-id", inst.ID)
	t.value("meta-data/instance-life-cycle", "on-demand")
	t.value("meta-data/instance-type", inst.InstanceType)
	t.value("meta-data/profile", "default-hvm")
	t.value("meta-data/reservation-id", instanceReservationID(inst))
	t.value("meta-data/placement/availability-zone", az)
	t.value("meta-data/placement/region", v.Region)
	t.value("meta-data/services/domain", imdsDomain)
	t.value("meta-data/services/partition", imdsPartition)

	if id := availabilityZoneID(az); id != "" {
		t.value("meta-data/placement/availability-zone-id", id)
	}

	if inst.PrivateIP != "" {
		host := privateHostname(inst.PrivateIP, v.Region)
		t.value("meta-data/local-ipv4", inst.PrivateIP)
		t.value("meta-data/local-hostname", host)
		t.value("meta-data/hostname", host)
	}

	if inst.PublicIPAddress != "" {
		t.value("meta-data/public-ipv4", inst.PublicIPAddress)
	}

	if inst.PublicDNSName != "" {
		t.value("meta-data/public-hostname", inst.PublicDNSName)
	}

	if len(v.Interfaces) > 0 {
		t.value("meta-data/mac", imdsMAC(v.Interfaces[0].ID))
	}

	names := make([]string, 0, len(inst.SecurityGroups))
	for _, id := range inst.SecurityGroups {
		names = append(names, v.SGNames[id])
	}

	t.value("meta-data/security-groups", strings.Join(names, "\n"))

	if inst.KeyName != "" && v.KeyPublic != "" {
		t.value("meta-data/public-keys/0/openssh-key", v.KeyPublic)
		t.custom["meta-data/public-keys"] = "0=" + inst.KeyName
	}
}

// privateHostname renders the EC2 resource-based private DNS name for ip.
func privateHostname(ip, region string) string {
	name := "ip-" + strings.ReplaceAll(ip, ".", "-")
	if region == regionUSEast1 {
		return name + ".ec2.internal"
	}

	return name + "." + region + ".compute.internal"
}

// imdsMAC derives a stable locally-administered MAC from an interface ID.
func imdsMAC(eniID string) string {
	sum := sha256.Sum256([]byte(eniID))
	parts := make([]string, 1, 1+imdsMACBytes)
	parts[0] = "02"

	for _, b := range sum[:imdsMACBytes] {
		parts = append(parts, fmt.Sprintf("%02x", b))
	}

	return strings.Join(parts, ":")
}

func (t *imdsTree) addNetworkLeaves(v *imdsView) {
	for _, ifc := range v.Interfaces {
		base := "meta-data/network/interfaces/macs/" + imdsMAC(ifc.ID) + "/"

		t.value(base+"device-number", strconv.Itoa(ifc.DeviceIndex))
		t.value(base+"interface-id", ifc.ID)
		t.value(base+"mac", imdsMAC(ifc.ID))
		t.value(base+"owner-id", v.AccountID)
		t.value(base+"subnet-id", ifc.SubnetID)
		t.value(base+"vpc-id", ifc.VPCID)
		t.value(base+"security-group-ids", strings.Join(ifc.SecurityGroupIDs, "\n"))

		names := make([]string, 0, len(ifc.SecurityGroupIDs))
		for _, id := range ifc.SecurityGroupIDs {
			names = append(names, v.SGNames[id])
		}

		t.value(base+"security-groups", strings.Join(names, "\n"))

		if ifc.SubnetCIDR != "" {
			t.value(base+"subnet-ipv4-cidr-block", ifc.SubnetCIDR)
		}

		if ifc.VPCCIDR != "" {
			t.value(base+"vpc-ipv4-cidr-block", ifc.VPCCIDR)
			t.value(base+"vpc-ipv4-cidr-blocks", ifc.VPCCIDR)
		}

		if ifc.PrivateIP != "" {
			t.value(base+"local-ipv4s", strings.Join(append([]string{ifc.PrivateIP}, ifc.SecondaryPrivateIPs...), "\n"))
			t.value(base+"local-hostname", privateHostname(ifc.PrivateIP, v.Region))
		}

		if ifc.DeviceIndex == 0 && v.Instance.PublicIPAddress != "" {
			t.value(base+"public-ipv4s", v.Instance.PublicIPAddress)
			t.value(base+"public-hostname", v.Instance.PublicDNSName)
		}
	}
}

func (t *imdsTree) addBlockDevices(v *imdsView) {
	if v.Instance.RootDeviceName != "" {
		t.value("meta-data/block-device-mapping/ami", v.Instance.RootDeviceName)
		t.value("meta-data/block-device-mapping/root", v.Instance.RootDeviceName)
	}

	n := 0

	for _, att := range v.BlockDevice {
		if att.Device == v.Instance.RootDeviceName {
			continue
		}

		n++
		t.value("meta-data/block-device-mapping/ebs"+strconv.Itoa(n), att.Device)
	}

	t.dirs["meta-data/block-device-mapping"] = true
}

type imdsIdentityDocument struct {
	AccountID               string  `json:"accountId"`
	Architecture            string  `json:"architecture"`
	AvailabilityZone        string  `json:"availabilityZone"`
	BillingProducts         *string `json:"billingProducts"`
	DevpayProductCodes      *string `json:"devpayProductCodes"`
	MarketplaceProductCodes *string `json:"marketplaceProductCodes"`
	ImageID                 string  `json:"imageId"`
	InstanceID              string  `json:"instanceId"`
	InstanceType            string  `json:"instanceType"`
	KernelID                *string `json:"kernelId"`
	PendingTime             string  `json:"pendingTime"`
	PrivateIP               string  `json:"privateIp"`
	RamdiskID               *string `json:"ramdiskId"`
	Region                  string  `json:"region"`
	Version                 string  `json:"version"`
}

func (t *imdsTree) addIdentityDocument(v *imdsView) {
	inst := &v.Instance
	doc := imdsIdentityDocument{
		AccountID:        v.AccountID,
		Architecture:     instanceArchitecture(inst.InstanceType),
		AvailabilityZone: inst.Placement.AvailabilityZone,
		ImageID:          inst.ImageID,
		InstanceID:       inst.ID,
		InstanceType:     inst.InstanceType,
		PendingTime:      inst.LaunchTime.UTC().Format(imdsTimeLayout),
		PrivateIP:        inst.PrivateIP,
		Region:           v.Region,
		Version:          imdsDocVersion,
	}

	t.leaves["dynamic/instance-identity/document"] = func() (string, error) {
		out, err := json.MarshalIndent(doc, "", imdsDocumentIndex)

		return string(out), err
	}
}

// instanceArchitecture reports arm64 for Graviton families (t4g, m6g, c7gn, ...), else x86_64.
func instanceArchitecture(instanceType string) string {
	family, _, _ := strings.Cut(instanceType, ".")

	digits := strings.IndexFunc(family, func(r rune) bool { return r >= '0' && r <= '9' })
	if digits < 0 {
		return "x86_64"
	}

	rest := strings.TrimLeft(family[digits:], "0123456789")
	if strings.HasPrefix(rest, "g") {
		return "arm64"
	}

	return "x86_64"
}

type imdsInfoDocument struct {
	Code               string `json:"Code"`
	LastUpdated        string `json:"LastUpdated"`
	InstanceProfileArn string `json:"InstanceProfileArn"`
	InstanceProfileID  string `json:"InstanceProfileId"`
}

type imdsCredentialDocument struct {
	Code            string `json:"Code"`
	LastUpdated     string `json:"LastUpdated"`
	Type            string `json:"Type"`
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string `json:"SecretAccessKey"`
	Token           string `json:"Token"`
	Expiration      string `json:"Expiration"`
}

func (h *Handler) addIAMLeaves(t *imdsTree, v *imdsView) {
	if v.ProfileARN == "" {
		return
	}

	platform := h.imds.platformOf()
	if platform == nil {
		return
	}

	prof, err := platform.InstanceProfileRole(iamProfileName(v.ProfileARN))
	if err != nil || prof.RoleName == "" {
		return
	}

	updated := v.Instance.LaunchTime.UTC().Format(imdsTimeLayout)

	t.leaves["meta-data/iam/info"] = func() (string, error) {
		out, mErr := json.MarshalIndent(imdsInfoDocument{
			Code: imdsCodeSuccess, LastUpdated: updated, InstanceProfileArn: prof.ARN, InstanceProfileID: prof.ID,
		}, "", imdsDocumentIndex)

		return string(out), mErr
	}

	t.leaves["meta-data/iam/security-credentials/"+prof.RoleName] = func() (string, error) {
		creds, cErr := h.imds.credentialsFor(v.Instance.ID, prof)
		if cErr != nil {
			return "", cErr
		}

		out, mErr := json.MarshalIndent(imdsCredentialDocument{
			Code: imdsCodeSuccess, LastUpdated: updated, Type: imdsCredType,
			AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey, Token: creds.SessionToken,
			Expiration: creds.Expires.UTC().Format(imdsTimeLayout),
		}, "", imdsDocumentIndex)

		return string(out), mErr
	}
}
