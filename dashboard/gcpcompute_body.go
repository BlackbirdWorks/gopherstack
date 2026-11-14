package dashboard

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	computeMaxLabels     = 64
	computeMaxLabelLen   = 63
	computeMaxDiskGB     = 65536
	computeNICAccessName = "External NAT"
	computeNICAccessType = "ONE_TO_ONE_NAT"
	computeKeyName       = "name"
)

var (
	errComputeBody       = errors.New("invalid request body")
	errComputeSnapName   = errors.New("snapshot name must be 1-63 lowercase letters, digits or dashes")
	errComputeInstFields = errors.New("a valid instance name and machine type are required")
	errComputeOneEach    = errors.New("exactly one boot disk and one network interface are required")
	errComputeImage      = errors.New("a boot image (projects/<project>/global/images/[family/]<name>) is required")
	errComputeDiskSize   = errors.New("disk size is out of range")
	errComputeNetwork    = errors.New("invalid network")
	errComputeSubnet     = errors.New("invalid subnetwork")
	errComputeLabels     = errors.New("too many labels")
	errComputeLabel      = errors.New("invalid label")

	computeNameRE    = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	computeMachineRE = regexp.MustCompile(`^(?:zones/[a-z0-9-]+/machineTypes/)?[a-z0-9][-a-z0-9]{0,62}$`)
	computeImageRE   = regexp.MustCompile(
		`^projects/[a-z][-a-z0-9]{0,61}[a-z0-9]/global/images/(?:family/)?[a-z0-9][-a-z0-9]{0,62}$`,
	)
	computeNetworkRE = regexp.MustCompile(`^(?:global/networks/)?[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	computeSubnetRE  = regexp.MustCompile(`^(?:regions/[a-z0-9-]+/subnetworks/)?[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	computeLabelRE   = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
)

type computeInstanceIn struct {
	Labels            map[string]string `json:"labels"`
	Name              string            `json:"name"`
	MachineType       string            `json:"machineType"`
	Disks             []computeDiskIn   `json:"disks"`
	NetworkInterfaces []computeNICIn    `json:"networkInterfaces"`
}

type computeDiskIn struct {
	InitializeParams struct {
		SourceImage string `json:"sourceImage"`
		DiskSizeGb  string `json:"diskSizeGb"`
	} `json:"initializeParams"`
}

type computeNICIn struct {
	Network       string `json:"network"`
	Subnetwork    string `json:"subnetwork"`
	AccessConfigs []struct {
		Type string `json:"type"`
	} `json:"accessConfigs"`
}

// gcpComputeBody returns the body forwarded upstream: nothing for reads and actions, and a body rebuilt from fixed
// fields for creating an instance or a snapshot, so metadata, service accounts and every other field are dropped.
func gcpComputeBody(method string, parts []string, in io.Reader) ([]byte, error) {
	if method != http.MethodPost {
		return nil, nil
	}

	switch {
	case len(parts) == computeScopedList && parts[computeScopedColl] == computeInstances:
		return rebuildComputeInstance(in, parts[computeScopedList-2])
	case len(parts) == computeScopedVerb && parts[computeScopedVerb-1] == computeSnapshotVerb:
		return rebuildComputeSnapshot(in)
	}

	return nil, nil
}

func decodeComputeBody(in io.Reader, out any) error {
	if err := json.UnmarshalDecode(jsontext.NewDecoder(in), out, jsonx.V1()); err != nil {
		return errComputeBody
	}

	return nil
}

func rebuildComputeSnapshot(in io.Reader) ([]byte, error) {
	var req struct {
		Name string `json:"name"`
	}

	if err := decodeComputeBody(in, &req); err != nil {
		return nil, err
	}

	if !computeNameRE.MatchString(req.Name) {
		return nil, errComputeSnapName
	}

	return json.Marshal(map[string]string{computeKeyName: req.Name}, jsonx.V1())
}

func rebuildComputeInstance(in io.Reader, zone string) ([]byte, error) {
	var req computeInstanceIn

	if err := decodeComputeBody(in, &req); err != nil {
		return nil, err
	}

	if !computeNameRE.MatchString(req.Name) || !computeMachineRE.MatchString(req.MachineType) {
		return nil, errComputeInstFields
	}

	if len(req.Disks) != 1 || len(req.NetworkInterfaces) != 1 {
		return nil, errComputeOneEach
	}

	disk, err := rebuildComputeDisk(req.Disks[0])
	if err != nil {
		return nil, err
	}

	nic, err := rebuildComputeNIC(req.NetworkInterfaces[0])
	if err != nil {
		return nil, err
	}

	labels, err := rebuildComputeLabels(req.Labels)
	if err != nil {
		return nil, err
	}

	machine := req.MachineType
	if machine == shortComputeName(machine) {
		machine = "zones/" + zone + "/machineTypes/" + machine
	}

	out := map[string]any{
		computeKeyName: req.Name, "machineType": machine,
		"disks": []any{disk}, "networkInterfaces": []any{nic},
	}
	if len(labels) > 0 {
		out["labels"] = labels
	}

	return json.Marshal(out, jsonx.V1())
}

func shortComputeName(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}

	return s
}

func rebuildComputeDisk(d computeDiskIn) (map[string]any, error) {
	p := d.InitializeParams
	if !computeImageRE.MatchString(p.SourceImage) {
		return nil, errComputeImage
	}

	init := map[string]any{"sourceImage": p.SourceImage}

	if p.DiskSizeGb != "" {
		n, err := strconv.Atoi(p.DiskSizeGb)
		if err != nil || n < 1 || n > computeMaxDiskGB {
			return nil, errComputeDiskSize
		}

		init["diskSizeGb"] = strconv.Itoa(n)
	}

	return map[string]any{"boot": true, "autoDelete": true, "initializeParams": init}, nil
}

func rebuildComputeNIC(n computeNICIn) (map[string]any, error) {
	out := map[string]any{}

	if n.Network != "" {
		if !computeNetworkRE.MatchString(n.Network) {
			return nil, errComputeNetwork
		}

		out["network"] = n.Network
	}

	if n.Subnetwork != "" {
		if !computeSubnetRE.MatchString(n.Subnetwork) {
			return nil, errComputeSubnet
		}

		out["subnetwork"] = n.Subnetwork
	}

	if len(n.AccessConfigs) > 0 {
		out["accessConfigs"] = []any{
			map[string]string{computeKeyName: computeNICAccessName, "type": computeNICAccessType},
		}
	}

	return out, nil
}

func rebuildComputeLabels(in map[string]string) (map[string]string, error) {
	if len(in) > computeMaxLabels {
		return nil, errComputeLabels
	}

	for k, v := range in {
		if !computeNameRE.MatchString(k) || !computeLabelRE.MatchString(v) {
			return nil, errComputeLabel
		}
	}

	return in, nil
}
