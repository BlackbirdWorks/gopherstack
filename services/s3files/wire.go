package s3files

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// Wire shapes mirror service/s3files serializers.go and deserializers.go (rest-json, lowerCamel members,
// epoch-seconds timestamps).

type wireTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func tagsToWire(m map[string]string) []wireTag {
	if len(m) == 0 {
		return nil
	}

	out := make([]wireTag, 0, len(m))

	for _, kv := range sortedKV(m) {
		out = append(out, wireTag(kv))
	}

	return out
}

func tagsFromWire(in []wireTag) map[string]string {
	if len(in) == 0 {
		return nil
	}

	m := make(map[string]string, len(in))
	for _, t := range in {
		m[t.Key] = t.Value
	}

	return m
}

type fileSystemWire struct {
	Name          string    `json:"name,omitempty"`
	ClientToken   string    `json:"clientToken,omitempty"`
	FileSystemARN string    `json:"fileSystemArn"`
	FileSystemID  string    `json:"fileSystemId"`
	KMSKeyID      string    `json:"kmsKeyId,omitempty"`
	Bucket        string    `json:"bucket"`
	OwnerID       string    `json:"ownerId"`
	Prefix        string    `json:"prefix,omitempty"`
	RoleARN       string    `json:"roleArn"`
	Status        string    `json:"status"`
	StatusMessage string    `json:"statusMessage,omitempty"`
	Tags          []wireTag `json:"tags,omitempty"`
	CreationTime  float64   `json:"creationTime"`
}

func fileSystemToWire(fs *FileSystem) fileSystemWire {
	return fileSystemWire{
		Bucket: fs.Bucket, ClientToken: fs.ClientToken, CreationTime: awstime.Epoch(fs.CreatedAt),
		FileSystemARN: fs.ARN, FileSystemID: fs.ID, KMSKeyID: fs.KMSKeyID, Name: fs.Tags["Name"],
		OwnerID: fs.OwnerID, Prefix: fs.Prefix, RoleARN: fs.RoleARN, Status: fs.Status,
		StatusMessage: fs.StatusMessage, Tags: tagsToWire(fs.Tags),
	}
}

type fileSystemSummaryWire struct {
	Bucket        string  `json:"bucket"`
	FileSystemARN string  `json:"fileSystemArn"`
	FileSystemID  string  `json:"fileSystemId"`
	OwnerID       string  `json:"ownerId"`
	RoleARN       string  `json:"roleArn"`
	Status        string  `json:"status"`
	Name          string  `json:"name,omitempty"`
	StatusMessage string  `json:"statusMessage,omitempty"`
	CreationTime  float64 `json:"creationTime"`
}

type mountTargetWire struct {
	MountTargetID      string   `json:"mountTargetId"`
	OwnerID            string   `json:"ownerId"`
	SubnetID           string   `json:"subnetId"`
	AvailabilityZoneID string   `json:"availabilityZoneId,omitempty"`
	FileSystemID       string   `json:"fileSystemId"`
	IPv4Address        string   `json:"ipv4Address,omitempty"`
	IPv6Address        string   `json:"ipv6Address,omitempty"`
	NetworkInterfaceID string   `json:"networkInterfaceId"`
	Status             string   `json:"status"`
	StatusMessage      string   `json:"statusMessage,omitempty"`
	VPCID              string   `json:"vpcId,omitempty"`
	SecurityGroups     []string `json:"securityGroups,omitempty"`
}

func mountTargetToWire(mt *MountTarget, withGroups bool) mountTargetWire {
	w := mountTargetWire{
		MountTargetID: mt.ID, OwnerID: mt.OwnerID, SubnetID: mt.SubnetID,
		AvailabilityZoneID: mt.AvailabilityZoneID, FileSystemID: mt.FileSystemID,
		IPv4Address: mt.IPv4Address, IPv6Address: mt.IPv6Address, NetworkInterfaceID: mt.NetworkInterfaceID,
		Status: mt.Status, StatusMessage: mt.StatusMessage, VPCID: mt.VPCID,
	}
	if withGroups {
		w.SecurityGroups = mt.SecurityGroups
	}

	return w
}

type accessPointWire struct {
	AccessPointARN string         `json:"accessPointArn"`
	AccessPointID  string         `json:"accessPointId"`
	ClientToken    string         `json:"clientToken,omitempty"`
	FileSystemID   string         `json:"fileSystemId"`
	OwnerID        string         `json:"ownerId"`
	Status         string         `json:"status"`
	Name           string         `json:"name,omitempty"`
	PosixUser      *PosixUser     `json:"posixUser,omitempty"`
	RootDirectory  *RootDirectory `json:"rootDirectory,omitempty"`
	Tags           []wireTag      `json:"tags,omitempty"`
}

func accessPointToWire(ap *AccessPoint, withTags bool) accessPointWire {
	w := accessPointWire{
		AccessPointARN: ap.ARN, AccessPointID: ap.ID, ClientToken: ap.ClientToken, FileSystemID: ap.FileSystemID,
		OwnerID: ap.OwnerID, Status: ap.Status, Name: ap.Tags["Name"], PosixUser: ap.PosixUser,
		RootDirectory: ap.RootDirectory,
	}
	if withTags {
		w.Tags = tagsToWire(ap.Tags)
	}

	return w
}

type createFileSystemRequest struct {
	Bucket      string    `json:"bucket"`
	RoleARN     string    `json:"roleArn"`
	ClientToken string    `json:"clientToken"`
	KMSKeyID    string    `json:"kmsKeyId"`
	Prefix      string    `json:"prefix"`
	Tags        []wireTag `json:"tags"`
}

type createMountTargetRequest struct {
	FileSystemID   string   `json:"fileSystemId"`
	SubnetID       string   `json:"subnetId"`
	IPAddressType  string   `json:"ipAddressType"`
	IPv4Address    string   `json:"ipv4Address"`
	IPv6Address    string   `json:"ipv6Address"`
	SecurityGroups []string `json:"securityGroups"`
}

type updateMountTargetRequest struct {
	SecurityGroups []string `json:"securityGroups"`
}

type createAccessPointRequest struct {
	FileSystemID  string         `json:"fileSystemId"`
	ClientToken   string         `json:"clientToken"`
	PosixUser     *PosixUser     `json:"posixUser"`
	RootDirectory *RootDirectory `json:"rootDirectory"`
	Tags          []wireTag      `json:"tags"`
}

type putPolicyRequest struct {
	Policy string `json:"policy"`
}

type putSyncRequest struct {
	LatestVersionNumber *int32               `json:"latestVersionNumber"`
	ImportDataRules     []ImportDataRule     `json:"importDataRules"`
	ExpirationDataRules []ExpirationDataRule `json:"expirationDataRules"`
}

type tagResourceRequest struct {
	Tags []wireTag `json:"tags"`
}
