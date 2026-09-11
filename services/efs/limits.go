package efs

import "fmt"

// Hard-coded resource caps enforced at the Create* operations whose real
// error catalog declares FileSystemLimitExceeded or AccessPointLimitExceeded
// -- confirmed per-op from aws-sdk-go@v1.55.8/models/apis/elasticfilesystem/
// 2015-02-01/api-2.json's operations[Op].errors (cross-checked against
// aws-sdk-go-v2/service/efs@v1.48.0/deserializers.go's own
// awsRestjson1_deserializeOpError<Op> switches):
//   - CreateFileSystem: FileSystemLimitExceeded (api-2.json operations.
//     CreateFileSystem.errors; deserializers.go:382-383)
//   - CreateReplicationConfiguration: FileSystemLimitExceeded (api-2.json
//     operations.CreateReplicationConfiguration.errors; deserializers.go:
//     1021-1022) -- creating a replication configuration implicitly creates
//     a destination file system, so it draws on the same per-account
//     file-system quota.
//   - CreateAccessPoint: AccessPointLimitExceeded (api-2.json operations.
//     CreateAccessPoint.errors; deserializers.go:133-134)
//
// Both exceptions carry httpStatusCode 403 in that same api-2.json (shapes.
// FileSystemLimitExceeded.error / shapes.AccessPointLimitExceeded.error) --
// unlike SecurityGroupLimitExceeded's 400, these are Service Quota "you're
// out of account allowance" 403s, matching gopherstack-ne9h's title.
//
// Values are the real, adjustable AWS defaults from
// https://docs.aws.amazon.com/efs/latest/ug/limits.html (WebFetch'd
// 2026-09-11), "Amazon EFS quotas that you can increase" table:
//   - "Number of file systems for each customer account in an AWS Region": 1,000
//   - "Number of access points for each file system": 10,000
//
// Every other exception the same class of ops declares but that efs leaves
// unenforced is deliberate, not an oversight -- see PARITY.md's gaps list:
// ThroughputLimitExceeded (CreateFileSystem/CreateReplicationConfiguration/
// UpdateFileSystem) overlaps the ProvisionedThroughputInMibps 1-1024
// structural bound already enforced as BadRequest in
// validateProvisionedThroughput/applyThroughputModeChange, and the current
// published per-file-system throughput quota is region-dependent (3-10
// GiBps), not the flat 1024 MiB/s the SDK doc string still cites --
// picking either number to also raise a second, distinct exception would be
// guessing, not citing. NetworkInterfaceLimitExceeded/NoFreeAddressesInSubnet/
// IpAddressInUse (CreateMountTarget) depend on real subnet CIDR occupancy and
// account-wide ENI counts, which are EC2/VPC quotas efs's own limits page
// never publishes a number for -- gopherstack has no subnet-IP-occupancy
// model to check them against without fabricating a threshold.
const (
	defaultMaxFileSystemsPerAccount  = 1000  // "Number of file systems for each customer account in an AWS Region"
	defaultMaxAccessPointsPerFileSys = 10000 // "Number of access points for each file system"
)

// resourceLimits holds the caps InMemoryBackend enforces, defaulted to the
// real EFS values above (defaultResourceLimits) and overridable via
// WithResourceLimits (store.go) -- the same constructor-option pattern
// services/ses/limits.go and services/glue/limits.go use -- so tests
// exercising FileSystemLimitExceeded/AccessPointLimitExceeded on a
// 1,000/10,000-sized cap don't have to create that many real resources.
type resourceLimits struct {
	fileSystemsPerAccount  int
	accessPointsPerFileSys int
}

func defaultResourceLimits() resourceLimits {
	return resourceLimits{
		fileSystemsPerAccount:  defaultMaxFileSystemsPerAccount,
		accessPointsPerFileSys: defaultMaxAccessPointsPerFileSys,
	}
}

// ResourceLimits overrides the resource caps a *InMemoryBackend enforces
// with FileSystemLimitExceeded/AccessPointLimitExceeded. A zero field keeps
// its real-EFS default (see WithResourceLimits in store.go) -- used by
// tests that need to trip a 1,000/10,000-sized cap without actually
// creating that many resources.
type ResourceLimits struct {
	FileSystemsPerAccount  int
	AccessPointsPerFileSys int
}

// applyResourceLimitOverrides copies every positive field of l onto rl,
// leaving fields left at zero in l unchanged.
func applyResourceLimitOverrides(rl *resourceLimits, l ResourceLimits) {
	if l.FileSystemsPerAccount > 0 {
		rl.fileSystemsPerAccount = l.FileSystemsPerAccount
	}

	if l.AccessPointsPerFileSys > 0 {
		rl.accessPointsPerFileSys = l.AccessPointsPerFileSys
	}
}

// fileSystemLimitExceededErr builds the FileSystemLimitExceeded wire error
// for a full per-account/region file-system quota.
func fileSystemLimitExceededErr(limit int) error {
	return fmt.Errorf("%w: account has reached the maximum of %d file systems", ErrFileSystemLimitExceeded, limit)
}

// accessPointLimitExceededErr builds the AccessPointLimitExceeded wire error
// for a full per-file-system access-point quota.
func accessPointLimitExceededErr(fileSystemID string, limit int) error {
	return fmt.Errorf(
		"%w: file system %s has reached the maximum of %d access points",
		ErrAccessPointLimitExceeded, fileSystemID, limit,
	)
}
