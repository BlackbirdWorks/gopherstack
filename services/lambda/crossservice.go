package lambda

// ECRResolver lets this backend validate a Code.ImageUri against the real
// services/ecr backend at CreateFunction/UpdateFunctionCode time. Real AWS
// rejects an Image package-type function whose image does not exist in ECR
// immediately, at create/update time, with InvalidParameterValueException
// ("Source image <uri> does not exist. Provide a valid source image.") --
// not only later at pull time. Wired in by cli.go's wireLambdaECR. A nil
// resolver (the default) accepts every ImageUri unvalidated, matching
// services/networkmanager's EC2Resolver pattern for isolated unit tests
// that never wire a cross-service backend.
type ECRResolver interface {
	ResolveImage(imageURI string) bool
}

// EFSMount is the host directory backing an EFS access point, plus its POSIX identity.
type EFSMount struct {
	HostDir string
	UID     int64
	GID     int64
}

// EFSResolver lets this backend validate FileSystemConfigs against services/efs
// and obtain the host directory to bind-mount. Wired by lambda_efs.go; the
// EFS backend is resolved at call time.
type EFSResolver interface {
	// CheckAccessPoint verifies the access point exists and its file system has
	// a mount target in a VPC reachable from subnetIDs.
	CheckAccessPoint(accessPointARN string, subnetIDs []string) error
	ResolveMount(accessPointARN string) (EFSMount, error)
}

// FileSystemConfigChecker is implemented by backends that can validate
// FileSystemConfigs against EFS.
type FileSystemConfigChecker interface {
	CheckFileSystemConfigs(cfgs []*FileSystemConfig, subnetIDs []string) error
}
