package elbv2

// EC2Resolver lets this backend validate SecurityGroups/Subnets passed to
// CreateLoadBalancer against the real services/ec2 backend, mirroring
// services/elb's EC2Resolver. Wired in by cli.go's wireELBv2CrossService. A
// nil resolver (the default) accepts every security-group/subnet id
// unvalidated -- e.g. isolated unit tests with no EC2 backend wired.
type EC2Resolver interface {
	SecurityGroupExists(id string) bool
	SubnetExists(id string) bool
}

// CertificateResolver lets this backend validate a listener's
// CertificateArn against the real services/acm and services/iam backends,
// and report attach/detach so ACM's InUseBy tracking -- and therefore
// DeleteCertificate's ResourceInUseException guard -- becomes reachable.
// Wired in by cli.go's wireELBv2CrossService. A nil resolver (the default)
// accepts every CertificateArn unvalidated and reports no usage.
type CertificateResolver interface {
	ResolveCertificate(certARN string) bool
	AddInUseBy(certARN, resourceARN string)
	RemoveInUseBy(certARN, resourceARN string)
}

// InstanceStateResolver is the optional EC2Resolver extension DescribeTargetHealth uses to
// report targets on stopped or terminated instances as unused (Target.InvalidState).
type InstanceStateResolver interface {
	InstanceStateName(id string) (string, bool)
}

// RegionalSubnetPlacementResolver resolves a subnet within the load balancer's own region; subnet IDs such as
// subnet-default exist in every region, so a region-blind lookup can answer for the wrong one.
type RegionalSubnetPlacementResolver interface {
	SubnetVPCIn(region, id string) string
	SubnetAZIn(region, id string) string
}

// SubnetPlacementResolver is the optional EC2Resolver extension CreateLoadBalancer uses to
// report a subnet's real VPC and Availability Zone instead of placeholders.
type SubnetPlacementResolver interface {
	SubnetVPC(id string) string
	SubnetAZ(id string) string
}

// CertificateKeyPairResolver is the optional CertificateResolver extension real listeners use to
// terminate TLS: it returns the PEM certificate (with chain) and private key behind a certificate ARN.
type CertificateKeyPairResolver interface {
	CertificateKeyPair(certARN string) (certPEM, keyPEM []byte, ok bool)
}
