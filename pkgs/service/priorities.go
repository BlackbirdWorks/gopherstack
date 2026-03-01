package service

// Routing priority constants control the order in which service matchers are evaluated.
// Higher values are evaluated first. Services are grouped into tiers:
//
//   - 100 (HeaderExact):   Services matched by an exact X-Amz-Target prefix.
//     These are the most specific and should always win.
//   - 95 (HeaderPartial):  Services matched by a looser header prefix check
//     (e.g. Lambda by path + method, KMS/SecretsManager by target prefix).
//   - 90 (FormEncoded):    STS – form-encoded POST to the root path.
//   - 85 (PathVersioned):  Services matched by a versioned path prefix (S3Control).
//   - 84 (FormRDS):        RDS – form-encoded, versioned query protocol.
//   - 83 (FormRedshift):   Redshift – form-encoded, version 2012-12-01.
//   - 82 (PathSubdomain):  OpenSearch / ElastiCache – path-prefix matchers that
//     could overlap with form-encoded services.
//   - 80 (FormStandard):   EC2, IAM, SES – standard form-encoded query protocol.
//   - 75 (TargetPrefixed): Kinesis – X-Amz-Target with a versioned prefix.
//   - 50 (PathUI):         Dashboard – path-based UI routes.
//   - 0  (CatchAll):       S3 – low-priority catch-all Host-header matcher.
const (
	// PriorityHeaderExact is for services matched by an exact X-Amz-Target prefix.
	PriorityHeaderExact = 100

	// PriorityHeaderPartial is for services matched by a partial header prefix (Lambda, KMS, SecretsManager).
	PriorityHeaderPartial = 95

	// PriorityFormEncoded is for STS form-encoded POST requests.
	PriorityFormEncoded = 90

	// PriorityPathVersioned is for services matched by a versioned path prefix (S3Control).
	PriorityPathVersioned = 85

	// PriorityFormRDS is for RDS form-encoded query protocol (version 2014-10-31).
	PriorityFormRDS = 84

	// PriorityFormRedshift is for Redshift form-encoded query protocol (version 2012-12-01).
	PriorityFormRedshift = 83

	// PriorityPathSubdomain is for OpenSearch and ElastiCache path-prefix matchers.
	PriorityPathSubdomain = 82

	// PriorityFormStandard is for EC2, IAM, and SES standard form-encoded query protocol.
	PriorityFormStandard = 80

	// PriorityTargetPrefixed is for Kinesis with its versioned X-Amz-Target prefix.
	PriorityTargetPrefixed = 75

	// PriorityPathUI is for the Dashboard UI path-based routes.
	PriorityPathUI = 50

	// PriorityCatchAll is for S3, which uses a low-priority Host-header catch-all.
	PriorityCatchAll = 0
)
