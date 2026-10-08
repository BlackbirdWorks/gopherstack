package s3

import (
	"slices"
)

// Object Lambda transformation actions (s3control ObjectLambdaTransformationConfigurationAction).
const (
	objectLambdaActionGetObject     = "GetObject"
	objectLambdaActionHeadObject    = "HeadObject"
	objectLambdaActionListObjects   = "ListObjects"
	objectLambdaActionListObjectsV2 = "ListObjectsV2"
)

// objectLambdaRoute is where a request addressed to an Object Lambda label goes.
type objectLambdaRoute struct {
	ap        *StoredObjectLambdaAccessPoint
	bucket    string
	lambdaARN string
}

// handles reports whether the route invokes the Lambda for action. A bucket-level
// config, or an access point that stored no actions, covers GetObject only.
func (r objectLambdaRoute) handles(action string) bool {
	if r.lambdaARN == "" {
		return false
	}

	if r.ap == nil || len(r.ap.Actions) == 0 {
		return action == objectLambdaActionGetObject
	}

	return slices.Contains(r.ap.Actions, action)
}
