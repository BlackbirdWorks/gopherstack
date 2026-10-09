package s3

import (
	"net/http"
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

// allowsRangeRequest reports whether r may carry Range or partNumber to action: without the matching
// AllowedFeatures entry S3 answers 501 NotImplemented (userguide range-get-olap).
func (r objectLambdaRoute) allowsRangeRequest(req *http.Request, action string) bool {
	if action != objectLambdaActionGetObject && action != objectLambdaActionHeadObject {
		return true
	}

	prefix := "GetObject"
	if action == objectLambdaActionHeadObject {
		prefix = "HeadObject"
	}

	q := req.URL.Query()
	if (req.Header.Get("Range") != "" || q.Has("Range")) && !r.allowedFeature(prefix+"-Range") {
		return false
	}

	return !q.Has("partNumber") || r.allowedFeature(prefix+"-PartNumber")
}

func (r objectLambdaRoute) allowedFeature(feature string) bool {
	return r.ap != nil && slices.Contains(r.ap.AllowedFeatures, feature)
}
