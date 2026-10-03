package s3

import (
	"context"
	"encoding/xml"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const errMsgUnableToValidate = "Unable to validate the following destination configurations"

// SetNotificationAuthorizer makes PutBucketNotificationConfiguration reject destinations whose
// resource policy does not allow s3.amazonaws.com from the bucket.
func (h *S3Handler) SetNotificationAuthorizer(a roleauth.Authorizer) {
	h.notificationMu.Lock()
	defer h.notificationMu.Unlock()

	h.notifyAuth = a
}

// notificationDestinationsAllowed reports whether every queue, topic and function destination
// in body accepts events from the bucket.
func (h *S3Handler) notificationDestinationsAllowed(bucket string, body []byte) bool {
	h.notificationMu.RLock()
	auth := h.notifyAuth
	h.notificationMu.RUnlock()

	if auth == nil {
		return true
	}

	var cfg notificationConfiguration
	if xml.Unmarshal(body, &cfg) != nil {
		return true
	}

	var dests []string
	for _, q := range cfg.QueueConfigurations {
		dests = append(dests, q.Queue)
	}

	for _, t := range cfg.TopicConfigurations {
		dests = append(dests, t.Topic)
	}

	for _, l := range cfg.LambdaConfigurations {
		dests = append(dests, l.CloudFunc)
	}

	source := "arn:aws:s3:::" + bucket

	for _, dest := range dests {
		action, ok := roleauth.ResourcePolicyAction(dest)
		if ok && roleauth.AuthorizeResource(auth, roleauth.PrincipalS3, action, dest, source) != nil {
			return false
		}
	}

	return true
}

func writeUnableToValidate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	httputils.WriteS3ErrorResponse(ctx, w, r,
		ErrorResponse{Code: errInvalidArgument, Message: errMsgUnableToValidate}, http.StatusBadRequest)
}
