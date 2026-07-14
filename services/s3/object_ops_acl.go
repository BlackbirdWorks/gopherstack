package s3

import (
	"context"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// getObjectACL returns a minimal owner-full-control ACL for the requested object.
// Object ACLs are not enforced in this mock implementation; all objects are owned
// by the mock account and grant full control to the owner only.
func (h *S3Handler) getObjectACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObjectAcl")

	// Verify the object exists before returning an ACL.
	versionID := r.URL.Query().Get("versionId")

	// If a stored ACL exists for the version, return it verbatim. AWS returns
	// the persisted XML; if a canned ACL was set we still need to synthesise
	// XML below.
	stored, aclErr := h.Backend.GetObjectACL(ctx, bucketName, key, versionID)
	if aclErr != nil {
		WriteError(ctx, w, r, aclErr)

		return
	}

	if strings.HasPrefix(strings.TrimSpace(stored), "<") {
		// Stored value looks like XML — pass through. The body originated from
		// the operator that called PutObjectAcl earlier; gosec G705 is fine
		// here since the destination is an XML response on a trusted endpoint.
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(stored)) //nolint:gosec // pre-stored XML body is trusted

		return
	}

	const ownerID = "gopherstack-mock-owner"

	acp := AccessControlPolicy{
		Xmlns: xmlNamespaceS3,
		Owner: Owner{ID: ownerID, DisplayName: gopherstackName},
		ACL: AccessControlList{
			Grants: []Grant{
				{
					Grantee: Grantee{
						XmlnsXsi: "http://www.w3.org/2001/XMLSchema-instance",
						XsiType:  "CanonicalUser",
						ID:       ownerID,
					},
					Permission: "FULL_CONTROL",
				},
			},
		},
	}

	httputils.WriteXML(ctx, w, http.StatusOK, acp)
}

// putObjectACL persists the ACL (canned via x-amz-acl header or full XML body)
// against the targeted object version.
func (h *S3Handler) putObjectACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectAcl")

	versionID := r.URL.Query().Get("versionId")

	// Caller can use either a canned ACL header or supply an XML body. Persist
	// whichever we received; on read we'll synthesise default XML when empty.
	canned := r.Header.Get("X-Amz-Acl")

	body, _ := httputils.ReadBody(r)

	acl := canned
	if len(body) > 0 {
		acl = string(body)
	}

	if err := h.enforceACLPolicy(ctx, bucketName, canned, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if err := h.Backend.PutObjectACL(ctx, bucketName, key, versionID, acl); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}
