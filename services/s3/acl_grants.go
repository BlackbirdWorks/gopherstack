package s3

import (
	"encoding/xml"
	"net/http"
	"regexp"
)

// Canned-ACL → grant-list expansion. GetBucketAcl previously always returned a
// single hardcoded owner FULL_CONTROL grant, ignoring the stored canned ACL, so
// a public-read bucket looked identical to a private one. cannedACLGrants maps a
// canned ACL name to the concrete grant list AWS materialises for it, so the ACL
// response reflects the stored configuration.

const (
	// S3 predefined group URIs.
	uriAllUsers           = "http://acs.amazonaws.com/groups/global/AllUsers"
	uriAuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
	uriLogDelivery        = "http://acs.amazonaws.com/groups/s3/LogDelivery"

	permFullControl = "FULL_CONTROL"
	permRead        = "READ"
	permWrite       = "WRITE"
	permReadACP     = "READ_ACP"
	permWriteACP    = "WRITE_ACP"

	xsiTypeGroup                 = "Group"
	xsiTypeCanonicalUser         = "CanonicalUser"
	xsiTypeAmazonCustomerByEmail = "AmazonCustomerByEmail"
	xmlNamespaceXSI              = "http://www.w3.org/2001/XMLSchema-instance"
)

// granteeSpecRe matches one id=/uri=/emailAddress= grantee spec inside an
// x-amz-grant-* header value (s3@v1.111.0 serializers.go: GrantFullControl,
// GrantRead, GrantReadACP, GrantWrite, GrantWriteACP), e.g.
// `id="1234", uri="http://acs.amazonaws.com/groups/global/AllUsers"`.
var granteeSpecRe = regexp.MustCompile(`(id|uri|emailAddress)="([^"]*)"`)

// granteesFromHeaderValue parses one x-amz-grant-* header value into the
// Grantee(s) it names.
func granteesFromHeaderValue(v string) []Grantee {
	matches := granteeSpecRe.FindAllStringSubmatch(v, -1)

	grantees := make([]Grantee, 0, len(matches))
	for _, m := range matches {
		switch m[1] {
		case "id":
			grantees = append(grantees, Grantee{
				XmlnsXsi: xmlNamespaceXSI, XsiType: xsiTypeCanonicalUser, ID: m[2],
			})
		case "uri":
			grantees = append(grantees, Grantee{
				XmlnsXsi: xmlNamespaceXSI, XsiType: xsiTypeGroup, URI: m[2],
			})
		case "emailAddress":
			grantees = append(grantees, Grantee{
				XmlnsXsi: xmlNamespaceXSI, XsiType: xsiTypeAmazonCustomerByEmail, EmailAddress: m[2],
			})
		}
	}

	return grantees
}

// grantHeaderSpecs maps each x-amz-grant-* header to the permission it grants.
// includeWrite controls whether X-Amz-Grant-Write is checked: real S3 declares
// it on bucket-ACL-setting ops (PutBucketAcl, CreateBucket) but not on
// object-ACL-setting ops (PutObjectAcl, CreateMultipartUpload) -- confirmed
// against s3@v1.111.0 serializers.go's per-op HttpBindings functions.
func grantHeaderSpecs(includeWrite bool) []struct {
	header string
	perm   string
} {
	specs := []struct {
		header string
		perm   string
	}{
		{"X-Amz-Grant-Full-Control", permFullControl},
		{"X-Amz-Grant-Read", permRead},
		{"X-Amz-Grant-Read-Acp", permReadACP},
		{"X-Amz-Grant-Write-Acp", permWriteACP},
	}
	if includeWrite {
		specs = append(specs, struct {
			header string
			perm   string
		}{"X-Amz-Grant-Write", permWrite})
	}

	return specs
}

// grantsFromHeaders builds the grant list AWS synthesizes from x-amz-grant-*
// request headers, prepending the owner's own FULL_CONTROL grant (same shape
// cannedACLGrants returns). Returns nil when no grant header is present.
func grantsFromHeaders(h http.Header, ownerID, ownerName string, includeWrite bool) []Grant {
	var grants []Grant

	for _, spec := range grantHeaderSpecs(includeWrite) {
		v := h.Get(spec.header)
		if v == "" {
			continue
		}

		for _, grantee := range granteesFromHeaderValue(v) {
			grants = append(grants, Grant{Grantee: grantee, Permission: spec.perm})
		}
	}

	if grants == nil {
		return nil
	}

	return append([]Grant{ownerGrant(ownerID, ownerName)}, grants...)
}

// aclXMLFromGrantHeaders synthesizes the AccessControlPolicy XML body real S3
// builds from x-amz-grant-* headers, for storage via the same
// "stored ACL is either a canned name or raw XML" convention PutBucketAcl/
// PutObjectAcl already use. Returns "" when no grant header is present.
func aclXMLFromGrantHeaders(h http.Header, ownerID, ownerName string, includeWrite bool) string {
	grants := grantsFromHeaders(h, ownerID, ownerName, includeWrite)
	if grants == nil {
		return ""
	}

	acp := AccessControlPolicy{
		Xmlns: xmlNamespaceS3,
		Owner: Owner{ID: ownerID, DisplayName: ownerName},
		ACL:   AccessControlList{Grants: grants},
	}

	out, err := xml.Marshal(acp)
	if err != nil {
		return ""
	}

	return string(out)
}

// ownerGrant returns the always-present owner FULL_CONTROL grant.
func ownerGrant(ownerID, ownerName string) Grant {
	return Grant{
		Grantee: Grantee{
			XmlnsXsi:    xmlNamespaceXSI,
			XsiType:     xsiTypeCanonicalUser,
			ID:          ownerID,
			DisplayName: ownerName,
		},
		Permission: permFullControl,
	}
}

// groupGrant builds a grant to one of the predefined S3 groups.
func groupGrant(uri, permission string) Grant {
	return Grant{
		Grantee: Grantee{
			XmlnsXsi: xmlNamespaceXSI,
			XsiType:  xsiTypeGroup,
			URI:      uri,
		},
		Permission: permission,
	}
}

// cannedACLGrants expands a canned ACL name into the grant list S3 returns for
// it, always beginning with the owner's FULL_CONTROL grant. An unknown or empty
// canned name is treated as "private".
func cannedACLGrants(canned, ownerID, ownerName string) []Grant {
	grants := []Grant{ownerGrant(ownerID, ownerName)}

	switch canned {
	case aclPublicRead:
		grants = append(grants, groupGrant(uriAllUsers, permRead))
	case aclPublicReadWrite:
		grants = append(grants,
			groupGrant(uriAllUsers, permRead),
			groupGrant(uriAllUsers, permWrite))
	case aclAuthenticatedRead:
		grants = append(grants, groupGrant(uriAuthenticatedUsers, permRead))
	case aclLogDeliveryWrite:
		grants = append(grants,
			groupGrant(uriLogDelivery, permWrite),
			groupGrant(uriLogDelivery, permReadACP))
	}

	return grants
}
