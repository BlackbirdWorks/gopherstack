package s3

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

	xsiTypeGroup         = "Group"
	xsiTypeCanonicalUser = "CanonicalUser"
	xmlNamespaceXSI      = "http://www.w3.org/2001/XMLSchema-instance"
)

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
