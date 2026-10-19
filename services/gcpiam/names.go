package gcpiam

import (
	"regexp"
	"strings"
)

const (
	rootProjects     = "projects"
	rootOrgs         = "organizations"
	wildcard         = "-"
	partsAccount     = 4
	partsKey         = 6
	partsRole        = 4
	maxDisplayName   = 100
	maxDescription   = 256
	roleIDMin        = 3
	roleIDMax        = 64
	accountDomain    = "iam.gserviceaccount.com"
	accountIDPattern = `^[a-z]([-a-z0-9]*[a-z0-9])$`
	accountIDMin     = 6
	accountIDMax     = 30
)

var (
	accountIDRe = regexp.MustCompile(accountIDPattern)
	roleIDRe    = regexp.MustCompile(`^[a-zA-Z0-9_.]+$`)
	permRe      = regexp.MustCompile(`^[a-z][a-zA-Z0-9]*(\.[a-zA-Z0-9]+){2,}$`)
)

func accountEmail(accountID, project string) string {
	return accountID + "@" + project + "." + accountDomain
}

func accountName(project, email string) string {
	return "projects/" + project + "/serviceAccounts/" + email
}

// parseProject extracts the project from "projects/{p}".
func parseProject(parent string) (string, error) {
	parts := strings.Split(parent, "/")
	if len(parts) != 2 || parts[0] != rootProjects || parts[1] == "" || parts[1] == wildcard {
		return "", errInvalid("Invalid project resource name %q; expected projects/{PROJECT_ID}.", parent)
	}

	return parts[1], nil
}

// parseAccountName splits "projects/{p|-}/serviceAccounts/{email|uniqueId}".
func parseAccountName(name string) (string, string, error) {
	parts := strings.Split(name, "/")
	if len(parts) != partsAccount || parts[0] != rootProjects || parts[2] != "serviceAccounts" ||
		parts[1] == "" || parts[3] == "" {
		return "", "", errInvalid(
			"Invalid service account name %q; expected projects/{PROJECT_ID}/serviceAccounts/{ACCOUNT}.",
			name,
		)
	}

	return parts[1], parts[3], nil
}

// parseKeyName splits ".../serviceAccounts/{account}/keys/{keyId}" into the account name and key id.
func parseKeyName(name string) (string, string, error) {
	parts := strings.Split(name, "/")
	if len(parts) != partsKey || parts[4] != "keys" || parts[5] == "" {
		return "", "", errInvalid("Invalid service account key name %q; expected "+
			"projects/{PROJECT_ID}/serviceAccounts/{ACCOUNT}/keys/{KEY}.", name)
	}

	return strings.Join(parts[:partsAccount], "/"), parts[5], nil
}

// validateCustomRoleName checks a custom role name: projects/{p}/roles/{id} or organizations/{o}/roles/{id}.
func validateCustomRoleName(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != partsRole || parts[2] != "roles" || parts[1] == "" || parts[3] == "" ||
		(parts[0] != rootProjects && parts[0] != rootOrgs) || parts[1] == "*" {
		return errInvalid("Invalid custom role name %q.", name)
	}

	return nil
}

func parseRoleParent(parent string) error {
	parts := strings.Split(parent, "/")
	if len(parts) != 2 || (parts[0] != rootProjects && parts[0] != rootOrgs) || parts[1] == "" || parts[1] == "*" {
		return errInvalid(
			"Invalid parent %q; expected projects/{PROJECT_ID} or organizations/{ORGANIZATION_ID}.",
			parent,
		)
	}

	return nil
}
