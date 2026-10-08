package redshift

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	svcIntLakeFormation  = "LakeFormation"
	svcIntRedshift       = "Redshift"
	svcIntS3AccessGrants = "S3AccessGrants"

	authEnabled  = "Enabled"
	authDisabled = "Disabled"
)

// serviceIntegrationScopes lists each ServiceIntegrationsUnion member with its only scope member.
func serviceIntegrationScopes() [][2]string {
	return [][2]string{
		{svcIntLakeFormation, "LakeFormationQuery"},
		{svcIntRedshift, "Connect"},
		{svcIntS3AccessGrants, "ReadWriteAccess"},
	}
}

// ServiceIntegration is one member of a RedshiftIdcApplication ServiceIntegrations union.
type ServiceIntegration struct {
	Service        string   `json:"service"`
	Scope          string   `json:"scope"`
	Authorizations []string `json:"authorizations"`
}

// parseServiceIntegrations reads ServiceIntegrations.member.N.<Service>.member.M.<Scope>.Authorization.
func parseServiceIntegrations(vals url.Values) ([]ServiceIntegration, error) {
	var out []ServiceIntegration

	for i := 1; i <= maxListItems; i++ {
		prefix := fmt.Sprintf("ServiceIntegrations.member.%d.", i)

		si, err := parseServiceIntegrationMember(vals, prefix)
		if err != nil {
			return nil, err
		}

		if si == nil {
			return out, nil
		}

		out = append(out, *si)
	}

	return out, nil
}

// parseServiceIntegrationMember reads one union member, or nil when the index is absent.
func parseServiceIntegrationMember(vals url.Values, prefix string) (*ServiceIntegration, error) {
	for _, pair := range serviceIntegrationScopes() {
		svc, scope := pair[0], pair[1]

		auths, err := parseScopeAuthorizations(vals, prefix+svc+".member.", svc, scope)
		if err != nil {
			return nil, err
		}

		if len(auths) > 0 {
			return &ServiceIntegration{Service: svc, Scope: scope, Authorizations: auths}, nil
		}
	}

	return nil, nil //nolint:nilnil // nil member marks the end of the list
}

func parseScopeAuthorizations(vals url.Values, prefix, svc, scope string) ([]string, error) {
	var auths []string

	for j := 1; j <= maxListItems; j++ {
		memberPrefix := fmt.Sprintf("%s%d.", prefix, j)
		auth := vals.Get(memberPrefix + scope + ".Authorization")

		if auth == "" {
			if hasPrefixedKey(vals, memberPrefix) {
				return nil, fmt.Errorf("%w: %s.%s.Authorization is required", ErrInvalidParameter, svc, scope)
			}

			return auths, nil
		}

		if auth != authEnabled && auth != authDisabled {
			return nil, fmt.Errorf("%w: Authorization must be %s or %s", ErrInvalidParameter, authEnabled, authDisabled)
		}

		auths = append(auths, auth)
	}

	return auths, nil
}

func hasPrefixedKey(vals url.Values, prefix string) bool {
	for k := range vals {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}

	return false
}

type xmlServiceIntegrationAuth struct {
	Authorization string `xml:"Authorization"`
}

type xmlServiceIntegrationScope struct {
	LakeFormationQuery *xmlServiceIntegrationAuth `xml:"LakeFormationQuery,omitempty"`
	Connect            *xmlServiceIntegrationAuth `xml:"Connect,omitempty"`
	ReadWriteAccess    *xmlServiceIntegrationAuth `xml:"ReadWriteAccess,omitempty"`
}

type xmlServiceIntegrationScopes struct {
	Members []xmlServiceIntegrationScope `xml:"member"`
}

type xmlServiceIntegration struct {
	LakeFormation  *xmlServiceIntegrationScopes `xml:"LakeFormation,omitempty"`
	Redshift       *xmlServiceIntegrationScopes `xml:"Redshift,omitempty"`
	S3AccessGrants *xmlServiceIntegrationScopes `xml:"S3AccessGrants,omitempty"`
}

func serviceIntegrationsToXML(in []ServiceIntegration) []xmlServiceIntegration {
	out := make([]xmlServiceIntegration, 0, len(in))

	for _, si := range in {
		scopes := &xmlServiceIntegrationScopes{}

		for _, auth := range si.Authorizations {
			a := &xmlServiceIntegrationAuth{Authorization: auth}

			var m xmlServiceIntegrationScope

			switch si.Service {
			case svcIntLakeFormation:
				m.LakeFormationQuery = a
			case svcIntRedshift:
				m.Connect = a
			default:
				m.ReadWriteAccess = a
			}

			scopes.Members = append(scopes.Members, m)
		}

		var x xmlServiceIntegration

		switch si.Service {
		case svcIntLakeFormation:
			x.LakeFormation = scopes
		case svcIntRedshift:
			x.Redshift = scopes
		default:
			x.S3AccessGrants = scopes
		}

		out = append(out, x)
	}

	return out
}
