package apigateway

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// openAPISecurityScheme is a securityDefinitions / securitySchemes entry carrying
// the x-amazon-apigateway-authorizer extension.
type openAPISecurityScheme struct {
	Authorizer *openAPIAuthorizer `json:"x-amazon-apigateway-authorizer"`
	Name       string             `json:"name"`
	AuthType   string             `json:"x-amazon-apigateway-authtype"`
}

type openAPIAuthorizer struct {
	Type                         string   `json:"type"`
	AuthorizerURI                string   `json:"authorizerUri"`
	AuthorizerCredentials        string   `json:"authorizerCredentials"`
	IdentitySource               string   `json:"identitySource"`
	IdentityValidationExpression string   `json:"identityValidationExpression"`
	ProviderARNs                 []string `json:"providerARNs"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds"`
}

func (d *openAPIDoc) securitySchemes() map[string]openAPISecurityScheme {
	if len(d.SecurityDefinitions) > 0 {
		return d.SecurityDefinitions
	}
	if d.Components != nil {
		return d.Components.SecuritySchemes
	}

	return nil
}

// importAuthorizers creates an Authorizer for each security scheme that carries
// x-amazon-apigateway-authorizer, reusing a same-named existing one.
func importAuthorizers(b *InMemoryBackend, api *RestAPI, doc *openAPIDoc) map[string]*Authorizer {
	schemes := doc.securitySchemes()
	out := make(map[string]*Authorizer, len(schemes))
	for _, name := range collections.SortedKeys(schemes) {
		s := schemes[name]
		if s.Authorizer == nil {
			continue
		}
		if existing := findAuthorizerByName(b, api.ID, name); existing != nil {
			out[name] = existing

			continue
		}
		a := newImportedAuthorizer(api.ID, name, s)
		b.authorizers.Put(a)
		out[name] = a
	}

	return out
}

func findAuthorizerByName(b *InMemoryBackend, restAPIID, name string) *Authorizer {
	for _, a := range b.authorizers.All() {
		if a.RestAPIID == restAPIID && a.Name == name {
			return a
		}
	}

	return nil
}

func newImportedAuthorizer(restAPIID, name string, s openAPISecurityScheme) *Authorizer {
	x := s.Authorizer
	typ := strings.ToUpper(x.Type)
	if strings.EqualFold(x.Type, "cognito_user_pools") {
		typ = "COGNITO_USER_POOLS"
	}
	identity := x.IdentitySource
	if identity == "" && typ != "REQUEST" {
		header := s.Name
		if header == "" {
			header = "Authorization"
		}
		identity = "method.request.header." + header
	}

	return &Authorizer{
		ID:                           randomID(resourceIDLength),
		RestAPIID:                    restAPIID,
		Name:                         name,
		Type:                         typ,
		AuthorizerURI:                x.AuthorizerURI,
		AuthorizerCredentials:        x.AuthorizerCredentials,
		IdentitySource:               identity,
		IdentityValidationExpression: x.IdentityValidationExpression,
		AuthType:                     s.AuthType,
		AuthorizerResultTTLInSeconds: x.AuthorizerResultTTLInSeconds,
		ProviderARNs:                 x.ProviderARNs,
	}
}

// applyAuthorizerSecurity binds the method to the authorizer named in its security requirement.
func applyAuthorizerSecurity(method *Method, op *openAPIOperation, auths map[string]*Authorizer) {
	for _, req := range op.Security {
		for scheme := range req {
			a, ok := auths[scheme]
			if !ok {
				continue
			}
			method.AuthorizerID = a.ID
			method.AuthorizationType = AuthTypeCustom
			if a.Type == "COGNITO_USER_POOLS" {
				method.AuthorizationType = AuthTypeCognitoUserPool
			}
		}
	}
}
