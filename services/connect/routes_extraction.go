package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func extractionRoutes() []restroute.Route {
	const (
		list = "/extraction-definitions/{InstanceId}"
		one  = list + "/{ExtractionDefinitionId}"
		prof = "/authentication-profiles/{InstanceId}/{AuthenticationProfileId}"
	)

	return []restroute.Route{
		{Method: http.MethodPost, Path: list, Op: "CreateExtractionDefinition"},
		{Method: http.MethodGet, Path: list, Op: "ListExtractionDefinitions"},
		{Method: http.MethodGet, Path: one, Op: "DescribeExtractionDefinition"},
		{Method: http.MethodPut, Path: one, Op: "UpdateExtractionDefinition"},
		{Method: http.MethodDelete, Path: one, Op: "DeleteExtractionDefinition"},
		{
			Method: http.MethodGet,
			Path:   "/authentication-profiles-summary/{InstanceId}",
			Op:     "ListAuthenticationProfiles",
		},
		{Method: http.MethodGet, Path: prof, Op: "DescribeAuthenticationProfile"},
		{Method: http.MethodPost, Path: prof, Op: "UpdateAuthenticationProfile"},
		{Method: http.MethodGet, Path: "/contact/associated/{InstanceId}", Op: "ListAssociatedContacts"},
		{Method: http.MethodGet, Path: "/contact/references/{InstanceId}/{ContactId}", Op: "ListContactReferences"},
		{Method: http.MethodPost, Path: "/contact/schedule", Op: "UpdateContactSchedule"},
		{Method: http.MethodPost, Path: "/contact/delete/{InstanceId}/{ContactId}", Op: "DeleteContactData"},
	}
}
