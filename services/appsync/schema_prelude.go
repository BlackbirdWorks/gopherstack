package appsync

import (
	"strings"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

const preludeAuthLocations = "OBJECT | FIELD_DEFINITION | INPUT_OBJECT | INTERFACE | UNION | ENUM"

// appSyncScalars are the scalars AppSync defines implicitly.
var appSyncScalars = []string{ //nolint:gochecknoglobals // immutable lookup
	"AWSDate", "AWSTime", "AWSDateTime", "AWSTimestamp", "AWSEmail",
	"AWSJSON", "AWSURL", "AWSPhone", "AWSIPAddress",
}

// appSyncDirectives are the directives AppSync defines implicitly.
var appSyncDirectives = map[string]string{ //nolint:gochecknoglobals // immutable lookup
	"aws_subscribe":          "(mutations: [String]) on FIELD_DEFINITION",
	"aws_publish":            "(subscriptions: [String]) on FIELD_DEFINITION",
	"aws_auth":               "(cognito_groups: [String]) on FIELD_DEFINITION",
	"aws_api_key":            "on " + preludeAuthLocations,
	"aws_iam":                "on " + preludeAuthLocations,
	"aws_oidc":               "on " + preludeAuthLocations,
	"aws_lambda":             "on " + preludeAuthLocations,
	"aws_cognito_user_pools": "(cognito_groups: [String]) on " + preludeAuthLocations,
}

// loadAppSyncSchema parses sdl with AppSync's implicit scalars and directives
// defined, skipping any the schema declares itself.
func loadAppSyncSchema(sdl string) (*ast.Schema, error) {
	user := &ast.Source{Name: "schema.graphql", Input: sdl}
	declared := map[string]bool{}

	if doc, err := parser.ParseSchema(user); err == nil {
		for _, d := range doc.Definitions {
			declared[d.Name] = true
		}

		for _, d := range doc.Directives {
			declared["@"+d.Name] = true
		}
	}

	var sb strings.Builder

	for _, s := range appSyncScalars {
		if !declared[s] {
			sb.WriteString("scalar " + s + "\n")
		}
	}

	for name, def := range appSyncDirectives {
		if !declared["@"+name] {
			sb.WriteString("directive @" + name + " " + def + "\n")
		}
	}

	prelude := &ast.Source{Name: "appsync_prelude.graphql", Input: sb.String(), BuiltIn: true}

	return gqlparser.LoadSchema(prelude, user)
}
