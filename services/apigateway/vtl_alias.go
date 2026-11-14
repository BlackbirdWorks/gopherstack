package apigateway

import "github.com/blackbirdworks/gopherstack/pkgs/vtl"

// VTLContext is the mapping-template context, shared via pkgs/vtl.
type VTLContext = vtl.Context

// RenderTemplate renders a Velocity template, returning it unchanged when it does not parse.
func RenderTemplate(tmpl string, ctx VTLContext) string { return vtl.Render(tmpl, ctx) }

// RenderTemplateStrict renders a Velocity template, returning an error when it does not parse.
func RenderTemplateStrict(tmpl string, ctx VTLContext) (string, error) {
	return vtl.RenderStrict(tmpl, ctx)
}

const jsonNull = "null"

func parseOrderedJSON(s string) (any, error) { return vtl.ParseOrderedJSON(s) }

func javaString(v any) string { return vtl.JavaString(v) }

func evalJSONPath(root any, path string) any { return vtl.EvalJSONPath(root, path) }
