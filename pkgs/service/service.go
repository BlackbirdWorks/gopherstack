package service

import "net/http"

// Handler represents an AWS service mock handler that can serve HTTP
// requests and report which operations it supports.
type Handler interface {
	http.Handler
	GetSupportedOperations() []string
}

// Matcher determines whether an incoming request should be routed
// to a particular service handler.
type Matcher func(r *http.Request) bool

// Registration binds a service handler to its routing logic.
// A nil Match func designates the fallback/default handler.
type Registration struct {
	Match   Matcher
	Handler Handler
	Name    string
}
