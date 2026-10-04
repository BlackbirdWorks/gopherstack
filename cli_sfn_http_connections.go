package main

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// connectionAuthSource is the in-process EventBridge connection store's read-only credential lookup.
type connectionAuthSource interface {
	ResolveConnectionAuth(connectionARN string) (*ebbackend.ResolvedAPIDestination, error)
}

type sfnConnectionResolver struct {
	src connectionAuthSource
}

func (r sfnConnectionResolver) ResolveConnectionCredentials(
	_ context.Context, connectionARN string,
) (sfnbackend.ConnectionCredentials, error) {
	d, err := r.src.ResolveConnectionAuth(connectionARN)

	switch {
	case errors.Is(err, ebbackend.ErrNotFound):
		return sfnbackend.ConnectionCredentials{}, sfnbackend.ErrConnectionNotFound
	case errors.Is(err, ebbackend.ErrConnectionNotAuthorized):
		return sfnbackend.ConnectionCredentials{}, sfnbackend.ErrConnectionInvalidState
	case err != nil:
		return sfnbackend.ConnectionCredentials{}, err
	}

	c := sfnbackend.ConnectionCredentials{
		AuthType: d.AuthType, APIKeyName: d.APIKeyName, APIKeyValue: d.APIKeyValue,
		Username: d.BasicUsername, Password: d.BasicPassword,
		Headers: nameValues(d.HeaderParameters, func(p ebbackend.ConnectionHeaderParameter) (string, string) {
			return p.Key, p.Value
		}),
		Query: nameValues(d.QueryStringParameters, func(p ebbackend.ConnectionQueryStringParameter) (string, string) {
			return p.Key, p.Value
		}),
		Body: nameValues(d.BodyParameters, func(p ebbackend.ConnectionBodyParameter) (string, string) {
			return p.Key, p.Value
		}),
	}

	if o := d.OAuth; o != nil {
		c.OAuth = &sfnbackend.OAuthCredentials{
			Endpoint:     o.AuthorizationEndpoint,
			Method:       o.HTTPMethod,
			ClientID:     o.ClientID,
			ClientSecret: o.ClientSecret,
			Headers: nameValues(o.HeaderParameters, func(p ebbackend.ConnectionHeaderParameter) (string, string) {
				return p.Key, p.Value
			}),
			Query: nameValues(
				o.QueryStringParameters,
				func(p ebbackend.ConnectionQueryStringParameter) (string, string) {
					return p.Key, p.Value
				},
			),
			Body: nameValues(o.BodyParameters, func(p ebbackend.ConnectionBodyParameter) (string, string) {
				return p.Key, p.Value
			}),
		}
	}

	return c, nil
}

func nameValues[T any](in []T, kv func(T) (string, string)) []sfnbackend.NameValue {
	out := make([]sfnbackend.NameValue, len(in))

	for i, p := range in {
		out[i].Key, out[i].Value = kv(p)
	}

	return out
}

// sfnConnectionsFor returns the EventBridge-backed resolver, or nil when unavailable.
func sfnConnectionsFor(byName map[string]service.Registerable) sfnbackend.ConnectionResolver {
	h, ok := byName["EventBridge"].(*ebbackend.Handler)
	if !ok {
		return nil
	}

	src, ok := h.Backend.(connectionAuthSource)
	if !ok {
		return nil
	}

	return sfnConnectionResolver{src: src}
}
