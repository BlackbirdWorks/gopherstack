package mwaa

import (
	"context"
	"fmt"
)

// validRestAPIMethods returns the set of HTTP methods accepted by InvokeRestApi's
// Method field (AWS: "Valid Values: GET | PUT | POST | PATCH | DELETE").
func validRestAPIMethods() map[string]struct{} {
	return map[string]struct{}{
		"GET":    {},
		"PUT":    {},
		"POST":   {},
		"PATCH":  {},
		"DELETE": {},
	}
}

// InvokeRestAPI simulates calling the Apache Airflow REST API on the specified
// environment's webserver. Like CreateCliToken/CreateWebLoginToken (the other
// two operations that reach the environment's Airflow webserver), the
// environment must be AVAILABLE: the webserver process doesn't exist yet while
// an environment is CREATING/UPDATING/etc, so AWS returns
// ResourceNotFoundException for any non-AVAILABLE state, not just a missing name.
func (b *InMemoryBackend) InvokeRestAPI(
	ctx context.Context,
	envName string,
	req *invokeRestAPIRequest,
) (*InvokeRestAPIResponse, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("InvokeRestAPI")
	defer b.mu.RUnlock()

	env, ok := b.environments.Get(regionKey(region, envName))
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	if env.Status != envStatusAvailable {
		return nil, ErrEnvironmentNotFound
	}

	if req.Method == "" {
		return nil, fmt.Errorf("%w: Method is required", ErrInvalidParameter)
	}

	if _, validMethod := validRestAPIMethods()[req.Method]; !validMethod {
		return nil, fmt.Errorf(
			"%w: Method must be one of GET/PUT/POST/PATCH/DELETE, got %q",
			ErrInvalidParameter, req.Method,
		)
	}

	if req.Path == "" {
		return nil, fmt.Errorf("%w: Path is required", ErrInvalidParameter)
	}

	return &InvokeRestAPIResponse{
		RestAPIStatusCode: restAPISuccessCode,
		RestAPIResponse:   map[string]any{},
	}, nil
}
