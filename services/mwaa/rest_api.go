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

// InvokeRestAPI simulates calling the Apache Airflow REST API on the specified environment's webserver.
func (b *InMemoryBackend) InvokeRestAPI(
	ctx context.Context,
	envName string,
	req *invokeRestAPIRequest,
) (*InvokeRestAPIResponse, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("InvokeRestAPI")
	defer b.mu.RUnlock()

	if !b.environments.Has(regionKey(region, envName)) {
		return nil, ErrEnvironmentNotFound
	}

	if req.Method == "" {
		return nil, fmt.Errorf("%w: Method is required", ErrInvalidParameter)
	}

	if _, ok := validRestAPIMethods()[req.Method]; !ok {
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
