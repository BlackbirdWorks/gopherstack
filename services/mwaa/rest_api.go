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
	e, base, version, err := b.restTarget(ctx, envName)
	if err != nil {
		return nil, err
	}

	if err = validateInvokeRequest(req); err != nil {
		return nil, err
	}

	if e != nil {
		return e.proxyRest(ctx, base, version, req)
	}

	return &InvokeRestAPIResponse{
		RestAPIStatusCode: restAPISuccessCode,
		RestAPIResponse:   map[string]any{},
	}, nil
}

// restTarget checks the environment is AVAILABLE; the engine is non-nil when a real Airflow serves it.
func (b *InMemoryBackend) restTarget(ctx context.Context, envName string) (*airflowEngine, string, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("InvokeRestAPI")
	defer b.mu.RUnlock()

	env, ok := b.environments.Get(regionKey(region, envName))
	if !ok || env.Status != envStatusAvailable {
		return nil, "", "", ErrEnvironmentNotFound
	}

	e, base, version, managed := b.restTargetLocked(regionKey(region, envName))
	if !managed {
		return nil, "", "", nil
	}

	return e, base, version, nil
}

func validateInvokeRequest(req *invokeRestAPIRequest) error {
	if req.Method == "" {
		return fmt.Errorf("%w: Method is required", ErrInvalidParameter)
	}

	if _, validMethod := validRestAPIMethods()[req.Method]; !validMethod {
		return fmt.Errorf(
			"%w: Method must be one of GET/PUT/POST/PATCH/DELETE, got %q",
			ErrInvalidParameter, req.Method,
		)
	}

	if req.Path == "" {
		return fmt.Errorf("%w: Path is required", ErrInvalidParameter)
	}

	return nil
}
