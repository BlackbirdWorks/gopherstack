package app

import (
	"context"
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

var errELBv2LambdaUnavailable = errors.New("lambda service is not registered")

// elbv2LambdaInvoker resolves the Lambda backend at call time so wiring order does not matter.
type elbv2LambdaInvoker struct {
	byName map[string]service.Registerable
}

func (a elbv2LambdaInvoker) InvokeFunction(
	ctx context.Context,
	functionARN string,
	payload []byte,
) ([]byte, string, error) {
	h, ok := a.byName["Lambda"].(*lambdabackend.Handler)
	if !ok {
		return nil, "", errELBv2LambdaUnavailable
	}

	bk, ok := h.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil, "", errELBv2LambdaUnavailable
	}

	name, qualifier := splitFunctionARN(functionARN)

	result, _, fnErr, _, err := bk.InvokeFunctionWithQualifier(
		ctx, name, qualifier, "", "", lambdabackend.InvocationTypeRequestResponse, payload,
	)

	return result, fnErr, err
}

// splitFunctionARN separates a trailing version/alias qualifier from a function ARN.
func splitFunctionARN(arn string) (string, string) {
	_, after, found := strings.Cut(arn, ":function:")
	if !found {
		return arn, ""
	}

	_, qualifier, hasQualifier := strings.Cut(after, ":")
	if !hasQualifier {
		return arn, ""
	}

	return strings.TrimSuffix(arn, ":"+qualifier), qualifier
}

// elbv2InstanceAddresses reports the container IP of docker-launched EC2 instances.
type elbv2InstanceAddresses struct {
	byName map[string]service.Registerable
}

func (a elbv2InstanceAddresses) InstanceAddress(id string) (string, bool) {
	h, ok := a.byName["EC2"].(*ec2backend.Handler)
	if !ok {
		return "", false
	}

	for _, b := range h.RegionBackends() {
		for _, inst := range b.DescribeInstances([]string{id}, "") {
			if inst.ProviderID != "" && inst.PrivateIP != "" && inst.State.Name == "running" {
				return inst.PrivateIP, true
			}
		}
	}

	return "", false
}

// wireELBv2DataPlane lets ELBv2 listeners forward to lambda and docker-backed instance targets.
func wireELBv2DataPlane(byName map[string]service.Registerable) {
	h, ok := byName["ELBv2"].(*elbv2backend.Handler)
	if !ok {
		return
	}

	h.SetLambdaInvoker(elbv2LambdaInvoker{byName: byName})
	h.SetInstanceAddressResolver(elbv2InstanceAddresses{byName: byName})
}
