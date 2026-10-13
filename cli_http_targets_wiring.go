package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

var errMalformedExecuteAPIARN = errors.New("malformed execute-api target ARN")

type ebAPIGatewayAdapter struct{ handler *apigwbackend.Handler }

func (a *ebAPIGatewayAdapter) InvokeAPIGateway(
	ctx context.Context, req ebbackend.APIGatewayRequest,
) (ebbackend.APIResponse, error) {
	resp := a.handler.InvokeStage(ctx, apigwbackend.StageRequest{
		APIID: req.APIID, Stage: req.Stage, Method: req.Method, Path: req.Path,
		Headers: req.Headers, Query: req.Query, Body: req.Body,
	})

	return ebbackend.APIResponse{Status: resp.Status, Body: resp.Body}, nil
}

func ebHTTPParameters(hp *pipesbackend.TargetHTTPParameters) *ebbackend.HTTPParameters {
	if hp == nil {
		return nil
	}

	return &ebbackend.HTTPParameters{
		HeaderParameters:      hp.HeaderParameters,
		QueryStringParameters: hp.QueryStringParameters,
		PathParameterValues:   hp.PathParameterValues,
	}
}

type pipesAPIGatewayAdapter struct{ eb *ebAPIGatewayAdapter }

func (a *pipesAPIGatewayAdapter) InvokeAPIGateway(
	ctx context.Context, apiARN string, hp *pipesbackend.TargetHTTPParameters, payload []byte,
) (pipesbackend.HTTPResponse, error) {
	req, ok := ebbackend.BuildAPIGatewayRequest(apiARN, ebHTTPParameters(hp), payload)
	if !ok {
		return pipesbackend.HTTPResponse{}, errMalformedExecuteAPIARN
	}

	resp, err := a.eb.InvokeAPIGateway(ctx, req)

	return pipesbackend.HTTPResponse{Status: resp.Status, Body: resp.Body}, err
}

type pipesAPIDestinationAdapter struct{ backend *ebbackend.InMemoryBackend }

func (a *pipesAPIDestinationAdapter) InvokeAPIDestination(
	ctx context.Context, destARN string, hp *pipesbackend.TargetHTTPParameters, payload []byte,
) (pipesbackend.HTTPResponse, error) {
	resp, err := a.backend.InvokeAPIDestination(ctx, destARN, payload, ebHTTPParameters(hp))

	return pipesbackend.HTTPResponse{Status: resp.Status, Body: resp.Body}, err
}

// wireHTTPTargets lets EventBridge rule targets and Pipes targets/enrichment call API Gateway stages
// in-process, and Pipes call API destinations.
func wireHTTPTargets(byName map[string]service.Registerable) {
	apigwH, _ := byName["APIGateway"].(*apigwbackend.Handler)
	ebH, _ := byName["EventBridge"].(*ebbackend.Handler)
	pipesH, _ := byName["Pipes"].(*pipesbackend.Handler)

	var ebBk *ebbackend.InMemoryBackend
	if ebH != nil {
		ebBk, _ = ebH.Backend.(*ebbackend.InMemoryBackend)
	}

	var gw *ebAPIGatewayAdapter
	if apigwH != nil {
		gw = &ebAPIGatewayAdapter{handler: apigwH}
	}

	if ebBk != nil && gw != nil {
		ebBk.ConfigureDeliveryTargets(func(dt *ebbackend.DeliveryTargets) { dt.APIGateway = gw })
	}

	if pipesH == nil {
		return
	}

	var targets pipesbackend.HTTPTargets

	if gw != nil {
		targets.APIGateway = &pipesAPIGatewayAdapter{eb: gw}
	}

	if ebBk != nil {
		targets.APIDestination = &pipesAPIDestinationAdapter{backend: ebBk}
	}

	pipesH.GetRunner().SetHTTPTargets(targets)
}

type schedUniversalAdapter struct {
	sdk asl.SDKIntegration
}

func (a *schedUniversalAdapter) InvokeUniversalTarget(
	ctx context.Context, region, roleARN, svc, action string, input []byte,
) error {
	var params any
	if err := json.Unmarshal(input, &params); err != nil {
		return fmt.Errorf("universal target input is not valid JSON: %w", err)
	}

	_, err := a.sdk.SFNCallSDK(ctx, asl.SDKCall{
		Service: svc, Action: action, Params: params, Region: region, RoleArn: roleARN,
	})

	return err
}

// wireSchedulerUniversalTargets lets aws-sdk universal schedule targets call every registered service
// in-process; with IAM enforcement on, calls run as the schedule's role.
func wireSchedulerUniversalTargets(e http.Handler, services []service.Registerable, region string, enforceIAM bool) {
	byName := serviceByName(services)

	schedH, ok := byName["Scheduler"].(*schedulerbackend.Handler)
	if !ok {
		return
	}

	var roles sfnbackend.RoleAssumer

	if stsH, stsOk := byName["STS"].(*stsbackend.Handler); stsOk && enforceIAM {
		if stsBk, bkOk := stsH.Backend.(*stsbackend.InMemoryBackend); bkOk {
			roles = &serviceRoleAssumer{
				sts: stsBk, principal: "scheduler.amazonaws.com", session: "scheduler-execution",
			}
		}
	}

	schedH.GetRunner().SetUniversalTargetInvoker(
		&schedUniversalAdapter{sdk: sfnbackend.NewSDKIntegrationWithRoles(e, region, roles)},
	)
}
