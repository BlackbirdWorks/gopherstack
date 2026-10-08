package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

var errHookFunctionError = errors.New("lambda function error")

type ecsAlarmStates struct {
	handler *cloudwatchbackend.Handler
}

func (a ecsAlarmStates) TriggeredAlarms(region string, alarmNames []string) []string {
	if len(alarmNames) == 0 {
		return nil
	}

	metric, composite, logAlarms, err := a.handler.BackendFor(region).DescribeAlarms(
		alarmNames, nil, "", "ALARM", "", 0, "", "", "",
	)
	if err != nil {
		return nil
	}

	var out []string

	for _, m := range metric.Data {
		out = append(out, m.AlarmName)
	}

	for _, c := range composite.Data {
		out = append(out, c.AlarmName)
	}

	for _, l := range logAlarms.Data {
		out = append(out, l.AlarmName)
	}

	return out
}

type ecsHookInvoker struct {
	backend *lambdabackend.InMemoryBackend
}

func (a ecsHookInvoker) Invoke(ctx context.Context, functionARN string, payload []byte) ([]byte, error) {
	out, functionError, _, _, err := a.backend.InvokeFunctionWithQualifier(
		ctx, functionARN, "", "", "", lambdabackend.InvocationTypeRequestResponse, payload,
	)
	if err != nil {
		return nil, err
	}

	if functionError != "" {
		return out, fmt.Errorf("%w: %s", errHookFunctionError, functionError)
	}

	return out, nil
}

// wireECSDeploymentHooks feeds CloudWatch alarm state into ECS deployment alarms and lets
// AWS_LAMBDA lifecycle hooks invoke Lambda.
func wireECSDeploymentHooks(byName map[string]service.Registerable) {
	ecsH, ok := byName["ECS"].(*ecsbackend.Handler)
	if !ok {
		return
	}

	ecsBk, ok := ecsH.Backend.(*ecsbackend.InMemoryBackend)
	if !ok {
		return
	}

	if cwH, cwOK := byName["CloudWatch"].(*cloudwatchbackend.Handler); cwOK {
		ecsBk.SetAlarmStateProvider(ecsAlarmStates{handler: cwH})
	}

	if lambdaH, lOK := byName["Lambda"].(*lambdabackend.Handler); lOK {
		if lambdaBk, bkOK := lambdaH.Backend.(*lambdabackend.InMemoryBackend); bkOK {
			ecsBk.SetLambdaInvoker(ecsHookInvoker{backend: lambdaBk})
		}
	}
}
