package main

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
)

// swfLambdaInvoker adapts Lambda to swf.LambdaInvoker.
type swfLambdaInvoker struct {
	backend *lambdabackend.InMemoryBackend
}

func (a swfLambdaInvoker) InvokeLambda(ctx context.Context, name string, payload []byte) ([]byte, string, error) {
	result, _, functionError, _, err := a.backend.InvokeFunctionWithQualifier(
		ctx, name, "", "", "", lambdabackend.InvocationTypeRequestResponse, payload,
	)

	return result, functionError, err
}

// wireSWFLambda lets SWF ScheduleLambdaFunction decisions invoke Lambda functions.
func wireSWFLambda(byName map[string]service.Registerable) {
	swfH, ok := byName["SWF"].(*swfbackend.Handler)
	if !ok {
		return
	}

	lambdaH, ok := byName["Lambda"].(*lambdabackend.Handler)
	if !ok {
		return
	}

	lambdaBk, ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return
	}

	swfH.SetLambdaInvoker(swfLambdaInvoker{backend: lambdaBk})
}
