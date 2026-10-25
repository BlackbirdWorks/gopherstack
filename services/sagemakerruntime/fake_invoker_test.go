package sagemakerruntime_test

import (
	"context"
	"io"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

// echoInvoker is a test double for the model container: it returns the request payload unchanged.
type echoInvoker struct{}

func (echoInvoker) InvokeEndpoint(_ context.Context, req sagemaker.InvokeRequest) (*sagemaker.InvokeResult, error) {
	if err := scripted(req.EndpointName); err != nil {
		return nil, err
	}

	ct := req.Accept
	if ct == "" {
		ct = "application/octet-stream"
	}

	return &sagemaker.InvokeResult{
		Body: append([]byte("echo:"), req.Body...), ContentType: ct,
		CustomAttributes: req.CustomAttributes, InvokedVariant: variantOf(req.TargetVariant),
	}, nil
}

func (echoInvoker) InvokeEndpointStream(
	_ context.Context,
	req sagemaker.InvokeRequest,
) (*sagemaker.InvokeStream, error) {
	if err := scripted(req.EndpointName); err != nil {
		return nil, err
	}

	return &sagemaker.InvokeStream{
		Body: io.NopCloser(strings.NewReader("echo:" + string(req.Body))), ContentType: req.Accept,
		CustomAttributes: req.CustomAttributes, InvokedVariant: variantOf(req.TargetVariant),
	}, nil
}

func (echoInvoker) SubmitAsyncInference(
	_ context.Context,
	req sagemaker.AsyncRequest,
) (*sagemaker.AsyncAccepted, error) {
	if err := scripted(req.EndpointName); err != nil {
		return nil, err
	}

	id := req.InferenceID
	if id == "" {
		id = "gopherstack-generated"
	}

	name := req.Filename
	if name == "" {
		name = id + ".out"
	}

	base := "s3://async-out"
	if req.OutputPathExtension != "" {
		base += "/" + req.OutputPathExtension
	}

	return &sagemaker.AsyncAccepted{
		InferenceID: id, OutputLocation: base + "/" + name, FailureLocation: base + "/" + id + "-error.out",
	}, nil
}

// scripted lets a test pick a failure by endpoint name.
func scripted(endpoint string) error {
	switch endpoint {
	case "model-error":
		return &sagemaker.ModelError{OriginalStatusCode: 500, OriginalMessage: "boom", LogStreamArn: "arn:aws:logs:::x"}
	case "engine-off":
		return sagemaker.ErrEngineDisabled
	case "not-async":
		return sagemaker.ErrNotAsyncEndpoint
	default:
		return nil
	}
}

func variantOf(v string) string {
	if v == "" {
		return "AllTraffic"
	}

	return v
}

// newEchoBackend returns a backend whose invocations are answered by echoInvoker.
func newEchoBackend() *sagemakerruntime.InMemoryBackend {
	b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetInvoker(echoInvoker{})

	return b
}
