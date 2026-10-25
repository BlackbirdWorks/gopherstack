package sagemaker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const maxResponseBytes = 25 << 20

var (
	// ErrEngineDisabled means InvokeEndpoint needs the container engine, which is switched off.
	ErrEngineDisabled = errors.New(
		"the SageMaker container engine is not enabled, so endpoints have no model container to invoke",
	)
	// ErrNotAsyncEndpoint means the endpoint has no AsyncInferenceConfig.
	ErrNotAsyncEndpoint = errors.New("endpoint is not configured for asynchronous inference")
)

// ModelError is returned when the model container answers an invocation with an error status.
type ModelError struct {
	OriginalMessage    string
	LogStreamArn       string
	OriginalStatusCode int
}

func (e *ModelError) Error() string {
	return fmt.Sprintf("received server error (%d) from model container: %s", e.OriginalStatusCode, e.OriginalMessage)
}

// InvokeRequest is one synchronous inference request.
type InvokeRequest struct {
	EndpointName            string
	TargetVariant           string
	TargetContainerHostname string
	TargetModel             string
	ContentType             string
	Accept                  string
	CustomAttributes        string
	InferenceID             string
	Body                    []byte
}

// InvokeResult is the model container's reply.
type InvokeResult struct {
	ContentType      string
	CustomAttributes string
	InvokedVariant   string
	Body             []byte
}

// InvokeStream is a model reply whose body is still being produced.
type InvokeStream struct {
	Body             io.ReadCloser
	ContentType      string
	CustomAttributes string
	InvokedVariant   string
}

func (r InvokeRequest) invocation() invocation {
	return invocation{
		contentType: r.ContentType, accept: r.Accept, custom: r.CustomAttributes,
		targetModel: r.TargetModel, inferenceID: r.InferenceID, body: r.Body,
	}
}

// invokeRaw forwards req to the endpoint's container. The caller closes the response body.
func (b *InMemoryBackend) invokeRaw(ctx context.Context, req InvokeRequest) (*http.Response, string, error) {
	eng := b.engineOrNil()
	if eng == nil {
		return nil, "", ErrEngineDisabled
	}

	region := getRegion(ctx, b.region)

	ep, err := b.DescribeEndpoint(ctx, req.EndpointName)
	if err != nil {
		return nil, "", err
	}

	ed := eng.deployment(engineKey("endpoint", region, req.EndpointName))
	if ed == nil {
		return nil, "", fmt.Errorf("%w: %s", ErrEndpointNotRunning, req.EndpointName)
	}

	md, variant, err := ed.choose(ep, req.TargetVariant)
	if err != nil {
		return nil, "", err
	}

	resp, err := md.invoke(ctx, req.invocation(), req.TargetContainerHostname)
	if err != nil {
		return nil, variant, err
	}

	if resp.StatusCode/100 != 2 { //nolint:mnd // HTTP status class
		defer resp.Body.Close()

		return nil, variant, &ModelError{
			OriginalStatusCode: resp.StatusCode, OriginalMessage: readSnippet(resp.Body),
			LogStreamArn: arn.Build("logs", region, b.accountID, "log-group:/aws/sagemaker/Endpoints/"+req.EndpointName),
		}
	}

	return resp, variant, nil
}

// InvokeEndpoint runs one request through the endpoint's model container.
func (b *InMemoryBackend) InvokeEndpoint(ctx context.Context, req InvokeRequest) (*InvokeResult, error) {
	resp, variant, err := b.invokeRaw(ctx, req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read model response: %w", err)
	}

	return &InvokeResult{
		Body: body, InvokedVariant: variant, ContentType: resp.Header.Get("Content-Type"),
		CustomAttributes: resp.Header.Get(headerCustomAttributes),
	}, nil
}

// InvokeEndpointStream forwards the request and hands back the still-open response body.
func (b *InMemoryBackend) InvokeEndpointStream(ctx context.Context, req InvokeRequest) (*InvokeStream, error) {
	resp, variant, err := b.invokeRaw(ctx, req)
	if err != nil {
		return nil, err
	}

	return &InvokeStream{
		Body: resp.Body, InvokedVariant: variant, ContentType: resp.Header.Get("Content-Type"),
		CustomAttributes: resp.Header.Get(headerCustomAttributes),
	}, nil
}

// AsyncRequest is one asynchronous inference request.
type AsyncRequest struct {
	EndpointName        string
	InferenceID         string
	InputLocation       string
	ContentType         string
	Accept              string
	CustomAttributes    string
	Filename            string
	OutputPathExtension string
	Body                []byte
	TimeoutSeconds      int32
}

// AsyncAccepted reports where an accepted asynchronous request writes its result.
type AsyncAccepted struct {
	InferenceID     string
	OutputLocation  string
	FailureLocation string
}

const defaultAsyncTimeout = 900 * time.Second

// SubmitAsyncInference accepts an asynchronous request and processes it in the background: the
// result lands at OutputLocation, failures at FailureLocation, with SNS notifications when configured.
func (b *InMemoryBackend) SubmitAsyncInference(ctx context.Context, req AsyncRequest) (*AsyncAccepted, error) {
	eng := b.engineOrNil()
	if eng == nil {
		return nil, ErrEngineDisabled
	}

	ep, err := b.DescribeEndpoint(ctx, req.EndpointName)
	if err != nil {
		return nil, err
	}

	if ep.AsyncInferenceConfig == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotAsyncEndpoint, req.EndpointName)
	}

	out := ep.AsyncInferenceConfig.OutputConfig
	if req.InferenceID == "" {
		req.InferenceID = uuid.NewString()
	}

	name := req.Filename
	if name == "" {
		name = req.InferenceID + ".out"
	}

	failBase := out.S3FailurePath
	if failBase == "" {
		failBase = out.S3OutputPath
	}

	acc := &AsyncAccepted{
		InferenceID:     req.InferenceID,
		OutputLocation:  joinS3(out.S3OutputPath, req.OutputPathExtension, name),
		FailureLocation: joinS3(failBase, req.OutputPathExtension, req.InferenceID+"-error.out"),
	}

	region := getRegion(ctx, b.region)
	jctx, cancel := b.jobContext(region)
	received := time.Now().UTC()

	b.goEngine(func() {
		defer cancel()

		b.runAsync(jctx, eng, req, acc, out.NotificationConfig, received)
	})

	return acc, nil
}

func (b *InMemoryBackend) runAsync(
	ctx context.Context, eng *engineState, req AsyncRequest, acc *AsyncAccepted, nc *AsyncNotificationConfig,
	received time.Time,
) {
	store := eng.svc().Store

	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = defaultAsyncTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	body := req.Body

	var (
		result *InvokeResult
		err    error
	)

	if req.InputLocation != "" {
		body, err = getS3URI(ctx, store, req.InputLocation)
	}

	if err == nil {
		result, err = b.InvokeEndpoint(ctx, InvokeRequest{
			EndpointName: req.EndpointName, ContentType: req.ContentType, Accept: req.Accept,
			CustomAttributes: req.CustomAttributes, InferenceID: req.InferenceID, Body: body,
		})
	}

	region := getRegion(ctx, b.region)
	params := map[string]any{"endpointName": req.EndpointName}
	setStr(params, "contentType", req.ContentType)
	setStr(params, "inputLocation", req.InputLocation)

	if err == nil {
		err = putS3URI(ctx, store, acc.OutputLocation, result.Body)
	}

	if err != nil {
		_ = putS3URI(context.WithoutCancel(ctx), store, acc.FailureLocation, []byte(err.Error()))
		b.notifyAsync(ctx, eng, region, nc, false, map[string]any{
			"invocationStatus": "Failed", "failureReason": err.Error(), "inferenceId": req.InferenceID,
			"requestParameters": params, "responseParameters": map[string]any{"outputLocation": acc.FailureLocation},
		}, received)

		return
	}

	b.notifyAsync(ctx, eng, region, nc, true, map[string]any{
		"invocationStatus": "Completed", "inferenceId": req.InferenceID, "requestParameters": params,
		"responseParameters": map[string]any{"contentType": result.ContentType, "outputLocation": acc.OutputLocation},
	}, received)
}

func setStr(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}

func (b *InMemoryBackend) notifyAsync(
	ctx context.Context, eng *engineState, region string, nc *AsyncNotificationConfig, ok bool,
	msg map[string]any, received time.Time,
) {
	if nc == nil {
		return
	}

	topic := nc.ErrorTopic
	if ok {
		topic = nc.SuccessTopic
	}

	notifier := eng.svc().Notifier
	if topic == "" || notifier == nil {
		return
	}

	const layout = "2006-01-02T15:04:05.000Z"

	msg["awsRegion"] = region
	msg["eventTime"] = time.Now().UTC().Format(layout)
	msg["receivedTime"] = received.Format(layout)
	msg["eventVersion"] = "1.0"
	msg["eventSource"] = "aws:sagemaker"
	msg["eventName"] = "InferenceResult"

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	_ = notifier.Publish(context.WithoutCancel(ctx), region, topic, string(data))
}
