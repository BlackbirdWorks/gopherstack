package main

import (
	"bytes"
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
)

const (
	svcFirehose       = "firehose"
	svcSecretsManager = "secretsmanager"
	amzJSON10         = "application/x-amz-json-1.0"
	amzJSON11         = "application/x-amz-json-1.1"
)

type apigwJSONTarget struct {
	prefix      string
	contentType string
}

// apigwJSONTargets maps the service token of an arn:aws:apigateway:{region}:{service}:action/...
// integration URI to the X-Amz-Target wire shape of its JSON-protocol service; the token is also
// the SigV4 signing name.
//
//nolint:gochecknoglobals // lookup table
var apigwJSONTargets = map[string]apigwJSONTarget{
	"dynamodb":        {"DynamoDB_20120810", amzJSON10},
	"states":          {"AWSStepFunctions", amzJSON10},
	"kinesis":         {"Kinesis_20131202", amzJSON11},
	svcFirehose:       {"Firehose_20150804", amzJSON11},
	"events":          {"AWSEvents", amzJSON11},
	svcSecretsManager: {svcSecretsManager, amzJSON11},
	"ssm":             {"AmazonSSM", amzJSON11},
	"logs":            {"Logs_20140328", amzJSON11},
	"kms":             {"TrentService", amzJSON11},
	"athena":          {"AmazonAthena", amzJSON11},
	"glue":            {"AWSGlue", amzJSON11},
	"ecs":             {"AmazonEC2ContainerServiceV20141113", amzJSON11},
}

// apigwServiceInvoker serves AWS-integration calls by replaying them as signed-looking
// wire requests against the in-process server.
type apigwServiceInvoker struct {
	handler http.Handler
}

var _ apigwbackend.AWSServiceInvoker = (*apigwServiceInvoker)(nil)

func (a *apigwServiceInvoker) Supports(svc string) bool {
	if svc == "s3" {
		return true
	}

	_, ok := apigwJSONTargets[svc]

	return ok
}

func (a *apigwServiceInvoker) InvokeAWSService(
	ctx context.Context, req apigwbackend.AWSServiceRequest,
) (apigwbackend.AWSServiceResponse, error) {
	var (
		method, path, signing string
		header                = http.Header{}
	)

	switch {
	case req.Service == "s3" && req.Kind == "path":
		method, path, signing = req.Method, "/"+strings.TrimPrefix(req.Spec, "/"), "s3"
	case req.Kind == "action":
		t := apigwJSONTargets[req.Service]
		method, path, signing = http.MethodPost, "/", req.Service
		header.Set("X-Amz-Target", t.prefix+"."+req.Spec)
		header.Set("Content-Type", t.contentType)
	default:
		return apigwbackend.AWSServiceResponse{Status: http.StatusBadRequest}, nil
	}

	hr, err := http.NewRequestWithContext(ctx, method, "http://localhost:8000"+path, bytes.NewReader(req.Body))
	if err != nil {
		return apigwbackend.AWSServiceResponse{}, err
	}

	maps.Copy(hr.Header, header)

	hr.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/"+
		time.Now().UTC().Format("20060102")+"/"+req.Region+"/"+signing+
		"/aws4_request, SignedHeaders=host, Signature=00")

	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, hr)

	return apigwbackend.AWSServiceResponse{Body: rec.Body.Bytes(), Status: rec.Code}, nil
}

// wireAPIGatewayAWSServiceInvoker lets API Gateway AWS integrations reach every JSON-protocol
// service (and s3 path style) in-process.
func wireAPIGatewayAWSServiceInvoker(e http.Handler, services []service.Registerable) {
	apigwH, ok := serviceByName(services)["APIGateway"].(*apigwbackend.Handler)
	if !ok {
		return
	}

	apigwH.SetAWSServiceInvoker(&apigwServiceInvoker{handler: e})
}
