package stepfunctions

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/smithy-go"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

const (
	eksCallAction        = "call"
	eksTokenPrefix       = "k8s-aws-v1." //nolint:gosec // public wire prefix, not a credential
	eksClusterIDHeader   = "X-K8s-Aws-Id"
	eksTokenTTLSeconds   = "60"
	eksEmptyPayloadHash  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	eksSyntheticHost     = ".eks.amazonaws.com"
	eksCallTimeout       = 30 * time.Second
	eksMaxResponseBytes  = 10 << 20
	eksNotFoundErrorCode = "ResourceNotFoundException"
)

var errNotPEM = errors.New("not a PEM certificate")

func syntheticEKSEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)

	return err == nil && strings.HasSuffix(u.Hostname(), eksSyntheticHost)
}

func isNotFound(err error) bool {
	apiErr, ok := errors.AsType[smithy.APIError](err)

	return ok && apiErr.ErrorCode() == eksNotFoundErrorCode
}

func eksSyncSpecFor(key string) (syncSpec, bool) {
	switch key {
	case "eks.createCluster":
		return eksClusterSpec("CREATING", "CREATE_FAILED", false), true
	case "eks.deleteCluster":
		return eksClusterSpec("DELETING", "DELETE_FAILED", true), true
	case "eks.createNodegroup":
		return eksNodegroupSpec("CREATING", "CREATE_FAILED", false), true
	case "eks.deleteNodegroup":
		return eksNodegroupSpec("DELETING", "DELETE_FAILED", true), true
	default:
		return syncSpec{}, false
	}
}

func eksClusterSpec(running, failed string, gone bool) syncSpec {
	return syncSpec{
		pollAction: "describeCluster",
		pollParams: func(s map[string]any) any { return map[string]any{"Name": child(s, "Cluster")["Name"]} },
		state:      func(p map[string]any) string { return str(child(p, "Cluster")["Status"]) },
		running:    []string{running},
		failed:     []string{statusFailed, failed},
		result:     func(p map[string]any, _ string) any { return p },
		goneIsDone: gone,
	}
}

func eksNodegroupSpec(running, failed string, gone bool) syncSpec {
	return syncSpec{
		pollAction: "describeNodegroup",
		pollParams: func(s map[string]any) any {
			ng := child(s, "Nodegroup")

			return map[string]any{"ClusterName": ng["ClusterName"], "NodegroupName": ng["NodegroupName"]}
		},
		state:      func(p map[string]any) string { return str(child(p, "Nodegroup")["Status"]) },
		running:    []string{running},
		failed:     []string{failed},
		result:     func(p map[string]any, _ string) any { return p },
		goneIsDone: gone,
	}
}

// invokeModelOutput decodes InvokeModel's Body into JSON when it parses, else text.
func invokeModelOutput(out any) any {
	m, ok := out.(map[string]any)
	if !ok {
		return out
	}

	enc, _ := m["Body"].(string)

	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return out
	}

	var parsed any
	if json.Unmarshal(raw, &parsed) != nil {
		parsed = string(raw)
	}

	m["Body"] = parsed

	return m
}

type eksCallSpec struct {
	query    url.Values
	body     any
	cluster  string
	ca       string
	endpoint string
	method   string
	path     string
}

func parseEKSCall(params any) (eksCallSpec, error) {
	m, _ := params.(map[string]any)
	spec := eksCallSpec{
		cluster: str(m["ClusterName"]), ca: str(m["CertificateAuthority"]), endpoint: str(m["Endpoint"]),
		method: strings.ToUpper(str(m["Method"])), path: str(m["Path"]), body: m["RequestBody"], query: url.Values{},
	}

	for k, v := range child(m, "QueryParameters") {
		list, _ := v.([]any)
		for _, item := range list {
			spec.query.Add(k, str(item))
		}
	}

	if spec.cluster == "" || spec.endpoint == "" || spec.method == "" || spec.path == "" {
		return spec, runtimeFailure("eks:call requires ClusterName, Endpoint, Method and Path")
	}

	return spec, nil
}

// invokeEKSCall sends a Kubernetes API request to the cluster endpoint, authenticated like `aws eks get-token`.
func (a *sdkAdapter) invokeEKSCall(ctx context.Context, call asl.SDKCall) (any, error) {
	spec, err := parseEKSCall(call.Params)
	if err != nil {
		return nil, err
	}

	if syntheticEKSEndpoint(spec.endpoint) {
		return nil, &asl.FailError{
			ErrCode: errCodeTaskFailed,
			Cause:   "Eks.SdkClientException: cluster endpoint " + spec.endpoint + " is not reachable: EKS docker engine is off",
		}
	}

	token, err := a.eksToken(ctx, call, spec.cluster)
	if err != nil {
		return nil, sdkFailure("Eks", err)
	}

	req, err := spec.request(ctx, token)
	if err != nil {
		return nil, err
	}

	client, err := eksHTTPClient(spec.ca)
	if err != nil {
		return nil, runtimeFailure("invalid CertificateAuthority: %v", err)
	}

	defer client.CloseIdleConnections()

	return doEKSCall(client, req)
}

func (s eksCallSpec) request(ctx context.Context, token string) (*http.Request, error) {
	var body io.Reader

	if s.body != nil {
		b, err := json.Marshal(s.body)
		if err != nil {
			return nil, runtimeFailure("invalid RequestBody: %v", err)
		}

		body = bytes.NewReader(b)
	}

	target := strings.TrimRight(s.endpoint, "/") + "/" + strings.TrimLeft(s.path, "/")
	if len(s.query) > 0 {
		target += "?" + s.query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, s.method, target, body)
	if err != nil {
		return nil, runtimeFailure("invalid eks:call request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

func eksHTTPClient(caB64 string) (*http.Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if caB64 != "" {
		pemBytes, err := base64.StdEncoding.DecodeString(caB64)
		if err != nil {
			return nil, err
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, errNotPEM
		}

		tlsCfg.RootCAs = pool
	}

	return &http.Client{
		Timeout:   eksCallTimeout,
		Transport: &http.Transport{TLSClientConfig: tlsCfg, DisableKeepAlives: true},
	}, nil
}

// eksToken mints the bearer `aws eks get-token` produces: a presigned STS GetCallerIdentity URL.
func (a *sdkAdapter) eksToken(ctx context.Context, call asl.SDKCall, cluster string) (string, error) {
	creds, err := a.credentials(call.RoleArn).Retrieve(ctx)
	if err != nil {
		return "", err
	}

	region := call.Region
	if region == "" {
		region = a.region
	}

	stsURL := "https://sts." + region + ".amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15&X-Amz-Expires=" +
		eksTokenTTLSeconds

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, stsURL, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set(eksClusterIDHeader, cluster)

	signed, _, err := v4.NewSigner().PresignHTTP(ctx, aws.Credentials{
		AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey, SessionToken: creds.SessionToken,
	}, req, eksEmptyPayloadHash, "sts", region, time.Now())
	if err != nil {
		return "", err
	}

	return eksTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(signed)), nil
}

func doEKSCall(client *http.Client, req *http.Request) (any, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, transportFailure(req.Context(), err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, eksMaxResponseBytes))
	if err != nil {
		return nil, transportFailure(req.Context(), err)
	}

	var parsed any
	if json.Unmarshal(data, &parsed) != nil {
		parsed = string(data)
	}

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return map[string]any{"ResponseBody": parsed}, nil
	}

	cause, _ := json.Marshal(parsed)

	return nil, &asl.FailError{ErrCode: errCodeTaskFailed, Cause: string(cause)}
}
