package iot

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	authorizerMaxPolicyDocs   = 10
	authorizerMaxPolicyLength = 2048
	authorizerMinSeconds      = 300
	authorizerMaxSeconds      = 86400
)

var principalIDRE = regexp.MustCompile(`^[a-zA-Z0-9]{1,128}$`)

// ErrInvalidAuthorizerResponse is returned when the authorizer Lambda's answer is not a valid authorizer response.
var ErrInvalidAuthorizerResponse = errors.New("invalid authorizer response")

// AuthorizerHTTPContext is the HTTP part of an authorizer request.
type AuthorizerHTTPContext struct {
	Headers     map[string]string
	QueryString string
}

// AuthorizerMQTTContext is the MQTT part of an authorizer request.
type AuthorizerMQTTContext struct {
	ClientID string
	Username string
	Password []byte
}

// AuthorizerTLSContext is the TLS part of an authorizer request.
type AuthorizerTLSContext struct {
	ServerName string
}

// AuthorizerRequest is what a custom authorizer is invoked with.
type AuthorizerRequest struct {
	HTTP           *AuthorizerHTTPContext
	MQTT           *AuthorizerMQTTContext
	TLS            *AuthorizerTLSContext
	Token          string
	TokenSignature string
}

// TestInvokeAuthorizerInput is the input for TestInvokeAuthorizer.
type TestInvokeAuthorizerInput = AuthorizerRequest

// TestInvokeAuthorizerOutput is the result of an authorizer invocation.
type TestInvokeAuthorizerOutput struct {
	PrincipalID              string
	PolicyDocuments          []string
	IsAuthenticated          bool
	DisconnectAfterInSeconds int32
	RefreshAfterInSeconds    int32
}

type authorizerEvent struct {
	ProtocolData       authorizerProtocolData `json:"protocolData"`
	ConnectionMetadata map[string]string      `json:"connectionMetadata"`
	Token              string                 `json:"token,omitempty"`
	Protocols          []string               `json:"protocols"`
	SignatureVerified  bool                   `json:"signatureVerified"`
}

type authorizerProtocolData struct {
	TLS  *authorizerTLSData  `json:"tls,omitempty"`
	HTTP *authorizerHTTPData `json:"http,omitempty"`
	MQTT *authorizerMQTTData `json:"mqtt,omitempty"`
}

type authorizerTLSData struct {
	ServerName string `json:"serverName"`
}

type authorizerHTTPData struct {
	Headers     map[string]string `json:"headers,omitempty"`
	QueryString string            `json:"queryString,omitempty"`
}

type authorizerMQTTData struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	ClientID string `json:"clientId,omitempty"`
}

type authorizerResponse struct {
	DisconnectAfterInSeconds *int64            `json:"disconnectAfterInSeconds"`
	RefreshAfterInSeconds    *int64            `json:"refreshAfterInSeconds"`
	PrincipalID              string            `json:"principalId"`
	PolicyDocuments          []json.RawMessage `json:"policyDocuments"`
	IsAuthenticated          bool              `json:"isAuthenticated"`
}

// TestInvokeAuthorizer invokes the authorizer's Lambda function with the request.
func (b *InMemoryBackend) TestInvokeAuthorizer(
	ctx context.Context, authorizerName string, input *TestInvokeAuthorizerInput,
) (*TestInvokeAuthorizerOutput, error) {
	return b.InvokeAuthorizer(ctx, authorizerName, input)
}

// InvokeAuthorizer verifies the token signature when signing is enabled, invokes the authorizer's
// Lambda function and validates its answer. A rejected signature or inactive authorizer yields
// IsAuthenticated=false without invoking the function.
func (b *InMemoryBackend) InvokeAuthorizer(
	ctx context.Context, authorizerName string, req *AuthorizerRequest,
) (*TestInvokeAuthorizerOutput, error) {
	b.mu.RLock("InvokeAuthorizer")
	a, ok := b.authorizers.Get(authorizerName)

	var auth roleauth.Authorizer

	var lambda LambdaRequester

	var cp *Authorizer

	if ok {
		cp = cloneAuthorizer(a)
		auth = b.roleAuth

		if b.targets != nil {
			lambda = b.targets.Lambda
		}
	}

	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("authorizer %q not found: %w", authorizerName, ErrResourceNotFound)
	}

	if cp.Status != "" && cp.Status != statusActive {
		return &TestInvokeAuthorizerOutput{}, nil
	}

	verified := false

	if !cp.SigningDisabled {
		if req.Token == "" || req.TokenSignature == "" || !verifyTokenSignature(cp.TokenSigningPublicKeys, req) {
			return &TestInvokeAuthorizerOutput{}, nil
		}

		verified = true
	}

	if lambda == nil {
		return nil, ErrActionTargetUnavailable
	}

	if err := roleauth.AuthorizeResource(auth, roleauth.PrincipalIoT, "lambda:InvokeFunction",
		cp.AuthorizerFunctionARN, cp.AuthorizerARN); err != nil {
		return nil, fmt.Errorf("%w: %w", errRuleActionDenied, err)
	}

	payload, err := json.Marshal(buildAuthorizerEvent(req, verified))
	if err != nil {
		return nil, fmt.Errorf("authorizer event: %w", err)
	}

	region, _ := ruleRegionAccount(cp.AuthorizerFunctionARN)
	if region == "" {
		region = b.region
	}

	raw, err := lambda.RequestLambda(ctx, region, cp.AuthorizerFunctionARN, payload)
	if err != nil {
		return nil, fmt.Errorf("%w: authorizer function failed: %w", ErrInvalidAuthorizerResponse, err)
	}

	return parseAuthorizerResponse(raw)
}

func buildAuthorizerEvent(req *AuthorizerRequest, verified bool) authorizerEvent {
	ev := authorizerEvent{
		Token:              req.Token,
		SignatureVerified:  verified,
		Protocols:          []string{},
		ConnectionMetadata: map[string]string{"id": uuid.NewString()},
	}

	if req.TLS != nil {
		ev.Protocols = append(ev.Protocols, "tls")
		ev.ProtocolData.TLS = &authorizerTLSData{ServerName: req.TLS.ServerName}
	}

	if req.HTTP != nil {
		ev.Protocols = append(ev.Protocols, "http")
		ev.ProtocolData.HTTP = &authorizerHTTPData{Headers: req.HTTP.Headers, QueryString: req.HTTP.QueryString}
	}

	if req.MQTT != nil {
		ev.Protocols = append(ev.Protocols, "mqtt")
		ev.ProtocolData.MQTT = &authorizerMQTTData{
			Username: req.MQTT.Username, ClientID: req.MQTT.ClientID,
		}

		if len(req.MQTT.Password) > 0 {
			ev.ProtocolData.MQTT.Password = base64.StdEncoding.EncodeToString(req.MQTT.Password)
		}
	}

	return ev
}

// verifyTokenSignature checks the base64 RSA/SHA-256 signature of the token against any registered public key.
func verifyTokenSignature(keys map[string]string, req *AuthorizerRequest) bool {
	sig, err := base64.StdEncoding.DecodeString(req.TokenSignature)
	if err != nil {
		return false
	}

	digest := sha256.Sum256([]byte(req.Token))

	for _, pemKey := range keys {
		block, _ := pem.Decode([]byte(pemKey))
		if block == nil {
			continue
		}

		pub, parseErr := x509.ParsePKIXPublicKey(block.Bytes)
		if parseErr != nil {
			continue
		}

		if rsaKey, isRSA := pub.(*rsa.PublicKey); isRSA &&
			rsa.VerifyPKCS1v15(rsaKey, crypto.SHA256, digest[:], sig) == nil {
			return true
		}
	}

	return false
}

func parseAuthorizerResponse(raw []byte) (*TestInvokeAuthorizerOutput, error) {
	var resp authorizerResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("%w: not a JSON object", ErrInvalidAuthorizerResponse)
	}

	if !resp.IsAuthenticated {
		return &TestInvokeAuthorizerOutput{}, nil
	}

	if !principalIDRE.MatchString(resp.PrincipalID) {
		return nil, fmt.Errorf("%w: principalId must be 1-128 alphanumeric characters", ErrInvalidAuthorizerResponse)
	}

	if len(resp.PolicyDocuments) > authorizerMaxPolicyDocs {
		return nil, fmt.Errorf("%w: at most %d policy documents", ErrInvalidAuthorizerResponse, authorizerMaxPolicyDocs)
	}

	out := &TestInvokeAuthorizerOutput{IsAuthenticated: true, PrincipalID: resp.PrincipalID}

	for _, d := range resp.PolicyDocuments {
		doc := string(d)

		var asString string
		if json.Unmarshal(d, &asString) == nil {
			doc = asString
		}

		if len(doc) > authorizerMaxPolicyLength {
			return nil, fmt.Errorf("%w: policy document exceeds %d characters",
				ErrInvalidAuthorizerResponse, authorizerMaxPolicyLength)
		}

		out.PolicyDocuments = append(out.PolicyDocuments, doc)
	}

	out.DisconnectAfterInSeconds = authorizerMaxSeconds

	if resp.DisconnectAfterInSeconds != nil {
		v, err := authorizerSeconds("disconnectAfterInSeconds", *resp.DisconnectAfterInSeconds)
		if err != nil {
			return nil, err
		}

		out.DisconnectAfterInSeconds = v
	}

	if resp.RefreshAfterInSeconds != nil {
		v, err := authorizerSeconds("refreshAfterInSeconds", *resp.RefreshAfterInSeconds)
		if err != nil {
			return nil, err
		}

		out.RefreshAfterInSeconds = v
	}

	return out, nil
}

func authorizerSeconds(name string, v int64) (int32, error) {
	if v < authorizerMinSeconds || v > authorizerMaxSeconds {
		return 0, fmt.Errorf("%w: %s must be between %d and %d",
			ErrInvalidAuthorizerResponse, name, authorizerMinSeconds, authorizerMaxSeconds)
	}

	return int32(v), nil
}
