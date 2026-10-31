package cloudfront

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	eventOriginRequest  = "origin-request"
	eventOriginResponse = "origin-response"
	edgeRegion          = "us-east-1"
	edgeARNParts        = 8
	edgeARNPartsNoVer   = 7
)

// ErrInvalidLambdaFunctionAssociation is returned for a Lambda@Edge association that breaks the documented rules.
var ErrInvalidLambdaFunctionAssociation = awserr.New("InvalidLambdaFunctionAssociation", awserr.ErrInvalidParameter)

// ErrEdgeLimitExceeded reports a Lambda@Edge throttle or timeout; the edge answers 503.
var ErrEdgeLimitExceeded = errors.New("lambda@edge limit exceeded")

// LambdaEdge runs and resolves the Lambda function versions associated with cache behaviors.
type LambdaEdge interface {
	// InvokeEdge runs a published version synchronously; fnErr is the Lambda function error type, empty on success.
	InvokeEdge(ctx context.Context, name, version string, payload []byte) (out []byte, fnErr string, err error)
	// EdgeVersionExists reports whether name has the published numbered version.
	EdgeVersionExists(name, version string) bool
}

type dpEdgeAssoc struct {
	ARN         string `xml:"LambdaFunctionARN"`
	EventType   string `xml:"EventType"`
	IncludeBody bool   `xml:"IncludeBody"`
}

// SetLambdaEdge wires a resolver for the Lambda backend, called per request so wiring order does not matter.
func (h *Handler) SetLambdaEdge(f func() LambdaEdge) { h.dp.edge.Store(&f) }

func (h *Handler) lambdaEdge() LambdaEdge {
	if f := h.dp.edge.Load(); f != nil {
		return (*f)()
	}

	return nil
}

type edgeARN struct{ account, region, name, version string }

func parseEdgeARN(s string) (edgeARN, bool) {
	p := strings.Split(s, ":")
	if len(p) != edgeARNParts && len(p) != edgeARNPartsNoVer {
		return edgeARN{}, false
	}

	if p[0] != "arn" || p[2] != "lambda" || p[5] != "function" || p[6] == "" {
		return edgeARN{}, false
	}

	a := edgeARN{region: p[3], account: p[4], name: p[6]}
	if len(p) == edgeARNParts {
		a.version = p[7]
	}

	return a, true
}

func isNumberedVersion(v string) bool {
	if v == "" {
		return false
	}

	for _, c := range v {
		if c < '0' || c > '9' {
			return false
		}
	}

	return v != "0"
}

func edgeInvalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidLambdaFunctionAssociation, fmt.Sprintf(format, args...))
}

func isRequestEvent(e string) bool { return e == eventViewerRequest || e == eventOriginRequest }

func isEdgeEvent(e string) bool {
	return isRequestEvent(e) || e == eventViewerResponse || e == eventOriginResponse
}

func (h *Handler) validateEdgeAssociations(raw []byte) error {
	cfg, err := parseDistConfig(raw)
	if err == nil {
		edge := h.lambdaEdge()
		behaviors := append([]dpBehavior{cfg.Default}, cfg.Behaviors...)

		for i := range behaviors {
			if vErr := validateBehaviorEdge(&behaviors[i], edge, h.Backend.accountID); vErr != nil {
				return vErr
			}
		}
	}

	return nil
}

func validateBehaviorEdge(beh *dpBehavior, edge LambdaEdge, account string) error {
	seen := map[string]bool{}

	for i := range beh.Edge {
		a := &beh.Edge[i]
		if err := validateEdgeAssoc(a, edge, account); err != nil {
			return err
		}

		if seen[a.EventType] {
			return edgeInvalid("more than one Lambda@Edge function is associated with %s", a.EventType)
		}

		seen[a.EventType] = true

		for _, f := range beh.Functions {
			if f.EventType == a.EventType && (a.EventType == eventViewerRequest || a.EventType == eventViewerResponse) {
				return edgeInvalid("a CloudFront function and a Lambda@Edge function cannot share the %s event",
					a.EventType)
			}
		}
	}

	return nil
}

func validateEdgeAssoc(a *dpEdgeAssoc, edge LambdaEdge, account string) error {
	arn, ok := parseEdgeARN(a.ARN)

	switch {
	case !ok:
		return edgeInvalid("%q is not a valid Lambda function ARN", a.ARN)
	case arn.region != edgeRegion:
		return edgeInvalid("the Lambda function must be in the %s Region", edgeRegion)
	case account != "" && arn.account != account:
		return edgeInvalid("the Lambda function must be owned by the account that owns the distribution")
	case !isNumberedVersion(arn.version):
		return edgeInvalid("the Lambda function ARN must reference a numbered version, not $LATEST or an alias")
	case !isEdgeEvent(a.EventType):
		return edgeInvalid("%q is not a valid Lambda@Edge event type", a.EventType)
	case a.IncludeBody && !isRequestEvent(a.EventType):
		return edgeInvalid("IncludeBody is only valid for viewer-request and origin-request events")
	case edge != nil && !edge.EdgeVersionExists(arn.name, arn.version):
		return edgeInvalid("the Lambda function version %s:%s does not exist", arn.name, arn.version)
	}

	return nil
}
