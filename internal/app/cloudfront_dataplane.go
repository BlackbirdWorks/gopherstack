package app

import (
	"context"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// cloudFrontALBForwarder serves load balancer origins through the elbv2 data plane in-process.
type cloudFrontALBForwarder struct{ h *elbv2backend.Handler }

func (f cloudFrontALBForwarder) ServeALB(w http.ResponseWriter, r *http.Request) bool {
	c := echo.NewContext(r, w)
	if !f.h.RouteMatcher()(c) {
		return false
	}

	_ = f.h.Handler()(c)

	return true
}

// cloudFrontLambdaEdge runs Lambda@Edge associations through the Lambda backend.
type cloudFrontLambdaEdge struct {
	bk *lambdabackend.InMemoryBackend
}

func (e cloudFrontLambdaEdge) InvokeEdge(
	ctx context.Context, name, version string, payload []byte,
) ([]byte, string, error) {
	out, _, fnErr, status, err := e.bk.InvokeFunctionWithQualifier(
		ctx, name, version, "", "", lambdabackend.InvocationTypeRequestResponse, payload,
	)

	if errors.Is(err, lambdabackend.ErrInvocationTimeout) || status == http.StatusTooManyRequests {
		return nil, "", cloudfrontbackend.ErrEdgeLimitExceeded
	}

	return out, fnErr, err
}

func (e cloudFrontLambdaEdge) EdgeVersionExists(name, version string) bool {
	_, err := e.bk.GetFunctionByQualifier(name, version)

	return err == nil
}

// wireCloudFrontDataPlane lets distributions read S3 origins and ALB origins, resolved at request time.
func wireCloudFrontDataPlane(byName map[string]service.Registerable) {
	cf, ok := byName["CloudFront"].(*cloudfrontbackend.Handler)
	if !ok {
		return
	}

	cf.SetS3Backend(func() s3backend.StorageBackend {
		if h, found := byName["S3"].(*s3backend.S3Handler); found {
			return h.Backend
		}

		return nil
	})
	cf.SetLambdaEdge(func() cloudfrontbackend.LambdaEdge {
		if h, found := byName["Lambda"].(*lambdabackend.Handler); found {
			if bk, isMem := h.Backend.(*lambdabackend.InMemoryBackend); isMem {
				return cloudFrontLambdaEdge{bk: bk}
			}
		}

		return nil
	})
	cf.SetALBForwarder(func() cloudfrontbackend.ALBForwarder {
		if h, found := byName["ELBv2"].(*elbv2backend.Handler); found {
			return cloudFrontALBForwarder{h: h}
		}

		return nil
	})
}
