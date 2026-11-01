package app

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocifunctions "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

// GetOCIAPIGatewaySettings returns OCI API Gateway settings (ociapigateway.ConfigProvider).
func (c *CLI) GetOCIAPIGatewaySettings() ociapigateway.Settings { return c.OCIAPIGateway }

// ociAPIGatewayFunctions adapts the OCI Functions backend to the API Gateway FunctionInvoker interface.
type ociAPIGatewayFunctions struct {
	fn *ocifunctions.InMemoryBackend
}

var errAPIGatewayFnStatus = errors.New("function invocation failed")

func (f ociAPIGatewayFunctions) InvokeFunction(
	ctx context.Context, functionID string, in ociapigateway.FunctionRequest,
) (ociapigateway.FunctionResponse, error) {
	out, err := f.fn.Invoke(ctx, functionID, ocifunctions.InvokeIn{Header: in.Header, URL: in.Path, Body: in.Body})
	if err != nil {
		return ociapigateway.FunctionResponse{}, err
	}

	if out.Status == 0 {
		return ociapigateway.FunctionResponse{}, errAPIGatewayFnStatus
	}

	return ociapigateway.FunctionResponse{Status: out.Status, Header: out.Header, Body: out.Body}, nil
}

// wireOCIAPIGateway shares OCI Identity keys and compartments, validates subnets against OCI Core and
// routes function backends and authorizers to OCI Functions.
func wireOCIAPIGateway(byName map[string]service.Registerable) {
	gwH, ok := byName["OCIAPIGateway"].(*ociapigateway.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		gwH.Backend.Auth.Link(identityH.Backend.Auth)
		gwH.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		gwH.Backend.SetSubnetChecker(func(id string) bool {
			_, live := coreH.Backend.LookupSubnet(id)

			return live
		})
	}

	if fnH, found := byName["OCIFunctions"].(*ocifunctions.Handler); found {
		gwH.Backend.SetFunctionInvoker(ociAPIGatewayFunctions{fn: fnH.Backend})
	}
}
