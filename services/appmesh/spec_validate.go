package appmesh

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Structural validation for the shallow spec shapes (MeshSpec,
// VirtualRouterSpec, VirtualServiceSpec). VirtualNodeSpec, RouteSpec,
// VirtualGatewaySpec, and GatewayRouteSpec are deliberately left as opaque
// passthrough — their listener/TLS/matcher/retry-policy union fan-out is too
// deep to model to full field depth in one pass (see PARITY.md).
//
// Enum members and field constraints verified against
// aws-sdk-go-v2/service/appmesh@v1.38.4/types/{enums,types}.go and
// https://docs.aws.amazon.com/app-mesh/latest/APIReference/API_PortMapping.html.

func isValidEgressFilterType(t string) bool {
	return t == "ALLOW_ALL" || t == "DROP_ALL"
}

func isValidIPPreference(p string) bool {
	switch p {
	case "IPv4_ONLY", "IPv4_PREFERRED", "IPv6_ONLY", "IPv6_PREFERRED":
		return true
	default:
		return false
	}
}

func isValidPortProtocol(p string) bool {
	switch p {
	case "http", "tcp", "http2", "grpc":
		return true
	default:
		return false
	}
}

// validateMeshSpec structurally validates a MeshSpec body (types.MeshSpec).
func validateMeshSpec(spec json.RawMessage) error {
	if len(spec) == 0 {
		return nil
	}
	var body struct {
		EgressFilter *struct {
			Type string `json:"type"`
		} `json:"egressFilter"`
		ServiceDiscovery *struct {
			IPPreference string `json:"ipPreference"`
		} `json:"serviceDiscovery"`
	}
	if err := json.Unmarshal(spec, &body); err != nil {
		return awserr.Newf("spec: %s", awserr.ErrInvalidParameter, err)
	}
	if body.EgressFilter != nil && !isValidEgressFilterType(body.EgressFilter.Type) {
		return awserr.New("spec.egressFilter.type must be ALLOW_ALL or DROP_ALL", awserr.ErrInvalidParameter)
	}
	if body.ServiceDiscovery != nil && body.ServiceDiscovery.IPPreference != "" &&
		!isValidIPPreference(body.ServiceDiscovery.IPPreference) {
		return awserr.New(
			"spec.serviceDiscovery.ipPreference must be IPv4_ONLY, IPv4_PREFERRED, IPv6_ONLY, or IPv6_PREFERRED",
			awserr.ErrInvalidParameter,
		)
	}

	return nil
}

type portMappingBody struct {
	Port     *int64 `json:"port"`
	Protocol string `json:"protocol"`
}

func validatePortMapping(pm *portMappingBody) error {
	if pm == nil {
		return awserr.New("spec.listeners[].portMapping is required", awserr.ErrInvalidParameter)
	}
	if pm.Port == nil || *pm.Port < 1 || *pm.Port > 65535 {
		return awserr.New("spec.listeners[].portMapping.port must be between 1 and 65535", awserr.ErrInvalidParameter)
	}
	if !isValidPortProtocol(pm.Protocol) {
		return awserr.New(
			"spec.listeners[].portMapping.protocol must be one of http, tcp, http2, grpc",
			awserr.ErrInvalidParameter,
		)
	}

	return nil
}

// validateVirtualRouterSpec structurally validates a VirtualRouterSpec body
// (types.VirtualRouterSpec).
func validateVirtualRouterSpec(spec json.RawMessage) error {
	if len(spec) == 0 {
		return nil
	}
	var body struct {
		Listeners []struct {
			PortMapping *portMappingBody `json:"portMapping"`
		} `json:"listeners"`
	}
	if err := json.Unmarshal(spec, &body); err != nil {
		return awserr.Newf("spec: %s", awserr.ErrInvalidParameter, err)
	}
	for _, l := range body.Listeners {
		if err := validatePortMapping(l.PortMapping); err != nil {
			return err
		}
	}

	return nil
}

type virtualServiceProviderBody struct {
	VirtualNode *struct {
		VirtualNodeName *string `json:"virtualNodeName"`
	} `json:"virtualNode"`
	VirtualRouter *struct {
		VirtualRouterName *string `json:"virtualRouterName"`
	} `json:"virtualRouter"`
}

// validateVirtualServiceSpec structurally validates a VirtualServiceSpec body
// (types.VirtualServiceSpec). provider is a smithy union: exactly one of
// virtualNode/virtualRouter may be set.
func validateVirtualServiceSpec(spec json.RawMessage) error {
	if len(spec) == 0 {
		return nil
	}
	var body struct {
		Provider *virtualServiceProviderBody `json:"provider"`
	}
	if err := json.Unmarshal(spec, &body); err != nil {
		return awserr.Newf("spec: %s", awserr.ErrInvalidParameter, err)
	}
	if body.Provider == nil {
		return nil
	}

	return validateVirtualServiceProvider(body.Provider)
}

func validateVirtualServiceProvider(p *virtualServiceProviderBody) error {
	set := 0
	if p.VirtualNode != nil {
		set++
		if p.VirtualNode.VirtualNodeName == nil || *p.VirtualNode.VirtualNodeName == "" {
			return awserr.New("spec.provider.virtualNode.virtualNodeName is required", awserr.ErrInvalidParameter)
		}
	}
	if p.VirtualRouter != nil {
		set++
		if p.VirtualRouter.VirtualRouterName == nil || *p.VirtualRouter.VirtualRouterName == "" {
			return awserr.New("spec.provider.virtualRouter.virtualRouterName is required", awserr.ErrInvalidParameter)
		}
	}
	if set != 1 {
		return awserr.New(
			"spec.provider must set exactly one of virtualNode or virtualRouter",
			awserr.ErrInvalidParameter,
		)
	}

	return nil
}
