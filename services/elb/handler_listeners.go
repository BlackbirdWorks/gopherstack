package elb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

const (
	// smtpPort is SMTP port 25.
	smtpPort = 25

	// httpPort is the standard HTTP port.
	httpPort = 80

	// httpsPort is the standard HTTPS port.
	httpsPort = 443

	// smtpsPort is the SMTPS port.
	smtpsPort = 465

	// submissionPort is the mail submission port.
	submissionPort = 587

	// minDynamicPort is the lowest user/dynamic port.
	minDynamicPort = 1024

	// maxPort is the maximum TCP/UDP port number.
	maxPort = 65535
)

func (h *Handler) handleCreateLoadBalancerListeners(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	listeners, parseErr := parseListeners(vals)
	if parseErr != nil {
		return nil, parseErr
	}

	if len(listeners) == 0 {
		return nil, fmt.Errorf("%w: at least one listener is required", ErrInvalidParameter)
	}

	if createErr := h.Backend.CreateLoadBalancerListeners(ctx, name, listeners); createErr != nil {
		return nil, createErr
	}

	return &createLoadBalancerListenersResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-createlisteners-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancerListeners(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	ports := parseListenerPorts(vals, "LoadBalancerPorts.member")

	if err := h.Backend.DeleteLoadBalancerListeners(ctx, name, ports); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerListenersResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-deletelisteners-" + name},
	}, nil
}

func (h *Handler) handleSetLoadBalancerListenerSSLCertificate(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	port, err := parseInt32(vals.Get("LoadBalancerPort"))
	if err != nil || port == 0 {
		return nil, fmt.Errorf("%w: LoadBalancerPort is required", ErrInvalidParameter)
	}

	certID := vals.Get("SSLCertificateId")
	if certID == "" {
		return nil, fmt.Errorf("%w: SSLCertificateId is required", ErrInvalidParameter)
	}

	if certErr := validateCertificateID(certID); certErr != nil {
		return nil, certErr
	}

	if setErr := h.Backend.SetLoadBalancerListenerSSLCertificate(ctx, name, port, certID); setErr != nil {
		return nil, setErr
	}

	return &setLoadBalancerListenerSSLCertificateResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-sslcert-" + name},
	}, nil
}

// parseListenerPort parses a port string and validates it against the AWS-allowed port set.
func parseListenerPort(raw, fieldName string) (int32, error) {
	port, err := parseInt32(raw)
	if err != nil || port < 1 || port > maxPort {
		return 0, fmt.Errorf(
			"%w: %s must be between 1 and 65535",
			ErrInvalidParameter, fieldName,
		)
	}

	return port, nil
}

// parseOneListener parses and validates a single listener from form values at index i.
func parseOneListener(vals url.Values, i int) (*Listener, error) {
	proto := vals.Get(fmt.Sprintf("Listeners.member.%d.Protocol", i))
	if proto == "" {
		return nil, nil //nolint:nilnil // nil signals "skip this index"
	}

	proto = strings.ToUpper(proto)

	switch proto {
	case protoHTTP, protoHTTPS, protoTCP, protoSSL:
	default:
		return nil, fmt.Errorf(
			"%w: Protocol must be one of HTTP, HTTPS, TCP, SSL",
			ErrUnsupportedProtocol,
		)
	}

	lbPort, err := parseListenerPort(
		vals.Get(fmt.Sprintf("Listeners.member.%d.LoadBalancerPort", i)),
		"LoadBalancerPort",
	)
	if err != nil {
		return nil, err
	}

	if !isAllowedListenerPort(lbPort) {
		return nil, fmt.Errorf(
			"%w: LoadBalancerPort %d is not in the allowed range "+
				"(25, 80, 443, 465, 587, or 1024-65535)",
			ErrInvalidParameter, lbPort,
		)
	}

	instProto := strings.ToUpper(
		vals.Get(fmt.Sprintf("Listeners.member.%d.InstanceProtocol", i)),
	)
	if instProto == "" {
		instProto = proto
	}

	if pairErr := validateProtocolPairing(proto, instProto); pairErr != nil {
		return nil, pairErr
	}

	instPort, err := parseListenerPort(
		vals.Get(fmt.Sprintf("Listeners.member.%d.InstancePort", i)),
		"InstancePort",
	)
	if err != nil {
		return nil, err
	}

	certID := vals.Get(fmt.Sprintf("Listeners.member.%d.SSLCertificateId", i))

	// HTTPS/SSL requires a certificate.
	if (proto == protoHTTPS || proto == protoSSL) && certID == "" {
		return nil, fmt.Errorf(
			"%w: SSLCertificateId is required for %s listeners",
			ErrInvalidParameter, proto,
		)
	}

	return &Listener{
		Protocol:         proto,
		LoadBalancerPort: lbPort,
		InstanceProtocol: instProto,
		InstancePort:     instPort,
		SSLCertificateID: certID,
	}, nil
}

// parseListeners extracts listener definitions from Listeners.member.N.* form values.
func parseListeners(vals url.Values) ([]Listener, error) {
	indexes := collectMemberIndexes(vals, "Listeners.member.")
	result := make([]Listener, 0, len(indexes))

	for _, i := range indexes {
		l, err := parseOneListener(vals, i)
		if err != nil {
			return nil, err
		}

		if l == nil {
			continue
		}

		result = append(result, *l)
	}

	return result, nil
}

// isAllowedListenerPort returns true if port is in the AWS-allowed ELB listener port set:
// 25, 80, 443, 465, 587, and 1024-65535.
func isAllowedListenerPort(port int32) bool {
	switch port {
	case smtpPort, httpPort, httpsPort, smtpsPort, submissionPort:
		return true
	}

	return port >= minDynamicPort && port <= maxPort
}

// validateProtocolPairing returns an error if the frontend/backend protocol pairing is invalid.
// AWS rules: HTTP↔HTTP/HTTPS, HTTPS↔HTTP/HTTPS, TCP↔TCP, SSL↔TCP/SSL.
func validateProtocolPairing(protocol, instanceProtocol string) error {
	valid := map[string]map[string]bool{
		protoHTTP:  {protoHTTP: true, protoHTTPS: true},
		protoHTTPS: {protoHTTP: true, protoHTTPS: true},
		protoTCP:   {protoTCP: true},
		protoSSL:   {protoTCP: true, protoSSL: true},
	}

	if allowed, ok := valid[protocol]; !ok || !allowed[instanceProtocol] {
		return fmt.Errorf(
			"%w: InstanceProtocol %q is not a valid pairing for Protocol %q",
			ErrInvalidParameter, instanceProtocol, protocol,
		)
	}

	return nil
}

// parseListenerPorts extracts integer ports from LoadBalancerPorts.member.N form values.
// Uses gap-tolerant scanning.
func parseListenerPorts(vals url.Values, prefix string) []int32 {
	indexes := collectMemberIndexes(vals, prefix+".")
	result := make([]int32, 0, len(indexes))

	for _, i := range indexes {
		v := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			continue
		}

		p, err := parseInt32(v)
		if err != nil {
			continue
		}

		if p >= 1 {
			result = append(result, p)
		}
	}

	return result
}

// certIDRe matches an ACM or IAM certificate ARN.
// ACM:  arn:aws:acm:<region>:<acct>:certificate/<uuid>
// IAM:  arn:aws:iam::<acct>:server-certificate/<name>.
var certIDRe = regexp.MustCompile(`^arn:aws:(acm|iam):`)

// validateCertificateID returns an error if certID does not look like a
// valid ACM or IAM certificate ARN.
func validateCertificateID(certID string) error {
	if !certIDRe.MatchString(certID) {
		return fmt.Errorf("%w: SSLCertificateId %q is not a valid certificate ARN", ErrInvalidParameter, certID)
	}

	return nil
}

type createLoadBalancerListenersResult struct{}

type createLoadBalancerListenersResponse struct {
	XMLName          xml.Name                          `xml:"CreateLoadBalancerListenersResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	Result           createLoadBalancerListenersResult `xml:"CreateLoadBalancerListenersResult"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
}

type deleteLoadBalancerListenersResult struct{}

type deleteLoadBalancerListenersResponse struct {
	XMLName          xml.Name                          `xml:"DeleteLoadBalancerListenersResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	Result           deleteLoadBalancerListenersResult `xml:"DeleteLoadBalancerListenersResult"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
}

type setLoadBalancerListenerSSLCertificateResult struct{}

type setLoadBalancerListenerSSLCertificateResponse struct {
	XMLName          xml.Name                                    `xml:"SetLoadBalancerListenerSSLCertificateResponse"`
	Xmlns            string                                      `xml:"xmlns,attr"`
	Result           setLoadBalancerListenerSSLCertificateResult `xml:"SetLoadBalancerListenerSSLCertificateResult"`
	ResponseMetadata xmlResponseMetadata                         `xml:"ResponseMetadata"`
}
