package elb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func (h *Handler) handleConfigureHealthCheck(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	// Check LB exists before validating the remaining parameters; AWS returns
	// LoadBalancerNotFound before complaining about invalid HC params.
	if _, err := h.Backend.DescribeLoadBalancers(ctx, []string{name}); err != nil {
		return nil, err
	}

	hc, err := parseHealthCheck(vals)
	if err != nil {
		return nil, err
	}

	result, hcErr := h.Backend.ConfigureHealthCheck(ctx, name, hc)
	if hcErr != nil {
		return nil, hcErr
	}

	return &configureHealthCheckResponse{
		Xmlns: elbXMLNS,
		Result: configureHealthCheckResult{
			HealthCheck: toXMLHealthCheck(result),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-hc-" + name},
	}, nil
}

// parseHealthCheck validates and parses health check parameters from form values.
func parseHealthCheck(vals url.Values) (HealthCheck, error) {
	target := vals.Get("HealthCheck.Target")
	if target == "" {
		return HealthCheck{}, fmt.Errorf("%w: HealthCheck.Target is required", ErrInvalidParameter)
	}

	if err := validateHealthCheckTarget(target); err != nil {
		return HealthCheck{}, err
	}

	// Normalize target protocol to uppercase (AWS stores uppercase).
	if colonIdx := strings.Index(target, ":"); colonIdx > 0 {
		target = strings.ToUpper(target[:colonIdx]) + target[colonIdx:]
	}

	// Normalize target: strip query string and fragment from HTTP/HTTPS paths.
	if colonIdx := strings.Index(target, ":"); colonIdx > 0 {
		proto := target[:colonIdx]
		rest := target[colonIdx+1:]

		switch proto {
		case protoHTTP, protoHTTPS:
			if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
				path := rest[slashIdx:]
				if qIdx := strings.IndexAny(path, "?#"); qIdx >= 0 {
					path = path[:qIdx]
				}

				target = proto + ":" + rest[:slashIdx] + path
			}
		}
	}

	interval, timeout, err := parseHealthCheckTimings(vals)
	if err != nil {
		return HealthCheck{}, err
	}

	unhealthy, healthy, err := parseHealthCheckThresholds(vals)
	if err != nil {
		return HealthCheck{}, err
	}

	return HealthCheck{
		Target:             target,
		Interval:           interval,
		Timeout:            timeout,
		UnhealthyThreshold: unhealthy,
		HealthyThreshold:   healthy,
	}, nil
}

// parseHealthCheckTimings validates and returns the Interval and Timeout parameters.
func parseHealthCheckTimings(vals url.Values) (int32, int32, error) {
	interval, parseErr := parseInt32(vals.Get("HealthCheck.Interval"))
	if parseErr != nil || interval < 5 || interval > 300 {
		return 0, 0, fmt.Errorf("%w: HealthCheck.Interval must be between 5 and 300", ErrInvalidParameter)
	}

	timeout, parseErr := parseInt32(vals.Get("HealthCheck.Timeout"))
	if parseErr != nil || timeout < 2 || timeout > 60 {
		return 0, 0, fmt.Errorf("%w: HealthCheck.Timeout must be between 2 and 60", ErrInvalidParameter)
	}

	if timeout >= interval {
		return 0, 0, fmt.Errorf(
			"%w: HealthCheck.Timeout must be less than HealthCheck.Interval",
			ErrInvalidParameter,
		)
	}

	return interval, timeout, nil
}

// parseHealthCheckThresholds validates and returns UnhealthyThreshold and HealthyThreshold.
func parseHealthCheckThresholds(vals url.Values) (int32, int32, error) {
	unhealthy, parseErr := parseInt32(vals.Get("HealthCheck.UnhealthyThreshold"))
	if parseErr != nil || unhealthy < 2 || unhealthy > 10 {
		return 0, 0, fmt.Errorf(
			"%w: HealthCheck.UnhealthyThreshold must be between 2 and 10",
			ErrInvalidParameter,
		)
	}

	healthy, parseErr := parseInt32(vals.Get("HealthCheck.HealthyThreshold"))
	if parseErr != nil || healthy < 2 || healthy > 10 {
		return 0, 0, fmt.Errorf(
			"%w: HealthCheck.HealthyThreshold must be between 2 and 10",
			ErrInvalidParameter,
		)
	}

	return unhealthy, healthy, nil
}

// validateHealthCheckTarget validates the HealthCheck Target format expected by AWS:
// PROTOCOL:PORT for TCP/SSL or PROTOCOL:PORT/PATH for HTTP/HTTPS.
// validateHealthCheckHTTPTarget validates the port/path portion of an HTTP(S)
// health-check target string (everything after the colon).
func validateHealthCheckHTTPTarget(rest string) error {
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 1 {
		return fmt.Errorf(
			"%w: HealthCheck.Target for HTTP/HTTPS must include a path (e.g. HTTP:80/health)",
			ErrInvalidParameter,
		)
	}

	if err := validateTargetPort(rest[:slashIdx]); err != nil {
		return err
	}

	path := rest[slashIdx:]

	const maxPathLen = 1024
	if len(path) > maxPathLen {
		return fmt.Errorf(
			"%w: HealthCheck.Target path must not exceed %d characters",
			ErrInvalidParameter,
			maxPathLen,
		)
	}

	for _, ch := range path {
		if ch == '\r' || ch == '\n' || ch == ' ' {
			return fmt.Errorf(
				"%w: HealthCheck.Target path contains invalid whitespace or control characters",
				ErrInvalidParameter,
			)
		}
	}

	return nil
}

// validateTargetPort parses and validates a port string for a health-check target.
func validateTargetPort(portStr string) error {
	p, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil || p < 1 || p > maxPort {
		return fmt.Errorf(
			"%w: HealthCheck.Target port must be between 1 and 65535",
			ErrInvalidParameter,
		)
	}

	return nil
}

func validateHealthCheckTarget(target string) error {
	colonIdx := strings.Index(target, ":")
	if colonIdx < 1 {
		return fmt.Errorf(
			"%w: HealthCheck.Target must be in the format PROTOCOL:PORT or PROTOCOL:PORT/PATH",
			ErrInvalidParameter,
		)
	}

	proto := strings.ToUpper(target[:colonIdx])
	rest := target[colonIdx+1:]

	switch proto {
	case protoHTTP, protoHTTPS:
		return validateHealthCheckHTTPTarget(rest)
	case protoTCP, protoSSL:
		return validateTargetPort(rest)
	default:
		return fmt.Errorf(
			"%w: HealthCheck.Target protocol must be one of HTTP, HTTPS, TCP, SSL",
			ErrInvalidParameter,
		)
	}
}

type xmlHealthCheck struct {
	Target             string `xml:"Target"`
	Interval           int32  `xml:"Interval"`
	Timeout            int32  `xml:"Timeout"`
	UnhealthyThreshold int32  `xml:"UnhealthyThreshold"`
	HealthyThreshold   int32  `xml:"HealthyThreshold"`
}

func toXMLHealthCheck(hc *HealthCheck) xmlHealthCheck {
	return xmlHealthCheck{
		Target:             hc.Target,
		Interval:           hc.Interval,
		Timeout:            hc.Timeout,
		UnhealthyThreshold: hc.UnhealthyThreshold,
		HealthyThreshold:   hc.HealthyThreshold,
	}
}

type configureHealthCheckResult struct {
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type configureHealthCheckResponse struct {
	XMLName          xml.Name                   `xml:"ConfigureHealthCheckResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           configureHealthCheckResult `xml:"ConfigureHealthCheckResult"`
}
