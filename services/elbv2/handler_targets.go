package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// resolveTargetGroupPort looks up a target group's configured port, used to
// default omitted Targets.member.N.Port values. AWS treats Port as optional
// for instance/ip/alb target types and defaults it to the target group's port;
// Lambda target groups have no port (tgPort is 0 and no defaulting occurs).
func (h *Handler) resolveTargetGroupPort(tgArn string) (int32, error) {
	tgs, err := h.Backend.DescribeTargetGroups([]string{tgArn}, nil, "")
	if err != nil {
		return 0, err
	}

	if len(tgs) == 0 {
		return 0, ErrTargetGroupNotFound
	}

	return tgs[0].Port, nil
}

// defaultTargetPorts fills in omitted (zero-value) target ports with the
// target group's port. A zero tgPort (e.g. Lambda target groups) is a no-op.
func defaultTargetPorts(targets []Target, tgPort int32) []Target {
	if tgPort == 0 {
		return targets
	}

	out := make([]Target, len(targets))

	for i, t := range targets {
		if t.Port == 0 {
			t.Port = tgPort
		}

		out[i] = t
	}

	return out
}

func (h *Handler) handleRegisterTargets(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	targets := parseTargets(vals, "Targets.member")

	tgPort, err := h.resolveTargetGroupPort(tgArn)
	if err != nil {
		return nil, err
	}

	targets = defaultTargetPorts(targets, tgPort)

	if regErr := h.Backend.RegisterTargets(tgArn, targets); regErr != nil {
		return nil, regErr
	}

	return &registerTargetsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-register-targets"},
	}, nil
}

func (h *Handler) handleDeregisterTargets(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	targets := parseTargets(vals, "Targets.member")

	tgPort, err := h.resolveTargetGroupPort(tgArn)
	if err != nil {
		return nil, err
	}

	targets = defaultTargetPorts(targets, tgPort)

	if deregErr := h.Backend.DeregisterTargets(tgArn, targets); deregErr != nil {
		return nil, deregErr
	}

	return &deregisterTargetsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-deregister-targets"},
	}, nil
}

func (h *Handler) handleDescribeTargetHealth(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	targets, err := h.Backend.DescribeTargetHealth(tgArn)
	if err != nil {
		return nil, err
	}

	// When specific targets are requested, include only those targets.
	// Targets that are requested but not registered get state "unused" with
	// reason "Target.NotRegistered", matching real AWS behaviour.
	requestedTargets := parseTargets(vals, "Targets.member")
	if len(requestedTargets) > 0 {
		if tgPort, pErr := h.resolveTargetGroupPort(tgArn); pErr == nil {
			requestedTargets = defaultTargetPorts(requestedTargets, tgPort)
		}

		registeredMap := make(map[string]TargetHealthDescription, len(targets))
		for _, t := range targets {
			registeredMap[t.Target.ID+":"+strconv.Itoa(int(t.Target.Port))] = t
		}

		filtered := make([]TargetHealthDescription, 0, len(requestedTargets))
		for _, rt := range requestedTargets {
			key := rt.ID + ":" + strconv.Itoa(int(rt.Port))
			if registered, ok := registeredMap[key]; ok {
				filtered = append(filtered, registered)
			} else {
				filtered = append(filtered, TargetHealthDescription{
					Target:       rt,
					HealthState:  "unused",
					HealthReason: "Target.NotRegistered",
				})
			}
		}

		targets = filtered
	}

	members := make([]xmlTargetHealthDescription, 0, len(targets))
	for _, t := range targets {
		members = append(members, xmlTargetHealthDescription{
			Target: xmlTargetDescription{ID: t.Target.ID, Port: t.Target.Port},
			TargetHealth: xmlTargetHealth{
				State:  t.HealthState,
				Reason: t.HealthReason,
			},
		})
	}

	return &describeTargetHealthResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetHealthResult{
			TargetHealthDescriptions: xmlTargetHealthDescriptionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-target-health"},
	}, nil
}

func parseTargets(vals url.Values, prefix string) []Target {
	result := make([]Target, 0)

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("%s.%d.Id", prefix, i))
		if id == "" {
			break
		}

		port, _ := parseInt32(vals.Get(fmt.Sprintf("%s.%d.Port", prefix, i)))

		result = append(result, Target{ID: id, Port: port})
	}

	return result
}

type xmlTargetDescription struct {
	ID   string `xml:"Id"`
	Port int32  `xml:"Port,omitempty"`
}

type xmlTargetHealth struct {
	State       string `xml:"State"`
	Reason      string `xml:"Reason,omitempty"`
	Description string `xml:"Description,omitempty"`
}

type xmlTargetHealthDescription struct {
	TargetHealth xmlTargetHealth      `xml:"TargetHealth"`
	Target       xmlTargetDescription `xml:"Target"`
}

type xmlTargetHealthDescriptionList struct {
	Members []xmlTargetHealthDescription `xml:"member"`
}

type registerTargetsResponse struct {
	Result           struct{}            `xml:"RegisterTargetsResult"`
	XMLName          xml.Name            `xml:"RegisterTargetsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deregisterTargetsResponse struct {
	Result           struct{}            `xml:"DeregisterTargetsResult"`
	XMLName          xml.Name            `xml:"DeregisterTargetsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeTargetHealthResult struct {
	TargetHealthDescriptions xmlTargetHealthDescriptionList `xml:"TargetHealthDescriptions"`
}

type describeTargetHealthResponse struct {
	XMLName          xml.Name                   `xml:"DescribeTargetHealthResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           describeTargetHealthResult `xml:"DescribeTargetHealthResult"`
}
