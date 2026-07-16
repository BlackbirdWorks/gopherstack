package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Traffic Mirror Filter (HTTP dispatch) ----

// TestHTTP_TrafficMirrorFilter verifies the Traffic Mirror filter family is
// served by the real handlers (not the generic stub) end to end through the
// HTTP dispatch table: CRUD round-trip, list, and not-found.
func TestHTTP_TrafficMirrorFilter(t *testing.T) { //nolint:paralleltest // shares handler state across subtests.
	h := newHandler()

	var filterID string

	t.Run("create", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := postForm(t, h, "Action=CreateTrafficMirrorFilter&Version=2016-11-15&Description=my-filter")
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "CreateTrafficMirrorFilterResponse")
		assert.Contains(t, rec.Body.String(), "tmf-")
		assert.Contains(t, rec.Body.String(), "my-filter")

		b, ok := h.Backend.(*ec2.InMemoryBackend)
		require.True(t, ok)
		filters := b.DescribeTrafficMirrorFilters(nil)
		require.Len(t, filters, 1)
		filterID = filters[0].TrafficMirrorFilterID
	})

	t.Run("describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DescribeTrafficMirrorFilters&Version=2016-11-15&TrafficMirrorFilterId.1=" + filterID
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "trafficMirrorFilterSet")
		assert.Contains(t, rec.Body.String(), filterID)
	})

	t.Run("modify", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=ModifyTrafficMirrorFilterNetworkServices&Version=2016-11-15" +
			"&TrafficMirrorFilterId=" + filterID + "&AddNetworkService.1=amazon-dns"
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)

		describe := "Action=DescribeTrafficMirrorFilters&Version=2016-11-15&TrafficMirrorFilterId.1=" + filterID
		rec = postForm(t, h, describe)
		assert.Contains(t, rec.Body.String(), "networkServiceSet")
		assert.Contains(t, rec.Body.String(), "amazon-dns")
	})

	t.Run("delete", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := postForm(t, h, "Action=DeleteTrafficMirrorFilter&Version=2016-11-15&TrafficMirrorFilterId="+filterID)
		require.Equal(t, http.StatusOK, rec.Code)

		describe := "Action=DescribeTrafficMirrorFilters&Version=2016-11-15&TrafficMirrorFilterId.1=" + filterID
		rec = postForm(t, h, describe)
		assert.NotContains(t, rec.Body.String(), filterID)
	})

	t.Run("delete not found", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DeleteTrafficMirrorFilter&Version=2016-11-15&TrafficMirrorFilterId=tmf-nonexistent"
		rec := postForm(t, h, body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidTrafficMirrorFilterId.NotFound")
	})
}

// ---- Traffic Mirror Filter Rule (HTTP dispatch) ----

func TestHTTP_TrafficMirrorFilterRule(t *testing.T) { //nolint:paralleltest // shares handler state across subtests.
	h := newHandler()

	setupRec := postForm(t, h, "Action=CreateTrafficMirrorFilter&Version=2016-11-15&Description=rule-filter")
	require.Equal(t, http.StatusOK, setupRec.Code)

	b, ok := h.Backend.(*ec2.InMemoryBackend)
	require.True(t, ok)
	filters := b.DescribeTrafficMirrorFilters(nil)
	require.Len(t, filters, 1)
	filterID := filters[0].TrafficMirrorFilterID

	var ruleID string

	t.Run("create with port ranges", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=CreateTrafficMirrorFilterRule&Version=2016-11-15" +
			"&TrafficMirrorFilterId=" + filterID +
			"&TrafficDirection=ingress&RuleAction=accept" +
			"&SourceCidrBlock=10.0.0.0/8&DestinationCidrBlock=0.0.0.0/0" +
			"&RuleNumber=100&Protocol=6" +
			"&SourcePortRange.FromPort=1&SourcePortRange.ToPort=1024" +
			"&DestinationPortRange.FromPort=80&DestinationPortRange.ToPort=80"
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
		respBody := rec.Body.String()
		assert.Contains(t, respBody, "CreateTrafficMirrorFilterRuleResponse")
		assert.Contains(t, respBody, "tmfr-")
		assert.Contains(t, respBody, "<sourcePortRange>")
		assert.Contains(t, respBody, "<destinationPortRange>")
		assert.Contains(t, respBody, "<fromPort>1</fromPort>")
		assert.Contains(t, respBody, "<toPort>80</toPort>")

		rules, rerr := b.DescribeTrafficMirrorFilterRules(filterID)
		require.NoError(t, rerr)
		require.Len(t, rules, 1)
		ruleID = rules[0].TrafficMirrorFilterRuleID
	})

	t.Run("describe for filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DescribeTrafficMirrorFilterRules&Version=2016-11-15&TrafficMirrorFilterId=" + filterID
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), ruleID)
	})

	t.Run("modify", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=ModifyTrafficMirrorFilterRule&Version=2016-11-15" +
			"&TrafficMirrorFilterRuleId=" + ruleID + "&RuleAction=reject"
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("delete", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DeleteTrafficMirrorFilterRule&Version=2016-11-15&TrafficMirrorFilterRuleId=" + ruleID
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)

		rules, rerr := b.DescribeTrafficMirrorFilterRules(filterID)
		require.NoError(t, rerr)
		assert.Empty(t, rules)
	})

	t.Run("delete not found", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DeleteTrafficMirrorFilterRule&Version=2016-11-15&TrafficMirrorFilterRuleId=tmfr-nonexistent"
		rec := postForm(t, h, body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidTrafficMirrorFilterRuleId.NotFound")
	})

	t.Run("describe for filter not found", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DescribeTrafficMirrorFilterRules&Version=2016-11-15&TrafficMirrorFilterId=tmf-nonexistent"
		rec := postForm(t, h, body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidTrafficMirrorFilterId.NotFound")
	})
}

// ---- Traffic Mirror Session (HTTP dispatch) ----

func TestHTTP_TrafficMirrorSession(t *testing.T) { //nolint:paralleltest // shares handler state across subtests.
	h := newHandler()

	var sessionID string

	t.Run("create with packet length", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=CreateTrafficMirrorSession&Version=2016-11-15" +
			"&NetworkInterfaceId=eni-12345678&TrafficMirrorTargetId=tmt-abc123" +
			"&TrafficMirrorFilterId=tmf-abc123&SessionNumber=1&PacketLength=120" +
			"&Description=my-session"
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
		respBody := rec.Body.String()
		assert.Contains(t, respBody, "CreateTrafficMirrorSessionResponse")
		assert.Contains(t, respBody, "tms-")
		assert.Contains(t, respBody, "<packetLength>120</packetLength>")
		assert.Contains(t, respBody, "<ownerId>000000000000</ownerId>")
		assert.Contains(t, respBody, "<virtualNetworkId>")

		b, ok := h.Backend.(*ec2.InMemoryBackend)
		require.True(t, ok)
		sessions := b.DescribeTrafficMirrorSessions(nil)
		require.Len(t, sessions, 1)
		sessionID = sessions[0].TrafficMirrorSessionID
	})

	t.Run("describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DescribeTrafficMirrorSessions&Version=2016-11-15&TrafficMirrorSessionId.1=" + sessionID
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), sessionID)
	})

	t.Run("modify target", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=ModifyTrafficMirrorSession&Version=2016-11-15" +
			"&TrafficMirrorSessionId=" + sessionID + "&TrafficMirrorTargetId=tmt-new"
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)

		describe := "Action=DescribeTrafficMirrorSessions&Version=2016-11-15&TrafficMirrorSessionId.1=" + sessionID
		rec = postForm(t, h, describe)
		assert.Contains(t, rec.Body.String(), "tmt-new")
	})

	t.Run("delete", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DeleteTrafficMirrorSession&Version=2016-11-15&TrafficMirrorSessionId=" + sessionID
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)

		describe := "Action=DescribeTrafficMirrorSessions&Version=2016-11-15&TrafficMirrorSessionId.1=" + sessionID
		rec = postForm(t, h, describe)
		assert.NotContains(t, rec.Body.String(), sessionID)
	})

	t.Run("delete not found", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DeleteTrafficMirrorSession&Version=2016-11-15&TrafficMirrorSessionId=tms-nonexistent"
		rec := postForm(t, h, body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidTrafficMirrorSessionId.NotFound")
	})
}

// ---- Traffic Mirror Target (HTTP dispatch) ----

func TestHTTP_TrafficMirrorTarget(t *testing.T) { //nolint:paralleltest // shares handler state across subtests.
	h := newHandler()

	t.Run("create with network interface", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=CreateTrafficMirrorTarget&Version=2016-11-15&NetworkInterfaceId=eni-12345678"
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
		respBody := rec.Body.String()
		assert.Contains(t, respBody, "tmt-")
		assert.Contains(t, respBody, "<type>network-interface</type>")
		assert.Contains(t, respBody, "<ownerId>000000000000</ownerId>")
	})

	t.Run("create with gateway load balancer endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=CreateTrafficMirrorTarget&Version=2016-11-15&GatewayLoadBalancerEndpointId=vpce-12345678"
		rec := postForm(t, h, body)
		require.Equal(t, http.StatusOK, rec.Code)
		respBody := rec.Body.String()
		assert.Contains(t, respBody, "<gatewayLoadBalancerEndpointId>vpce-12345678</gatewayLoadBalancerEndpointId>")
		assert.Contains(t, respBody, "<type>gateway-load-balancer-endpoint</type>")
	})

	t.Run("describe all lists both", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := postForm(t, h, "Action=DescribeTrafficMirrorTargets&Version=2016-11-15")
		require.Equal(t, http.StatusOK, rec.Code)

		b, ok := h.Backend.(*ec2.InMemoryBackend)
		require.True(t, ok)
		targets := b.DescribeTrafficMirrorTargets(nil)
		assert.Len(t, targets, 2)
	})

	t.Run("delete not found", func(t *testing.T) { //nolint:paralleltest // existing issue.
		body := "Action=DeleteTrafficMirrorTarget&Version=2016-11-15&TrafficMirrorTargetId=tmt-nonexistent"
		rec := postForm(t, h, body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidTrafficMirrorTargetId.NotFound")
	})
}

func TestTrafficMirrorFilter(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var filterID string

	t.Run("create filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		f, err := b.CreateTrafficMirrorFilter("test filter")
		require.NoError(t, err)
		assert.NotEmpty(t, f.TrafficMirrorFilterID)
		assert.Equal(t, "test filter", f.Description)
		filterID = f.TrafficMirrorFilterID
	})

	t.Run("describe returns created filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		require.Len(t, filters, 1)
		assert.Equal(t, "test filter", filters[0].Description)
	})

	t.Run("describe all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		filters := b.DescribeTrafficMirrorFilters(nil)
		assert.NotEmpty(t, filters)
	})

	t.Run("modify network services add", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyTrafficMirrorFilterNetworkServices(filterID, []string{"amazon-dns"}, nil))
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		require.Len(t, filters, 1)
		assert.Contains(t, filters[0].NetworkServices, "amazon-dns")
	})

	t.Run("modify network services remove", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyTrafficMirrorFilterNetworkServices(filterID, nil, []string{"amazon-dns"}))
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		require.Len(t, filters, 1)
		assert.Empty(t, filters[0].NetworkServices)
	})

	t.Run("delete filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTrafficMirrorFilter(filterID))
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		assert.Empty(t, filters)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteTrafficMirrorFilter("tmf-nonexistent"))
	})

	t.Run("modify non-existent filter returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyTrafficMirrorFilterNetworkServices("tmf-nonexistent", nil, nil))
	})
}

// ---- Traffic Mirror Filter Rule ----

func TestTrafficMirrorFilterRule(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	f, ferr := b.CreateTrafficMirrorFilter("filter-for-rules")
	require.NoError(t, ferr)
	filterID := f.TrafficMirrorFilterID

	var ruleID string

	t.Run("create ingress rule", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rule, err := b.CreateTrafficMirrorFilterRule(
			filterID, "ingress", "accept",
			"10.0.0.0/8", "0.0.0.0/0", "ingress rule",
			100, 6,
		)
		require.NoError(t, err)
		assert.NotEmpty(t, rule.TrafficMirrorFilterRuleID)
		assert.Equal(t, "ingress", rule.TrafficDirection)
		assert.Equal(t, "accept", rule.RuleAction)
		ruleID = rule.TrafficMirrorFilterRuleID
	})

	t.Run("describe rules returns created rule", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rules, err := b.DescribeTrafficMirrorFilterRules(filterID)
		require.NoError(t, err)
		require.Len(t, rules, 1)
		assert.Equal(t, ruleID, rules[0].TrafficMirrorFilterRuleID)
	})

	t.Run("create egress rule", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateTrafficMirrorFilterRule(
			filterID, "egress", "reject",
			"0.0.0.0/0", "0.0.0.0/0", "egress rule",
			200, 0,
		)
		require.NoError(t, err)

		rules, err := b.DescribeTrafficMirrorFilterRules(filterID)
		require.NoError(t, err)
		assert.Len(t, rules, 2)
	})

	t.Run("modify rule", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyTrafficMirrorFilterRule(ruleID, "reject", "modified"))
	})

	t.Run("delete rule", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTrafficMirrorFilterRule(ruleID))
		rules, err := b.DescribeTrafficMirrorFilterRules(filterID)
		require.NoError(t, err)
		assert.Len(t, rules, 1)
	})

	t.Run( //nolint:paralleltest // existing issue.
		"create rule on non-existent filter returns error",
		func(t *testing.T) {
			_, err := b.CreateTrafficMirrorFilterRule(
				"tmf-nonexistent", "ingress", "accept",
				"0.0.0.0/0", "0.0.0.0/0", "", 1, 0,
			)
			require.Error(t, err)
		},
	)

	t.Run("delete non-existent rule returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteTrafficMirrorFilterRule("tmfr-nonexistent"))
	})
}

// ---- Traffic Mirror Target ----.

// ---- Traffic Mirror Target ----.
func TestTrafficMirrorTarget(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var targetID string

	t.Run("create target with network interface", func(t *testing.T) { //nolint:paralleltest // existing issue.
		target, err := b.CreateTrafficMirrorTarget("eni-12345678", "", "test target")
		require.NoError(t, err)
		assert.NotEmpty(t, target.TrafficMirrorTargetID)
		assert.Equal(t, "eni-12345678", target.NetworkInterfaceID)
		targetID = target.TrafficMirrorTargetID
	})

	t.Run("describe returns created target", func(t *testing.T) { //nolint:paralleltest // existing issue.
		targets := b.DescribeTrafficMirrorTargets([]string{targetID})
		require.Len(t, targets, 1)
		assert.Equal(t, "test target", targets[0].Description)
	})

	t.Run("create target with nlb arn", func(t *testing.T) { //nolint:paralleltest // existing issue.
		target, err := b.CreateTrafficMirrorTarget(
			"",
			"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/test/abc",
			"nlb target",
		)
		require.NoError(t, err)
		assert.NotEmpty(t, target.TrafficMirrorTargetID)
		assert.NotEmpty(t, target.NetworkLoadBalancerArn)
	})

	t.Run("describe all targets", func(t *testing.T) { //nolint:paralleltest // existing issue.
		targets := b.DescribeTrafficMirrorTargets(nil)
		assert.Len(t, targets, 2)
	})

	t.Run("delete target", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTrafficMirrorTarget(targetID))
		targets := b.DescribeTrafficMirrorTargets(nil)
		assert.Len(t, targets, 1)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteTrafficMirrorTarget("tmt-nonexistent"))
	})
}

// ---- Traffic Mirror Session ----.

// ---- Traffic Mirror Session ----.
func TestTrafficMirrorSession(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var sessionID string

	t.Run("create session", func(t *testing.T) { //nolint:paralleltest // existing issue.
		s, err := b.CreateTrafficMirrorSession("eni-12345678", "tmt-abc123", "tmf-abc123", "test session", 1)
		require.NoError(t, err)
		assert.NotEmpty(t, s.TrafficMirrorSessionID)
		assert.Equal(t, 1, s.SessionNumber)
		assert.Equal(t, "test session", s.Description)
		sessionID = s.TrafficMirrorSessionID
	})

	t.Run("describe returns created session", func(t *testing.T) { //nolint:paralleltest // existing issue.
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		require.Len(t, sessions, 1)
		assert.Equal(t, "eni-12345678", sessions[0].NetworkInterfaceID)
	})

	t.Run("modify session description", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyTrafficMirrorSession(sessionID, "", "", "modified"))
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		require.Len(t, sessions, 1)
		assert.Equal(t, "modified", sessions[0].Description)
	})

	t.Run("modify session target", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyTrafficMirrorSession(sessionID, "tmt-new", "", ""))
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		require.Len(t, sessions, 1)
		assert.Equal(t, "tmt-new", sessions[0].TrafficMirrorTargetID)
	})

	t.Run("delete session", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTrafficMirrorSession(sessionID))
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		assert.Empty(t, sessions)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteTrafficMirrorSession("tms-nonexistent"))
	})

	t.Run("modify non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyTrafficMirrorSession("tms-nonexistent", "", "", "x"))
	})
}

// ---- EC2 Fleet ----.
