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
