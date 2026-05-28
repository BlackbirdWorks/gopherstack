package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Traffic Mirror Filter ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_TrafficMirrorFilter(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var filterID string

	t.Run("create filter", func(t *testing.T) {
		f, err := b.CreateTrafficMirrorFilter("test filter")
		require.NoError(t, err)
		assert.NotEmpty(t, f.TrafficMirrorFilterID)
		assert.Equal(t, "test filter", f.Description)
		filterID = f.TrafficMirrorFilterID
	})

	t.Run("describe returns created filter", func(t *testing.T) {
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		require.Len(t, filters, 1)
		assert.Equal(t, "test filter", filters[0].Description)
	})

	t.Run("describe all", func(t *testing.T) {
		filters := b.DescribeTrafficMirrorFilters(nil)
		assert.NotEmpty(t, filters)
	})

	t.Run("modify network services add", func(t *testing.T) {
		require.NoError(t, b.ModifyTrafficMirrorFilterNetworkServices(filterID, []string{"amazon-dns"}, nil))
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		require.Len(t, filters, 1)
		assert.Contains(t, filters[0].NetworkServices, "amazon-dns")
	})

	t.Run("modify network services remove", func(t *testing.T) {
		require.NoError(t, b.ModifyTrafficMirrorFilterNetworkServices(filterID, nil, []string{"amazon-dns"}))
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		require.Len(t, filters, 1)
		assert.Empty(t, filters[0].NetworkServices)
	})

	t.Run("delete filter", func(t *testing.T) {
		require.NoError(t, b.DeleteTrafficMirrorFilter(filterID))
		filters := b.DescribeTrafficMirrorFilters([]string{filterID})
		assert.Empty(t, filters)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) {
		require.Error(t, b.DeleteTrafficMirrorFilter("tmf-nonexistent"))
	})

	t.Run("modify non-existent filter returns error", func(t *testing.T) {
		require.Error(t, b.ModifyTrafficMirrorFilterNetworkServices("tmf-nonexistent", nil, nil))
	})
}

// ---- Traffic Mirror Filter Rule ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_TrafficMirrorFilterRule(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	f, err := b.CreateTrafficMirrorFilter("filter-for-rules")
	require.NoError(t, err)
	filterID := f.TrafficMirrorFilterID

	var ruleID string

	t.Run("create ingress rule", func(t *testing.T) {
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

	t.Run("describe rules returns created rule", func(t *testing.T) {
		rules, err := b.DescribeTrafficMirrorFilterRules(filterID)
		require.NoError(t, err)
		require.Len(t, rules, 1)
		assert.Equal(t, ruleID, rules[0].TrafficMirrorFilterRuleID)
	})

	t.Run("create egress rule", func(t *testing.T) {
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

	t.Run("modify rule", func(t *testing.T) {
		require.NoError(t, b.ModifyTrafficMirrorFilterRule(ruleID, "reject", "modified"))
	})

	t.Run("delete rule", func(t *testing.T) {
		require.NoError(t, b.DeleteTrafficMirrorFilterRule(ruleID))
		rules, err := b.DescribeTrafficMirrorFilterRules(filterID)
		require.NoError(t, err)
		assert.Len(t, rules, 1)
	})

	t.Run("create rule on non-existent filter returns error", func(t *testing.T) {
		_, err := b.CreateTrafficMirrorFilterRule(
			"tmf-nonexistent", "ingress", "accept",
			"0.0.0.0/0", "0.0.0.0/0", "", 1, 0,
		)
		require.Error(t, err)
	})

	t.Run("delete non-existent rule returns error", func(t *testing.T) {
		require.Error(t, b.DeleteTrafficMirrorFilterRule("tmfr-nonexistent"))
	})
}

// ---- Traffic Mirror Target ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_TrafficMirrorTarget(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var targetID string

	t.Run("create target with network interface", func(t *testing.T) {
		target, err := b.CreateTrafficMirrorTarget("eni-12345678", "", "test target")
		require.NoError(t, err)
		assert.NotEmpty(t, target.TrafficMirrorTargetID)
		assert.Equal(t, "eni-12345678", target.NetworkInterfaceID)
		targetID = target.TrafficMirrorTargetID
	})

	t.Run("describe returns created target", func(t *testing.T) {
		targets := b.DescribeTrafficMirrorTargets([]string{targetID})
		require.Len(t, targets, 1)
		assert.Equal(t, "test target", targets[0].Description)
	})

	t.Run("create target with nlb arn", func(t *testing.T) {
		target, err := b.CreateTrafficMirrorTarget(
			"",
			"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/net/test/abc",
			"nlb target",
		)
		require.NoError(t, err)
		assert.NotEmpty(t, target.TrafficMirrorTargetID)
		assert.NotEmpty(t, target.NetworkLoadBalancerArn)
	})

	t.Run("describe all targets", func(t *testing.T) {
		targets := b.DescribeTrafficMirrorTargets(nil)
		assert.Len(t, targets, 2)
	})

	t.Run("delete target", func(t *testing.T) {
		require.NoError(t, b.DeleteTrafficMirrorTarget(targetID))
		targets := b.DescribeTrafficMirrorTargets(nil)
		assert.Len(t, targets, 1)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) {
		require.Error(t, b.DeleteTrafficMirrorTarget("tmt-nonexistent"))
	})
}

// ---- Traffic Mirror Session ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_TrafficMirrorSession(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var sessionID string

	t.Run("create session", func(t *testing.T) {
		s, err := b.CreateTrafficMirrorSession("eni-12345678", "tmt-abc123", "tmf-abc123", "test session", 1)
		require.NoError(t, err)
		assert.NotEmpty(t, s.TrafficMirrorSessionID)
		assert.Equal(t, 1, s.SessionNumber)
		assert.Equal(t, "test session", s.Description)
		sessionID = s.TrafficMirrorSessionID
	})

	t.Run("describe returns created session", func(t *testing.T) {
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		require.Len(t, sessions, 1)
		assert.Equal(t, "eni-12345678", sessions[0].NetworkInterfaceID)
	})

	t.Run("modify session description", func(t *testing.T) {
		require.NoError(t, b.ModifyTrafficMirrorSession(sessionID, "", "", "modified"))
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		require.Len(t, sessions, 1)
		assert.Equal(t, "modified", sessions[0].Description)
	})

	t.Run("modify session target", func(t *testing.T) {
		require.NoError(t, b.ModifyTrafficMirrorSession(sessionID, "tmt-new", "", ""))
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		require.Len(t, sessions, 1)
		assert.Equal(t, "tmt-new", sessions[0].TrafficMirrorTargetID)
	})

	t.Run("delete session", func(t *testing.T) {
		require.NoError(t, b.DeleteTrafficMirrorSession(sessionID))
		sessions := b.DescribeTrafficMirrorSessions([]string{sessionID})
		assert.Empty(t, sessions)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) {
		require.Error(t, b.DeleteTrafficMirrorSession("tms-nonexistent"))
	})

	t.Run("modify non-existent returns error", func(t *testing.T) {
		require.Error(t, b.ModifyTrafficMirrorSession("tms-nonexistent", "", "", "x"))
	})
}

// ---- EC2 Fleet ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_Fleet(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var fleetID string

	t.Run("create fleet", func(t *testing.T) {
		f, err := b.CreateFleet("maintain", 5)
		require.NoError(t, err)
		assert.NotEmpty(t, f.FleetID)
		assert.Equal(t, "active", f.FleetState)
		assert.Equal(t, "maintain", f.FleetType)
		assert.Equal(t, 5, f.TotalTargetCapacity)
		fleetID = f.FleetID
	})

	t.Run("describe returns created fleet", func(t *testing.T) {
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		assert.Equal(t, "active", fleets[0].FleetState)
	})

	t.Run("describe all fleets", func(t *testing.T) {
		fleets := b.DescribeFleets(nil)
		assert.NotEmpty(t, fleets)
	})

	t.Run("modify fleet capacity", func(t *testing.T) {
		require.NoError(t, b.ModifyFleet(fleetID, 10, ""))
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		assert.Equal(t, 10, fleets[0].TotalTargetCapacity)
	})

	t.Run("modify fleet excess policy", func(t *testing.T) {
		require.NoError(t, b.ModifyFleet(fleetID, 0, "no-termination"))
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		assert.Equal(t, "no-termination", fleets[0].ExcessCapacityTerminationPolicy)
	})

	t.Run("delete fleet", func(t *testing.T) {
		deleted := b.DeleteFleets([]string{fleetID})
		assert.Len(t, deleted, 1)
		assert.Equal(t, fleetID, deleted[0])
		fleets := b.DescribeFleets([]string{fleetID})
		assert.Empty(t, fleets)
	})

	t.Run("delete non-existent fleet returns empty", func(t *testing.T) {
		deleted := b.DeleteFleets([]string{"fleet-nonexistent"})
		assert.Empty(t, deleted)
	})

	t.Run("modify non-existent returns error", func(t *testing.T) {
		require.Error(t, b.ModifyFleet("fleet-nonexistent", 1, ""))
	})

	t.Run("create fleet with default type", func(t *testing.T) {
		f, err := b.CreateFleet("", 1)
		require.NoError(t, err)
		assert.Equal(t, "maintain", f.FleetType)
	})
}

// ---- Network Insights Path ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_NetworkInsightsPath(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var pathID string

	t.Run("create path", func(t *testing.T) {
		p, err := b.CreateNetworkInsightsPath("eni-src", "eni-dst", "tcp", 80)
		require.NoError(t, err)
		assert.NotEmpty(t, p.NetworkInsightsPathID)
		assert.NotEmpty(t, p.NetworkInsightsPathArn)
		assert.Equal(t, "eni-src", p.SourceID)
		assert.Equal(t, "tcp", p.Protocol)
		assert.Equal(t, 80, p.DestinationPort)
		pathID = p.NetworkInsightsPathID
	})

	t.Run("describe returns created path", func(t *testing.T) {
		paths := b.DescribeNetworkInsightsPaths([]string{pathID})
		require.Len(t, paths, 1)
		assert.Equal(t, "eni-dst", paths[0].DestinationID)
	})

	t.Run("describe all paths", func(t *testing.T) {
		paths := b.DescribeNetworkInsightsPaths(nil)
		assert.NotEmpty(t, paths)
	})

	t.Run("delete path", func(t *testing.T) {
		require.NoError(t, b.DeleteNetworkInsightsPath(pathID))
		paths := b.DescribeNetworkInsightsPaths([]string{pathID})
		assert.Empty(t, paths)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) {
		require.Error(t, b.DeleteNetworkInsightsPath("nip-nonexistent"))
	})

	t.Run("create with empty source returns error", func(t *testing.T) {
		_, err := b.CreateNetworkInsightsPath("", "dst", "tcp", 80)
		require.Error(t, err)
	})
}

// ---- Network Insights Analysis ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_NetworkInsightsAnalysis(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	path, err := b.CreateNetworkInsightsPath("eni-a", "eni-b", "tcp", 443)
	require.NoError(t, err)
	pathID := path.NetworkInsightsPathID

	var analysisID string

	t.Run("start analysis", func(t *testing.T) {
		a, err := b.StartNetworkInsightsAnalysis(pathID)
		require.NoError(t, err)
		assert.NotEmpty(t, a.NetworkInsightsAnalysisID)
		assert.Equal(t, "succeeded", a.Status)
		assert.True(t, a.NetworkPathFound)
		assert.Equal(t, pathID, a.NetworkInsightsPathID)
		analysisID = a.NetworkInsightsAnalysisID
	})

	t.Run("describe returns analysis", func(t *testing.T) {
		analyses := b.DescribeNetworkInsightsAnalyses([]string{analysisID})
		require.Len(t, analyses, 1)
		assert.Equal(t, "succeeded", analyses[0].Status)
	})

	t.Run("describe all", func(t *testing.T) {
		analyses := b.DescribeNetworkInsightsAnalyses(nil)
		assert.NotEmpty(t, analyses)
	})

	t.Run("delete analysis", func(t *testing.T) {
		require.NoError(t, b.DeleteNetworkInsightsAnalysis(analysisID))
		analyses := b.DescribeNetworkInsightsAnalyses([]string{analysisID})
		assert.Empty(t, analyses)
	})

	t.Run("start analysis on non-existent path returns error", func(t *testing.T) {
		_, err := b.StartNetworkInsightsAnalysis("nip-nonexistent")
		require.Error(t, err)
	})

	t.Run("delete non-existent analysis returns error", func(t *testing.T) {
		require.Error(t, b.DeleteNetworkInsightsAnalysis("nia-nonexistent"))
	})
}

// ---- Network Insights Access Scope ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_NetworkInsightsAccessScope(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var scopeID string

	t.Run("create scope", func(t *testing.T) {
		s, err := b.CreateNetworkInsightsAccessScope()
		require.NoError(t, err)
		assert.NotEmpty(t, s.NetworkInsightsAccessScopeID)
		assert.NotEmpty(t, s.NetworkInsightsAccessScopeArn)
		scopeID = s.NetworkInsightsAccessScopeID
	})

	t.Run("describe returns created scope", func(t *testing.T) {
		scopes := b.DescribeNetworkInsightsAccessScopes([]string{scopeID})
		require.Len(t, scopes, 1)
		assert.Equal(t, scopeID, scopes[0].NetworkInsightsAccessScopeID)
	})

	t.Run("start scope analysis", func(t *testing.T) {
		a, err := b.StartNetworkInsightsAccessScopeAnalysis(scopeID)
		require.NoError(t, err)
		assert.NotEmpty(t, a.NetworkInsightsAccessScopeAnalysisID)
		assert.Equal(t, "succeeded", a.Status)
		assert.Equal(t, scopeID, a.NetworkInsightsAccessScopeID)
	})

	t.Run("describe scope analyses", func(t *testing.T) {
		analyses := b.DescribeNetworkInsightsAccessScopeAnalyses(nil)
		assert.NotEmpty(t, analyses)
	})

	t.Run("delete scope analysis", func(t *testing.T) {
		analyses := b.DescribeNetworkInsightsAccessScopeAnalyses(nil)
		require.NotEmpty(t, analyses)
		require.NoError(t, b.DeleteNetworkInsightsAccessScopeAnalysis(analyses[0].NetworkInsightsAccessScopeAnalysisID))
	})

	t.Run("delete scope", func(t *testing.T) {
		require.NoError(t, b.DeleteNetworkInsightsAccessScope(scopeID))
		scopes := b.DescribeNetworkInsightsAccessScopes([]string{scopeID})
		assert.Empty(t, scopes)
	})

	t.Run("start analysis on non-existent scope returns error", func(t *testing.T) {
		_, err := b.StartNetworkInsightsAccessScopeAnalysis("nias-nonexistent")
		require.Error(t, err)
	})

	t.Run("delete non-existent scope returns error", func(t *testing.T) {
		require.Error(t, b.DeleteNetworkInsightsAccessScope("nias-nonexistent"))
	})

	t.Run("delete non-existent analysis returns error", func(t *testing.T) {
		require.Error(t, b.DeleteNetworkInsightsAccessScopeAnalysis("niasa-nonexistent"))
	})
}

// ---- BYOIP ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_BYOIP(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("provision cidr", func(t *testing.T) {
		entry, err := b.ProvisionByoipCidr("198.51.100.0/24", "test provision")
		require.NoError(t, err)
		assert.Equal(t, "198.51.100.0/24", entry.Cidr)
		assert.Equal(t, "pending-provision", entry.State)
	})

	t.Run("provision empty cidr returns error", func(t *testing.T) {
		_, err := b.ProvisionByoipCidr("", "")
		require.Error(t, err)
	})

	t.Run("deprovision cidr", func(t *testing.T) {
		entry, err := b.DeprovisionByoipCidr("198.51.100.0/24")
		require.NoError(t, err)
		assert.Equal(t, "pending-deprovision", entry.State)
	})

	t.Run("deprovision non-existent returns error", func(t *testing.T) {
		_, err := b.DeprovisionByoipCidr("10.0.0.0/8")
		require.Error(t, err)
	})

	t.Run("withdraw cidr", func(t *testing.T) {
		// Re-provision first
		_, err := b.ProvisionByoipCidr("203.0.113.0/24", "")
		require.NoError(t, err)

		entry, err := b.WithdrawByoipCidr("203.0.113.0/24")
		require.NoError(t, err)
		assert.Equal(t, "advertised", entry.State)
	})

	t.Run("withdraw creates entry if not exists", func(t *testing.T) {
		entry, err := b.WithdrawByoipCidr("192.0.2.0/24")
		require.NoError(t, err)
		assert.Equal(t, "advertised", entry.State)
	})
}

// ---- Carrier Gateway ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_CarrierGateway(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var gwID string

	t.Run("create carrier gateway", func(t *testing.T) {
		gw, err := b.CreateCarrierGateway("vpc-12345678")
		require.NoError(t, err)
		assert.NotEmpty(t, gw.CarrierGatewayID)
		assert.Equal(t, "vpc-12345678", gw.VpcID)
		assert.Equal(t, "available", gw.State)
		assert.Equal(t, "000000000000", gw.OwnerID)
		gwID = gw.CarrierGatewayID
	})

	t.Run("describe returns created gateway", func(t *testing.T) {
		gws := b.DescribeCarrierGateways([]string{gwID})
		require.Len(t, gws, 1)
		assert.Equal(t, "vpc-12345678", gws[0].VpcID)
	})

	t.Run("describe all", func(t *testing.T) {
		gws := b.DescribeCarrierGateways(nil)
		assert.NotEmpty(t, gws)
	})

	t.Run("create second gateway", func(t *testing.T) {
		_, err := b.CreateCarrierGateway("vpc-87654321")
		require.NoError(t, err)

		gws := b.DescribeCarrierGateways(nil)
		assert.Len(t, gws, 2)
	})

	t.Run("delete gateway", func(t *testing.T) {
		require.NoError(t, b.DeleteCarrierGateway(gwID))
		gws := b.DescribeCarrierGateways([]string{gwID})
		assert.Empty(t, gws)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) {
		require.Error(t, b.DeleteCarrierGateway("cagw-nonexistent"))
	})

	t.Run("create with empty vpc returns error", func(t *testing.T) {
		_, err := b.CreateCarrierGateway("")
		require.Error(t, err)
	})
}

// ---- Reserved Instances ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch5_ReservedInstances(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed an offering for purchase
	b.SeedReservedInstancesOffering(
		"rio-test-offering-001",
		"t3.medium",
		"us-east-1a",
		"Linux/UNIX",
		"All Upfront",
		94608000,
		500.0,
		0.0,
	)

	t.Run("describe offerings returns seeded offering", func(t *testing.T) {
		offerings := b.DescribeReservedInstancesOfferings("", "", "")
		assert.NotEmpty(t, offerings)
	})

	t.Run("describe offerings by instance type", func(t *testing.T) {
		offerings := b.DescribeReservedInstancesOfferings("t3.medium", "", "")
		require.Len(t, offerings, 1)
		assert.Equal(t, "t3.medium", offerings[0].InstanceType)
	})

	t.Run("describe offerings by az", func(t *testing.T) {
		offerings := b.DescribeReservedInstancesOfferings("", "us-east-1a", "")
		require.Len(t, offerings, 1)
	})

	t.Run("describe offerings by product description", func(t *testing.T) {
		offerings := b.DescribeReservedInstancesOfferings("", "", "Linux/UNIX")
		require.Len(t, offerings, 1)
	})

	t.Run("describe offerings no match", func(t *testing.T) {
		offerings := b.DescribeReservedInstancesOfferings("m5.xlarge", "", "")
		assert.Empty(t, offerings)
	})

	var riID string

	t.Run("purchase offering", func(t *testing.T) {
		ri, err := b.PurchaseReservedInstancesOffering("rio-test-offering-001", 3)
		require.NoError(t, err)
		assert.NotEmpty(t, ri.ReservedInstancesID)
		assert.Equal(t, "active", ri.State)
		assert.Equal(t, 3, ri.InstanceCount)
		riID = ri.ReservedInstancesID
	})

	t.Run("describe reserved instances", func(t *testing.T) {
		ris := b.DescribeReservedInstances(nil)
		require.Len(t, ris, 1)
		assert.Equal(t, riID, ris[0].ReservedInstancesID)
	})

	t.Run("describe reserved instances by id", func(t *testing.T) {
		ris := b.DescribeReservedInstances([]string{riID})
		require.Len(t, ris, 1)
	})

	t.Run("purchase non-existent offering returns error", func(t *testing.T) {
		_, err := b.PurchaseReservedInstancesOffering("rio-nonexistent", 1)
		require.Error(t, err)
	})

	var listingID string

	t.Run("create listing", func(t *testing.T) {
		l, err := b.CreateReservedInstancesListing(riID, 2)
		require.NoError(t, err)
		assert.NotEmpty(t, l.ReservedInstancesListingID)
		assert.Equal(t, "active", l.Status)
		listingID = l.ReservedInstancesListingID
	})

	t.Run("describe listings", func(t *testing.T) {
		listings := b.DescribeReservedInstancesListings(nil)
		require.Len(t, listings, 1)
		assert.Equal(t, listingID, listings[0].ReservedInstancesListingID)
	})

	t.Run("cancel listing", func(t *testing.T) {
		require.NoError(t, b.CancelReservedInstancesListing(listingID))
		listings := b.DescribeReservedInstancesListings([]string{listingID})
		require.Len(t, listings, 1)
		assert.Equal(t, "cancelled", listings[0].Status)
	})

	t.Run("cancel non-existent listing returns error", func(t *testing.T) {
		require.Error(t, b.CancelReservedInstancesListing("rsl-nonexistent"))
	})

	t.Run("modify reserved instances", func(t *testing.T) {
		mod, err := b.ModifyReservedInstances([]string{riID}, "t3.large", 3)
		require.NoError(t, err)
		assert.NotEmpty(t, mod.ReservedInstancesModificationID)
		assert.Equal(t, "fulfilled", mod.Status)
	})

	t.Run("describe modifications", func(t *testing.T) {
		mods := b.DescribeReservedInstancesModifications(nil)
		assert.NotEmpty(t, mods)
	})

	t.Run("delete queued reserved instances", func(t *testing.T) {
		b.DeleteQueuedReservedInstances([]string{riID})
		ris := b.DescribeReservedInstances([]string{riID})
		assert.Empty(t, ris)
	})
}
