package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestApplicationStatusChecks_CreateValidation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"),
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("ftp"),
		Port:     new(80),
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"),
		Port:     new(70000),
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"),
		Port:     new(80),
		Path:     new("no-leading-slash"),
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"),
		Port:     new(80),
		Timeout:  new(60),
		Interval: new(60),
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter, "Timeout must be less than Interval")

	_, err = bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol:    new("http"),
		Port:        new(80),
		Aggregation: new("sometimes"),
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestApplicationStatusChecks_CreateAppliesRealDefaults(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	check, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("https"),
		Port:     new(443),
	})
	require.NoError(t, err)

	assert.Contains(t, check.ApplicationStatusCheckID, "asc-")
	assert.Equal(t, "included", check.Aggregation)
	assert.Equal(t, "/", check.Path)
	assert.Equal(t, 60, check.Interval)
	assert.Equal(t, 6, check.Timeout)
	assert.Equal(t, 2, check.FailureThreshold)
	assert.Equal(t, 5, check.SuccessThreshold)
	assert.Equal(t, "200", check.StatusCodeMatcher)
	assert.Equal(t, 300, check.InitializationGracePeriodSeconds)
	assert.False(t, check.CreationTime.IsZero())
}

func TestApplicationStatusChecks_MaxPerAccount(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	for range 50 {
		_, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
			Protocol: new("http"),
			Port:     new(80),
		})
		require.NoError(t, err)
	}

	_, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"),
		Port:     new(80),
	})
	require.ErrorIs(t, err, ec2.ErrTooManyApplicationStatusChecks)
}

func TestApplicationStatusChecks_ModifyRetainsUnsetFields(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	check, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"),
		Port:     new(80),
		Path:     new("/health"),
	})
	require.NoError(t, err)

	updated, err := bk.ModifyApplicationStatusCheck(check.ApplicationStatusCheckID, ec2.ApplicationStatusCheckParams{
		Port: new(8080),
	})
	require.NoError(t, err)
	assert.Equal(t, 8080, updated.Port)
	assert.Equal(t, "/health", updated.Path, "unset fields must retain their current value")
	assert.Equal(t, "http", updated.Protocol)
	assert.True(t, updated.LastUpdatedAt.After(check.LastUpdatedAt) || updated.LastUpdatedAt.Equal(check.LastUpdatedAt))

	_, err = bk.ModifyApplicationStatusCheck("asc-nonexistent", ec2.ApplicationStatusCheckParams{Port: new(1)})
	require.ErrorIs(t, err, ec2.ErrApplicationStatusCheckNotFound)

	_, err = bk.ModifyApplicationStatusCheck("", ec2.ApplicationStatusCheckParams{})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	// A Modify that would violate Timeout < Interval is rejected without mutating state.
	_, err = bk.ModifyApplicationStatusCheck(check.ApplicationStatusCheckID, ec2.ApplicationStatusCheckParams{
		Timeout: new(999),
	})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestApplicationStatusChecks_DeleteCascadesAndRetainsForIncludeAll(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	insts, err := bk.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	instID := insts[0].ID

	check, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"),
		Port:     new(80),
	})
	require.NoError(t, err)

	_, _, err = bk.AssociateApplicationStatusCheck(check.ApplicationStatusCheckID, []string{instID}, nil)
	require.NoError(t, err)

	deleted, err := bk.DeleteApplicationStatusCheck(check.ApplicationStatusCheckID)
	require.NoError(t, err)
	assert.True(t, deleted.Deleted)
	assert.False(t, deleted.DeletionTime.IsZero())

	// Default Describe excludes deleted checks.
	checks := bk.DescribeApplicationStatusChecks(nil, nil, false)
	assert.Empty(t, checks)

	// IncludeAll still shows it.
	checks = bk.DescribeApplicationStatusChecks(nil, nil, true)
	require.Len(t, checks, 1)
	assert.True(t, checks[0].Deleted)

	// Associations were cascaded away.
	assocs := bk.DescribeApplicationStatusCheckAssociations([]string{check.ApplicationStatusCheckID}, nil)
	assert.Empty(t, assocs)

	// Deleting again fails.
	_, err = bk.DeleteApplicationStatusCheck(check.ApplicationStatusCheckID)
	require.ErrorIs(t, err, ec2.ErrApplicationStatusCheckNotFound)

	_, err = bk.DeleteApplicationStatusCheck("")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestApplicationStatusChecks_DescribeFiltersAndIDs(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	included, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"), Port: new(80),
	})
	require.NoError(t, err)

	excluded, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"), Port: new(81), Aggregation: new("excluded"),
	})
	require.NoError(t, err)

	byID := bk.DescribeApplicationStatusChecks([]string{included.ApplicationStatusCheckID}, nil, false)
	require.Len(t, byID, 1)
	assert.Equal(t, included.ApplicationStatusCheckID, byID[0].ApplicationStatusCheckID)

	byFilter := bk.DescribeApplicationStatusChecks(nil, map[string][]string{"aggregation": {"excluded"}}, false)
	require.Len(t, byFilter, 1)
	assert.Equal(t, excluded.ApplicationStatusCheckID, byFilter[0].ApplicationStatusCheckID)

	all := bk.DescribeApplicationStatusChecks(nil, nil, false)
	assert.Len(t, all, 2)
}

func TestApplicationStatusChecks_AssociateInstanceLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	insts, err := bk.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	instID := insts[0].ID

	check, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"), Port: new(80),
	})
	require.NoError(t, err)

	// Must specify exactly one of instances/tags.
	_, _, err = bk.AssociateApplicationStatusCheck(check.ApplicationStatusCheckID, nil, nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameterCombination)

	_, _, err = bk.AssociateApplicationStatusCheck(
		check.ApplicationStatusCheckID,
		[]string{instID},
		[]ec2.CustomTagKeyValue{{Key: "env", Value: "prod"}},
	)
	require.ErrorIs(t, err, ec2.ErrInvalidParameterCombination)

	_, _, err = bk.AssociateApplicationStatusCheck("asc-nonexistent", []string{instID}, nil)
	require.ErrorIs(t, err, ec2.ErrApplicationStatusCheckNotFound)

	successful, unsuccessful, err := bk.AssociateApplicationStatusCheck(
		check.ApplicationStatusCheckID, []string{instID, "i-doesnotexist"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, successful, 1)
	assert.Equal(t, "INSTANCE_ID", successful[0].AssociationType)
	assert.Equal(t, instID, successful[0].AssociationValue)
	require.Len(t, unsuccessful, 1)
	assert.Equal(t, "i-doesnotexist", unsuccessful[0].AssociationValue)
	assert.NotEmpty(t, unsuccessful[0].Reason)

	assocs := bk.DescribeApplicationStatusCheckAssociations([]string{check.ApplicationStatusCheckID}, nil)
	require.Len(t, assocs, 1)
	assert.Equal(t, "instance-id", assocs[0].AssociationType)
	assert.Equal(t, instID, assocs[0].InstanceID)

	byFilter := bk.DescribeApplicationStatusCheckAssociations(nil, map[string][]string{
		"association-type": {"tag"},
	})
	assert.Empty(t, byFilter)

	dSuccessful, dUnsuccessful, err := bk.DisassociateApplicationStatusCheck(
		check.ApplicationStatusCheckID, []string{instID}, nil,
	)
	require.NoError(t, err)
	assert.Len(t, dSuccessful, 1)
	assert.Empty(t, dUnsuccessful)

	assocs = bk.DescribeApplicationStatusCheckAssociations([]string{check.ApplicationStatusCheckID}, nil)
	assert.Empty(t, assocs)

	// Disassociating again reports unsuccessful, not an error.
	_, dUnsuccessful, err = bk.DisassociateApplicationStatusCheck(
		check.ApplicationStatusCheckID, []string{instID}, nil,
	)
	require.NoError(t, err)
	assert.Len(t, dUnsuccessful, 1)
}

func TestApplicationStatusChecks_AssociateTagLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	check, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"), Port: new(80),
	})
	require.NoError(t, err)

	successful, unsuccessful, err := bk.AssociateApplicationStatusCheck(
		check.ApplicationStatusCheckID,
		nil,
		[]ec2.CustomTagKeyValue{{Key: "env", Value: "prod"}, {Key: "", Value: "blank"}},
	)
	require.NoError(t, err)
	require.Len(t, successful, 1)
	assert.Equal(t, "EC2TAG", successful[0].AssociationType)
	assert.Equal(t, "env=prod", successful[0].AssociationValue)
	require.Len(t, unsuccessful, 1)
	assert.Contains(t, unsuccessful[0].Reason, "blank")

	assocs := bk.DescribeApplicationStatusCheckAssociations([]string{check.ApplicationStatusCheckID}, nil)
	require.Len(t, assocs, 1)
	assert.Equal(t, "tag", assocs[0].AssociationType)
	assert.Equal(t, "env", assocs[0].TagKey)
	assert.Equal(t, "prod", assocs[0].TagValue)
}

func TestApplicationStatusChecks_Suppression(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	insts, err := bk.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	instID := insts[0].ID

	successful, unsuccessful := bk.EnableApplicationStatusCheckSuppression([]string{instID, "i-nope"}, 0)
	require.Len(t, successful, 1)
	assert.True(t, successful[0].ResumeAt.IsZero(), "no DurationSeconds means indefinite suppression")
	require.Len(t, unsuccessful, 1)
	assert.Equal(t, "i-nope", unsuccessful[0].InstanceID)

	successful, unsuccessful = bk.EnableApplicationStatusCheckSuppression([]string{instID}, 60)
	require.Len(t, successful, 1)
	assert.False(t, successful[0].ResumeAt.IsZero())
	assert.Empty(t, unsuccessful)

	dSuccessful, dUnsuccessful := bk.DisableApplicationStatusCheckSuppression([]string{instID})
	require.Len(t, dSuccessful, 1)
	assert.Empty(t, dUnsuccessful)

	dSuccessful, dUnsuccessful = bk.DisableApplicationStatusCheckSuppression([]string{"i-nope"})
	assert.Empty(t, dSuccessful)
	require.Len(t, dUnsuccessful, 1)
}

// TestApplicationStatusChecks_DescribeStatusNeverFabricates is the core
// honesty test: it asserts DescribeApplicationStatus only ever returns the
// three real, derivable statuses, and specifically that an instance with an
// "included" check but no suppression is reported "insufficient-data" (never
// a fabricated "ok"/"impaired"/"initializing").
func TestApplicationStatusChecks_DescribeStatusNeverFabricates(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	insts, err := bk.RunInstances("ami-123", "t2.micro", "", 3)
	require.NoError(t, err)
	noCheckInst := insts[0].ID
	instanceAssocInst := insts[1].ID
	tagAssocInst := insts[2].ID

	require.NoError(t, bk.CreateTags([]string{tagAssocInst}, map[string]string{"env": "prod"}))

	includedCheck, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"), Port: new(80),
	})
	require.NoError(t, err)

	excludedCheck, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("http"), Port: new(81), Aggregation: new("excluded"),
	})
	require.NoError(t, err)

	_, _, err = bk.AssociateApplicationStatusCheck(
		includedCheck.ApplicationStatusCheckID, []string{instanceAssocInst}, nil,
	)
	require.NoError(t, err)
	_, _, err = bk.AssociateApplicationStatusCheck(
		includedCheck.ApplicationStatusCheckID, nil, []ec2.CustomTagKeyValue{{Key: "env", Value: "prod"}},
	)
	require.NoError(t, err)
	// An excluded-aggregation check associated with noCheckInst must NOT make
	// it anything other than not-applicable.
	_, _, err = bk.AssociateApplicationStatusCheck(excludedCheck.ApplicationStatusCheckID, []string{noCheckInst}, nil)
	require.NoError(t, err)

	statuses := bk.DescribeApplicationStatus(nil, nil)
	require.Len(t, statuses, 3)

	byID := make(map[string]*ec2.InstanceApplicationStatus, len(statuses))
	for _, s := range statuses {
		byID[s.InstanceID] = s
	}

	assert.Equal(t, "not-applicable", byID[noCheckInst].Status)
	assert.Equal(t, "insufficient-data", byID[instanceAssocInst].Status)
	assert.Equal(t, "insufficient-data", byID[tagAssocInst].Status)

	for _, s := range statuses {
		assert.Contains(
			t,
			[]string{"not-applicable", "insufficient-data", "suppressed"},
			s.Status,
			"DescribeApplicationStatus must never report a fabricated ok/impaired/initializing result",
		)
	}

	// Suppression overrides everything else.
	_, unsuccessful := bk.EnableApplicationStatusCheckSuppression([]string{instanceAssocInst}, 3600)
	assert.Empty(t, unsuccessful)

	statuses = bk.DescribeApplicationStatus([]string{instanceAssocInst}, nil)
	require.Len(t, statuses, 1)
	assert.Equal(t, "suppressed", statuses[0].Status)
	assert.False(t, statuses[0].ResumeAt.IsZero())

	// "status" filter.
	filtered := bk.DescribeApplicationStatus(nil, map[string][]string{"status": {"not-applicable"}})
	require.Len(t, filtered, 1)
	assert.Equal(t, noCheckInst, filtered[0].InstanceID)
}

func TestApplicationStatusChecks_SnapshotRestore(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	insts, err := bk.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	instID := insts[0].ID

	check, err := bk.CreateApplicationStatusCheck(ec2.ApplicationStatusCheckParams{
		Protocol: new("https"),
		Port:     new(8443),
		Path:     new("/status"),
	})
	require.NoError(t, err)

	_, _, err = bk.AssociateApplicationStatusCheck(check.ApplicationStatusCheckID, []string{instID}, nil)
	require.NoError(t, err)

	_, unsuccessful := bk.EnableApplicationStatusCheckSuppression([]string{instID}, 120)
	require.Empty(t, unsuccessful)

	snap := bk.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	checks := restored.DescribeApplicationStatusChecks([]string{check.ApplicationStatusCheckID}, nil, false)
	require.Len(t, checks, 1)
	assert.Equal(t, "/status", checks[0].Path)
	assert.Equal(t, 8443, checks[0].Port)

	assocs := restored.DescribeApplicationStatusCheckAssociations([]string{check.ApplicationStatusCheckID}, nil)
	require.Len(t, assocs, 1)
	assert.Equal(t, instID, assocs[0].InstanceID)

	statuses := restored.DescribeApplicationStatus([]string{instID}, nil)
	require.Len(t, statuses, 1)
	assert.Equal(t, "suppressed", statuses[0].Status)
}
