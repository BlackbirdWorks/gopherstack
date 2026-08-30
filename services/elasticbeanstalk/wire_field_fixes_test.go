package elasticbeanstalk_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func newWireFixClient(t *testing.T) *ebsdk.Client {
	t.Helper()

	h := elasticbeanstalk.NewHandler(elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1"))

	return newTestEBClient(t, h)
}

func createTaggedApp(t *testing.T, client *ebsdk.Client, appName string) {
	t.Helper()

	_, err := client.CreateApplication(t.Context(), &ebsdk.CreateApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err)
}

// TestEnvironmentDescription_NeverModeledFields drives CreateEnvironment
// through the real SDK client and asserts the three EnvironmentDescription
// members that were previously never emitted: TemplateName (tracked by the
// backend but dropped), AbortableOperationInProgress (a real *bool member;
// omitting it entirely decodes as a nil pointer a real client's own
// documented "true: update in progress / false: no update in progress"
// contract implies is never nil), and HealthStatus (the real
// EnvironmentHealthStatus enum, not "Green" which isn't a member of it).
func TestEnvironmentDescription_NeverModeledFields(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)
	createTaggedApp(t, client, "eb-app")

	out, err := client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("eb-app"),
		EnvironmentName:   aws.String("eb-env"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
		TemplateName:      aws.String("eb-tmpl"),
	})
	require.NoError(t, err)
	assert.Equal(t, "eb-tmpl", aws.ToString(out.TemplateName))
	require.NotNil(t, out.AbortableOperationInProgress)
	assert.False(t, *out.AbortableOperationInProgress)
	assert.Equal(t, types.EnvironmentHealthStatusOk, out.HealthStatus)

	upd, err := client.UpdateEnvironment(t.Context(), &ebsdk.UpdateEnvironmentInput{
		EnvironmentName: aws.String("eb-env"),
		TemplateName:    aws.String("eb-tmpl-2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "eb-tmpl-2", aws.ToString(upd.TemplateName))
}

// TestDescribeEnvironmentHealth_HealthStatusEnum asserts HealthStatus holds
// a real EnvironmentHealthStatus enum value ("Ok"), not this backend's
// internal color label ("Green", a member of the separate EnvironmentHealth
// enum only). Also drives the previously-unsupported EnvironmentId filter.
func TestDescribeEnvironmentHealth_HealthStatusEnum(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)
	createTaggedApp(t, client, "eb-health-app")

	envOut, err := client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("eb-health-app"),
		EnvironmentName:   aws.String("eb-health-env"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
	})
	require.NoError(t, err)

	byName, err := client.DescribeEnvironmentHealth(t.Context(), &ebsdk.DescribeEnvironmentHealthInput{
		EnvironmentName: aws.String("eb-health-env"),
	})
	require.NoError(t, err)
	assert.Equal(t, string(types.EnvironmentHealthStatusOk), aws.ToString(byName.HealthStatus))

	byID, err := client.DescribeEnvironmentHealth(t.Context(), &ebsdk.DescribeEnvironmentHealthInput{
		EnvironmentId: envOut.EnvironmentId,
	})
	require.NoError(t, err)
	assert.Equal(t, "eb-health-env", aws.ToString(byID.EnvironmentName))
}

// TestDescribeEnvironments_VersionLabelFilter asserts the real
// DescribeEnvironmentsInput.VersionLabel filter is honored, not silently
// discarded (it was previously never read from the request at all).
func TestDescribeEnvironments_VersionLabelFilter(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)
	createTaggedApp(t, client, "eb-vl-app")

	_, err := client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("eb-vl-app"),
		EnvironmentName:   aws.String("eb-vl-env-1"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
		VersionLabel:      aws.String("v1"),
	})
	require.NoError(t, err)

	_, err = client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("eb-vl-app"),
		EnvironmentName:   aws.String("eb-vl-env-2"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
		VersionLabel:      aws.String("v2"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEnvironments(t.Context(), &ebsdk.DescribeEnvironmentsInput{
		ApplicationName: aws.String("eb-vl-app"),
		VersionLabel:    aws.String("v1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Environments, 1)
	assert.Equal(t, "eb-vl-env-1", aws.ToString(out.Environments[0].EnvironmentName))
}

// TestDescribeEnvironments_Pagination asserts MaxRecords/NextToken (real
// DescribeEnvironmentsInput/Output members, both previously discarded) are
// honored.
func TestDescribeEnvironments_Pagination(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)
	createTaggedApp(t, client, "eb-page-app")

	for _, name := range []string{"eb-page-env-1", "eb-page-env-2", "eb-page-env-3"} {
		_, err := client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
			ApplicationName:   aws.String("eb-page-app"),
			EnvironmentName:   aws.String(name),
			SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeEnvironments(t.Context(), &ebsdk.DescribeEnvironmentsInput{
		ApplicationName: aws.String("eb-page-app"),
		MaxRecords:      aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, first.Environments, 2)
	require.NotNil(t, first.NextToken)

	second, err := client.DescribeEnvironments(t.Context(), &ebsdk.DescribeEnvironmentsInput{
		ApplicationName: aws.String("eb-page-app"),
		MaxRecords:      aws.Int32(2),
		NextToken:       first.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, second.Environments, 1)
}

// TestDescribeEvents_NeverModeledFields asserts EventDescription's
// PlatformArn/TemplateName/VersionLabel (previously never captured on the
// stored event at all) round-trip, and that filtering by them works.
func TestDescribeEvents_NeverModeledFields(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)
	createTaggedApp(t, client, "eb-evt-app")

	_, err := client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("eb-evt-app"),
		EnvironmentName:   aws.String("eb-evt-env"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
		VersionLabel:      aws.String("v9"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEvents(t.Context(), &ebsdk.DescribeEventsInput{
		EnvironmentName: aws.String("eb-evt-env"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Events)
	assert.Equal(t, "v9", aws.ToString(out.Events[0].VersionLabel))

	filtered, err := client.DescribeEvents(t.Context(), &ebsdk.DescribeEventsInput{
		EnvironmentName: aws.String("eb-evt-env"),
		VersionLabel:    aws.String("v9"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, filtered.Events)

	notFound, err := client.DescribeEvents(t.Context(), &ebsdk.DescribeEventsInput{
		EnvironmentName: aws.String("eb-evt-env"),
		VersionLabel:    aws.String("v-does-not-exist"),
	})
	require.NoError(t, err)
	assert.Empty(t, notFound.Events)
}

// TestManagedActionHistory_ExecutedTime asserts ExecutedTime (a real
// ManagedActionHistoryItem member, previously never emitted at all) is
// populated.
func TestManagedActionHistory_ExecutedTime(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)
	createTaggedApp(t, client, "eb-mah-app")

	_, err := client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("eb-mah-app"),
		EnvironmentName:   aws.String("eb-mah-env"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.0.0 running Python 3.11"),
	})
	require.NoError(t, err)

	_, err = client.ApplyEnvironmentManagedAction(t.Context(), &ebsdk.ApplyEnvironmentManagedActionInput{
		EnvironmentName: aws.String("eb-mah-env"),
		ActionId:        aws.String("action-1"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEnvironmentManagedActionHistory(
		t.Context(),
		&ebsdk.DescribeEnvironmentManagedActionHistoryInput{EnvironmentName: aws.String("eb-mah-env")},
	)
	require.NoError(t, err)
	require.Len(t, out.ManagedActionHistoryItems, 1)
	require.NotNil(t, out.ManagedActionHistoryItems[0].ExecutedTime)
	assert.Equal(t, *out.ManagedActionHistoryItems[0].FinishedTime, *out.ManagedActionHistoryItems[0].ExecutedTime)
}

// TestDescribeInstancesHealth_RefreshedAt asserts RefreshedAt (a real *time.Time
// member) is populated even though InstanceHealthList is always empty --
// omitting it entirely would decode as a nil pointer.
func TestDescribeInstancesHealth_RefreshedAt(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)

	out, err := client.DescribeInstancesHealth(t.Context(), &ebsdk.DescribeInstancesHealthInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.RefreshedAt)
	assert.Empty(t, out.InstanceHealthList)
}

// TestPlatformVersionShapes asserts CreatePlatformVersion/DeletePlatformVersion
// use the real, narrower PlatformSummary shape (no PlatformName member --
// this compiles only because types.PlatformSummary genuinely lacks one) and
// that PlatformOwner ("self", a real member neither shape previously
// emitted) is populated on both PlatformSummary and PlatformDescription.
func TestPlatformVersionShapes(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)

	created, err := client.CreatePlatformVersion(t.Context(), &ebsdk.CreatePlatformVersionInput{
		PlatformName:    aws.String("eb-platform"),
		PlatformVersion: aws.String("1.0.0"),
		PlatformDefinitionBundle: &types.S3Location{
			S3Bucket: aws.String("bucket"),
			S3Key:    aws.String("key"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "self", aws.ToString(created.PlatformSummary.PlatformOwner))

	desc, err := client.DescribePlatformVersion(t.Context(), &ebsdk.DescribePlatformVersionInput{
		PlatformArn: created.PlatformSummary.PlatformArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "self", aws.ToString(desc.PlatformDescription.PlatformOwner))
	assert.Equal(t, "eb-platform", aws.ToString(desc.PlatformDescription.PlatformName))

	deleted, err := client.DeletePlatformVersion(t.Context(), &ebsdk.DeletePlatformVersionInput{
		PlatformArn: created.PlatformSummary.PlatformArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "self", aws.ToString(deleted.PlatformSummary.PlatformOwner))
}

// TestListPlatformVersions_FieldsFilterAndPagination asserts the real
// PlatformSummary.PlatformVersion member (previously never emitted on
// ListPlatformVersions items), the Filters.Type request field (previously
// entirely discarded), and MaxRecords/NextToken pagination all work.
func TestListPlatformVersions_FieldsFilterAndPagination(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)

	for _, ver := range []string{"1.0.0", "2.0.0"} {
		_, err := client.CreatePlatformVersion(t.Context(), &ebsdk.CreatePlatformVersionInput{
			PlatformName:    aws.String("eb-lpv-platform"),
			PlatformVersion: aws.String(ver),
			PlatformDefinitionBundle: &types.S3Location{
				S3Bucket: aws.String("bucket"),
				S3Key:    aws.String("key"),
			},
		})
		require.NoError(t, err)
	}

	all, err := client.ListPlatformVersions(t.Context(), &ebsdk.ListPlatformVersionsInput{})
	require.NoError(t, err)
	require.Len(t, all.PlatformSummaryList, 2)

	versions := []string{
		aws.ToString(all.PlatformSummaryList[0].PlatformVersion),
		aws.ToString(all.PlatformSummaryList[1].PlatformVersion),
	}
	assert.ElementsMatch(t, []string{"1.0.0", "2.0.0"}, versions)

	filtered, err := client.ListPlatformVersions(t.Context(), &ebsdk.ListPlatformVersionsInput{
		Filters: []types.PlatformFilter{
			{Type: aws.String("PlatformVersion"), Values: []string{"1.0.0"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.PlatformSummaryList, 1)
	assert.Equal(t, "1.0.0", aws.ToString(filtered.PlatformSummaryList[0].PlatformVersion))

	paged, err := client.ListPlatformVersions(t.Context(), &ebsdk.ListPlatformVersionsInput{
		MaxRecords: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, paged.PlatformSummaryList, 1)
	require.NotNil(t, paged.NextToken)
}

// TestListPlatformVersions_FilterMultipleValues_OrMatch covers gopherstack-6flj
// wrapper-key sweep (workspaces/codebuild/elasticbeanstalk pass): real
// SearchFilter.Values (elasticbeanstalk@v1.37.4/types/types.go: "The list of
// values applied to the Attribute and Operator attributes") is a list --
// gopherstack's filter parsing only ever read Filters.member.N.Values.member.1,
// silently dropping every value past the first. A caller filtering on
// multiple candidate values (the standard AWS SearchFilter OR-list idiom, same
// as EC2's Filter.N.Value.M) got a filter that matched at most the first
// listed value and silently excluded platforms matching any other.
func TestListPlatformVersions_FilterMultipleValues_OrMatch(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)

	for _, ver := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		_, err := client.CreatePlatformVersion(t.Context(), &ebsdk.CreatePlatformVersionInput{
			PlatformName:    aws.String("eb-multi-filter-platform"),
			PlatformVersion: aws.String(ver),
			PlatformDefinitionBundle: &types.S3Location{
				S3Bucket: aws.String("bucket"),
				S3Key:    aws.String("key"),
			},
		})
		require.NoError(t, err)
	}

	filtered, err := client.ListPlatformVersions(t.Context(), &ebsdk.ListPlatformVersionsInput{
		Filters: []types.PlatformFilter{
			{Type: aws.String("PlatformVersion"), Values: []string{"1.0.0", "3.0.0"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.PlatformSummaryList, 2,
		"filter must OR-match against every listed value, not just Values.member.1")

	versions := []string{
		aws.ToString(filtered.PlatformSummaryList[0].PlatformVersion),
		aws.ToString(filtered.PlatformSummaryList[1].PlatformVersion),
	}
	assert.ElementsMatch(t, []string{"1.0.0", "3.0.0"}, versions)
}

// TestListPlatformBranches_FilterMultipleValues_OrMatch covers the same
// Values-truncation bug (gopherstack-6flj) on ListPlatformBranches' filter
// parsing, which shares the identical Filters.member.N.Values.member.1
// pattern.
func TestListPlatformBranches_FilterMultipleValues_OrMatch(t *testing.T) {
	t.Parallel()

	client := newWireFixClient(t)

	out, err := client.ListPlatformBranches(t.Context(), &ebsdk.ListPlatformBranchesInput{
		Filters: []types.SearchFilter{
			{Attribute: aws.String("PlatformName"), Values: []string{"Java", "Ruby"}},
		},
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.PlatformBranchSummaryList))
	for _, b := range out.PlatformBranchSummaryList {
		names = append(names, aws.ToString(b.PlatformName))
	}
	assert.Contains(t, names, "Java")
	assert.Contains(t, names, "Ruby")
	assert.NotContains(t, names, "Node.js",
		"filter must exclude non-matching platforms while OR-matching every listed value")
}
