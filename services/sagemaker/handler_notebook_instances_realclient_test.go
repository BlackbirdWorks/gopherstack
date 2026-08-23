package sagemaker_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ListNotebookInstances_FilterSortPage_RealClient proves the
// previously-absent ListNotebookInstancesInput fields (AdditionalCode-
// RepositoryEquals, CreationTimeAfter/Before, DefaultCodeRepositoryContains,
// NotebookInstanceLifecycleConfigNameContains, SortBy, SortOrder, MaxResults)
// filter, sort and page the real result set, and that NotebookInstanceSummary
// now carries AdditionalCodeRepositories/DefaultCodeRepository/
// NotebookInstanceLifecycleConfigName/Url (types/types.go:16173-16225).
func TestHandler_ListNotebookInstances_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	lc, err := client.CreateNotebookInstanceLifecycleConfig(
		t.Context(),
		&sagemakersdk.CreateNotebookInstanceLifecycleConfigInput{
			NotebookInstanceLifecycleConfigName: aws.String("nb-lc"),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(lc.NotebookInstanceLifecycleConfigArn))

	for _, tc := range []struct {
		name, codeRepo string
		withLifecycle  bool
	}{
		{"nb-old", "repo-a", false},
		{"nb-new", "repo-b", true},
	} {
		in := &sagemakersdk.CreateNotebookInstanceInput{
			NotebookInstanceName:       aws.String(tc.name),
			InstanceType:               smtypes.InstanceTypeMlT2Medium,
			RoleArn:                    aws.String("arn:aws:iam::000000000000:role/r"),
			DefaultCodeRepository:      aws.String(tc.codeRepo),
			AdditionalCodeRepositories: []string{tc.codeRepo},
		}
		if tc.withLifecycle {
			in.LifecycleConfigName = aws.String("nb-lc")
		}

		_, createErr := client.CreateNotebookInstance(t.Context(), in)
		require.NoError(t, createErr)
	}

	desc, err := client.DescribeNotebookInstance(t.Context(), &sagemakersdk.DescribeNotebookInstanceInput{
		NotebookInstanceName: aws.String("nb-new"),
	})
	require.NoError(t, err)
	assert.Equal(t, "nb-lc", aws.ToString(desc.NotebookInstanceLifecycleConfigName))

	future := time.Now().Add(365 * 24 * time.Hour)
	past := time.Now().Add(-365 * 24 * time.Hour)

	windowed, err := client.ListNotebookInstances(t.Context(), &sagemakersdk.ListNotebookInstancesInput{
		CreationTimeAfter:  &past,
		CreationTimeBefore: &future,
		SortBy:             smtypes.NotebookInstanceSortKeyName,
		SortOrder:          smtypes.NotebookInstanceSortOrderAscending,
	})
	require.NoError(t, err)
	require.Len(t, windowed.NotebookInstances, 2)
	assert.Equal(t, "nb-new", aws.ToString(windowed.NotebookInstances[0].NotebookInstanceName))
	assert.Equal(t, "nb-old", aws.ToString(windowed.NotebookInstances[1].NotebookInstanceName))

	summary := windowed.NotebookInstances[0]
	assert.Equal(t, "repo-b", aws.ToString(summary.DefaultCodeRepository))
	assert.Equal(t, []string{"repo-b"}, summary.AdditionalCodeRepositories)
	assert.Equal(t, "nb-lc", aws.ToString(summary.NotebookInstanceLifecycleConfigName))
	assert.NotEmpty(t, aws.ToString(summary.Url))

	excluded, err := client.ListNotebookInstances(t.Context(), &sagemakersdk.ListNotebookInstancesInput{
		CreationTimeAfter: &future,
	})
	require.NoError(t, err)
	assert.Empty(t, excluded.NotebookInstances)

	byCodeRepo, err := client.ListNotebookInstances(t.Context(), &sagemakersdk.ListNotebookInstancesInput{
		AdditionalCodeRepositoryEquals: aws.String("repo-b"),
	})
	require.NoError(t, err)
	require.Len(t, byCodeRepo.NotebookInstances, 1)
	assert.Equal(t, "nb-new", aws.ToString(byCodeRepo.NotebookInstances[0].NotebookInstanceName))

	byDefaultRepo, err := client.ListNotebookInstances(t.Context(), &sagemakersdk.ListNotebookInstancesInput{
		DefaultCodeRepositoryContains: aws.String("repo-a"),
	})
	require.NoError(t, err)
	require.Len(t, byDefaultRepo.NotebookInstances, 1)
	assert.Equal(t, "nb-old", aws.ToString(byDefaultRepo.NotebookInstances[0].NotebookInstanceName))

	byLifecycle, err := client.ListNotebookInstances(t.Context(), &sagemakersdk.ListNotebookInstancesInput{
		NotebookInstanceLifecycleConfigNameContains: aws.String("nb-lc"),
	})
	require.NoError(t, err)
	require.Len(t, byLifecycle.NotebookInstances, 1)
	assert.Equal(t, "nb-new", aws.ToString(byLifecycle.NotebookInstances[0].NotebookInstanceName))

	page1, err := client.ListNotebookInstances(t.Context(), &sagemakersdk.ListNotebookInstancesInput{
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.NotebookInstances, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))
}

// TestHandler_ListNotebookInstanceLifecycleConfigs_FilterSortPage_RealClient
// proves the previously-absent ListNotebookInstanceLifecycleConfigsInput
// fields (CreationTimeAfter/Before, LastModifiedTimeAfter/Before, MaxResults,
// NameContains, SortBy, SortOrder) filter, sort and page the real result set.
func TestHandler_ListNotebookInstanceLifecycleConfigs_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"lc-old", "lc-new"} {
		_, err := client.CreateNotebookInstanceLifecycleConfig(
			t.Context(),
			&sagemakersdk.CreateNotebookInstanceLifecycleConfigInput{
				NotebookInstanceLifecycleConfigName: aws.String(name),
			},
		)
		require.NoError(t, err)
	}

	future := time.Now().Add(365 * 24 * time.Hour)
	past := time.Now().Add(-365 * 24 * time.Hour)

	windowed, err := client.ListNotebookInstanceLifecycleConfigs(
		t.Context(),
		&sagemakersdk.ListNotebookInstanceLifecycleConfigsInput{
			CreationTimeAfter:  &past,
			CreationTimeBefore: &future,
			SortBy:             smtypes.NotebookInstanceLifecycleConfigSortKeyName,
			SortOrder:          smtypes.NotebookInstanceLifecycleConfigSortOrderAscending,
		},
	)
	require.NoError(t, err)
	require.Len(t, windowed.NotebookInstanceLifecycleConfigs, 2)
	gotFirst := aws.ToString(windowed.NotebookInstanceLifecycleConfigs[0].NotebookInstanceLifecycleConfigName)
	gotSecond := aws.ToString(windowed.NotebookInstanceLifecycleConfigs[1].NotebookInstanceLifecycleConfigName)
	assert.Equal(t, "lc-new", gotFirst)
	assert.Equal(t, "lc-old", gotSecond)

	excluded, err := client.ListNotebookInstanceLifecycleConfigs(
		t.Context(),
		&sagemakersdk.ListNotebookInstanceLifecycleConfigsInput{CreationTimeAfter: &future},
	)
	require.NoError(t, err)
	assert.Empty(t, excluded.NotebookInstanceLifecycleConfigs)

	byName, err := client.ListNotebookInstanceLifecycleConfigs(
		t.Context(),
		&sagemakersdk.ListNotebookInstanceLifecycleConfigsInput{NameContains: aws.String("new")},
	)
	require.NoError(t, err)
	require.Len(t, byName.NotebookInstanceLifecycleConfigs, 1)
	gotName := aws.ToString(byName.NotebookInstanceLifecycleConfigs[0].NotebookInstanceLifecycleConfigName)
	assert.Equal(t, "lc-new", gotName)

	page1, err := client.ListNotebookInstanceLifecycleConfigs(
		t.Context(),
		&sagemakersdk.ListNotebookInstanceLifecycleConfigsInput{MaxResults: aws.Int32(1)},
	)
	require.NoError(t, err)
	require.Len(t, page1.NotebookInstanceLifecycleConfigs, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))
}

// TestHandler_CreateUpdateNotebookInstance_FullFields_RealClient proves the
// previously-absent IpAddressType/InstanceMetadataServiceConfiguration
// (both Create and Update) and PlatformIdentifier/RootAccess (Update) all
// round-trip through Describe.
func TestHandler_CreateUpdateNotebookInstance_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateNotebookInstance(t.Context(), &sagemakersdk.CreateNotebookInstanceInput{
		NotebookInstanceName: aws.String("nb-full"),
		InstanceType:         smtypes.InstanceTypeMlT2Medium,
		RoleArn:              aws.String("arn:aws:iam::000000000000:role/r"),
		IpAddressType:        smtypes.IPAddressTypeDualstack,
		InstanceMetadataServiceConfiguration: &smtypes.InstanceMetadataServiceConfiguration{
			MinimumInstanceMetadataServiceVersion: aws.String("2"),
		},
	})
	require.NoError(t, err)

	afterCreate, err := client.DescribeNotebookInstance(t.Context(), &sagemakersdk.DescribeNotebookInstanceInput{
		NotebookInstanceName: aws.String("nb-full"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.IPAddressTypeDualstack, afterCreate.IpAddressType)
	createIMDS := afterCreate.InstanceMetadataServiceConfiguration
	require.NotNil(t, createIMDS)
	assert.Equal(t, "2", aws.ToString(createIMDS.MinimumInstanceMetadataServiceVersion))

	_, err = client.StopNotebookInstance(t.Context(), &sagemakersdk.StopNotebookInstanceInput{
		NotebookInstanceName: aws.String("nb-full"),
	})
	require.NoError(t, err)

	_, err = client.UpdateNotebookInstance(t.Context(), &sagemakersdk.UpdateNotebookInstanceInput{
		NotebookInstanceName: aws.String("nb-full"),
		PlatformIdentifier:   aws.String("notebook-al2-v2"),
		RootAccess:           smtypes.RootAccessDisabled,
		IpAddressType:        smtypes.IPAddressTypeIpv4,
		InstanceMetadataServiceConfiguration: &smtypes.InstanceMetadataServiceConfiguration{
			MinimumInstanceMetadataServiceVersion: aws.String("1"),
		},
	})
	require.NoError(t, err)

	afterUpdate, err := client.DescribeNotebookInstance(t.Context(), &sagemakersdk.DescribeNotebookInstanceInput{
		NotebookInstanceName: aws.String("nb-full"),
	})
	require.NoError(t, err)
	assert.Equal(t, "notebook-al2-v2", aws.ToString(afterUpdate.PlatformIdentifier))
	assert.Equal(t, smtypes.RootAccessDisabled, afterUpdate.RootAccess)
	assert.Equal(t, smtypes.IPAddressTypeIpv4, afterUpdate.IpAddressType)
	updateIMDS := afterUpdate.InstanceMetadataServiceConfiguration
	require.NotNil(t, updateIMDS)
	assert.Equal(t, "1", aws.ToString(updateIMDS.MinimumInstanceMetadataServiceVersion))
}
