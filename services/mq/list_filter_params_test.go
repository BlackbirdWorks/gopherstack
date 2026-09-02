package mq_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListConfigurations_DefaultPageSize proves ListConfigurations honors
// its documented default MaxResults of 20, not an invented one -- every
// mq List/Describe pagination op (ListBrokers, ListConfigurations, ListUsers,
// DescribeSharedResources) documents "20 by default" in its own SDK input
// struct (mq@v1.39.4 api_op_ListConfigurations.go: "The maximum number of
// brokers that Amazon MQ can return per page (20 by default)"). ListUsers
// already gets this right (mqUsersDefaultPageSize); ListBrokers and
// ListConfigurations shared a page.New default of mqDefaultPageSize=100
// instead.
func TestListConfigurations_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMQClient(t, h)

	const seeded = 25

	for i := range seeded {
		_, err := client.CreateConfiguration(t.Context(), &mqsdk.CreateConfigurationInput{
			Name:          aws.String(fmt.Sprintf("cfg-%02d", i)),
			EngineType:    "ACTIVEMQ",
			EngineVersion: aws.String("5.15.14"),
		})
		require.NoError(t, err)
	}

	out, err := client.ListConfigurations(t.Context(), &mqsdk.ListConfigurationsInput{})
	require.NoError(t, err)

	assert.Len(t, out.Configurations, 20, "no MaxResults given: must default to the documented 20, not an invented 100")
	assert.NotEmpty(t, aws.ToString(out.NextToken), "25 configs > default page size of 20: a next page must exist")
}

// TestListBrokers_DefaultPageSize is ListConfigurations' sibling test for
// ListBrokers, which documents the same "20 by default" default
// (api_op_ListBrokers.go).
func TestListBrokers_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMQClient(t, h)

	const seeded = 25

	for i := range seeded {
		_, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
			BrokerName:         aws.String(fmt.Sprintf("broker-%02d", i)),
			EngineType:         "ACTIVEMQ",
			EngineVersion:      aws.String("5.15.14"),
			HostInstanceType:   aws.String("mq.t3.micro"),
			DeploymentMode:     "SINGLE_INSTANCE",
			PubliclyAccessible: aws.Bool(false),
		})
		require.NoError(t, err)
	}

	out, err := client.ListBrokers(t.Context(), &mqsdk.ListBrokersInput{})
	require.NoError(t, err)

	assert.Len(
		t,
		out.BrokerSummaries,
		20,
		"no MaxResults given: must default to the documented 20, not an invented 100",
	)
	assert.NotEmpty(t, aws.ToString(out.NextToken), "25 brokers > default page size of 20: a next page must exist")
}

// TestListConfigurationRevisions_Pagination proves ListConfigurationRevisions
// honors MaxResults/NextToken (api_op_ListConfigurationRevisions.go) at all --
// pre-fix the handler ignored both query parameters entirely and always
// returned every revision, unbounded.
func TestListConfigurationRevisions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMQClient(t, h)

	created, err := client.CreateConfiguration(t.Context(), &mqsdk.CreateConfigurationInput{
		Name:          aws.String("revisions-config"),
		EngineType:    "ACTIVEMQ",
		EngineVersion: aws.String("5.15.14"),
	})
	require.NoError(t, err)

	const extraRevisions = 24 // + revision 1 from Create = 25 total

	for range extraRevisions {
		_, err = client.UpdateConfiguration(t.Context(), &mqsdk.UpdateConfigurationInput{
			ConfigurationId: created.Id,
			Data:            aws.String("PGJyb2tlcj48L2Jyb2tlcj4="),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListConfigurationRevisions(t.Context(), &mqsdk.ListConfigurationRevisionsInput{
		ConfigurationId: created.Id,
		MaxResults:      aws.Int32(10),
	})
	require.NoError(t, err)
	assert.Len(t, page1.Revisions, 10, "MaxResults=10 must cap the page at 10")
	require.NotEmpty(t, aws.ToString(page1.NextToken), "25 revisions > 10 per page: a next page must exist")

	page2, err := client.ListConfigurationRevisions(t.Context(), &mqsdk.ListConfigurationRevisionsInput{
		ConfigurationId: created.Id,
		MaxResults:      aws.Int32(10),
		NextToken:       page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.Revisions, 10, "second page must also be capped at 10")
	assert.NotEqual(
		t,
		page1.Revisions[0].Revision,
		page2.Revisions[0].Revision,
		"second page must be the remainder, not a repeat of page 1",
	)
}
