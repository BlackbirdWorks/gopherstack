package rds_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRDS_EventSubscriptions(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// CreateEventSubscription
	rec := postRDSForm(t, h, url.Values{
		"Action":           []string{"CreateEventSubscription"},
		"SubscriptionName": []string{"my-sub"},
		"SnsTopicArn":      []string{"arn:aws:sns:us-east-1:123456789012:rds-events"},
		"Enabled":          []string{"true"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeEventSubscriptions
	rec = postRDSForm(t, h, url.Values{
		"Action":           []string{"DescribeEventSubscriptions"},
		"SubscriptionName": []string{"my-sub"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// ModifyEventSubscription
	rec = postRDSForm(t, h, url.Values{
		"Action":           []string{"ModifyEventSubscription"},
		"SubscriptionName": []string{"my-sub"},
		"Enabled":          []string{"false"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeEvents
	rec = postRDSForm(t, h, url.Values{
		"Action": []string{"DescribeEvents"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeEventCategories
	rec = postRDSForm(t, h, url.Values{
		"Action": []string{"DescribeEventCategories"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DeleteEventSubscription
	rec = postRDSForm(t, h, url.Values{
		"Action":           []string{"DeleteEventSubscription"},
		"SubscriptionName": []string{"my-sub"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

func TestRDS_BlueGreenDeployments(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// DescribeBlueGreenDeployments (list, should be empty)
	rec := postRDSForm(t, h, url.Values{
		"Action": []string{"DescribeBlueGreenDeployments"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

func TestRDS_OptionGroups(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// CreateOptionGroup
	rec := postRDSForm(t, h, url.Values{
		"Action":                 []string{"CreateOptionGroup"},
		"OptionGroupName":        []string{"my-option-group"},
		"EngineName":             []string{"mysql"},
		"MajorEngineVersion":     []string{"8.0"},
		"OptionGroupDescription": []string{"My option group"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeOptionGroups
	rec = postRDSForm(t, h, url.Values{
		"Action":          []string{"DescribeOptionGroups"},
		"OptionGroupName": []string{"my-option-group"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DeleteOptionGroup
	rec = postRDSForm(t, h, url.Values{
		"Action":          []string{"DeleteOptionGroup"},
		"OptionGroupName": []string{"my-option-group"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}
