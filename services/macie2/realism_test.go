package macie2_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	macie2types "github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func oneTimeJobInput(token string) *macie2sdk.CreateClassificationJobInput {
	return &macie2sdk.CreateClassificationJobInput{
		ClientToken: aws.String(token),
		JobType:     macie2types.JobTypeOneTime,
		Name:        aws.String("job"),
		S3JobDefinition: &macie2types.S3JobDefinition{
			BucketDefinitions: []macie2types.S3BucketDefinitionForJob{
				{AccountId: aws.String("000000000000"), Buckets: []string{"bucket-one"}},
			},
		},
	}
}

func TestClassificationJobCompletesAfterWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status macie2types.JobStatus
		delay  time.Duration
	}{
		{name: "elapsed", delay: time.Nanosecond, status: macie2types.JobStatusComplete},
		{name: "running", delay: time.Hour, status: macie2types.JobStatusRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
			backend.SetLifecycleDelay(tt.delay)
			client := newTestMacie2SDKClient(t, macie2.NewHandler(backend))

			out, err := client.CreateClassificationJob(t.Context(), oneTimeJobInput("tok"))
			require.NoError(t, err)

			got, err := client.DescribeClassificationJob(
				t.Context(),
				&macie2sdk.DescribeClassificationJobInput{JobId: out.JobId},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.status, got.JobStatus)

			list, err := client.ListClassificationJobs(t.Context(), &macie2sdk.ListClassificationJobsInput{})
			require.NoError(t, err)
			require.Len(t, list.Items, 1)
			assert.Equal(t, tt.status, list.Items[0].JobStatus)
		})
	}
}

func TestClassificationJobIdempotentToken(t *testing.T) {
	t.Parallel()

	client := newTestMacie2SDKClient(t, newTestHandler(t))

	first, err := client.CreateClassificationJob(t.Context(), oneTimeJobInput("same"))
	require.NoError(t, err)

	second, err := client.CreateClassificationJob(t.Context(), oneTimeJobInput("same"))
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.JobId), aws.ToString(second.JobId))
}

func TestCreateClassificationJobValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *macie2sdk.CreateClassificationJobInput)
		name   string
		code   string
	}{
		{
			name:   "long name",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) { in.Name = aws.String(strings.Repeat("n", 501)) },
			code:   "ValidationException",
		},
		{
			name:   "sampling zero",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) { in.SamplingPercentage = aws.Int32(0) },
			code:   "ValidationException",
		},
		{
			name:   "sampling high",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) { in.SamplingPercentage = aws.Int32(101) },
			code:   "ValidationException",
		},
		{
			name:   "scheduled without frequency",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) { in.JobType = macie2types.JobTypeScheduled },
			code:   "ValidationException",
		},
		{
			name: "one time with frequency",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) {
				in.ScheduleFrequency = &macie2types.JobScheduleFrequency{DailySchedule: &macie2types.DailySchedule{}}
			},
			code: "ValidationException",
		},
		{
			name: "recommended with managed ids",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) {
				in.ManagedDataIdentifierIds = []string{"EMAIL_ADDRESS"}
			},
			code: "ValidationException",
		},
		{
			name: "none without custom ids",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) {
				in.ManagedDataIdentifierSelector = macie2types.ManagedDataIdentifierSelectorNone
			},
			code: "ValidationException",
		},
		{
			name:   "unknown allow list",
			mutate: func(in *macie2sdk.CreateClassificationJobInput) { in.AllowListIds = []string{"missing"} },
			code:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMacie2SDKClient(t, newTestHandler(t))
			in := oneTimeJobInput("tok")
			tt.mutate(in)

			_, err := client.CreateClassificationJob(t.Context(), in)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.code)
			assert.NotContains(t, err.Error(), tt.code+": "+tt.code)
		})
	}
}

func TestListInvalidNextToken(t *testing.T) {
	t.Parallel()

	client := newTestMacie2SDKClient(t, newTestHandler(t))

	tests := []struct {
		call func() error
		name string
	}{
		{name: "jobs", call: func() error {
			_, err := client.ListClassificationJobs(
				t.Context(),
				&macie2sdk.ListClassificationJobsInput{NextToken: aws.String("zzz")},
			)

			return err
		}},
		{name: "allow lists", call: func() error {
			_, err := client.ListAllowLists(t.Context(), &macie2sdk.ListAllowListsInput{NextToken: aws.String("zzz")})

			return err
		}},
		{name: "findings filters", call: func() error {
			_, err := client.ListFindingsFilters(
				t.Context(),
				&macie2sdk.ListFindingsFiltersInput{NextToken: aws.String("zzz")},
			)

			return err
		}},
		{name: "members", call: func() error {
			_, err := client.ListMembers(t.Context(), &macie2sdk.ListMembersInput{NextToken: aws.String("zzz")})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}

func TestCreateAllowListAndFilterValidation(t *testing.T) {
	t.Parallel()

	client := newTestMacie2SDKClient(t, newTestHandler(t))

	_, err := client.CreateAllowList(t.Context(), &macie2sdk.CreateAllowListInput{
		ClientToken: aws.String("a"), Name: aws.String("al"),
		Criteria: &macie2types.AllowListCriteria{Regex: aws.String("(")},
	})
	require.ErrorContains(t, err, "ValidationException")

	_, err = client.CreateAllowList(t.Context(), &macie2sdk.CreateAllowListInput{
		ClientToken: aws.String("b"), Name: aws.String("al"),
		Criteria: &macie2types.AllowListCriteria{
			Regex:       aws.String("a+"),
			S3WordsList: &macie2types.S3WordsList{BucketName: aws.String("bkt"), ObjectKey: aws.String("k")},
		},
	})
	require.ErrorContains(t, err, "ValidationException")

	_, err = client.CreateFindingsFilter(t.Context(), &macie2sdk.CreateFindingsFilterInput{
		ClientToken: aws.String("c"), Name: aws.String("filter"), Action: "BOGUS",
		FindingCriteria: &macie2types.FindingCriteria{},
	})
	require.ErrorContains(t, err, "ValidationException")
}

func TestCreateAllowListClientTokenReplay(t *testing.T) {
	t.Parallel()

	client := newTestMacie2SDKClient(t, newTestHandler(t))
	in := &macie2sdk.CreateAllowListInput{
		ClientToken: aws.String("replay"), Name: aws.String("al"),
		Criteria: &macie2types.AllowListCriteria{Regex: aws.String("a+")},
	}

	first, err := client.CreateAllowList(t.Context(), in)
	require.NoError(t, err)

	second, err := client.CreateAllowList(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.Id), aws.ToString(second.Id))

	in.Name = aws.String("other")
	_, err = client.CreateAllowList(t.Context(), in)
	require.ErrorContains(t, err, "ConflictException")
}

func TestMemberARNShape(t *testing.T) {
	t.Parallel()

	client := newTestMacie2SDKClient(t, newTestHandler(t))

	_, err := client.CreateMember(t.Context(), &macie2sdk.CreateMemberInput{
		Account: &macie2types.AccountDetail{AccountId: aws.String("111111111111"), Email: aws.String("a@example.com")},
	})
	require.NoError(t, err)

	got, err := client.GetMember(t.Context(), &macie2sdk.GetMemberInput{Id: aws.String("111111111111")})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:macie2:us-east-1:000000000000:member/111111111111", aws.ToString(got.Arn))
}
