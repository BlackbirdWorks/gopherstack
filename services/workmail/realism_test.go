package workmail_test

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/aws/aws-sdk-go-v2/service/workmail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

func TestCreateOrganization_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		alias   string
		domains []string
		wantErr bool
	}{
		{name: "valid", alias: "my-org-1"},
		{name: "space", alias: "my org", wantErr: true},
		{name: "underscore", alias: "my_org", wantErr: true},
		{name: "double hyphen", alias: "my--org", wantErr: true},
		{name: "directory prefix", alias: "d-123456", wantErr: true},
		{name: "too long", alias: strings.Repeat("a", 63), wantErr: true},
		{name: "max length", alias: strings.Repeat("a", 62)},
		{name: "bad domain", alias: "dom-org", domains: []string{"not_a_domain"}, wantErr: true},
		{name: "good domain", alias: "dom-org-ok", domains: []string{"mail.example.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newWorkMailSDKClient(t, newTestHandler(t))
			in := &workmailsdk.CreateOrganizationInput{Alias: aws.String(tt.alias)}
			for _, d := range tt.domains {
				in.Domains = append(in.Domains, types.Domain{DomainName: aws.String(d)})
			}

			out, err := client.CreateOrganization(t.Context(), in)
			if tt.wantErr {
				var apiErr *types.InvalidParameterException
				require.ErrorAs(t, err, &apiErr)
				assert.NotContains(t, apiErr.ErrorMessage(), "InvalidParameterException:")

				return
			}

			require.NoError(t, err)
			assert.Len(t, aws.ToString(out.OrganizationId), 34)
		})
	}
}

func TestEntityName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entity  string
		wantErr bool
	}{
		{name: "user ok", entity: "alice.smith"},
		{name: "user space", entity: "alice smith", wantErr: true},
		{name: "user too long", entity: strings.Repeat("u", 65), wantErr: true},
		{name: "resource ok", entity: "room1"},
		{name: "resource too long", entity: strings.Repeat("r", 21), wantErr: true},
		{name: "group ok", entity: "team-a"},
		{name: "group bang", entity: "team!", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newWorkMailSDKClient(t, newTestHandler(t))
			org := newWorkMailOrg(t, client)

			var err error

			switch {
			case strings.HasPrefix(tt.name, "user"):
				_, err = client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: org, Name: aws.String(tt.entity), DisplayName: aws.String("d"),
				})
			case strings.HasPrefix(tt.name, "resource"):
				_, err = client.CreateResource(t.Context(), &workmailsdk.CreateResourceInput{
					OrganizationId: org, Name: aws.String(tt.entity), Type: types.ResourceTypeRoom,
				})
			default:
				_, err = client.CreateGroup(t.Context(), &workmailsdk.CreateGroupInput{
					OrganizationId: org, Name: aws.String(tt.entity),
				})
			}

			if tt.wantErr {
				var apiErr *types.InvalidParameterException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestListOrganizations_PageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
		max   int32
		bad   bool
	}{
		{name: "defaults", max: 0},
		{name: "max too big", max: 101, bad: true},
		{name: "max negative", max: -1, bad: true},
		{name: "garbage token", max: 1, token: "garbage", bad: true},
		{name: "numeric token", max: 1, token: "1", bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newWorkMailSDKClient(t, newTestHandler(t))
			newWorkMailOrg(t, client)

			in := &workmailsdk.ListOrganizationsInput{}
			if tt.max != 0 {
				in.MaxResults = aws.Int32(tt.max)
			}
			if tt.token != "" {
				in.NextToken = aws.String(tt.token)
			}

			_, err := client.ListOrganizations(t.Context(), in)
			if tt.bad {
				var apiErr *types.InvalidParameterException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestListOrganizations_OpaqueTokenRoundTrip(t *testing.T) {
	t.Parallel()

	client := newWorkMailSDKClient(t, newTestHandler(t))
	for range 3 {
		newWorkMailOrg(t, client)
	}

	seen := map[string]bool{}
	pager := workmailsdk.NewListOrganizationsPaginator(client, &workmailsdk.ListOrganizationsInput{
		MaxResults: aws.Int32(1),
	})

	for pager.HasMorePages() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, s := range page.OrganizationSummaries {
			seen[aws.ToString(s.OrganizationId)] = true
		}
	}

	assert.Len(t, seen, 3)
}

func TestOrganizationLifecycle_Delay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
		wait  time.Duration
	}{
		{name: "no delay", delay: 0, wait: 0, want: "ACTIVE"},
		{name: "requested", delay: 10 * time.Second, wait: time.Second, want: "REQUESTED"},
		{name: "creating", delay: 10 * time.Second, wait: 7 * time.Second, want: "CREATING"},
		{name: "active", delay: 10 * time.Second, wait: 10 * time.Second, want: "ACTIVE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := workmail.NewInMemoryBackend("000000000000", "us-east-1")
				b.SetLifecycleDelay(tt.delay)

				org, err := b.CreateOrganization(context.Background(), "life-org", nil, false)
				require.NoError(t, err)

				time.Sleep(tt.wait)

				got, err := b.DescribeOrganization(org.OrgID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got.State)

				list, _, err := b.ListOrganizations(context.Background(), 10, "")
				require.NoError(t, err)
				require.Len(t, list, 1)
				assert.Equal(t, tt.want, list[0].State)
			})
		})
	}
}

func TestOrganizationLifecycle_StateException(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newTestHandler(t)
		b, ok := h.Backend.(*workmail.InMemoryBackend)
		require.True(t, ok)
		b.SetLifecycleDelay(10 * time.Second)

		org, err := b.CreateOrganization(context.Background(), "state-org", nil, false)
		require.NoError(t, err)

		rec := doOp(t, h, "ListUsers", `{"OrganizationId":"`+org.OrgID+`"}`)
		assert.Equal(t, 400, rec.Code)
		assert.Contains(t, rec.Body.String(), "OrganizationStateException")

		time.Sleep(10 * time.Second)

		rec = doOp(t, h, "ListUsers", `{"OrganizationId":"`+org.OrgID+`"}`)
		assert.Equal(t, 200, rec.Code)
	})
}

func TestMailboxExportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		want         string
		delay        time.Duration
		wait         time.Duration
		wantProgress int32
	}{
		{name: "default stays running", delay: 0, wait: time.Hour, want: "RUNNING"},
		{name: "midway", delay: 10 * time.Second, wait: 5 * time.Second, want: "RUNNING", wantProgress: 50},
		{name: "complete", delay: 10 * time.Second, wait: 10 * time.Second, want: "COMPLETED", wantProgress: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := workmail.NewInMemoryBackend("000000000000", "us-east-1")
				org, err := b.CreateOrganization(context.Background(), "exp-org", nil, false)
				require.NoError(t, err)
				user, err := b.CreateUser(org.OrgID, "exporter", workmail.CreateUserParams{DisplayName: "E"})
				require.NoError(t, err)

				b.SetLifecycleDelay(tt.delay)

				job, err := b.StartMailboxExportJob(
					org.OrgID, user.UserID, "", "arn:aws:iam::000000000000:role/r",
					"arn:aws:kms:us-east-1:000000000000:key/k", "bucket", "p",
				)
				require.NoError(t, err)

				time.Sleep(tt.wait)

				got, err := b.DescribeMailboxExportJob(org.OrgID, job.JobID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got.State)
				assert.Equal(t, tt.wantProgress, got.EstimatedProgress)

				err = b.CancelMailboxExportJob(org.OrgID, job.JobID)
				if tt.want == "COMPLETED" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}

func TestStartMailboxExportJob_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*workmailsdk.StartMailboxExportJobInput)
		want   string
	}{
		{
			name:   "missing kms",
			mutate: func(in *workmailsdk.StartMailboxExportJobInput) { in.KmsKeyArn = aws.String("") },
			want:   "invalid",
		},
		{
			name:   "unknown entity",
			mutate: func(in *workmailsdk.StartMailboxExportJobInput) { in.EntityId = aws.String("nope") },
			want:   "notfound",
		},
		{name: "ok", mutate: func(*workmailsdk.StartMailboxExportJobInput) {}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newWorkMailSDKClient(t, newTestHandler(t))
			org := newWorkMailOrg(t, client)
			user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
				OrganizationId: org, Name: aws.String("exp"), DisplayName: aws.String("Exp"),
			})
			require.NoError(t, err)

			in := &workmailsdk.StartMailboxExportJobInput{
				OrganizationId: org, EntityId: user.UserId, ClientToken: aws.String("tok-" + tt.name),
				RoleArn:   aws.String("arn:aws:iam::000000000000:role/r"),
				KmsKeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/k"), S3BucketName: aws.String("b"),
				S3Prefix: aws.String("p"),
			}
			tt.mutate(in)

			_, err = client.StartMailboxExportJob(t.Context(), in)

			switch tt.want {
			case "invalid":
				var apiErr *types.InvalidParameterException
				require.ErrorAs(t, err, &apiErr)
			case "notfound":
				var apiErr *types.EntityNotFoundException
				require.ErrorAs(t, err, &apiErr)
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestOrganizationNotFound_Message(t *testing.T) {
	t.Parallel()

	client := newWorkMailSDKClient(t, newTestHandler(t))
	_, err := client.DescribeOrganization(t.Context(), &workmailsdk.DescribeOrganizationInput{
		OrganizationId: aws.String("m-0123456789abcdef0123456789abcdef"),
	})

	var apiErr *types.OrganizationNotFoundException
	require.ErrorAs(t, err, &apiErr)
	assert.NotContains(t, apiErr.ErrorMessage(), "OrganizationNotFoundException")
	assert.Contains(t, apiErr.ErrorMessage(), "not found")
}
