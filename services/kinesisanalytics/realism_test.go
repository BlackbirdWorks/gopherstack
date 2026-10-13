package kinesisanalytics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kasdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealism_ErrorMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *kasdk.Client) error
		name    string
		wantMsg string
	}{
		{
			name: "describe_missing",
			call: func(c *kasdk.Client) error {
				_, err := c.DescribeApplication(t.Context(), &kasdk.DescribeApplicationInput{
					ApplicationName: aws.String("nope"),
				})

				return err
			},
			wantMsg: "Specified application can't be found.",
		},
		{
			name: "create_duplicate",
			call: func(c *kasdk.Client) error {
				in := &kasdk.CreateApplicationInput{ApplicationName: aws.String("dup")}
				if _, err := c.CreateApplication(t.Context(), in); err != nil {
					return err
				}

				_, err := c.CreateApplication(t.Context(), in)

				return err
			},
			wantMsg: "application dup already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandlerWithBackend(t)
			err := tt.call(newTestKASDKClient(t, h))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
			assert.NotRegexp(t, `Exception: \w*Exception`, err.Error())
		})
	}
}

func TestRealism_ListApplicationsCursor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		start string
		want  []string
	}{
		{name: "existing", start: "b", want: []string{"c"}},
		{name: "between", start: "bb", want: []string{"c"}},
		{name: "past_end", start: "zzz", want: []string{}},
		{name: "before_all", start: "A", want: []string{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandlerWithBackend(t)
			client := newTestKASDKClient(t, h)

			for _, n := range []string{"a", "b", "c"} {
				in := &kasdk.CreateApplicationInput{ApplicationName: aws.String(n)}
				_, err := client.CreateApplication(t.Context(), in)
				require.NoError(t, err)
			}

			out, err := client.ListApplications(t.Context(), &kasdk.ListApplicationsInput{
				ExclusiveStartApplicationName: aws.String(tt.start),
			})
			require.NoError(t, err)

			got := []string{}
			for _, s := range out.ApplicationSummaries {
				got = append(got, aws.ToString(s.ApplicationName))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
