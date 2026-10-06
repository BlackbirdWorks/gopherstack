package ses_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListTemplates_ReportsCreatedTimestampAcrossUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		update bool
	}{
		{name: "after create"},
		{name: "kept after update", update: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSESClient(t, newHandler())
			ctx := t.Context()

			_, err := c.CreateTemplate(ctx, &sessdk.CreateTemplateInput{Template: &types.Template{
				TemplateName: aws.String("t"), SubjectPart: aws.String("s"),
			}})
			require.NoError(t, err)

			first, err := c.ListTemplates(ctx, &sessdk.ListTemplatesInput{})
			require.NoError(t, err)
			require.Len(t, first.TemplatesMetadata, 1)
			require.NotNil(t, first.TemplatesMetadata[0].CreatedTimestamp)

			if tc.update {
				_, err = c.UpdateTemplate(ctx, &sessdk.UpdateTemplateInput{Template: &types.Template{
					TemplateName: aws.String("t"), SubjectPart: aws.String("s2"),
				}})
				require.NoError(t, err)
			}

			got, err := c.ListTemplates(ctx, &sessdk.ListTemplatesInput{})
			require.NoError(t, err)
			require.Len(t, got.TemplatesMetadata, 1)
			assert.True(
				t,
				first.TemplatesMetadata[0].CreatedTimestamp.Equal(*got.TemplatesMetadata[0].CreatedTimestamp),
			)
		})
	}
}
