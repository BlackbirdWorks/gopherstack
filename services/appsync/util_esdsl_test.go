package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateMappingTemplate_QueryDSL(t *testing.T) {
	t.Parallel()

	const doc = `{"upvotes":{"ne":15,"range":[10,20]},"title":{"eq":"hihihi","wildcard":"h*i"}}`

	tests := []struct {
		name     string
		template string
		want     string
		wantErr  bool
	}{
		{
			name:     "documented example elasticsearch",
			template: `$util.transform.toElasticsearchQueryDSL($util.parseJson('` + doc + `'))`,
			want: `{"bool":{"must":[` +
				`{"bool":{"must":[{"term":{"title":"hihihi"}},{"wildcard":{"title":"h*i"}}]}},` +
				`{"bool":{"must":[{"bool":{"must_not":{"term":{"upvotes":15}}}},` +
				`{"range":{"upvotes":{"gte":10,"lte":20}}}]}}]}}`,
		},
		{
			name:     "opensearch alias matches",
			template: `$util.transform.toOpenSearchQueryDSL($util.parseJson('{"a":{"gt":1,"matchPhrase":"x y"}}'))`,
			want: `{"bool":{"must":[{"bool":{"must":[{"range":{"a":{"gt":1}}},` +
				`{"match_phrase":{"a":"x y"}}]}}]}}`,
		},
		{
			name:     "or and not groups",
			template: `$util.transform.toOpenSearchQueryDSL($util.parseJson('{"or":[{"a":{"eq":1}},{"b":{"exists":false}}],"not":{"c":{"eq":2}}}'))`,
			want: `{"bool":{"must":[` +
				`{"bool":{"must_not":{"bool":{"must":[{"bool":{"must":[{"term":{"c":2}}]}}]}}}},` +
				`{"bool":{"should":[{"bool":{"must":[{"bool":{"must":[{"term":{"a":1}}]}}]}},` +
				`{"bool":{"must":[{"bool":{"must":[{"bool":{"must_not":{"exists":{"field":"b"}}}}]}}]}}]}}]}}`,
		},
		{
			name:     "unknown operator fails",
			template: `$util.transform.toElasticsearchQueryDSL($util.parseJson('{"a":{"bogus":1}}'))`,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			client := newTestAppsyncClient(t, h)

			out, err := client.EvaluateMappingTemplate(t.Context(), &appsyncsdk.EvaluateMappingTemplateInput{
				Template: aws.String(tt.template), Context: aws.String(`{}`),
			})

			if tt.wantErr {
				if err == nil {
					require.NotNil(t, out.Error)
				}

				return
			}

			require.NoError(t, err)
			require.Nil(t, out.Error)
			assert.JSONEq(t, tt.want, aws.ToString(out.EvaluationResult))
		})
	}
}
