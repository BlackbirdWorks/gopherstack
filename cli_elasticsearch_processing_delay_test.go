package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElasticsearchProcessingDelayWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		delay          time.Duration
		wantProcessing bool
	}{
		{name: "default_settles", delay: 0, wantProcessing: false},
		{name: "delay_processing", delay: time.Hour, wantProcessing: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newWiredSDKConfig(t, CLI{ElasticsearchProcessingDelay: tt.delay})

			c := elasticsearchservice.NewFromConfig(cfg)

			_, err := c.CreateElasticsearchDomain(t.Context(), &elasticsearchservice.CreateElasticsearchDomainInput{
				DomainName: aws.String("wired"),
			})
			require.NoError(t, err)

			out, err := c.DescribeElasticsearchDomain(
				t.Context(),
				&elasticsearchservice.DescribeElasticsearchDomainInput{
					DomainName: aws.String("wired"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantProcessing, aws.ToBool(out.DomainStatus.Processing))
		})
	}
}
