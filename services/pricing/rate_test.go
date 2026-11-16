package pricing_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/pricing"
)

func TestOnDemandRate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		match   map[string]string
		name    string
		service string
		region  string
		unit    string
		want    float64
		wantOK  bool
	}{
		{
			name: "ec2 linux", service: "AmazonEC2", region: "us-east-1", unit: "Hrs", want: 0.0416, wantOK: true,
			match: map[string]string{"usagetype": "BoxUsage:t3.medium", "operation": "RunInstances"},
		},
		{
			name:    "ec2 windows",
			service: "AmazonEC2",
			region:  "us-east-1",
			unit:    "Hrs",
			want:    0.0416 * 1.9,
			wantOK:  true,
			match:   map[string]string{"usagetype": "BoxUsage:t3.medium", "operation": "RunInstances:0002"},
		},
		{
			name:    "regional factor",
			service: "AmazonEC2",
			region:  "sa-east-1",
			unit:    "Hrs",
			want:    0.0416 * 1.5,
			wantOK:  true,
			match:   map[string]string{"usagetype": "BoxUsage:t3.medium", "operation": "RunInstances"},
		},
		{
			name: "s3 first tier", service: "AmazonS3", region: "us-east-1", unit: "GB-Mo", want: 0.023, wantOK: true,
			match: map[string]string{"usagetype": "TimedStorage-ByteHrs"},
		},
		{
			name: "unknown region", service: "AmazonEC2", region: "eu-north-1", unit: "Hrs",
			match: map[string]string{"usagetype": "BoxUsage:t3.medium", "operation": "RunInstances"},
		},
		{
			name: "unit mismatch", service: "AmazonEC2", region: "us-east-1", unit: "GB-Mo",
			match: map[string]string{"usagetype": "BoxUsage:t3.medium", "operation": "RunInstances"},
		},
		{name: "unknown service", service: "Nope", region: "us-east-1", unit: "Hrs"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := pricing.OnDemandRate(tc.service, tc.region, tc.match, tc.unit)
			assert.Equal(t, tc.wantOK, ok)
			assert.InDelta(t, tc.want, got, 1e-9)
		})
	}
}
