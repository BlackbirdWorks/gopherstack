package elbv2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func TestCreateTargetGroup_AttributeDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     map[string]string
		name     string
		wantNone []string
		input    elbv2.CreateTargetGroupInput
	}{
		{
			name:  "alb instance",
			input: elbv2.CreateTargetGroupInput{Protocol: "HTTP", Port: 80, VpcID: "vpc-1"},
			want: map[string]string{
				"stickiness.type":                             "lb_cookie",
				"stickiness.lb_cookie.duration_seconds":       "86400",
				"load_balancing.algorithm.anomaly_mitigation": "off",
				"load_balancing.cross_zone.enabled":           "use_load_balancer_configuration",
			},
			wantNone: []string{"proxy_protocol_v2.enabled", "lambda.multi_value_headers.enabled"},
		},
		{
			name:  "alb lambda",
			input: elbv2.CreateTargetGroupInput{TargetType: "lambda"},
			want:  map[string]string{"lambda.multi_value_headers.enabled": "false"},
			wantNone: []string{
				"deregistration_delay.timeout_seconds", "slow_start.duration_seconds",
			},
		},
		{
			name:  "nlb tcp ip",
			input: elbv2.CreateTargetGroupInput{Protocol: "TCP", Port: 80, VpcID: "vpc-1", TargetType: "ip"},
			want: map[string]string{
				"stickiness.type":                                     "source_ip",
				"preserve_client_ip.enabled":                          "false",
				"proxy_protocol_v2.enabled":                           "false",
				"deregistration_delay.connection_termination.enabled": "false",
			},
			wantNone: []string{"slow_start.duration_seconds"},
		},
		{
			name:  "nlb udp instance",
			input: elbv2.CreateTargetGroupInput{Protocol: "UDP", Port: 53, VpcID: "vpc-1"},
			want: map[string]string{
				"preserve_client_ip.enabled":                          "true",
				"deregistration_delay.connection_termination.enabled": "true",
			},
		},
		{
			name:  "gwlb",
			input: elbv2.CreateTargetGroupInput{Protocol: "GENEVE", Port: 6081, VpcID: "vpc-1"},
			want: map[string]string{
				"stickiness.type":                   "source_ip_dest_ip",
				"target_failover.on_unhealthy":      "no_rebalance",
				"target_failover.on_deregistration": "no_rebalance",
			},
			wantNone: []string{"load_balancing.cross_zone.enabled"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
			t.Cleanup(b.Close)
			tt.input.Name = "tg-defaults"

			tg, err := b.CreateTargetGroup(tt.input)
			require.NoError(t, err)

			attrs, err := b.DescribeTargetGroupAttributes(tg.TargetGroupArn)
			require.NoError(t, err)

			for k, v := range tt.want {
				assert.Equal(t, v, attrs[k], k)
			}

			for _, k := range tt.wantNone {
				assert.NotContains(t, attrs, k)
			}
		})
	}
}
