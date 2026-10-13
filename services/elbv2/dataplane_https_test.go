package elbv2_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataPlane_ForwardHTTPSTargetSelfSigned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "self signed cert", body: "secure"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)

			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(srv.Close)

			out, err := f.client.CreateTargetGroup(t.Context(), &elbv2sdk.CreateTargetGroupInput{
				Name: aws.String("tg-tls"), TargetType: elbv2types.TargetTypeEnumIp,
				Protocol: elbv2types.ProtocolEnumHttps, Port: aws.Int32(443), VpcId: aws.String("vpc-1"),
			})
			require.NoError(t, err)

			tg := aws.ToString(out.TargetGroups[0].TargetGroupArn)
			f.register(t, tg, srv.Listener.Addr().String())
			f.listener(t, forwardAction(tg))

			got := f.do(t, dpReq{path: "/x"})
			assert.Equal(t, http.StatusOK, got.status)
			assert.Equal(t, tt.body, got.body)
		})
	}
}
