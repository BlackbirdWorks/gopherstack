package elbv2_test

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataPlane_WebSocketUpgradeEcho(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "upgrade echoes through routed data plane", path: "/ws"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			tg := f.tg(t, "tg-ws", elbv2types.TargetTypeEnumIp, "")
			f.register(t, tg, wsTarget(t))
			f.listener(t, forwardAction(tg))

			conn, err := net.Dial("tcp", f.srvAddr)
			require.NoError(t, err)

			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

			_, err = io.WriteString(conn, "GET /_gopherstack/elbv2/"+f.lbName+tt.path+" HTTP/1.1\r\nHost: "+f.srvAddr+
				"\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"+
				"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
			require.NoError(t, err)

			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

			_, err = io.WriteString(conn, "frame")
			require.NoError(t, err)

			buf := make([]byte, len("frame"))
			_, err = io.ReadFull(br, buf)
			require.NoError(t, err)
			assert.Equal(t, "frame", string(buf))
		})
	}
}

func TestDataPlane_XFFProcessingModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      string
		wantXFF   string
		wantAbsen bool
	}{
		{name: "preserve", mode: "preserve", wantXFF: "10.0.0.9"},
		{name: "remove", mode: "remove", wantAbsen: true},
		{name: "append", mode: "append", wantXFF: "10.0.0.9, 127.0.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			echo := newHeaderEcho(t)
			tg := f.tg(t, "tg-xff", elbv2types.TargetTypeEnumIp, "")
			f.register(t, tg, echo.addr())
			f.listener(t, forwardAction(tg))

			_, err := f.client.ModifyLoadBalancerAttributes(t.Context(), &elbv2sdk.ModifyLoadBalancerAttributesInput{
				LoadBalancerArn: aws.String(f.lbArn),
				Attributes: []elbv2types.LoadBalancerAttribute{
					{Key: aws.String("routing.http.xff_header_processing.mode"), Value: aws.String(tt.mode)},
				},
			})
			require.NoError(t, err)

			got := f.do(t, dpReq{path: "/x", headers: map[string]string{"X-Forwarded-For": "10.0.0.9"}})
			require.Equal(t, http.StatusOK, got.status)

			lines := headerLines(got.body)
			if tt.wantAbsen {
				assert.NotContains(t, got.body, "X-Forwarded-For")

				return
			}

			assert.Equal(t, tt.wantXFF, lines["X-Forwarded-For"])
		})
	}
}

func TestDataPlane_UpgradeNon101Passthrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		upgrade    string
		wantBody   string
		wantStatus int
	}{
		{name: "target rejects upgrade", upgrade: "tls/1.0", wantBody: "no upgrade", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			tg := f.tg(t, "tg-no101", elbv2types.TargetTypeEnumIp, "")
			f.register(t, tg, wsTarget(t))
			f.listener(t, forwardAction(tg))

			conn, err := net.Dial("tcp", f.srvAddr)
			require.NoError(t, err)

			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

			_, err = io.WriteString(conn, "GET /_gopherstack/elbv2/"+f.lbName+"/x HTTP/1.1\r\nHost: "+f.srvAddr+
				"\r\nConnection: Upgrade\r\nUpgrade: "+tt.upgrade+"\r\n\r\n")
			require.NoError(t, err)

			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Contains(t, string(body), tt.wantBody)
		})
	}
}
