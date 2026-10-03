package iot_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

const mrHome = "us-east-1"

func newRegionHandler(broker *iot.Broker) *iot.Handler {
	h := iot.NewHandler(iot.NewInMemoryBackend(), broker)
	h.EnableRegions()

	return h
}

func regionCall(t *testing.T, h *iot.Handler, region, method, path, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return out
}

func thingNames(t *testing.T, h *iot.Handler, region string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, http.MethodGet, "/things", "")["things"].([]any)

	names := make([]string, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		n, _ := m["thingName"].(string)
		names = append(names, n)
	}

	return names
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(nil)

			for _, r := range tc.regions {
				regionCall(t, h, r, http.MethodPost, "/things/shared", `{}`)
				regionCall(t, h, r, http.MethodPost, "/things/only-"+r, `{}`)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, thingNames(t, h, r))

				arn, _ := regionCall(t, h, r, http.MethodGet, "/things/shared", "")["thingArn"].(string)
				assert.Contains(t, arn, ":"+r+":")
			}
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(nil)
			regionCall(t, src, mrHome, http.MethodPost, "/things/home", `{}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", http.MethodPost, "/things/eu", `{}`)
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.(iot.Snapshottable).Snapshot(context.Background()), snap)
			}

			dst := newRegionHandler(nil)
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, thingNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(thingNames(t, dst, "eu-west-1")) == 1)

			old := iot.NewInMemoryBackend()
			require.NoError(t, old.Restore(context.Background(), snap))
		})
	}
}

type recordingDispatcher struct{ queues chan string }

func (d *recordingDispatcher) SendToSQS(queueURL, _ string) error {
	d.queues <- queueURL

	return nil
}

func (d *recordingDispatcher) InvokeLambda(context.Context, string, []byte) error { return nil }

func TestHandler_MultiRegionRulesFireOnSharedBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ruleRegion string
	}{
		{name: "home-rule", ruleRegion: mrHome},
		{name: "peer-rule", ruleRegion: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			home := iot.NewInMemoryBackend()
			disp := &recordingDispatcher{queues: make(chan string, 4)}
			home.SetRuleDispatcher(disp)

			port := freeTCPPort(t)
			h := iot.NewHandler(home, iot.NewBroker(home, port))
			h.EnableRegions()

			regionCall(t, h, tc.ruleRegion, http.MethodPost, "/rules/r1",
				`{"sql":"SELECT * FROM 'a/b'","actions":[{"sqs":{"queueUrl":"q-`+tc.ruleRegion+`","roleArn":"r"}}]}`)

			require.NoError(t, h.StartWorker(t.Context()))
			t.Cleanup(func() { h.Shutdown(context.Background()) })

			opts := pahomqtt.NewClientOptions().
				AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port)).
				SetClientID("mr-pub").
				SetConnectTimeout(200 * time.Millisecond)
			client := pahomqtt.NewClient(opts)

			require.Eventually(t, func() bool {
				tok := client.Connect()

				return tok.WaitTimeout(300*time.Millisecond) && tok.Error() == nil
			}, 5*time.Second, 50*time.Millisecond)

			defer client.Disconnect(100)

			tok := client.Publish("a/b", 0, false, []byte(`{"x":1}`))
			require.True(t, tok.WaitTimeout(3*time.Second))

			select {
			case q := <-disp.queues:
				assert.Equal(t, "q-"+tc.ruleRegion, q)
			case <-time.After(3 * time.Second):
				require.FailNow(t, "rule never fired")
			}
		})
	}
}
