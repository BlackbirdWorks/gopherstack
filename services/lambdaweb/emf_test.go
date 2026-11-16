package lambdaweb_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const emfLine = `{"_aws":{"Timestamp":1,"CloudWatchMetrics":[{"Namespace":"MyWebApp","Dimensions":[["Provider"]],` +
	`"Metrics":[{"Name":"CheckoutLatency","Unit":"Milliseconds"},{"Name":"OrdersPlaced","Unit":"Count"}]}]},` +
	`"Provider":"card","CheckoutLatency":[12.5,7.5],"OrdersPlaced":3,"requestPath":"/checkout"}`

func TestLogs_EmbeddedMetricFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		line        string
		wantMetrics bool
		wantRaw     bool
	}{
		{name: "emf line", line: emfLine, wantMetrics: true, wantRaw: true},
		{name: "missing dimension value", line: `{"_aws":{"CloudWatchMetrics":[{"Namespace":"N","Dimensions":[["D"]],` +
			`"Metrics":[{"Name":"M"}]}]},"M":1}`},
		{name: "plain json", line: `{"hello":"world"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, logs := newLoggedEnv(t, logOpts{
				emit: func(_ lambdaweb.LaunchSpec, line func(bool, string)) { line(false, tc.line) },
			})

			m := &fakeMetrics{}
			d.backend.SetMetricEmitter(m)

			require.Equal(t, http.StatusOK, d.get(t).StatusCode)

			if !tc.wantMetrics {
				assert.Nil(t, m.find("MyWebApp", "CheckoutLatency"))
				assert.Nil(t, m.find("N", "M"))

				return
			}

			assert.InDelta(t, 20, m.sum("MyWebApp", "CheckoutLatency"), 0, "an array records each observation")
			assert.InDelta(t, 3, m.sum("MyWebApp", "OrdersPlaced"), 0)

			p := m.find("MyWebApp", "CheckoutLatency")
			require.NotNil(t, p)
			assert.Equal(t, "Milliseconds", p.Unit)
			assert.Equal(t, map[string]string{"Provider": "card"}, dimMap(p))
			assert.Equal(t, "us-east-1", p.Region)

			var raw bool

			for _, l := range logs.snapshot("/aws/lambda/web/app") {
				raw = raw || l == tc.line
			}

			assert.Equal(t, tc.wantRaw, raw, "the EMF object is delivered unwrapped")
		})
	}
}
