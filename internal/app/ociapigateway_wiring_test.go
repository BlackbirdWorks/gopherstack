package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func TestReserveFixedServicePorts_OCIAPIGateway(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 8163, ociapigateway.DefaultPort)

	tests := []struct {
		name      string
		wantTaken bool
	}{
		{"port inside pool range", true},
		{"port outside pool range", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantTaken, ociapigateway.DefaultPort)

			cli := CLI{OCIAPIGateway: ociapigateway.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(port))
		})
	}
}

func TestWireOCIAPIGatewaySettingsAndLinking(t *testing.T) {
	t.Parallel()

	idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
	gwH := ociapigateway.NewHandler(ociapigateway.NewInMemoryBackend())

	assert.NotPanics(t, func() {
		wireOCIAPIGateway(map[string]service.Registerable{"OCIIdentity": idH, "OCIAPIGateway": gwH})
		wireOCIAPIGateway(map[string]service.Registerable{"OCIAPIGateway": gwH})
	})

	cli := CLI{OCIAPIGateway: ociapigateway.Settings{Port: 8163}}
	assert.Equal(t, 8163, cli.GetOCIAPIGatewaySettings().Port)
}

func TestWireOCIAPIGatewayResolvesVaultSecrets(t *testing.T) {
	t.Parallel()

	clock := time.Now().UTC()
	vaultB := ocivaultbackend.NewInMemoryBackend()
	vaultB.SetClock(func() time.Time { return clock })
	vaultB.SetTransitionDelay(0)

	vault, _, err := vaultB.CreateVault(ocivaultbackend.CreateVaultIn{
		CompartmentID: "ocid1.compartment.oc1..c", DisplayName: "v", VaultType: "DEFAULT",
	}, "localhost")
	require.NoError(t, err)

	key, _, err := vaultB.CreateKey(vault.ID, ocivaultbackend.CreateKeyIn{
		CompartmentID: "ocid1.compartment.oc1..c", DisplayName: "k", ProtectionMode: "SOFTWARE",
		KeyShape: ocivaultbackend.KeyShape{Algorithm: "AES", Length: 32},
	})
	require.NoError(t, err)

	clock = clock.Add(time.Hour)
	vaultB.Sweep()

	secret, _, err := vaultB.CreateSecret(ocivaultbackend.CreateSecretIn{
		CompartmentID: "ocid1.compartment.oc1..c", VaultID: vault.ID, KeyID: key.ID, SecretName: "client-secret",
		SecretContent: &ocivaultbackend.SecretContent{
			ContentType: "BASE64", Content: base64.StdEncoding.EncodeToString([]byte("s3cret")),
		},
	})
	require.NoError(t, err)

	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/introspect" {
			user, pass, _ := r.BasicAuth()
			_ = json.NewEncoder(w).Encode(map[string]any{"active": user == "cid" && pass == "s3cret"})

			return
		}

		_ = json.NewEncoder(w).Encode(map[string]string{"introspection_endpoint": "http://" + r.Host + "/introspect"})
	}))
	t.Cleanup(idp.Close)

	gwB := ociapigateway.NewInMemoryBackend()
	gwH := ociapigateway.NewHandler(gwB)
	gwH.Port = 0

	wireOCIAPIGateway(map[string]service.Registerable{
		"OCIAPIGateway": gwH, "OCIVault": ocivaultbackend.NewHandler(vaultB),
	})
	require.NoError(t, gwH.StartWorker(t.Context()))
	t.Cleanup(func() { gwH.Shutdown(context.Background()) })

	base := "http://localhost:" + strconv.Itoa(gwH.Port) + "/20190501"
	post := func(path, body string) string {
		req, rerr := http.NewRequestWithContext(t.Context(), http.MethodPost, base+path, strings.NewReader(body))
		require.NoError(t, rerr)

		resp, derr := testclient.New(t).Do(req)
		require.NoError(t, derr)

		defer resp.Body.Close()

		out, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusAccepted, resp.StatusCode, string(out))

		return string(out)
	}

	gwB.SetTransition(0)

	var gw map[string]any

	require.NoError(t, json.Unmarshal([]byte(post("/gateways",
		`{"compartmentId":"c","endpointType":"PUBLIC","subnetId":"s"}`)), &gw))

	gwB.Sweep()

	gwID, _ := gw["id"].(string)
	post("/deployments", `{"gatewayId":"`+gwID+`","compartmentId":"c","pathPrefix":"/i","specification":{"routes":[`+
		`{"path":"/x","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200}}],`+
		`"requestPolicies":{"authentication":{"type":"TOKEN_AUTHENTICATION","tokenHeader":"Authorization",`+
		`"tokenAuthScheme":"Bearer","validationPolicy":{"type":"REMOTE_DISCOVERY",`+
		`"clientDetails":{"type":"CUSTOM","clientId":"cid","clientSecretId":"`+secret.ID+`","clientSecretVersionNumber":1},`+
		`"sourceUriDetails":{"type":"DISCOVERY_URI","uri":"`+idp.URL+`/.well-known/openid-configuration"}}}}}}`)
	gwB.Sweep()

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		base[:len(base)-len("/20190501")]+"/_apigw/"+gwID+"/i/x",
		nil,
	)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer opaque")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "the vault secret authenticated the introspection call")
}
