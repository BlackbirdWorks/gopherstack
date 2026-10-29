package azurearm_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func flexSKUBody(sku, tier, props string) string {
	return `{"location":"x","sku":{"name":"` + sku + `","tier":"` + tier + `"},"properties":{` + props + `}}`
}

func loginPW(login string) string {
	return `"administratorLogin":"` + login + `","administratorLoginPassword":"` + flexPassword + `"`
}

// TestFlexDocumentedRules pins rules to the Microsoft Learn pages and spec patterns cited in PARITY.md.
func TestFlexDocumentedRules(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	pgLogin63 := strings.Repeat("a", 63)

	tests := []struct {
		name   string
		ns     string
		api    string
		body   string
		status int
	}{
		{"pg underscore login", pgNS, flexAPIPG, flexSKUBody("Standard_B1ms", "Burstable", loginPW("a_b")), 400},
		{"pg 64 char login", pgNS, flexAPIPG, flexSKUBody("Standard_B1ms", "Burstable", loginPW(pgLogin63+"a")), 400},
		{"pg 63 char login", pgNS, flexAPIPG, flexSKUBody("Standard_B1ms", "Burstable", loginPW(pgLogin63)), 201},
		{"pg d2ds_v6", pgNS, flexAPIPG, flexSKUBody("Standard_D2ds_v6", "GeneralPurpose", loginPW("a1")), 201},
		{"pg d192ds_v6", pgNS, flexAPIPG, flexSKUBody("Standard_D192ds_v6", "GeneralPurpose", loginPW("a1")), 201},
		{"pg e96ds_v6", pgNS, flexAPIPG, flexSKUBody("Standard_E96ds_v6", "MemoryOptimized", loginPW("a1")), 201},
		{"pg d96 v3 absent", pgNS, flexAPIPG, flexSKUBody("Standard_D96s_v3", "GeneralPurpose", loginPW("a1")), 400},
		{"pg e20 v3 absent", pgNS, flexAPIPG, flexSKUBody("Standard_E20s_v3", "MemoryOptimized", loginPW("a1")), 400},
		{"my reserved sa", myNS, flexAPIMy, flexSKUBody("Standard_B1ms", "Burstable", loginPW("sa")), 400},
		{"my e80ids_v4", myNS, flexAPIMy, flexSKUBody("Standard_E80ids_v4", "MemoryOptimized", loginPW("a1")), 201},
		{"my e96ds_v5", myNS, flexAPIMy, flexSKUBody("Standard_E96ds_v5", "MemoryOptimized", loginPW("a1")), 201},
		{"my e64ds_v5", myNS, flexAPIMy, flexSKUBody("Standard_E64ds_v5", "MemoryOptimized", loginPW("a1")), 201},
		{"my d96ds_v4 absent", myNS, flexAPIMy, flexSKUBody("Standard_D96ds_v4", "GeneralPurpose", loginPW("a1")), 400},
		{
			"my e80ads_v5 absent",
			myNS,
			flexAPIMy,
			flexSKUBody("Standard_E80ads_v5", "MemoryOptimized", loginPW("a1")),
			400,
		},
		{
			"my gp storage 16384", myNS, flexAPIMy,
			flexSKUBody("Standard_D2ds_v4", "GeneralPurpose", loginPW("a1")+`,"storage":{"storageSizeGB":16384}`), 201,
		},
		{
			"my gp storage 16385", myNS, flexAPIMy,
			flexSKUBody("Standard_D2ds_v4", "GeneralPurpose", loginPW("a1")+`,"storage":{"storageSizeGB":16385}`), 400,
		},
		{
			"my mo storage 32768", myNS, flexAPIMy,
			flexSKUBody("Standard_E2ds_v4", "MemoryOptimized", loginPW("a1")+`,"storage":{"storageSizeGB":32768}`), 201,
		},
		{
			"my mo storage 32769", myNS, flexAPIMy,
			flexSKUBody("Standard_E2ds_v4", "MemoryOptimized", loginPW("a1")+`,"storage":{"storageSizeGB":32769}`), 400,
		},
		{
			"my b1ms iops 640", myNS, flexAPIMy,
			flexSKUBody("Standard_B1ms", "Burstable", loginPW("a1")+`,"storage":{"storageSizeGB":20,"iops":640}`), 201,
		},
		{
			"my b1ms iops 641", myNS, flexAPIMy,
			flexSKUBody("Standard_B1ms", "Burstable", loginPW("a1")+`,"storage":{"storageSizeGB":20,"iops":641}`), 400,
		},
		{
			"my d2ds_v4 iops 3200", myNS, flexAPIMy,
			flexSKUBody(
				"Standard_D2ds_v4",
				"GeneralPurpose",
				loginPW("a1")+`,"storage":{"storageSizeGB":20,"iops":3200}`,
			),
			201,
		},
		{
			"my d2ds_v4 iops 3201", myNS, flexAPIMy,
			flexSKUBody(
				"Standard_D2ds_v4",
				"GeneralPurpose",
				loginPW("a1")+`,"storage":{"storageSizeGB":20,"iops":3201}`,
			),
			400,
		},
		{
			"my ha autogrow off", myNS, flexAPIMy,
			flexSKUBody(
				"Standard_D2ds_v4",
				"GeneralPurpose",
				loginPW(
					"a1",
				)+`,"highAvailability":{"mode":"SameZone"},"storage":{"storageSizeGB":20,"autoGrow":"Disabled"}`,
			),
			400,
		},
		{
			"my ha autogrow on", myNS, flexAPIMy,
			flexSKUBody(
				"Standard_D2ds_v4",
				"GeneralPurpose",
				loginPW(
					"a1",
				)+`,"highAvailability":{"mode":"SameZone"},"storage":{"storageSizeGB":20,"autoGrow":"Enabled"}`,
			),
			201,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name := "docs-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
			status, body := s.raw(t, http.MethodPut, flexServerPath(tt.ns, name), tt.body, tt.api)
			assert.Equal(t, tt.status, status, body)
		})
	}
}

func TestFlexDocumentedChildNames(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	s.pgCreate(t, "docs-pg-child")
	s.myCreate(t, "docs-my-child")

	fw := `{"properties":{"startIpAddress":"1.1.1.1","endIpAddress":"1.1.1.2"}}`

	tests := []struct {
		name   string
		ns     string
		server string
		api    string
		path   string
		body   string
		status int
	}{
		{"fw 80 chars", pgNS, "docs-pg-child", flexAPIPG, "/firewallRules/" + strings.Repeat("a", 80), fw, 201},
		{"fw 81 chars", pgNS, "docs-pg-child", flexAPIPG, "/firewallRules/" + strings.Repeat("a", 81), fw, 400},
		{"fw leading hyphen", pgNS, "docs-pg-child", flexAPIPG, "/firewallRules/-a", fw, 400},
		{"fw trailing hyphen", pgNS, "docs-pg-child", flexAPIPG, "/firewallRules/a-", fw, 400},
		{"my fw 81 chars", myNS, "docs-my-child", flexAPIMy, "/firewallRules/" + strings.Repeat("a", 81), fw, 400},
		{"pg db hyphen", pgNS, "docs-pg-child", flexAPIPG, "/databases/app-db", `{"properties":{}}`, 201},
		{"pg db dollar", pgNS, "docs-pg-child", flexAPIPG, "/databases/app$db", `{"properties":{}}`, 400},
		{
			"pg charset pattern", pgNS, "docs-pg-child", flexAPIPG, "/databases/cs",
			`{"properties":{"charset":"9bad"}}`, 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := s.raw(t, http.MethodPut, flexServerPath(tt.ns, tt.server)+tt.path, tt.body, tt.api)
			assert.Equal(t, tt.status, status, body)
		})
	}
}

func TestFlexMySQLConnectionLimits(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)

	tests := []struct {
		name, server, sku, tier, wantDefault, wantAllowed string
	}{
		{"b1s", "conn-b1s", "Standard_B1s", "Burstable", "85", "10-171"},
		{"b1ms", "conn-b1ms", "Standard_B1ms", "Burstable", "171", "10-341"},
		{"b12ms", "conn-b12", "Standard_B12ms", "Burstable", "4097", "10-8193"},
		{"d2ds_v4", "conn-d2", "Standard_D2ds_v4", "GeneralPurpose", "683", "10-1365"},
		{"e64ds_v5", "conn-e64", "Standard_E64ds_v5", "MemoryOptimized", "43691", "10-87383"},
		{"e96ads_v5", "conn-e96", "Standard_E96ads_v5", "MemoryOptimized", "50000", "10-100000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := s.raw(t, http.MethodPut, flexServerPath(myNS, tt.server),
				flexSKUBody(tt.sku, tt.tier, loginPW("a1")), flexAPIMy)
			require.Equal(t, http.StatusCreated, status, body)

			status, body = s.raw(
				t,
				http.MethodGet,
				flexServerPath(myNS, tt.server)+"/configurations/max_connections",
				"",
				flexAPIMy,
			)
			require.Equal(t, http.StatusOK, status, body)

			props, _ := body["properties"].(map[string]any)
			assert.Equal(t, tt.wantDefault, props["value"])
			assert.Equal(t, tt.wantDefault, props["defaultValue"])
			assert.Equal(t, tt.wantAllowed, props["allowedValues"])
		})
	}

	t.Run("range enforced", func(t *testing.T) {
		t.Parallel()

		status, body := s.raw(t, http.MethodPut, flexServerPath(myNS, "conn-range"),
			flexSKUBody("Standard_D2ds_v4", "GeneralPurpose", loginPW("a1")), flexAPIMy)
		require.Equal(t, http.StatusCreated, status, body)

		path := flexServerPath(myNS, "conn-range") + "/configurations/max_connections"
		over, _ := s.raw(t, http.MethodPut, path, `{"properties":{"value":"1366"}}`, flexAPIMy)
		under, _ := s.raw(t, http.MethodPut, path, `{"properties":{"value":"9"}}`, flexAPIMy)
		edge, _ := s.raw(t, http.MethodPut, path, `{"properties":{"value":"1365"}}`, flexAPIMy)

		assert.Equal(t, http.StatusBadRequest, over)
		assert.Equal(t, http.StatusBadRequest, under)
		assert.Equal(t, http.StatusOK, edge)
	})

	t.Run("log_bin_trust_function_creators", func(t *testing.T) {
		t.Parallel()

		status, body := s.raw(t, http.MethodPut, flexServerPath(myNS, "conn-logbin"),
			flexSKUBody("Standard_B1ms", "Burstable", loginPW("a1")), flexAPIMy)
		require.Equal(t, http.StatusCreated, status, body)

		_, body = s.raw(t, http.MethodGet,
			flexServerPath(myNS, "conn-logbin")+"/configurations/log_bin_trust_function_creators", "", flexAPIMy)
		props, _ := body["properties"].(map[string]any)
		assert.Equal(t, "ON", props["value"])
	})
}

func TestFlexPGSkuMemory(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)

	tests := []struct {
		sku, tier string
		wantMemMB int
	}{
		{"Standard_E64ds_v4", "MemoryOptimized", 432 * 1024},
		{"Standard_E64ds_v5", "MemoryOptimized", 512 * 1024},
		{"Standard_E96ds_v6", "MemoryOptimized", 768 * 1024},
		{"Standard_E192ds_v6", "MemoryOptimized", 1832 * 1024},
		{"Standard_D128ds_v6", "GeneralPurpose", 512 * 1024},
	}

	_, body := s.raw(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/providers/Microsoft.DBforPostgreSQL/locations/"+flexLoc+"/capabilities",
		"",
		flexAPIPG,
	)
	require.NotNil(t, body)

	mem := map[string]int{}

	for _, c := range flexCapabilityList(body) {
		mem[c.name] = c.memPerVcoreMB * c.vcores
	}

	for _, tt := range tests {
		t.Run(tt.sku, func(t *testing.T) {
			t.Parallel()

			assert.InDelta(t, tt.wantMemMB, mem[tt.sku], 192)
		})
	}
}

type flexCap struct {
	name          string
	vcores        int
	memPerVcoreMB int
}

func flexCapabilityList(body map[string]any) []flexCap {
	var out []flexCap

	vals, _ := body["value"].([]any)

	for _, v := range vals {
		ed, _ := v.(map[string]any)["supportedFlexibleServerEditions"].([]any)

		for _, e := range ed {
			vs, _ := e.(map[string]any)["supportedServerVersions"].([]any)
			if len(vs) == 0 {
				continue
			}

			cores, _ := vs[0].(map[string]any)["supportedVcores"].([]any)

			for _, c := range cores {
				m, _ := c.(map[string]any)
				name, _ := m["name"].(string)
				vc, _ := m["vCores"].(float64)
				mem, _ := m["supportedMemoryPerVcoreMB"].(float64)
				out = append(out, flexCap{name: name, vcores: int(vc), memPerVcoreMB: int(mem)})
			}
		}
	}

	return out
}

func TestFlexStopRestartErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, ns, api, create string
	}{
		{"pg", pgNS, flexAPIPG, pgReq(pgLoginPW)},
		{"mysql", myNS, flexAPIMy, myReq(myLoginPW)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			path := flexServerPath(tt.ns, "errs")

			status, body := s.raw(t, http.MethodPut, path, tt.create, tt.api)
			require.Equal(t, http.StatusCreated, status, body)

			frozen := time.Now().UTC().Add(time.Hour)
			s.pg.SetClock(func() time.Time { return frozen })
			s.my.SetClock(func() time.Time { return frozen })

			status, body = s.raw(t, http.MethodPost, path+"/stop", `{}`, tt.api)
			require.Less(t, status, http.StatusMultipleChoices, body)

			status, body = s.raw(t, http.MethodPost, path+"/restart", `{}`, tt.api)
			assert.Equal(t, http.StatusConflict, status)
			assert.Equal(t, "SeverBusyWithOtherOperation", flexErrCode(body))
			assert.Equal(
				t,
				"Cannot perform 'Restart' server operation because server 'errs' is busy processing other operation.",
				flexErrMessage(body),
			)

			s.pg.SetClock(func() time.Time { return frozen.Add(2 * time.Hour) })
			s.my.SetClock(func() time.Time { return frozen.Add(2 * time.Hour) })

			status, body = s.raw(t, http.MethodPost, path+"/restart", `{}`, tt.api)
			assert.Equal(t, http.StatusConflict, status)
			assert.Equal(t, "ServerIsNotReady", flexErrCode(body))
			assert.Equal(t, "Restart or Stop Server can only be performed on Started servers. "+
				"Server Name = errs, Current Server State = Stopped", flexErrMessage(body))
		})
	}
}

func flexErrMessage(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	m, _ := e["message"].(string)

	return m
}

func TestFlexReplicaRules(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	burstable := s.pgCreate(t, "rules-burst")
	gp := s.pgCreateGP(t, "rules-gp")
	myBurst := s.myCreate(t, "rules-myburst")
	myGP := s.myCreateGP(t, "rules-mygp")

	replica := func(src *string, extra string) string {
		return `{"location":"x","properties":{"createMode":"Replica","sourceServerResourceId":"` + *src + `"` + extra + `}}`
	}

	tests := []struct {
		name, ns, api, server, body string
		status                      int
	}{
		{"pg burstable source", pgNS, flexAPIPG, "rr-1", replica(burstable.ID, ""), 400},
		{"my burstable source", myNS, flexAPIMy, "rr-2", replica(myBurst.ID, ""), 400},
		{"pg ha replica", pgNS, flexAPIPG, "rr-3", replica(gp.ID, `,"highAvailability":{"mode":"SameZone"}`), 400},
		{"pg valid", pgNS, flexAPIPG, "rr-4", replica(gp.ID, ""), 201},
		{"my valid", myNS, flexAPIMy, "rr-5", replica(myGP.ID, ""), 201},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := s.raw(t, http.MethodPut, flexServerPath(tt.ns, tt.server), tt.body, tt.api)
			assert.Equal(t, tt.status, status, body)
		})
	}
}

func TestFlexMySQLNoCascadeAndStopRules(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	src := s.myCreateGP(t, "mc-src")

	status, body := s.raw(t, http.MethodPut, flexServerPath(myNS, "mc-rep"),
		`{"location":"x","properties":{"createMode":"Replica","sourceServerResourceId":"`+*src.ID+`"}}`, flexAPIMy)
	require.Equal(t, http.StatusCreated, status, body)

	nested := `{"location":"x","properties":{"createMode":"Replica","sourceServerResourceId":"` +
		flexServerPath(myNS, "mc-rep") + `"}}`
	status, _ = s.raw(t, http.MethodPut, flexServerPath(myNS, "mc-nested"), nested, flexAPIMy)
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = s.raw(t, http.MethodPost, flexServerPath(myNS, "mc-src")+"/stop", `{}`, flexAPIMy)
	assert.Equal(t, http.StatusBadRequest, status)
}
