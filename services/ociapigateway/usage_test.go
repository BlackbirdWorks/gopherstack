package ociapigateway_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func usageSpec() string {
	return `{"requestPolicies":{"usagePlans":{"tokenLocations":["request.headers[X-Api-Key]","request.query[key]"]}},` +
		`"routes":[` + stock("/u", `"GET"`, "ok") + `]}`
}

func (e *env) mkPlan(t *testing.T, name string, ents ...apigateway.Entitlement) string {
	t.Helper()

	resp, err := e.plans.CreateUsagePlan(t.Context(), apigateway.CreateUsagePlanRequest{
		CreateUsagePlanDetails: apigateway.CreateUsagePlanDetails{
			CompartmentId: new(testCompartment), DisplayName: new(name), Entitlements: ents,
		},
	})
	require.NoError(t, err)
	e.settle()

	return *resp.Id
}

func (e *env) mkSubscriber(t *testing.T, token string, plans ...string) string {
	t.Helper()

	resp, err := e.sub.CreateSubscriber(t.Context(), apigateway.CreateSubscriberRequest{
		CreateSubscriberDetails: apigateway.CreateSubscriberDetails{
			CompartmentId: new(testCompartment), UsagePlans: plans, DisplayName: new("sub-" + token),
			Clients: []apigateway.Client{{Name: new("client-" + token), Token: new(token)}},
		},
	})
	require.NoError(t, err)
	e.settle()

	return *resp.Id
}

func TestUsagePlanAndSubscriberCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "up-crud")
	dep := e.mkDeployment(t, *gw.Id, "/p", usageSpec())

	planID := e.mkPlan(t, "gold", apigateway.Entitlement{
		Name: new("e1"), RateLimit: &apigateway.RateLimit{Value: new(5), Unit: apigateway.RateLimitUnitSecond},
		Quota: &apigateway.Quota{
			Value: new(100), Unit: apigateway.QuotaUnitHour, ResetPolicy: apigateway.QuotaResetPolicyCalendar,
			OperationOnBreach: apigateway.QuotaOperationOnBreachReject,
		},
		Targets: []apigateway.EntitlementTarget{{DeploymentId: &dep}},
	})

	got, err := e.plans.GetUsagePlan(t.Context(), apigateway.GetUsagePlanRequest{UsagePlanId: &planID})
	require.NoError(t, err)
	assert.Equal(t, apigateway.UsagePlanLifecycleStateActive, got.LifecycleState)
	require.Len(t, got.Entitlements, 1)
	assert.Equal(t, dep, *got.Entitlements[0].Targets[0].DeploymentId)

	list, err := e.plans.ListUsagePlans(t.Context(), apigateway.ListUsagePlansRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "e1", *list.Items[0].Entitlements[0].Name)

	subID := e.mkSubscriber(t, "tok-1", planID)

	sub, err := e.sub.GetSubscriber(t.Context(), apigateway.GetSubscriberRequest{SubscriberId: &subID})
	require.NoError(t, err)
	assert.Equal(t, "tok-1", *sub.Clients[0].Token)

	subs, err := e.sub.ListSubscribers(t.Context(), apigateway.ListSubscribersRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Equal(t, "client-tok-1", *subs.Items[0].Clients[0].Name)

	_, err = e.sub.CreateSubscriber(t.Context(), apigateway.CreateSubscriberRequest{
		CreateSubscriberDetails: apigateway.CreateSubscriberDetails{
			CompartmentId: new(testCompartment), UsagePlans: []string{planID},
			Clients: []apigateway.Client{{Name: new("dup"), Token: new("tok-1")}},
		},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "client tokens are unique")

	_, err = e.plans.DeleteUsagePlan(t.Context(), apigateway.DeleteUsagePlanRequest{UsagePlanId: &planID})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "a subscribed plan cannot be deleted")

	_, err = e.sub.DeleteSubscriber(t.Context(), apigateway.DeleteSubscriberRequest{SubscriberId: &subID})
	require.NoError(t, err)
	e.settle()

	_, err = e.plans.DeleteUsagePlan(t.Context(), apigateway.DeleteUsagePlanRequest{UsagePlanId: &planID})
	require.NoError(t, err)
}

func TestUsagePlanValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	tests := []struct {
		name string
		ents []apigateway.Entitlement
	}{
		{"bad rate", []apigateway.Entitlement{{
			Name: new("e"), RateLimit: &apigateway.RateLimit{Value: new(0), Unit: apigateway.RateLimitUnitSecond},
		}}},
		{"unknown target", []apigateway.Entitlement{{
			Name: new("e"), Targets: []apigateway.EntitlementTarget{{DeploymentId: new("ocid1.none")}},
		}}},
		{"duplicate names", []apigateway.Entitlement{{Name: new("e")}, {Name: new("e")}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.plans.CreateUsagePlan(t.Context(), apigateway.CreateUsagePlanRequest{
				CreateUsagePlanDetails: apigateway.CreateUsagePlanDetails{
					CompartmentId: new(testCompartment), Entitlements: tt.ents,
				},
			})
			assert.GreaterOrEqual(t, svcErr(t, err).GetHTTPStatusCode(), http.StatusBadRequest)
		})
	}
}

func TestUsagePlanRateLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "up-rate")
	dep := e.mkDeployment(t, *gw.Id, "/p", usageSpec())
	plan := e.mkPlan(t, "rate", apigateway.Entitlement{
		Name: new("e1"), RateLimit: &apigateway.RateLimit{Value: new(2), Unit: apigateway.RateLimitUnitSecond},
		Targets: []apigateway.EntitlementTarget{{DeploymentId: &dep}},
	})
	e.mkSubscriber(t, "rate-tok", plan)
	e.mkSubscriber(t, "other-tok", plan)

	hdr := map[string]string{"X-Api-Key": "rate-tok"}
	codes := []int{}

	for range 3 {
		codes = append(codes, e.call(t, *gw.Id, http.MethodGet, "/p/u", hdr, "").status)
	}

	assert.Equal(t, []int{200, 200, 429}, codes)

	limited := e.call(t, *gw.Id, http.MethodGet, "/p/u", hdr, "")
	assert.Equal(t, "1", limited.header.Get("Retry-After"))

	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/p/u", map[string]string{"X-Api-Key": "other-tok"}, "").status,
		"each subscriber has its own bucket")

	e.clock.Advance(time.Second)
	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/p/u", hdr, "").status)
}

func TestUsagePlanQuota(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		breach   apigateway.QuotaOperationOnBreachEnum
		wantOver int
	}{
		{"reject", apigateway.QuotaOperationOnBreachReject, http.StatusTooManyRequests},
		{"allow", apigateway.QuotaOperationOnBreachAllow, http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			gw := e.mkGateway(t, "up-quota")
			dep := e.mkDeployment(t, *gw.Id, "/p", usageSpec())
			plan := e.mkPlan(t, "quota", apigateway.Entitlement{
				Name: new("e1"),
				Quota: &apigateway.Quota{
					Value: new(2), Unit: apigateway.QuotaUnitHour, ResetPolicy: apigateway.QuotaResetPolicyCalendar,
					OperationOnBreach: tt.breach,
				},
				Targets: []apigateway.EntitlementTarget{{DeploymentId: &dep}},
			})
			e.mkSubscriber(t, "quota-tok", plan)

			url := "/p/u?key=quota-tok"
			assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, url, nil, "").status, "token from the query location")
			assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, url, nil, "").status)

			over := e.call(t, *gw.Id, http.MethodGet, url, nil, "")
			assert.Equal(t, tt.wantOver, over.status)

			if tt.wantOver == http.StatusTooManyRequests {
				assert.NotEmpty(t, over.header.Get("Retry-After"))

				e.clock.Advance(time.Hour)
				assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, url, nil, "").status, "calendar window resets")
			}
		})
	}
}

func TestUsagePlanTokenHandling(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "up-token")
	dep := e.mkDeployment(t, *gw.Id, "/p", usageSpec())
	other := e.mkDeployment(t, *gw.Id, "/q", usageSpec())
	plan := e.mkPlan(t, "scoped", apigateway.Entitlement{
		Name: new("e1"), Targets: []apigateway.EntitlementTarget{{DeploymentId: &dep}},
	})
	e.mkSubscriber(t, "good", plan)

	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/p/u", nil, "").status, "missing token")
	assert.Equal(t, 401, e.call(t, *gw.Id, http.MethodGet, "/p/u", map[string]string{"X-Api-Key": "nope"}, "").status)
	assert.Equal(t, 200, e.call(t, *gw.Id, http.MethodGet, "/p/u", map[string]string{"X-Api-Key": "good"}, "").status)
	assert.Equal(t, 403, e.call(t, *gw.Id, http.MethodGet, "/q/u", map[string]string{"X-Api-Key": "good"}, "").status,
		"the plan does not entitle the subscriber to deployment "+other)
}
