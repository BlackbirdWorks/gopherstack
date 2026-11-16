package azurearm

import "strings"

const (
	acaProbeLiveness  = "liveness"
	acaProbeReadiness = "readiness"
	acaProbeStartup   = "startup"
	acaProbeMaxPeriod = 240
	acaProbeMaxDelay  = 60
	acaProbeMaxCount  = 10
)

// acaProbeRanges are the bounds the armappcontainers ContainerAppProbe model documents for each field.
//
//nolint:gochecknoglobals // static read-only table
var acaProbeRanges = []struct {
	key    string
	lo, hi float64
}{
	{"failureThreshold", 1, acaProbeMaxCount}, {"initialDelaySeconds", 1, acaProbeMaxDelay},
	{"periodSeconds", 1, acaProbeMaxPeriod}, {"successThreshold", 1, acaProbeMaxCount},
	{"timeoutSeconds", 1, acaProbeMaxPeriod},
}

func acaCheckProbeRanges(container, typ string, probe map[string]any) error {
	for _, r := range acaProbeRanges {
		if v, ok := acaNum(probe, r.key); ok && (v < r.lo || v > r.hi) {
			return acaBad("Container '%s' %s probe %s must be between %g and %g.", container, typ, r.key, r.lo, r.hi)
		}
	}

	if v, ok := acaNum(probe, "successThreshold"); ok && v != 1 && typ != acaProbeReadiness {
		return acaBad("Container '%s' %s probe successThreshold must be 1.", container, typ)
	}

	return nil
}

// acaProbeWires maps the liveness and readiness httpGet probes of a container onto the Container Instances
// probes the replica engine runs. Startup probes and tcpSocket probes have no counterpart there and stay stored.
func acaProbeWires(probes []any) (*aciProbeWire, *aciProbeWire) {
	var liveness, readiness *aciProbeWire

	for _, e := range probes {
		pr := mapOf(e)

		get := mapOf(pr["httpGet"])
		if len(get) == 0 {
			continue
		}

		w := &aciProbeWire{
			HTTPGet: &aciHTTPGetWire{
				Path: strOf(get, "path"), Port: acaIntOf(get, "port", 0), Scheme: strings.ToLower(strOf(get, "scheme")),
			},
			InitialDelaySeconds: acaIntOf(pr, "initialDelaySeconds", 0),
			PeriodSeconds:       acaIntOf(pr, "periodSeconds", aciProbeDefPeriod),
			FailureThreshold:    acaIntOf(pr, "failureThreshold", aciProbeDefFailure),
			SuccessThreshold:    acaIntOf(pr, "successThreshold", aciProbeDefSuccess),
			TimeoutSeconds:      acaIntOf(pr, "timeoutSeconds", aciProbeDefTimeout),
		}

		for _, h := range acaList(get["httpHeaders"]) {
			hm := mapOf(h)
			w.HTTPGet.HTTPHeaders = append(w.HTTPGet.HTTPHeaders,
				aciHeaderWire{Name: strOf(hm, fieldName), Value: strOf(hm, fieldValue)})
		}

		if w.HTTPGet.Scheme == "" {
			w.HTTPGet.Scheme = acaScaleHTTP
		}

		switch strings.ToLower(strOf(pr, fieldType)) {
		case acaProbeLiveness:
			liveness = w
		case acaProbeReadiness:
			readiness = w
		}
	}

	return liveness, readiness
}
