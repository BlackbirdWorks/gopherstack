package athena

import "encoding/json"

const (
	keySessionID  = "SessionId"
	keyState      = "State"
	keyStatus     = "Status"
	keyStatistics = "Statistics"
)

type startSessionInput struct {
	MonitoringConfiguration     MonitoringConfiguration `json:"MonitoringConfiguration"`
	WorkGroup                   string                  `json:"WorkGroup"`
	Description                 string                  `json:"Description"`
	NotebookVersion             string                  `json:"NotebookVersion"`
	NotebookID                  string                  `json:"NotebookId"`
	ExecutionRole               string                  `json:"ExecutionRole"`
	EngineConfiguration         EngineConfiguration     `json:"EngineConfiguration"`
	SessionIdleTimeoutInMinutes int32                   `json:"SessionIdleTimeoutInMinutes"`
}

type sessionIDInput struct {
	SessionID string `json:"SessionId"`
}

type listSessionsInput struct {
	WorkGroup   string `json:"WorkGroup"`
	StateFilter string `json:"StateFilter"`
}

type listNotebookSessionsInput struct {
	NotebookID string `json:"NotebookId"`
}

type listExecutorsInput struct {
	SessionID     string `json:"SessionId"`
	ExecutorState string `json:"ExecutorStateFilter"`
}

type getResourceDashboardInput struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) sessionCoreOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StartSession": func(b []byte) (any, error) {
			var input startSessionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			const secondsPerMinute = 60

			sessionCfg := SessionConfiguration{
				ExecutionRole: input.ExecutionRole,
				// StartSessionInput only carries SessionIdleTimeoutInMinutes; the
				// stored/returned model tracks IdleTimeoutSeconds (aws-sdk-go-v2
				// athena@v1.60.4 types.SessionConfiguration carries both, this
				// converts the one real clients actually send).
				IdleTimeoutSeconds: int64(input.SessionIdleTimeoutInMinutes) * secondsPerMinute,
			}

			id, state, err := h.Backend.StartSession(
				input.WorkGroup, input.Description, input.NotebookVersion,
				input.EngineConfiguration, sessionCfg,
				input.MonitoringConfiguration, input.NotebookID,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{keySessionID: id, keyState: state}, nil
		},
		"GetSession": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			s, err := h.Backend.GetSession(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				keySessionID:              s.SessionID,
				"Description":             s.Description,
				"WorkGroup":               s.WorkGroup,
				"EngineVersion":           pysparkEngineV3,
				"NotebookVersion":         s.NotebookVersion,
				"EngineConfiguration":     s.EngineConfiguration,
				"SessionConfiguration":    s.SessionConfiguration,
				"MonitoringConfiguration": s.MonitoringConfiguration,
				keyStatus:                 s.Status,
				keyStatistics:             s.Statistics,
			}, nil
		},
		"GetSessionStatus": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			st, err := h.Backend.GetSessionStatus(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keySessionID: input.SessionID, keyStatus: st}, nil
		},
		"GetSessionEndpoint": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			url, authToken, authTokenExpiration, err := h.Backend.GetSessionEndpoint(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"EndpointUrl":             url,
				"AuthToken":               authToken,
				"AuthTokenExpirationTime": authTokenExpiration,
			}, nil
		},
		"TerminateSession": func(b []byte) (any, error) {
			var input sessionIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			state, err := h.Backend.TerminateSession(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keyState: state}, nil
		},
	}
}

func (h *Handler) sessionListOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"ListSessions": func(b []byte) (any, error) {
			var input listSessionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			sums, err := h.Backend.ListSessions(input.WorkGroup, input.StateFilter)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Sessions": sums}, nil
		},
		"ListNotebookSessions": func(b []byte) (any, error) {
			var input listNotebookSessionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			sums, err := h.Backend.ListNotebookSessions(input.NotebookID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookSessionsList": sums}, nil
		},
	}
}

// sessionInfoOps covers session-adjacent read-only info operations: executor
// listing, available engine versions/DPU sizes, and the session dashboard URL.
func (h *Handler) sessionInfoOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"ListExecutors": func(b []byte) (any, error) {
			var input listExecutorsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			execs, err := h.Backend.ListExecutors(input.SessionID, input.ExecutorState)
			if err != nil {
				return nil, err
			}

			return map[string]any{"ExecutorsSummary": execs, keySessionID: input.SessionID}, nil
		},
		"ListEngineVersions": func(_ []byte) (any, error) {
			return map[string]any{"EngineVersions": h.Backend.ListEngineVersions()}, nil
		},
		"ListApplicationDPUSizes": func(_ []byte) (any, error) {
			return map[string]any{"ApplicationDPUSizes": h.Backend.ListApplicationDPUSizes()}, nil
		},
		"GetResourceDashboard": func(b []byte) (any, error) {
			var input getResourceDashboardInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			url, err := h.Backend.GetResourceDashboard(input.ResourceARN)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Url": url}, nil
		},
	}
}
