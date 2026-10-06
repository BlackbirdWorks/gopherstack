package athena

import "encoding/json"

type startCalculationInput struct {
	SessionID          string `json:"SessionId"`
	Description        string `json:"Description"`
	CodeBlock          string `json:"CodeBlock"`
	ClientRequestToken string `json:"ClientRequestToken"`
}

type calculationIDInput struct {
	CalculationExecutionID string `json:"CalculationExecutionId"`
}

type listCalculationsInput struct {
	NextToken   string `json:"NextToken"`
	SessionID   string `json:"SessionId"`
	StateFilter string `json:"StateFilter"`
	MaxResults  int    `json:"MaxResults"`
}

func (h *Handler) calcCoreOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StartCalculationExecution": func(b []byte) (any, error) {
			var input startCalculationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.replayCreate(
				"StartCalculationExecution", input.ClientRequestToken, input,
				found(h.Backend.GetCalculationExecution),
				func() (string, error) {
					id, _, err := h.Backend.StartCalculationExecution(
						input.SessionID, input.Description, input.CodeBlock,
					)

					return id, err
				},
			)
			if err != nil {
				return nil, err
			}

			calc, err := h.Backend.GetCalculationExecution(id)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CalculationExecutionId": id, keyState: calc.Status.State}, nil
		},
		"GetCalculationExecution": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			c, err := h.Backend.GetCalculationExecution(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"CalculationExecutionId": c.CalculationID,
				keySessionID:             c.SessionID,
				"Description":            c.Description,
				"WorkingDirectory":       c.WorkingDir,
				keyStatus:                c.Status,
				keyStatistics:            c.Statistics,
				"Result":                 c.Result,
			}, nil
		},
		"GetCalculationExecutionStatus": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			st, stats, err := h.Backend.GetCalculationExecutionStatus(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keyStatus: st, keyStatistics: stats}, nil
		},
		"GetCalculationExecutionCode": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			code, err := h.Backend.GetCalculationExecutionCode(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"CodeBlock": code}, nil
		},
	}
}

func (h *Handler) calcControlOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StopCalculationExecution": func(b []byte) (any, error) {
			var input calculationIDInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			state, err := h.Backend.StopCalculationExecution(input.CalculationExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{keyState: state}, nil
		},
		"ListCalculationExecutions": func(b []byte) (any, error) {
			var input listCalculationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			sums, err := h.Backend.ListCalculationExecutions(input.SessionID, input.StateFilter)
			if err != nil {
				return nil, err
			}

			page, next, pageErr := pageByKey(
				h.tokens,
				sums,
				func(s CalculationSummary) string { return s.CalculationID },
				input.MaxResults,
				input.NextToken,
			)
			if pageErr != nil {
				return nil, pageErr
			}

			return withNextToken(map[string]any{"Calculations": page}, next), nil
		},
	}
}
