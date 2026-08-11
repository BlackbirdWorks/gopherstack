package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleDescribeRiskConfigurationFull(
	_ context.Context,
	in *describeRiskConfigFullInput,
) (*describeRiskConfigFullOutput, error) {
	cfg, err := h.Backend.GetTypedRiskConfiguration(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeRiskConfigFullOutput{RiskConfiguration: toRiskConfigJSON(cfg)}, nil
}

func (h *Handler) handleSetRiskConfigurationFull(
	_ context.Context,
	in *setRiskConfigFullInput,
) (*setRiskConfigFullOutput, error) {
	cfg := &TypedRiskConfiguration{
		UserPoolID: in.UserPoolID,
		ClientID:   in.ClientID,
	}

	if in.CompromisedCredentialsRiskConfiguration != nil {
		c := &CompromisedCredentialsRiskConfig{
			EventFilter: in.CompromisedCredentialsRiskConfiguration.EventFilter,
		}

		if in.CompromisedCredentialsRiskConfiguration.Actions != nil {
			c.Actions = &CompromisedCredentialsActions{
				EventAction: in.CompromisedCredentialsRiskConfiguration.Actions.EventAction,
			}
		}

		cfg.CompromisedCredentialsRiskConfig = c
	}

	if in.AccountTakeoverRiskConfiguration != nil {
		at := &AccountTakeoverRiskConfig{}

		if in.AccountTakeoverRiskConfiguration.Actions != nil {
			at.Actions = fromAccountTakeoverActionsJSON(in.AccountTakeoverRiskConfiguration.Actions)
		}

		if in.AccountTakeoverRiskConfiguration.NotifyConfiguration != nil {
			at.NotifyConfiguration = fromNotifyConfigJSON(in.AccountTakeoverRiskConfiguration.NotifyConfiguration)
		}

		cfg.AccountTakeoverRiskConfig = at
	}

	if in.RiskExceptionConfiguration != nil {
		exc := make([]string, len(in.RiskExceptionConfiguration.BlockedIPRangeList))
		copy(exc, in.RiskExceptionConfiguration.BlockedIPRangeList)
		skp := make([]string, len(in.RiskExceptionConfiguration.SkippedIPRangeList))
		copy(skp, in.RiskExceptionConfiguration.SkippedIPRangeList)
		cfg.RiskExceptionConfiguration = &RiskExceptionConfig{
			BlockedIPRangeList: exc,
			SkippedIPRangeList: skp,
		}
	}

	if err := h.Backend.SetTypedRiskConfiguration(cfg); err != nil {
		return nil, err
	}

	stored, err := h.Backend.GetTypedRiskConfiguration(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &setRiskConfigFullOutput{RiskConfiguration: toRiskConfigJSON(stored)}, nil
}

func toRiskConfigJSON(cfg *TypedRiskConfiguration) *riskConfigurationJSON {
	out := &riskConfigurationJSON{
		UserPoolID: cfg.UserPoolID,
		ClientID:   cfg.ClientID,
	}

	if cfg.CompromisedCredentialsRiskConfig != nil {
		c := &compromisedCredRiskConfigJSON{
			EventFilter: cfg.CompromisedCredentialsRiskConfig.EventFilter,
		}

		if cfg.CompromisedCredentialsRiskConfig.Actions != nil {
			c.Actions = &compromisedCredActionsJSON{
				EventAction: cfg.CompromisedCredentialsRiskConfig.Actions.EventAction,
			}
		}

		out.CompromisedCredentialsRiskConfiguration = c
	}

	if cfg.AccountTakeoverRiskConfig != nil {
		at := &accountTakeoverRiskConfigJSON{}

		if cfg.AccountTakeoverRiskConfig.Actions != nil {
			at.Actions = toAccountTakeoverActionsJSON(cfg.AccountTakeoverRiskConfig.Actions)
		}

		if cfg.AccountTakeoverRiskConfig.NotifyConfiguration != nil {
			at.NotifyConfiguration = toNotifyConfigJSON(cfg.AccountTakeoverRiskConfig.NotifyConfiguration)
		}

		out.AccountTakeoverRiskConfiguration = at
	}

	if cfg.RiskExceptionConfiguration != nil {
		out.RiskExceptionConfiguration = &riskExceptionConfigJSON{
			BlockedIPRangeList: cfg.RiskExceptionConfiguration.BlockedIPRangeList,
			SkippedIPRangeList: cfg.RiskExceptionConfiguration.SkippedIPRangeList,
		}
	}

	return out
}

func toAccountTakeoverActionsJSON(a *AccountTakeoverActions) *accountTakeoverActionsJSON {
	out := &accountTakeoverActionsJSON{}

	if a.LowAction != nil {
		out.LowAction = &accountTakeoverActionTypeJSON{Notify: a.LowAction.Notify, EventAction: a.LowAction.EventAction}
	}

	if a.MediumAction != nil {
		out.MediumAction = &accountTakeoverActionTypeJSON{
			Notify:      a.MediumAction.Notify,
			EventAction: a.MediumAction.EventAction,
		}
	}

	if a.HighAction != nil {
		out.HighAction = &accountTakeoverActionTypeJSON{
			Notify:      a.HighAction.Notify,
			EventAction: a.HighAction.EventAction,
		}
	}

	return out
}

func fromAccountTakeoverActionsJSON(in *accountTakeoverActionsJSON) *AccountTakeoverActions {
	out := &AccountTakeoverActions{}

	if in.LowAction != nil {
		out.LowAction = &AccountTakeoverActionType{Notify: in.LowAction.Notify, EventAction: in.LowAction.EventAction}
	}

	if in.MediumAction != nil {
		out.MediumAction = &AccountTakeoverActionType{
			Notify:      in.MediumAction.Notify,
			EventAction: in.MediumAction.EventAction,
		}
	}

	if in.HighAction != nil {
		out.HighAction = &AccountTakeoverActionType{
			Notify:      in.HighAction.Notify,
			EventAction: in.HighAction.EventAction,
		}
	}

	return out
}

func toNotifyConfigJSON(n *NotifyConfigurationType) *notifyConfigJSON {
	out := &notifyConfigJSON{
		From:      n.From,
		ReplyTo:   n.ReplyTo,
		SourceArn: n.SourceArn,
	}

	if n.BlockEmail != nil {
		out.BlockEmail = &notifyEmailTypeJSON{
			HTMLBody: n.BlockEmail.HTMLBody,
			Subject:  n.BlockEmail.Subject,
			TextBody: n.BlockEmail.TextBody,
		}
	}

	if n.MfaEmail != nil {
		out.MfaEmail = &notifyEmailTypeJSON{
			HTMLBody: n.MfaEmail.HTMLBody,
			Subject:  n.MfaEmail.Subject,
			TextBody: n.MfaEmail.TextBody,
		}
	}

	if n.NoActionEmail != nil {
		out.NoActionEmail = &notifyEmailTypeJSON{
			HTMLBody: n.NoActionEmail.HTMLBody,
			Subject:  n.NoActionEmail.Subject,
			TextBody: n.NoActionEmail.TextBody,
		}
	}

	return out
}

func fromNotifyConfigJSON(in *notifyConfigJSON) *NotifyConfigurationType {
	out := &NotifyConfigurationType{
		From:      in.From,
		ReplyTo:   in.ReplyTo,
		SourceArn: in.SourceArn,
	}

	if in.BlockEmail != nil {
		out.BlockEmail = &NotifyEmailType{
			HTMLBody: in.BlockEmail.HTMLBody,
			Subject:  in.BlockEmail.Subject,
			TextBody: in.BlockEmail.TextBody,
		}
	}

	if in.MfaEmail != nil {
		out.MfaEmail = &NotifyEmailType{
			HTMLBody: in.MfaEmail.HTMLBody,
			Subject:  in.MfaEmail.Subject,
			TextBody: in.MfaEmail.TextBody,
		}
	}

	if in.NoActionEmail != nil {
		out.NoActionEmail = &NotifyEmailType{
			HTMLBody: in.NoActionEmail.HTMLBody,
			Subject:  in.NoActionEmail.Subject,
			TextBody: in.NoActionEmail.TextBody,
		}
	}

	return out
}

func (h *Handler) handleDescribeRiskConfiguration(
	_ context.Context,
	in *describeRiskConfigurationInput,
) (*describeRiskConfigurationOutput, error) {
	if _, err := h.Backend.DescribeRiskConfiguration(in.UserPoolID, in.ClientID); err != nil {
		return nil, err
	}

	return &describeRiskConfigurationOutput{RiskConfiguration: &riskConfigurationType{}}, nil
}

func (h *Handler) handleSetRiskConfiguration(
	_ context.Context,
	in *setRiskConfigurationInput,
) (*setRiskConfigurationOutput, error) {
	if err := h.Backend.SetRiskConfiguration(in.UserPoolID, in.ClientID, nil); err != nil {
		return nil, err
	}

	return &setRiskConfigurationOutput{RiskConfiguration: &riskConfigurationType{}}, nil
}

func (h *Handler) handleGetLogDeliveryConfiguration(
	_ context.Context,
	in *getLogDeliveryConfigurationInput,
) (*getLogDeliveryConfigurationOutput, error) {
	cfg, err := h.Backend.GetLogDeliveryConfiguration(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	raw := cfg.Raw
	if raw == nil {
		raw = map[string]any{}
	}

	return &getLogDeliveryConfigurationOutput{LogDeliveryConfiguration: raw}, nil
}

func (h *Handler) handleSetLogDeliveryConfiguration(
	_ context.Context,
	in *setLogDeliveryConfigurationInput,
) (*setLogDeliveryConfigurationOutput, error) {
	logConfigs := in.LogConfigurations
	if logConfigs == nil {
		logConfigs = []map[string]any{}
	}

	raw := map[string]any{
		"UserPoolId":        in.UserPoolID,
		"LogConfigurations": logConfigs,
	}

	if err := h.Backend.SetLogDeliveryConfiguration(in.UserPoolID, raw); err != nil {
		return nil, err
	}

	return &setLogDeliveryConfigurationOutput{LogDeliveryConfiguration: raw}, nil
}

func (h *Handler) securityConfigOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DescribeRiskConfiguration":   service.WrapOp(h.handleDescribeRiskConfiguration),
		"GetLogDeliveryConfiguration": service.WrapOp(h.handleGetLogDeliveryConfiguration),
		"SetLogDeliveryConfiguration": service.WrapOp(h.handleSetLogDeliveryConfiguration),
		"SetRiskConfiguration":        service.WrapOp(h.handleSetRiskConfiguration),
	}
}

func (h *Handler) securityConfigOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDescribeRiskConfiguration: wrapAccuracy(h.handleDescribeRiskConfigurationFull),
		opSetRiskConfiguration:      wrapAccuracy(h.handleSetRiskConfigurationFull),
	}
}
