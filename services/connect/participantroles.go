package connect

const (
	timerUnset      = "Unset"
	timerMinMinutes = 2
	timerMaxMinutes = 480
	timerIdle       = "IDLE"
	timerDisconnect = "DISCONNECT_NONCUSTOMER"
)

// UpdateParticipantRoleConfigInput is the UpdateParticipantRoleConfig request.
type UpdateParticipantRoleConfigInput struct {
	ChannelConfiguration *struct {
		Chat *struct {
			ParticipantTimerConfigList []struct {
				TimerValue *struct {
					ParticipantTimerDurationInMinutes *int32
					ParticipantTimerAction            string
				}
				ParticipantRole string
				TimerType       string
			}
		}
	}
	ContactID  string `json:"-" path:"ContactId"`
	InstanceID string `json:"-" path:"InstanceId"`
}

func timerKey(role, timerType string) string { return role + "/" + timerType }

func validateRoleConfig(in *UpdateParticipantRoleConfigInput) error {
	if in.ChannelConfiguration == nil || in.ChannelConfiguration.Chat == nil {
		return invalidParamErr("ChannelConfiguration.Chat is required")
	}

	list := in.ChannelConfiguration.Chat.ParticipantTimerConfigList
	if len(list) == 0 {
		return invalidParamErr("ParticipantTimerConfigList must contain at least one timer")
	}

	for i, t := range list {
		if err := oneOf("ParticipantRole", t.ParticipantRole, "CUSTOMER", "AGENT"); err != nil {
			return err
		}

		if err := oneOf("TimerType", t.TimerType, timerIdle, timerDisconnect); err != nil {
			return err
		}

		v := t.TimerValue
		if v == nil || (v.ParticipantTimerAction == "") == (v.ParticipantTimerDurationInMinutes == nil) {
			return invalidParamErr(
				"timer %d: exactly one of ParticipantTimerAction or ParticipantTimerDurationInMinutes is required",
				i,
			)
		}

		if v.ParticipantTimerAction != "" && v.ParticipantTimerAction != timerUnset {
			return invalidParamErr("ParticipantTimerAction must be %s", timerUnset)
		}

		if d := v.ParticipantTimerDurationInMinutes; d != nil && (*d < timerMinMinutes || *d > timerMaxMinutes) {
			return invalidParamErr("ParticipantTimerDurationInMinutes must be %d-%d", timerMinMinutes, timerMaxMinutes)
		}
	}

	return nil
}

// UpdateParticipantRoleConfig sets or unsets the idle and auto-disconnect timers of an active chat contact.
func (b *Backend) UpdateParticipantRoleConfig(in *UpdateParticipantRoleConfigInput) (*EmptyOutput, error) {
	if err := validateRoleConfig(in); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateParticipantRoleConfig")
	defer b.mu.Unlock()

	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.Channel != channelChat {
		return nil, invalidRequestErr("participant timers can only be set on chat contacts")
	}

	if c.ParticipantTimers == nil {
		c.ParticipantTimers = map[string]int32{}
	}

	for _, t := range in.ChannelConfiguration.Chat.ParticipantTimerConfigList {
		key := timerKey(t.ParticipantRole, t.TimerType)
		if t.TimerValue.ParticipantTimerDurationInMinutes == nil {
			delete(c.ParticipantTimers, key)

			continue
		}

		c.ParticipantTimers[key] = *t.TimerValue.ParticipantTimerDurationInMinutes
	}

	b.touch(c)

	return &EmptyOutput{}, nil
}
