package types

type RealThingException struct{ ErrorCodeOverride *string }

func (e *RealThingException) ErrorCode() string {
	if e.ErrorCodeOverride == nil {
		return "RealThingException"
	}

	return *e.ErrorCodeOverride
}
