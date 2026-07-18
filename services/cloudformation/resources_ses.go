package cloudformation

import "fmt"

// ---- SES ----

func (rc *ResourceCreator) createSESEmailIdentity(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SES == nil {
		return logicalID + "-stub", nil
	}

	emailIdentity := strProp(props, "EmailIdentity", params, physicalIDs)
	if emailIdentity == "" {
		emailIdentity = logicalID
	}

	if err := rc.backends.SES.Backend.VerifyEmailIdentity(emailIdentity); err != nil {
		return "", fmt.Errorf("create SES email identity %s: %w", emailIdentity, err)
	}

	return emailIdentity, nil
}

func (rc *ResourceCreator) deleteSESEmailIdentity(emailIdentity string) error {
	if rc.backends.SES == nil {
		return nil
	}

	rc.backends.SES.Backend.DeleteIdentity(emailIdentity)

	return nil
}
