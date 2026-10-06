package secretsmanager

import (
	"encoding/json"
	"fmt"
)

// rejectExplicitEmpty fails when any named member is present as "": the SDK
// models them as *string with a min length, so omitted keeps and "" is invalid.
func rejectExplicitEmpty(data []byte, op string, members ...string) error {
	var probe map[string]*string
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}

	for _, m := range members {
		if v := probe[m]; v != nil && *v == "" {
			return fmt.Errorf("%w: %s %s must not be empty", ErrInvalidParameter, op, m)
		}
	}

	return nil
}

type putSecretValueInputAlias PutSecretValueInput

// UnmarshalJSON rejects explicit-empty members the SDK models as optional pointers.
func (in *PutSecretValueInput) UnmarshalJSON(data []byte) error {
	if err := rejectExplicitEmpty(
		data, "PutSecretValue", "SecretString", "ClientRequestToken", "RotationToken",
	); err != nil {
		return err
	}

	return json.Unmarshal(data, (*putSecretValueInputAlias)(in))
}

type updateSecretInputAlias UpdateSecretInput

// UnmarshalJSON rejects explicit-empty members the SDK models as optional pointers.
func (in *UpdateSecretInput) UnmarshalJSON(data []byte) error {
	if err := rejectExplicitEmpty(data, "UpdateSecret", "SecretString", "ClientRequestToken"); err != nil {
		return err
	}

	return json.Unmarshal(data, (*updateSecretInputAlias)(in))
}

type updateSecretVersionStageInputAlias UpdateSecretVersionStageInput

// UnmarshalJSON rejects explicit-empty members the SDK models as optional pointers.
func (in *UpdateSecretVersionStageInput) UnmarshalJSON(data []byte) error {
	if err := rejectExplicitEmpty(
		data, "UpdateSecretVersionStage", "MoveToVersionId", "RemoveFromVersionId",
	); err != nil {
		return err
	}

	return json.Unmarshal(data, (*updateSecretVersionStageInputAlias)(in))
}
