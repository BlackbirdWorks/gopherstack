package secretsmanager

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// rejectExplicitEmpty fails when any named member is present as "": the SDK
// models them as *string with a min length, so omitted keeps and "" is invalid.
func rejectExplicitEmpty(data []byte, op string, members ...string) error {
	var probe map[string]jsontext.Value
	if err := json.Unmarshal(data, &probe, jsonx.V1()); err != nil {
		return err
	}

	for _, m := range members {
		if string(probe[m]) == `""` {
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

	return json.Unmarshal(data, (*putSecretValueInputAlias)(in), jsonx.V1())
}

type updateSecretInputAlias UpdateSecretInput

// UnmarshalJSON rejects explicit-empty members the SDK models as optional pointers.
func (in *UpdateSecretInput) UnmarshalJSON(data []byte) error {
	if err := rejectExplicitEmpty(data, "UpdateSecret", "SecretString", "ClientRequestToken"); err != nil {
		return err
	}

	return json.Unmarshal(data, (*updateSecretInputAlias)(in), jsonx.V1())
}

type updateSecretVersionStageInputAlias UpdateSecretVersionStageInput

// UnmarshalJSON rejects explicit-empty members the SDK models as optional pointers.
func (in *UpdateSecretVersionStageInput) UnmarshalJSON(data []byte) error {
	if err := rejectExplicitEmpty(
		data, "UpdateSecretVersionStage", "MoveToVersionId", "RemoveFromVersionId",
	); err != nil {
		return err
	}

	return json.Unmarshal(data, (*updateSecretVersionStageInputAlias)(in), jsonx.V1())
}
