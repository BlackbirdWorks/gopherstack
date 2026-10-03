package sts

import (
	"fmt"
)

const serviceSessionDurationDefault = DefaultDurationSeconds

// AssumeRoleForService issues credentials for roleArn to an AWS service
// principal (e.g. states.amazonaws.com), enforcing the role's trust policy.
func (b *InMemoryBackend) AssumeRoleForService(
	servicePrincipal, roleArn, sessionName string,
) (*AssumeRoleResponse, error) {
	input := &AssumeRoleInput{RoleArn: roleArn, RoleSessionName: sessionName}

	if err := validateAssumeRoleInput(input); err != nil {
		return nil, err
	}

	b.mu.RLock("AssumeRoleForService")
	rl := b.roleLookup
	strict := b.strictConditions
	b.mu.RUnlock()

	maxDuration := int32(MaxDurationSeconds)

	if rl != nil {
		meta, _ := rl.GetRoleByArn(roleArn)
		if meta == nil {
			return nil, fmt.Errorf("%w: role %s does not exist", ErrAccessDenied, roleArn)
		}

		err := evaluateAssumeRoleTrust(meta.TrustPolicy, trustEval{
			action:           actionAssumeRole,
			servicePrincipal: servicePrincipal,
			strictConditions: strict,
		})
		if err != nil {
			return nil, err
		}

		if meta.MaxSessionDuration > 0 {
			maxDuration = meta.MaxSessionDuration
		}
	}

	return b.issueCredentials(input, min(int32(serviceSessionDurationDefault), maxDuration))
}
