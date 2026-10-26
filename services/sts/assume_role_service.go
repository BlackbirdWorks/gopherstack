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
	return b.AssumeRoleForServiceWith(
		servicePrincipal,
		&AssumeRoleInput{RoleArn: roleArn, RoleSessionName: sessionName},
	)
}

// AssumeRoleForServiceWith is AssumeRoleForService carrying session tags, an inline policy and an external ID.
func (b *InMemoryBackend) AssumeRoleForServiceWith(
	servicePrincipal string, input *AssumeRoleInput,
) (*AssumeRoleResponse, error) {
	if err := validateAssumeRoleInput(input); err != nil {
		return nil, err
	}

	maxDuration, err := b.serviceRoleMaxDuration(servicePrincipal, input.RoleArn)
	if err != nil {
		return nil, err
	}

	return b.issueCredentials(input, min(int32(serviceSessionDurationDefault), maxDuration))
}

// CheckServiceRoleTrust verifies servicePrincipal may assume roleArn without issuing credentials.
func (b *InMemoryBackend) CheckServiceRoleTrust(servicePrincipal, roleArn string) error {
	_, err := b.serviceRoleMaxDuration(servicePrincipal, roleArn)

	return err
}

func (b *InMemoryBackend) serviceRoleMaxDuration(servicePrincipal, roleArn string) (int32, error) {
	b.mu.RLock("serviceRoleMaxDuration")
	rl := b.roleLookup
	strict := b.strictConditions
	b.mu.RUnlock()

	maxDuration := int32(MaxDurationSeconds)

	if rl == nil {
		return maxDuration, nil
	}

	meta, _ := rl.GetRoleByArn(roleArn)
	if meta == nil {
		return 0, fmt.Errorf("%w: role %s does not exist", ErrAccessDenied, roleArn)
	}

	err := evaluateAssumeRoleTrust(meta.TrustPolicy, trustEval{
		action:           actionAssumeRole,
		servicePrincipal: servicePrincipal,
		strictConditions: strict,
	})
	if err != nil {
		return 0, err
	}

	if meta.MaxSessionDuration > 0 {
		maxDuration = meta.MaxSessionDuration
	}

	return maxDuration, nil
}
