package organizations

import (
	"encoding/json"
	"fmt"
)

const defaultAccessRoleName = "OrganizationAccountAccessRole"

// AccountProvisioner creates the cross-account access role in a newly created member account.
type AccountProvisioner interface {
	ProvisionAccessRole(accountID, roleName, trustPolicy string) error
}

// SetAccountProvisioner wires the IAM seam; when nil, created accounts get no role.
func (b *InMemoryBackend) SetAccountProvisioner(p AccountProvisioner) {
	b.mu.Lock("SetAccountProvisioner")
	defer b.mu.Unlock()

	b.provisioner = p
}

func accessRoleTrustPolicy(managementAccountID string) string {
	doc, _ := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":    "Allow",
			"Principal": map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%s:root", managementAccountID)},
			"Action":    "sts:AssumeRole",
		}},
	})

	return string(doc)
}

// flushProvisions runs queued role provisioning outside the backend lock.
func (b *InMemoryBackend) flushProvisions() {
	b.mu.Lock("flushProvisions")
	queue := b.provisionQueue
	b.provisionQueue = nil
	prov := b.provisioner
	mgmt := b.accountID
	b.mu.Unlock()

	if prov == nil {
		return
	}

	trust := accessRoleTrustPolicy(mgmt)

	for _, r := range queue {
		role := r.roleName
		if role == "" {
			role = defaultAccessRoleName
		}

		// Account is already SUCCEEDED; a role that cannot be created must not undo it.
		_ = prov.ProvisionAccessRole(r.accountID, role, trust)
	}
}
