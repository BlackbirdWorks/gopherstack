package organizations

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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

// OnAccountCreate registers fn to run (outside the backend lock) after an account was created in the organization.
func (b *InMemoryBackend) OnAccountCreate(fn func(accountID string)) {
	b.mu.Lock("OnAccountCreate")
	defer b.mu.Unlock()

	b.accountCreateHooks = append(b.accountCreateHooks, fn)
}

func accessRoleTrustPolicy(managementAccountID string) string {
	doc, _ := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":    "Allow",
			"Principal": map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%s:root", managementAccountID)},
			"Action":    "sts:AssumeRole",
		}},
	}, jsonx.V1())

	return string(doc)
}

// runCreateHook isolates a panicking hook so it cannot fail the CreateAccount request or skip other hooks.
func runCreateHook(fn func(accountID string), accountID string) {
	defer func() {
		if r := recover(); r != nil {
			logger.Load(context.Background()).Error("organizations: account create hook panicked", "panic", r)
		}
	}()

	fn(accountID)
}

// flushProvisions runs queued role provisioning outside the backend lock.
func (b *InMemoryBackend) flushProvisions() {
	b.mu.Lock("flushProvisions")
	queue := b.provisionQueue
	b.provisionQueue = nil
	prov := b.provisioner
	mgmt := b.accountID
	hooks := slices.Clone(b.accountCreateHooks)
	b.mu.Unlock()

	defer func() {
		for _, r := range queue {
			for _, fn := range hooks {
				runCreateHook(fn, r.accountID)
			}
		}
	}()

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
