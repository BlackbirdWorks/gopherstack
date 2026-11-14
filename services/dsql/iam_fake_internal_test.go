package dsql

import (
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	fakeAccessKey = "AKIAIOSFODNN7EXAMPLE"
	fakeSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	fakeUserARN   = "arn:aws:iam::123456789012:user/alice"
)

type fakeIdentities struct {
	userARN      string
	boundary     string
	policies     []string
	rolePolicies []string
}

func (f fakeIdentities) ListAllAccessKeys() []iam.AccessKey {
	return []iam.AccessKey{{AccessKeyID: fakeAccessKey, SecretAccessKey: fakeSecretKey, Status: "Active"}}
}

func (f fakeIdentities) GetUserByAccessKeyID(string) (*iam.User, error) {
	arn := f.userARN
	if arn == "" {
		arn = fakeUserARN
	}

	return &iam.User{UserName: "alice", Arn: arn}, nil
}

func (f fakeIdentities) PermissionsBoundaryDocForUser(string) string { return f.boundary }

func (f fakeIdentities) PermissionsBoundaryDocForRole(string) string { return f.boundary }

func (f fakeIdentities) GetPoliciesForUser(string) ([]string, error) { return f.policies, nil }

func (f fakeIdentities) GetPoliciesForRole(string) ([]string, error) { return f.rolePolicies, nil }

type fakeSessions struct{ info *sts.SessionInfo }

func (f fakeSessions) LookupSession(string, string) *sts.SessionInfo { return f.info }
