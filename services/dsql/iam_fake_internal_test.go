package dsql

import (
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	fakeAccessKey = "AKIAIOSFODNN7EXAMPLE"
	fakeSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	fakeUserARN   = "arn:aws:iam::123456789012:user/alice"
)

type fakeIdentities struct {
	policies []string
}

func (f fakeIdentities) ListAllAccessKeys() []iam.AccessKey {
	return []iam.AccessKey{{AccessKeyID: fakeAccessKey, SecretAccessKey: fakeSecretKey, Status: "Active"}}
}

func (f fakeIdentities) GetUserByAccessKeyID(string) (*iam.User, error) {
	return &iam.User{UserName: "alice", Arn: fakeUserARN}, nil
}

func (f fakeIdentities) GetPoliciesForUser(string) ([]string, error) { return f.policies, nil }

func (f fakeIdentities) GetPoliciesForRole(string) ([]string, error) { return nil, nil }
