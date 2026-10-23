package transfer

import (
	"errors"
	"os"
	"path"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

// RolePolicySource yields the policy documents attached to an IAM role; the IAM emulator's backend satisfies it.
type RolePolicySource interface {
	GetPoliciesForRole(roleName string) ([]string, error)
}

type sftpOp int

const (
	opRead sftpOp = iota
	opWrite
	opList
	opDelete
	opMkdir
)

const (
	s3ActionGet         = "s3:GetObject"
	s3ActionPut         = "s3:PutObject"
	s3ActionDelete      = "s3:DeleteObject"
	s3ActionList        = "s3:ListBucket"
	s3ActionListBuckets = "s3:ListAllMyBuckets"
	efsActionMount      = "elasticfilesystem:ClientMount"
	efsActionWrite      = "elasticfilesystem:ClientWrite"
)

// sftpAuthz evaluates a login's role policies and session policy for each file operation.
type sftpAuthz struct {
	src       RolePolicySource
	role      string
	policy    string
	region    string
	accountID string
	efs       bool
}

func roleName(role string) string {
	if _, after, ok := strings.CutLast(role, "/"); ok {
		return after
	}

	return role
}

func expandTransferVars(policy, userName, home string) string {
	h := strings.TrimPrefix(path.Clean("/"+home), "/")
	bucket, folder, _ := strings.Cut(h, "/")

	return strings.NewReplacer(
		"${transfer:UserName}", userName,
		"${transfer:HomeDirectory}", h,
		"${transfer:HomeBucket}", bucket,
		"${transfer:HomeFolder}", folder,
	).Replace(policy)
}

// newSFTPAuthz returns nil (no enforcement) when no policy source is wired or the role is unknown to IAM.
func newSFTPAuthz(src RolePolicySource, sess *sftpSession, user, region, accountID string, efs bool) *sftpAuthz {
	if src == nil || sess.Role == "" {
		return nil
	}

	if _, err := src.GetPoliciesForRole(roleName(sess.Role)); errors.Is(err, iam.ErrRoleNotFound) {
		return nil
	}

	return &sftpAuthz{
		src: src, role: roleName(sess.Role), efs: efs, region: region, accountID: accountID,
		policy: expandTransferVars(sess.Policy, user, sess.Home),
	}
}

func (a *sftpAuthz) decide(action, resource string, extra map[string]string) bool {
	docs, err := a.src.GetPoliciesForRole(a.role)
	if err != nil {
		return false
	}

	ctx := iam.ConditionContext{Extra: extra}
	if iam.EvaluatePolicies(docs, action, resource, ctx) != iam.EvalAllow {
		return false
	}

	return a.policy == "" || iam.EvaluatePolicies([]string{a.policy}, action, resource, ctx) == iam.EvalAllow
}

// check reports whether op on bucket/key is permitted; nil receivers permit everything.
func (a *sftpAuthz) check(op sftpOp, bucket, key string) error {
	if a == nil {
		return nil
	}

	if a.efs {
		return permit(a.checkEFS(op, bucket))
	}

	return permit(a.checkS3(op, bucket, key))
}

func permit(ok bool) error {
	if ok {
		return nil
	}

	return os.ErrPermission
}

func (a *sftpAuthz) checkEFS(op sftpOp, fsID string) bool {
	action := efsActionMount
	if op == opWrite || op == opDelete || op == opMkdir {
		action = efsActionWrite
	}

	return a.decide(action, arn.Build("elasticfilesystem", a.region, a.accountID, "file-system/"+fsID), nil)
}

func (a *sftpAuthz) checkS3(op sftpOp, bucket, key string) bool {
	if bucket == "" {
		return a.decide(s3ActionListBuckets, arn.Build("s3", "", "", "*"), nil)
	}

	objectARN := arn.Build("s3", "", "", bucket+"/"+key)

	switch op {
	case opRead:
		return a.decide(s3ActionGet, objectARN, nil)
	case opWrite, opMkdir:
		return a.decide(s3ActionPut, objectARN, nil)
	case opDelete:
		return a.decide(s3ActionDelete, objectARN, nil)
	case opList:
		prefix := ""
		if key != "" {
			prefix = key + "/"
		}

		return a.decide(s3ActionList, arn.Build("s3", "", "", bucket),
			map[string]string{"s3:prefix": prefix, "s3:delimiter": "/"})
	}

	return false
}
