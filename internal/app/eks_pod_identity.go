package app

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const eksPodsServicePrincipal = eksbackend.PodIdentityAudience

// eksPodIdentityAssumer issues pod identity credentials through the emulator's STS.
type eksPodIdentityAssumer struct {
	sts *stsbackend.InMemoryBackend
}

func (a eksPodIdentityAssumer) AssumePodIdentityRole(
	req eksbackend.PodIdentityRoleRequest,
) (eksbackend.PodIdentityCredentials, error) {
	in := &stsbackend.AssumeRoleInput{
		RoleArn: req.RoleARN, RoleSessionName: req.SessionName, ExternalID: req.ExternalID,
		Policy: req.Policy, CallerArn: req.CallerARN,
	}

	for _, t := range req.Tags {
		in.Tags = append(in.Tags, stsbackend.Tag{Key: t.Key, Value: t.Value})
	}

	var (
		out *stsbackend.AssumeRoleResponse
		err error
	)

	if req.CallerARN == "" {
		out, err = a.sts.AssumeRoleForServiceWith(eksPodsServicePrincipal, in)
	} else {
		out, err = a.sts.AssumeRole(in)
	}

	if err != nil {
		return eksbackend.PodIdentityCredentials{}, err
	}

	r := out.AssumeRoleResult

	exp, err := time.Parse(time.RFC3339, r.Credentials.Expiration)
	if err != nil {
		return eksbackend.PodIdentityCredentials{}, err
	}

	return eksbackend.PodIdentityCredentials{
		AccessKeyID: r.Credentials.AccessKeyID, SecretAccessKey: r.Credentials.SecretAccessKey,
		SessionToken: r.Credentials.SessionToken, Expiration: exp,
		AssumedRoleARN: r.AssumedRoleUser.Arn, AssumedRoleID: r.AssumedRoleUser.AssumedRoleID,
	}, nil
}

// wireEKSPodIdentity lets the eks-auth AssumeRoleForPodIdentity endpoint issue credentials through STS.
func wireEKSPodIdentity(byName map[string]service.Registerable) {
	eksH, ok := byName["EKS"].(*eksbackend.Handler)
	if !ok {
		return
	}

	stsH, ok := byName["STS"].(*stsbackend.Handler)
	if !ok {
		return
	}

	if bk, isSTS := stsH.Backend.(*stsbackend.InMemoryBackend); isSTS {
		eksH.SetPodIdentityRoleAssumer(eksPodIdentityAssumer{sts: bk})
	}
}
