package codegurureviewer

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	maxNameLen        = 100
	maxTokenLen       = 256
	encryptionAWS     = "AWS_OWNED_CMK"
	encryptionCMK     = "CUSTOMER_MANAGED_CMK"
	maxAssocNameParts = 100
)

// AssociateRepositoryInput is the AssociateRepository request.
type AssociateRepositoryInput struct {
	Repository         *Repository       `json:"Repository"`
	KMSKeyDetails      *KMSKeyDetails    `json:"KMSKeyDetails,omitempty"`
	Tags               map[string]string `json:"Tags,omitempty"`
	ClientRequestToken string            `json:"ClientRequestToken,omitempty"`
}

// AssociationOutput is the Associate/Describe/DisassociateRepository response.
type AssociationOutput struct {
	RepositoryAssociation *RepositoryAssociation `json:"RepositoryAssociation"`
	Tags                  map[string]string      `json:"Tags,omitempty"`
}

// ListAssociationsInput is the ListRepositoryAssociations request.
type ListAssociationsInput struct {
	NextToken     string   `json:"-" query:"NextToken"`
	ProviderTypes []string `json:"-" query:"ProviderType"`
	States        []string `json:"-" query:"State"`
	Names         []string `json:"-" query:"Name"`
	Owners        []string `json:"-" query:"Owner"`
	MaxResults    int32    `json:"-" query:"MaxResults"`
}

// ListAssociationsOutput is the ListRepositoryAssociations response.
type ListAssociationsOutput struct {
	NextToken                      string `json:",omitempty"`
	RepositoryAssociationSummaries []RepositoryAssociationSummary
}

func providerTypes() []string {
	return []string{providerCodeCommit, providerGitHub, providerBitbucket, providerGHES, providerS3}
}

func associationStates() []string {
	return []string{stateAssociated, stateAssociating, stateFailed, stateDisassociating, stateDisassociated}
}

// view returns the association as seen at now: transitional states settle once their window has passed.
func (a *associationRecord) view(now time.Time) *associationRecord {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	if now.Before(a.SettleAt.Time) {
		return &cp
	}

	switch a.State {
	case stateAssociating:
		cp.State = stateAssociated
		cp.Updated = a.SettleAt
	case stateDisassociating:
		cp.State = stateDisassociated
		cp.Updated = a.SettleAt
	}

	return &cp
}

func (a *associationRecord) wire() *RepositoryAssociation {
	return &RepositoryAssociation{
		AssociationArn: a.ARN, AssociationID: a.ID, ConnectionArn: a.ConnectionARN,
		CreatedTimeStamp: a.Created, LastUpdatedTimeStamp: a.Updated, KMSKeyDetails: a.KMS,
		Name: a.Name, Owner: a.Owner, ProviderType: a.ProviderType, S3RepositoryDetails: a.S3,
		State: a.State, StateReason: a.StateReason,
	}
}

func (a *associationRecord) output() *AssociationOutput {
	return &AssociationOutput{RepositoryAssociation: a.wire(), Tags: maps.Clone(a.Tags)}
}

func checkTags(tags map[string]string) error {
	if len(tags) > maxTags {
		return validationErr("a resource can have at most %d tags", maxTags)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen ||
			strings.HasPrefix(strings.ToLower(k), "aws:") {
			return validationErr("tag %q is not valid", k)
		}
	}

	return nil
}

// repositoryTarget is a validated Repository union.
type repositoryTarget struct {
	provider, name, owner, connection, bucket string
}

func (r *Repository) target(account string) (repositoryTarget, error) {
	set := 0

	var t repositoryTarget

	if c := r.CodeCommit; c != nil {
		set++
		t = repositoryTarget{provider: providerCodeCommit, name: c.Name, owner: account}
	}

	if s := r.S3Bucket; s != nil {
		set++
		t = repositoryTarget{provider: providerS3, name: s.Name, owner: account, bucket: s.BucketName}
	}

	if x := r.Bitbucket; x != nil {
		set++
		t = repositoryTarget{provider: providerBitbucket, name: x.Name, owner: x.Owner, connection: x.ConnectionArn}
	}

	if x := r.GitHubEnterpriseServer; x != nil {
		set++
		t = repositoryTarget{provider: providerGHES, name: x.Name, owner: x.Owner, connection: x.ConnectionArn}
	}

	if set != 1 {
		return t, validationErr(
			"Repository must name exactly one of CodeCommit, S3Bucket, Bitbucket or GitHubEnterpriseServer",
		)
	}

	if t.name == "" || len(t.name) > maxNameLen || t.owner == "" || len(t.owner) > maxNameLen {
		return t, validationErr("repository Name and Owner must be 1-%d characters", maxNameLen)
	}

	if t.provider != providerCodeCommit && t.provider != providerS3 && !strings.HasPrefix(t.connection, "arn:") {
		return t, validationErr("ConnectionArn must be an AWS CodeStar Connections ARN")
	}

	return t, nil
}

func validateKMS(k *KMSKeyDetails) error {
	if k == nil {
		return nil
	}

	switch k.EncryptionOption {
	case encryptionAWS, "":
	case encryptionCMK:
		if k.KMSKeyID == "" {
			return validationErr("KMSKeyId is required for CUSTOMER_MANAGED_CMK")
		}
	default:
		return validationErr("EncryptionOption must be %s or %s", encryptionAWS, encryptionCMK)
	}

	return nil
}

// AssociateRepository starts associating a repository; it reads Associated once the lifecycle window passes.
func (b *InMemoryBackend) AssociateRepository(
	ctx context.Context,
	in *AssociateRepositoryInput,
) (*associationRecord, error) {
	if in.Repository == nil {
		return nil, validationErr("Repository is required")
	}

	if len(in.ClientRequestToken) > maxTokenLen {
		return nil, validationErr("ClientRequestToken must be at most %d characters", maxTokenLen)
	}

	account, region := b.scope(ctx)

	t, err := in.Repository.target(account)
	if err != nil {
		return nil, err
	}

	if err = errors2(validateKMS(in.KMSKeyDetails), checkTags(in.Tags)); err != nil {
		return nil, err
	}

	if err = b.checkTargetExists(ctx, region, t); err != nil {
		return nil, err
	}

	b.mu.Lock("AssociateRepository")
	defer b.mu.Unlock()

	now := b.now()

	for _, a := range b.associations.All() {
		v := a.view(now)
		if v.ProviderType == t.provider && v.Owner == t.owner && v.Name == t.name &&
			(v.State == stateAssociating || v.State == stateAssociated) {
			return nil, conflictErr("repository %s/%s is already associated", t.owner, t.name)
		}
	}

	id := uuid.NewString()
	rec := &associationRecord{
		ARN: b.associationARN(ctx, id), ID: id, Name: t.name, Owner: t.owner, ProviderType: t.provider,
		ConnectionARN: t.connection, State: stateAssociating, Created: awstime.NewEpochTime(now),
		Updated: awstime.NewEpochTime(now), SettleAt: awstime.NewEpochTime(now.Add(b.dwellLocked())),
		KMS: in.KMSKeyDetails, Tags: maps.Clone(in.Tags),
	}

	if t.provider == providerS3 {
		rec.S3 = &S3RepositoryDetails{BucketName: t.bucket}
	}

	if rec.KMS == nil {
		rec.KMS = &KMSKeyDetails{EncryptionOption: encryptionAWS}
	}

	b.associations.Put(rec)

	return rec.view(now), nil
}

func errors2(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

func (b *InMemoryBackend) checkTargetExists(ctx context.Context, region string, t repositoryTarget) error {
	switch t.provider {
	case providerCodeCommit:
		if exists, known := b.repositoryExists(region, t.name); known && !exists {
			return validationErr("CodeCommit repository %q does not exist in %s", t.name, region)
		}
	case providerS3:
		if !strings.HasPrefix(t.bucket, bucketPrefix) {
			return validationErr("S3 bucket names must begin with %s", bucketPrefix)
		}

		if exists, known := b.bucketExists(ctx, t.bucket); known && !exists {
			return validationErr("S3 bucket %q does not exist", t.bucket)
		}
	}

	return nil
}

func validAssociationARN(v string) bool {
	return strings.HasPrefix(v, "arn:") && strings.Contains(v, ":"+serviceName+":") &&
		strings.Contains(v, ":association:")
}

func (b *InMemoryBackend) associationLocked(arn string) (*associationRecord, error) {
	if !validAssociationARN(arn) {
		return nil, validationErr("AssociationArn %q is not a valid repository association ARN", arn)
	}

	a, ok := b.associations.Get(arn)
	if !ok {
		return nil, notFoundErr("repository association %q does not exist", arn)
	}

	return a, nil
}

// DescribeRepositoryAssociation returns an association with its current state.
func (b *InMemoryBackend) DescribeRepositoryAssociation(arn string) (*associationRecord, error) {
	b.mu.RLock("DescribeRepositoryAssociation")
	defer b.mu.RUnlock()

	a, err := b.associationLocked(arn)
	if err != nil {
		return nil, err
	}

	return a.view(b.now()), nil
}

// DisassociateRepository starts disassociating; it reads Disassociated once the lifecycle window passes.
func (b *InMemoryBackend) DisassociateRepository(arn string) (*associationRecord, error) {
	b.mu.Lock("DisassociateRepository")
	defer b.mu.Unlock()

	a, err := b.associationLocked(arn)
	if err != nil {
		return nil, err
	}

	now := b.now()
	v := a.view(now)

	if v.State == stateDisassociating || v.State == stateDisassociated {
		return v, nil
	}

	a.State = stateDisassociating
	a.Updated = awstime.NewEpochTime(now)
	a.SettleAt = awstime.NewEpochTime(now.Add(b.dwellLocked()))

	return a.view(now), nil
}

func validateEnums(field string, got, allowed []string) error {
	for _, g := range got {
		if !slices.Contains(allowed, g) {
			return validationErr("%s value %q is not valid", field, g)
		}
	}

	return nil
}

// ListRepositoryAssociations lists associations, filtered by provider, state, name and owner.
func (b *InMemoryBackend) ListRepositoryAssociations(
	in *ListAssociationsInput,
) ([]RepositoryAssociationSummary, string, error) {
	if err := errors2(
		validateEnums("ProviderType", in.ProviderTypes, providerTypes()),
		validateEnums("State", in.States, associationStates()),
	); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListRepositoryAssociations")

	now := b.now()

	var recs []*associationRecord

	for _, a := range b.associations.All() {
		v := a.view(now)
		if matchAny(in.ProviderTypes, v.ProviderType) && matchAny(in.States, v.State) &&
			matchAny(in.Names, v.Name) && matchAny(in.Owners, v.Owner) {
			recs = append(recs, v)
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(recs, func(x, y *associationRecord) int {
		if c := x.Created.Compare(y.Created.Time); c != 0 {
			return c
		}

		return strings.Compare(x.ARN, y.ARN)
	})

	pg, err := restroute.Paginate(recs, in.NextToken, int(in.MaxResults), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, "", validationErr("%v", err)
	}

	out := make([]RepositoryAssociationSummary, 0, len(pg.Data))

	for _, a := range pg.Data {
		out = append(out, RepositoryAssociationSummary{
			AssociationArn: a.ARN, AssociationID: a.ID, ConnectionArn: a.ConnectionARN,
			LastUpdatedTimeStamp: a.Updated, Name: a.Name, Owner: a.Owner,
			ProviderType: a.ProviderType, State: a.State,
		})
	}

	return out, pg.Next, nil
}

func matchAny(filter []string, v string) bool { return len(filter) == 0 || slices.Contains(filter, v) }
