package connect

import (
	"context"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	tdgActive        = "ACTIVE"
	tdgCreating      = "CREATION_IN_PROGRESS"
	maxTDGDistribute = 20
	percentTotal     = 100
)

// Distribution routes a share of traffic to a Region.
type Distribution struct {
	Region     string `json:"Region"`
	Percentage int32  `json:"Percentage"`
}

// SignInDistribution toggles agent sign-in for a Region.
type SignInDistribution struct {
	Region  string `json:"Region"`
	Enabled bool   `json:"Enabled"`
}

// DistributionConfig is the TelephonyConfig and AgentConfig shape.
type DistributionConfig struct {
	Distributions []Distribution `json:"Distributions"`
}

// SignInConfig is the SignInConfig shape.
type SignInConfig struct {
	Distributions []SignInDistribution `json:"Distributions"`
}

type tdgRec struct {
	Telephony  DistributionConfig `json:"telephony"`
	Agent      DistributionConfig `json:"agent"`
	SignIn     SignInConfig       `json:"signIn"`
	Created    awstime.EpochTime  `json:"created"`
	Tags       map[string]string  `json:"tags,omitempty"`
	InstanceID string             `json:"instanceId"`
	ID         string             `json:"id"`
	Arn        string             `json:"arn"`
	Name       string             `json:"name"`
	Desc       string             `json:"description,omitempty"`
	InstArn    string             `json:"instanceArn"`
	Users      []string           `json:"users,omitempty"`
}

// TrafficDistributionGroup is the described form.
type TrafficDistributionGroup struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Arn         string            `json:"Arn"`
	Description string            `json:"Description,omitempty"`
	ID          string            `json:"Id"`
	InstanceArn string            `json:"InstanceArn"`
	Name        string            `json:"Name"`
	Status      string            `json:"Status"`
	IsDefault   bool              `json:"IsDefault"`
}

// TrafficDistributionGroupSummary is the list shape.
type TrafficDistributionGroupSummary struct {
	Arn         string `json:"Arn"`
	ID          string `json:"Id"`
	InstanceArn string `json:"InstanceArn"`
	Name        string `json:"Name"`
	Status      string `json:"Status"`
	IsDefault   bool   `json:"IsDefault"`
}

// TDGRequest carries every traffic distribution group request.
type TDGRequest struct {
	Tags            map[string]string
	TelephonyConfig *DistributionConfig
	AgentConfig     *DistributionConfig
	SignInConfig    *SignInConfig
	InstanceID      string `json:"InstanceId"`
	Name            string
	Description     string
	ClientToken     string
	UserID          string `json:"UserId"`
	GroupID         string `json:"-"          path:"TrafficDistributionGroupId"`
	ID              string `json:"-"          path:"Id"`
	QueryUserID     string `json:"-"                                            query:"UserId"`
	QueryInstanceID string `json:"-"                                            query:"InstanceId"`
	ListInstanceID  string `json:"-"                                            query:"instanceId"`
	NextToken       string `json:"-"                                            query:"nextToken"`
	MaxResults      int32  `json:"-"                                            query:"maxResults"`
}

// TrafficDistributionOut is the GetTrafficDistribution response.
type TrafficDistributionOut struct {
	Arn             string             `json:"Arn"`
	ID              string             `json:"Id"`
	TelephonyConfig DistributionConfig `json:"TelephonyConfig"`
	AgentConfig     DistributionConfig `json:"AgentConfig"`
	SignInConfig    SignInConfig       `json:"SignInConfig"`
}

type tdgOut struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

// refID strips the ARN prefix from an ID-or-ARN reference.
func refID(ref string) string {
	if _, after, ok := strings.CutLast(ref, "/"); ok {
		return after
	}

	return ref
}

func (b *Backend) tdgByRef(ref string) (*tdgRec, error) {
	id := refID(ref)

	for _, t := range b.trafficGroups.table.All() {
		if t.ID == id {
			return t, nil
		}
	}

	return nil, notFoundErr("TrafficDistributionGroup", ref)
}

func (b *Backend) tdgStatus(t *tdgRec) string {
	if b.now().Sub(t.Created.Time) < b.delay {
		return tdgCreating
	}

	return tdgActive
}

func checkDistributions(field string, d []Distribution) error {
	if len(d) == 0 || len(d) > maxTDGDistribute {
		return invalidParamErr("%s must contain 1-%d distributions", field, maxTDGDistribute)
	}

	total := int32(0)
	seen := map[string]bool{}

	for _, x := range d {
		if x.Percentage < 0 || x.Percentage > percentTotal || strings.TrimSpace(x.Region) == "" || seen[x.Region] {
			return invalidParamErr("%s has an invalid or duplicated distribution", field)
		}

		seen[x.Region] = true
		total += x.Percentage
	}

	if total != percentTotal {
		return invalidParamErr("%s percentages must add up to 100", field)
	}

	return nil
}

func checkSignIn(c *SignInConfig) error {
	if len(c.Distributions) == 0 || len(c.Distributions) > maxTDGDistribute {
		return invalidParamErr("SignInConfig must contain 1-%d distributions", maxTDGDistribute)
	}

	seen := map[string]bool{}

	for _, x := range c.Distributions {
		if strings.TrimSpace(x.Region) == "" || seen[x.Region] {
			return invalidParamErr("SignInConfig has an invalid or duplicated distribution")
		}

		seen[x.Region] = true
	}

	return nil
}

// CreateTrafficDistributionGroup creates a traffic distribution group for an instance.
func (b *Backend) CreateTrafficDistributionGroup(ctx context.Context, in *TDGRequest) (*tdgOut, error) {
	if err := firstError(
		checkName("Name", in.Name, maxNameLen), checkDescription(in.Description), checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateTrafficDistributionGroup", in.InstanceID, func(inst string) (*tdgOut, error) {
		for _, t := range b.trafficGroups.table.All() {
			if strings.EqualFold(t.Name, in.Name) {
				return nil, duplicateErr("TrafficDistributionGroup", in.Name)
			}
		}

		_, region := b.scope(ctx)
		id := newID()
		ir, _ := b.instanceLocked(inst)
		rec := &tdgRec{
			Telephony: DistributionConfig{Distributions: []Distribution{{Region: region, Percentage: percentTotal}}},
			Agent:     DistributionConfig{Distributions: []Distribution{{Region: region, Percentage: percentTotal}}},
			SignIn:    SignInConfig{Distributions: []SignInDistribution{{Region: region, Enabled: true}}},
			Created:   awstime.NewEpochTime(b.now()), Tags: cloneTags(in.Tags), InstanceID: inst, ID: id,
			Arn:  b.buildARN(ctx, "traffic-distribution-group/"+id),
			Name: in.Name, Desc: in.Description, InstArn: ir.Instance.Arn,
		}
		b.trafficGroups.put(rec)

		return &tdgOut{Arn: rec.Arn, ID: id}, nil
	})
}

func (b *Backend) tdgView(t *tdgRec) TrafficDistributionGroup {
	return TrafficDistributionGroup{
		Tags: cloneTags(t.Tags), Arn: t.Arn, Description: t.Desc, ID: t.ID, InstanceArn: t.InstArn, Name: t.Name,
		Status: b.tdgStatus(t),
	}
}

// DescribeTrafficDistributionGroup returns a group.
func (b *Backend) DescribeTrafficDistributionGroup(ref string) (*TrafficDistributionGroup, error) {
	b.mu.RLock("DescribeTrafficDistributionGroup")
	defer b.mu.RUnlock()

	t, err := b.tdgByRef(ref)
	if err != nil {
		return nil, err
	}

	out := b.tdgView(t)

	return &out, nil
}

// ListTrafficDistributionGroups pages through groups, optionally of one instance.
func (b *Backend) ListTrafficDistributionGroups(in *TDGRequest) ([]TrafficDistributionGroupSummary, string, error) {
	b.mu.RLock("ListTrafficDistributionGroups")

	inst := ""

	if in.ListInstanceID != "" {
		r, err := b.instanceLocked(in.ListInstanceID)
		if err != nil {
			b.mu.RUnlock()

			return nil, "", err
		}

		inst = r.Instance.ID
	}

	var all []TrafficDistributionGroupSummary

	for _, t := range b.trafficGroups.table.All() {
		if inst == "" || t.InstanceID == inst {
			v := b.tdgView(t)
			all = append(all, TrafficDistributionGroupSummary{
				Arn: v.Arn, ID: v.ID, InstanceArn: v.InstanceArn, Name: v.Name, Status: v.Status,
			})
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c TrafficDistributionGroupSummary) int { return strings.Compare(a.ID, c.ID) })

	return paged(all, in.NextToken, in.MaxResults)
}

// GetTrafficDistribution returns the distribution configuration of a group.
func (b *Backend) GetTrafficDistribution(ref string) (*TrafficDistributionOut, error) {
	b.mu.RLock("GetTrafficDistribution")
	defer b.mu.RUnlock()

	t, err := b.tdgByRef(ref)
	if err != nil {
		return nil, err
	}

	return &TrafficDistributionOut{
		TelephonyConfig: t.Telephony, AgentConfig: t.Agent, SignInConfig: t.SignIn, Arn: t.Arn, ID: t.ID,
	}, nil
}

// UpdateTrafficDistribution replaces the supplied configurations of an active group.
func (b *Backend) UpdateTrafficDistribution(in *TDGRequest) (*empty, error) {
	configs := map[string]*DistributionConfig{"TelephonyConfig": in.TelephonyConfig, "AgentConfig": in.AgentConfig}

	for field, c := range configs {
		if c != nil {
			if err := checkDistributions(field, c.Distributions); err != nil {
				return nil, err
			}
		}
	}

	if in.SignInConfig != nil {
		if err := checkSignIn(in.SignInConfig); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateTrafficDistribution")
	defer b.mu.Unlock()

	t, err := b.tdgByRef(in.ID)
	if err != nil {
		return nil, err
	}

	if b.tdgStatus(t) != tdgActive {
		return nil, apiErr(
			statusNotFound,
			"ResourceNotReadyException",
			"TrafficDistributionGroup %q is not active",
			in.ID,
		)
	}

	if in.TelephonyConfig != nil {
		t.Telephony = *in.TelephonyConfig
	}

	if in.AgentConfig != nil {
		t.Agent = *in.AgentConfig
	}

	if in.SignInConfig != nil {
		t.SignIn = *in.SignInConfig
	}

	return &empty{}, nil
}

// DeleteTrafficDistributionGroup removes a group that has no associated users.
func (b *Backend) DeleteTrafficDistributionGroup(ref string) (*empty, error) {
	b.mu.Lock("DeleteTrafficDistributionGroup")
	defer b.mu.Unlock()

	t, err := b.tdgByRef(ref)
	if err != nil {
		return nil, err
	}

	if len(t.Users) > 0 {
		return nil, inUseErr("TrafficDistributionGroup %q still has associated users", ref)
	}

	return nil, b.trafficGroups.del(t.InstanceID, t.ID)
}

// AssociateTrafficDistributionGroupUser adds an agent to a group; repeating it is a no-op.
func (b *Backend) AssociateTrafficDistributionGroupUser(in *TDGRequest) (*empty, error) {
	b.mu.Lock("AssociateTrafficDistributionGroupUser")
	defer b.mu.Unlock()

	t, err := b.tdgUser(in.GroupID, in.InstanceID, in.UserID)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(t.Users, in.UserID) {
		t.Users = append(t.Users, in.UserID)
	}

	return &empty{}, nil
}

// tdgUser resolves the group and checks the user exists in the group's instance.
func (b *Backend) tdgUser(groupRef, instRef, userID string) (*tdgRec, error) {
	t, err := b.tdgByRef(groupRef)
	if err != nil {
		return nil, err
	}

	r, err := b.instanceLocked(instRef)
	if err != nil {
		return nil, err
	}

	if r.Instance.ID != t.InstanceID {
		return nil, invalidRequestErr("instance %q does not own TrafficDistributionGroup %q", instRef, groupRef)
	}

	if !b.users.has(t.InstanceID, userID) {
		return nil, notFoundErr("User", userID)
	}

	return t, nil
}

// DisassociateTrafficDistributionGroupUser removes an agent from a group.
func (b *Backend) DisassociateTrafficDistributionGroupUser(in *TDGRequest) (*empty, error) {
	b.mu.Lock("DisassociateTrafficDistributionGroupUser")
	defer b.mu.Unlock()

	t, err := b.tdgByRef(in.GroupID)
	if err != nil {
		return nil, err
	}

	r, err := b.instanceLocked(in.QueryInstanceID)
	if err != nil {
		return nil, err
	}

	if r.Instance.ID != t.InstanceID || !slices.Contains(t.Users, in.QueryUserID) {
		return nil, notFoundErr("TrafficDistributionGroupUser", in.QueryUserID)
	}

	t.Users = slices.DeleteFunc(slices.Clone(t.Users), func(u string) bool { return u == in.QueryUserID })

	return &empty{}, nil
}

// ListTrafficDistributionGroupUsers pages through the agents of a group.
func (b *Backend) ListTrafficDistributionGroupUsers(in *TDGRequest) ([]map[string]string, string, error) {
	b.mu.RLock("ListTrafficDistributionGroupUsers")

	t, err := b.tdgByRef(in.GroupID)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	users := slices.Clone(t.Users)

	b.mu.RUnlock()

	slices.Sort(users)

	all := make([]map[string]string, 0, len(users))
	for _, u := range users {
		all = append(all, map[string]string{"UserId": u})
	}

	return paged(all, in.NextToken, in.MaxResults)
}
