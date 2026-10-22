package connect

import (
	"context"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	kindFlow   = "flow"
	kindModule = "module"
)

// flowVersionRec is an immutable snapshot of a flow or flow module.
type flowVersionRec struct {
	Stamp       Stamp  `json:"stamp"`
	InstanceID  string `json:"instanceId"`
	Kind        string `json:"kind"`
	ParentID    string `json:"parentId"`
	Arn         string `json:"arn"`
	Content     string `json:"content"`
	Sha256      string `json:"sha256"`
	Description string `json:"description,omitempty"`
	Version     int64  `json:"version"`
}

// flowAliasRec names a version of a flow or flow module.
type flowAliasRec struct {
	Stamp       Stamp  `json:"stamp"`
	InstanceID  string `json:"instanceId"`
	Kind        string `json:"kind"`
	ParentID    string `json:"parentId"`
	AliasID     string `json:"aliasId"`
	Arn         string `json:"arn"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Version     int64  `json:"version"`
}

func versionKey(kind, parent string, v int64) string {
	return kind + "/" + parent + "/" + strconv.FormatInt(v, 10)
}

// versioned is the live (latest) side of a flow or flow module.
type versioned struct {
	version *int64
	arn     string
	content string
	sha     string
	status  string
}

func (b *Backend) versionedParent(kind, inst, id string) (versioned, error) {
	if kind == kindFlow {
		f, err := b.flows.get(inst, id)
		if err != nil {
			return versioned{}, err
		}

		return versioned{
			version: f.Flow.Version, arn: f.Flow.Arn, content: f.Flow.Content, sha: f.Flow.FlowContentSha256,
			status: f.Flow.Status,
		}, nil
	}

	m, err := b.modules.get(inst, id)
	if err != nil {
		return versioned{}, err
	}

	return versioned{
		version: m.Module.Version, arn: m.Module.Arn, content: m.Module.Content, sha: m.Module.FlowModuleContentSha256,
		status: m.Module.Status,
	}, nil
}

func (b *Backend) versionsOf(inst, kind, parent string) []*flowVersionRec {
	var out []*flowVersionRec

	b.flowVersions.table.Range(func(v *flowVersionRec) bool {
		if v.InstanceID == inst && v.Kind == kind && v.ParentID == parent {
			out = append(out, v)
		}

		return true
	})
	sortByVersion(out)

	return out
}

func sortByVersion(s []*flowVersionRec) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].Version < s[j-1].Version; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func (b *Backend) aliasesOf(inst, kind, parent string) []*flowAliasRec {
	var out []*flowAliasRec

	for _, a := range b.flowAliases.in(inst) {
		if a.Kind == kind && a.ParentID == parent {
			out = append(out, a)
		}
	}

	return out
}

// snapshotVersion records the current content of a flow or module as the given version.
func (b *Backend) snapshotVersion(ctx context.Context, inst, kind, parent string, p versioned, v int64, desc string) {
	b.flowVersions.put(&flowVersionRec{
		Stamp: b.stamp(
			ctx,
		),
		InstanceID:  inst,
		Kind:        kind,
		ParentID:    parent,
		Arn:         p.arn + ":" + strconv.FormatInt(v, 10),
		Content:     p.content,
		Sha256:      p.sha,
		Description: desc,
		Version:     v,
	})
}

// dropVersioned removes the versions and aliases of a deleted flow or module.
func (b *Backend) dropVersioned(inst, kind, parent string) {
	for _, v := range b.versionsOf(inst, kind, parent) {
		b.flowVersions.table.Delete(inst + "/" + versionKey(kind, parent, v.Version))
	}

	for _, a := range b.aliasesOf(inst, kind, parent) {
		b.flowAliases.table.Delete(inst + "/" + a.AliasID)
	}
}

// VersionInput is the Create/Delete/List version request for flows and modules.
type VersionInput struct {
	Version                 *int64 `json:"ContactFlowVersion"`
	Description             string
	FlowContentSha256       string
	FlowModuleContentSha256 string
	ParentID                string `json:"-"                  path:"ContactFlowId"`
	ModuleID                string `json:"-"                  path:"ContactFlowModuleId"`
	PathVersion             string `json:"-"                  path:"ContactFlowVersion"`
	ModulePathVersion       string `json:"-"                  path:"ContactFlowModuleVersion"`
	NextToken               string `json:"-"                                                  query:"nextToken"`
	instPath
	MaxResults int32 `json:"-"                                                  query:"maxResults"`
}

// AliasInput is the Create/Update/Describe/Delete alias request for flows and modules.
type AliasInput struct {
	Name          *string
	Description   *string
	FlowVersion   *int64 `json:"ContactFlowVersion"`
	ModuleVersion *int64 `json:"ContactFlowModuleVersion"`
	AliasName     string
	ParentID      string `json:"-"                        path:"ContactFlowId"`
	ModuleID      string `json:"-"                        path:"ContactFlowModuleId"`
	AliasID       string `json:"-"                        path:"AliasId"`
	NextToken     string `json:"-"                                                   query:"nextToken"`
	instPath
	MaxResults int32 `json:"-"                                                   query:"maxResults"`
}

func (in *VersionInput) parent(kind string) string {
	if kind == kindModule {
		return in.ModuleID
	}

	return in.ParentID
}

func (in *AliasInput) parent(kind string) string {
	if kind == kindModule {
		return in.ModuleID
	}

	return in.ParentID
}

func (in *AliasInput) version(kind string) *int64 {
	if kind == kindModule {
		return in.ModuleVersion
	}

	return in.FlowVersion
}

// VersionSummary is the list shape of a flow version.
type VersionSummary struct {
	Stamp
	Arn                string `json:"Arn"`
	VersionDescription string `json:"VersionDescription,omitempty"`
	Version            int64  `json:"Version"`
}

// AliasSummary is the list shape of a flow alias.
type AliasSummary struct {
	LastModifiedTime   *awstime.EpochTime `json:"LastModifiedTime,omitempty"`
	LastModifiedRegion string             `json:"LastModifiedRegion,omitempty"`
	AliasDescription   string             `json:"AliasDescription,omitempty"`
	AliasID            string             `json:"AliasId"`
	AliasName          string             `json:"AliasName"`
	Arn                string             `json:"Arn"`
	Version            int64              `json:"Version"`
}

// AliasInfo is the described form of a flow alias; the parent ARN and ID keys depend on the kind.
type AliasInfo map[string]any

type versionOut struct {
	Arn     string `json:"-"`
	Version int64  `json:"Version"`
}

type aliasOut struct {
	Arn string `json:"-"`
	ID  string `json:"Id"`
}

func (b *Backend) createVersion(ctx context.Context, kind string, in *VersionInput) (*versionOut, error) {
	parent := in.parent(kind)
	sha := in.FlowContentSha256

	if kind == kindModule {
		sha = in.FlowModuleContentSha256
	}

	if err := checkDescription(in.Description); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateContactFlowVersion", in.InstanceID, func(inst string) (*versionOut, error) {
		p, err := b.versionedParent(kind, inst, parent)
		if err != nil {
			return nil, err
		}

		if p.status != flowPublished {
			return nil, apiErr(
				http.StatusBadRequest,
				"ContactFlowNotPublishedException",
				"%s %q is not published",
				kind,
				parent,
			)
		}

		if sha != "" && sha != p.sha {
			return nil, invalidRequestErr("the content checksum does not match the latest content")
		}

		if in.Version != nil && !b.flowVersions.table.Has(inst+"/"+versionKey(kind, parent, *in.Version)) {
			return nil, notFoundErr("Version", strconv.FormatInt(*in.Version, 10))
		}

		next := int64(1)
		if vs := b.versionsOf(inst, kind, parent); len(vs) > 0 {
			next = vs[len(vs)-1].Version + 1
		}

		b.snapshotVersion(ctx, inst, kind, parent, p, next, in.Description)
		*p.version = next

		return &versionOut{Arn: p.arn, Version: next}, nil
	})
}

func (b *Backend) deleteVersion(kind string, in *VersionInput) (*empty, error) {
	parent := in.parent(kind)

	raw := in.PathVersion
	if kind == kindModule {
		raw = in.ModulePathVersion
	}

	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, invalidParamErr("version must be an integer")
	}

	return writeInstance(b, "DeleteContactFlowVersion", in.InstanceID, func(inst string) (*empty, error) {
		if _, err = b.versionedParent(kind, inst, parent); err != nil {
			return nil, err
		}

		if !b.flowVersions.table.Has(inst + "/" + versionKey(kind, parent, v)) {
			return nil, notFoundErr("Version", raw)
		}

		for _, a := range b.aliasesOf(inst, kind, parent) {
			if a.Version == v {
				return nil, inUseErr("version %d is used by alias %q", v, a.Name)
			}
		}

		b.flowVersions.table.Delete(inst + "/" + versionKey(kind, parent, v))

		return &empty{}, nil
	})
}

func (b *Backend) listVersions(kind string, in *VersionInput) ([]VersionSummary, string, error) {
	parent := in.parent(kind)

	all, err := readInstance(b, "ListContactFlowVersions", in.InstanceID, func(inst string) ([]VersionSummary, error) {
		if _, perr := b.versionedParent(kind, inst, parent); perr != nil {
			return nil, perr
		}

		var out []VersionSummary

		for _, v := range b.versionsOf(inst, kind, parent) {
			s := VersionSummary{Arn: v.Arn, Version: v.Version, VersionDescription: v.Description}
			if kind == kindFlow {
				s.Stamp = v.Stamp
			}

			out = append(out, s)
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

func (b *Backend) createAlias(ctx context.Context, kind string, in *AliasInput) (*aliasOut, error) {
	parent := in.parent(kind)
	ver := in.version(kind)

	if err := firstError(
		checkName("AliasName", in.AliasName, maxNameLen),
		checkOptDescription(in.Description),
	); err != nil {
		return nil, err
	}

	if ver == nil {
		return nil, invalidParamErr("a version is required")
	}

	return writeInstance(b, "CreateContactFlowAlias", in.InstanceID, func(inst string) (*aliasOut, error) {
		p, err := b.versionedParent(kind, inst, parent)
		if err != nil {
			return nil, err
		}

		if !b.flowVersions.table.Has(inst + "/" + versionKey(kind, parent, *ver)) {
			return nil, notFoundErr("Version", strconv.FormatInt(*ver, 10))
		}

		if b.aliasNameTaken(inst, kind, parent, in.AliasName, "") {
			return nil, duplicateErr("Alias", in.AliasName)
		}

		rec := &flowAliasRec{
			Stamp: b.stamp(ctx), InstanceID: inst, Kind: kind, ParentID: parent, AliasID: newID(),
			Arn: p.arn + ":" + in.AliasName, Name: in.AliasName, Description: derefStr(in.Description), Version: *ver,
		}
		b.flowAliases.put(rec)

		return &aliasOut{Arn: p.arn, ID: rec.AliasID}, nil
	})
}

func checkOptDescription(d *string) error {
	if d == nil {
		return nil
	}

	return checkDescription(*d)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func (b *Backend) aliasNameTaken(inst, kind, parent, name, except string) bool {
	for _, a := range b.aliasesOf(inst, kind, parent) {
		if a.Name == name && a.AliasID != except {
			return true
		}
	}

	return false
}

func (b *Backend) aliasOf(inst, kind, parent, id string) (*flowAliasRec, error) {
	if _, err := b.versionedParent(kind, inst, parent); err != nil {
		return nil, err
	}

	a, err := b.flowAliases.get(inst, id)
	if err != nil || a.Kind != kind || a.ParentID != parent {
		return nil, notFoundErr("Alias", id)
	}

	return a, nil
}

func (b *Backend) describeAlias(kind string, in *AliasInput) (AliasInfo, error) {
	return readInstance(b, "DescribeContactFlowAlias", in.InstanceID, func(inst string) (AliasInfo, error) {
		parent := in.parent(kind)

		a, err := b.aliasOf(inst, kind, parent, in.AliasID)
		if err != nil {
			return nil, err
		}

		p, _ := b.versionedParent(kind, inst, parent)
		info := AliasInfo{
			"AliasId": a.AliasID, "Name": a.Name, "Version": a.Version, "Description": a.Description,
			"LastModifiedRegion": a.Stamp.LastModifiedRegion, "LastModifiedTime": a.Stamp.LastModifiedTime,
		}

		if kind == kindModule {
			info["ContactFlowModuleArn"], info["ContactFlowModuleId"] = p.arn, parent
		} else {
			info["ContactFlowArn"], info["ContactFlowId"] = p.arn, parent
		}

		return info, nil
	})
}

func (b *Backend) updateAlias(ctx context.Context, kind string, in *AliasInput) (*empty, error) {
	parent := in.parent(kind)

	if err := checkOptDescription(in.Description); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateContactFlowAlias", in.InstanceID, func(inst string) (*empty, error) {
		a, err := b.aliasOf(inst, kind, parent, in.AliasID)
		if err != nil {
			return nil, err
		}

		if ver := in.version(kind); ver != nil {
			if !b.flowVersions.table.Has(inst + "/" + versionKey(kind, parent, *ver)) {
				return nil, notFoundErr("Version", strconv.FormatInt(*ver, 10))
			}

			a.Version = *ver
		}

		if in.Name != nil {
			if err = checkName("Name", *in.Name, maxNameLen); err != nil {
				return nil, err
			}

			if b.aliasNameTaken(inst, kind, parent, *in.Name, a.AliasID) {
				return nil, duplicateErr("Alias", *in.Name)
			}

			a.Name = *in.Name
			p, _ := b.versionedParent(kind, inst, parent)
			a.Arn = p.arn + ":" + a.Name
		}

		setIf(&a.Description, in.Description)
		a.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}

func (b *Backend) deleteAlias(kind string, in *AliasInput) (*empty, error) {
	return writeInstance(b, "DeleteContactFlowAlias", in.InstanceID, func(inst string) (*empty, error) {
		a, err := b.aliasOf(inst, kind, in.parent(kind), in.AliasID)
		if err != nil {
			return nil, err
		}

		return nil, b.flowAliases.del(inst, a.AliasID)
	})
}

func (b *Backend) listAliases(kind string, in *AliasInput) ([]AliasSummary, string, error) {
	parent := in.parent(kind)

	all, err := readInstance(b, "ListContactFlowAliases", in.InstanceID, func(inst string) ([]AliasSummary, error) {
		if _, perr := b.versionedParent(kind, inst, parent); perr != nil {
			return nil, perr
		}

		var out []AliasSummary

		for _, a := range b.aliasesOf(inst, kind, parent) {
			s := AliasSummary{
				LastModifiedTime: a.Stamp.LastModifiedTime, AliasDescription: a.Description,
				AliasID: a.AliasID, AliasName: a.Name, Arn: a.Arn, Version: a.Version,
			}
			if kind == kindFlow {
				s.LastModifiedRegion = a.Stamp.LastModifiedRegion
			}

			out = append(out, s)
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

// snapshotInitial records version 1 of a flow or module created in the PUBLISHED status.
func (b *Backend) snapshotInitial(ctx context.Context, inst, kind, id, status string) {
	if status != flowPublished {
		return
	}

	if p, err := b.versionedParent(kind, inst, id); err == nil {
		b.snapshotVersion(ctx, inst, kind, id, p, 1, "")
	}
}
