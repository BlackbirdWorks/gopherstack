package cloudformation

import (
	"slices"
	"strings"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

// connectHoursExtras are the override and parent/child properties of AWS::Connect::HoursOfOperation.
type connectHoursExtras struct {
	overrides []cfnHoursOverride
	parents   []string
	children  []string
}

type cfnHoursOverride struct {
	RecurrenceConfig    *connectbackend.RecurrenceConfig
	OverrideName        string
	OverrideDescription string
	OverrideType        string
	EffectiveFrom       string
	EffectiveTill       string
	OverrideConfig      []connectbackend.OverrideConfig
}

type cfnHoursIdentifier struct {
	ID string `json:"Id"`
}

func (c *connectCall) hoursExtras() (*connectHoursExtras, error) {
	overrides, err := connectList[cfnHoursOverride](c, "HoursOfOperationOverrides")
	if err != nil {
		return nil, err
	}

	ids := func(key string) ([]string, error) {
		refs, lerr := connectList[cfnHoursIdentifier](c, key)
		out := make([]string, 0, len(refs))

		for _, r := range refs {
			out = append(out, lastPathSegment(r.ID))
		}

		return out, lerr
	}

	parents, err := ids("ParentHoursOfOperations")
	if err != nil {
		return nil, err
	}

	children, err := ids("ChildHoursOfOperations")
	if err != nil {
		return nil, err
	}

	return &connectHoursExtras{overrides: overrides, parents: parents, children: children}, nil
}

func optionalString(s *string) *string {
	if *s == "" {
		return nil
	}

	return s
}

// syncHoursExtras makes the schedule's overrides, parents and children equal the template's.
func (c *connectCall) syncHoursExtras(inst, id string, ex *connectHoursExtras) error {
	if err := c.syncHoursOverrides(inst, id, ex.overrides); err != nil {
		return err
	}

	if err := c.syncHoursParents(inst, id, ex.parents); err != nil {
		return err
	}

	return c.syncHoursChildren(inst, id, ex.children)
}

func (c *connectCall) syncHoursOverrides(inst, id string, want []cfnHoursOverride) error {
	var have []connectbackend.HoursOfOperationOverride

	for token := ""; ; {
		out, err := c.be.ListHoursOfOperationOverrides(inst, id, token, 0)
		if err != nil {
			return err
		}

		have = append(have, out.HoursOfOperationOverrideList...)

		if out.NextToken == "" {
			break
		}

		token = out.NextToken
	}

	for _, h := range have {
		if slices.ContainsFunc(want, func(w cfnHoursOverride) bool { return w.OverrideName == h.Name }) {
			continue
		}

		if _, err := c.be.DeleteHoursOfOperationOverride(inst, id, h.HoursOfOperationOverrideID); err != nil {
			return err
		}
	}

	for _, w := range want {
		desc, typ := optionalString(&w.OverrideDescription), optionalString(&w.OverrideType)

		i := slices.IndexFunc(
			have,
			func(h connectbackend.HoursOfOperationOverride) bool { return h.Name == w.OverrideName },
		)
		if i < 0 {
			create := connectbackend.CreateHoursOfOperationOverrideInput{}
			create.InstanceID, create.HoursID = inst, id
			create.Name, create.Description, create.OverrideType = &w.OverrideName, desc, typ
			create.EffectiveFrom, create.EffectiveTill = &w.EffectiveFrom, &w.EffectiveTill
			create.Config, create.RecurrenceConfig = w.OverrideConfig, w.RecurrenceConfig

			if _, err := c.be.CreateHoursOfOperationOverride(c.ctx, &create); err != nil {
				return err
			}

			continue
		}

		upd := connectbackend.UpdateHoursOfOperationOverrideInput{OverrideID: have[i].HoursOfOperationOverrideID}
		upd.InstanceID, upd.HoursID = inst, id
		upd.Name, upd.Description, upd.OverrideType = &w.OverrideName, desc, typ
		upd.EffectiveFrom, upd.EffectiveTill = &w.EffectiveFrom, &w.EffectiveTill
		upd.Config, upd.RecurrenceConfig = w.OverrideConfig, w.RecurrenceConfig

		if _, err := c.be.UpdateHoursOfOperationOverride(c.ctx, &upd); err != nil {
			return err
		}
	}

	return nil
}

func hoursLink(inst, hoursID string, parents []string) *connectbackend.ParentHoursInput {
	in := connectbackend.ParentHoursInput{ParentHoursOfOperationIDs: parents}
	in.InstanceID, in.HoursID = inst, hoursID

	for _, p := range parents {
		in.ParentHoursOfOperationConfigs = append(
			in.ParentHoursOfOperationConfigs, connectbackend.ParentHoursConfig{HoursOfOperationID: p},
		)
	}

	return &in
}

const (
	hoursParentsStash  = "DeclaredParents"
	hoursChildrenStash = "DeclaredChildren"
)

// declaredLinks returns the ids this resource declared on its previous apply and records want for the next one.
func (c *connectCall) declaredLinks(key string, want []string) []string {
	k := c.logicalID + "/" + key
	prev := strings.Fields(c.phys[k])

	c.phys[k] = strings.Join(want, " ")

	return prev
}

func (c *connectCall) syncHoursParents(inst, id string, want []string) error {
	cur, err := c.be.DescribeHoursOfOperation(inst, id)
	if err != nil {
		return err
	}

	have := make([]string, 0, len(cur.ParentHoursOfOperations))
	for _, p := range cur.ParentHoursOfOperations {
		have = append(have, p.ID)
	}

	prev := c.declaredLinks(hoursParentsStash, want)

	return c.relinkHours(have, prev, want, func(ids []string, add bool) error {
		in := hoursLink(inst, id, ids)
		if add {
			_, err = c.be.AssociateHoursOfOperations(c.ctx, in)
		} else {
			_, err = c.be.DisassociateHoursOfOperations(c.ctx, in)
		}

		return err
	})
}

func (c *connectCall) syncHoursChildren(inst, id string, want []string) error {
	var have []string

	for token := ""; ; {
		out, err := c.be.ListChildHoursOfOperations(inst, id, token, 0)
		if err != nil {
			return err
		}

		for _, ch := range out.ChildHoursOfOperationsSummaryList {
			have = append(have, ch.ID)
		}

		if out.NextToken == "" {
			break
		}

		token = out.NextToken
	}

	prev := c.declaredLinks(hoursChildrenStash, want)

	return c.relinkHours(have, prev, want, func(ids []string, add bool) error {
		for _, child := range ids {
			in := hoursLink(inst, child, []string{id})

			var err error
			if add {
				_, err = c.be.AssociateHoursOfOperations(c.ctx, in)
			} else {
				_, err = c.be.DisassociateHoursOfOperations(c.ctx, in)
			}

			if err != nil {
				return err
			}
		}

		return nil
	})
}

// relinkHours adds the wanted links and removes only links this resource declared before; links another
// resource of the stack declares on its own side are left alone.
func (c *connectCall) relinkHours(have, prev, want []string, apply func(ids []string, add bool) error) error {
	remove := slices.DeleteFunc(slices.Clone(have), func(id string) bool {
		return slices.Contains(want, id) || !slices.Contains(prev, id)
	})
	add := slices.DeleteFunc(slices.Clone(want), func(id string) bool { return slices.Contains(have, id) })

	if len(remove) > 0 {
		if err := apply(remove, false); err != nil {
			return err
		}
	}

	if len(add) > 0 {
		return apply(add, true)
	}

	return nil
}
