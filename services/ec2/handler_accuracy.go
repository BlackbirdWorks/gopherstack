package ec2

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// parseRunInstancesCounts validates and returns MinCount and MaxCount from RunInstances params.
// MinCount defaults to 1 when absent. MaxCount defaults to MinCount when absent.
func parseRunInstancesCounts(vals url.Values) (int, int, error) {
	minCnt := 1
	if v := vals.Get("MinCount"); v != "" {
		if _, scanErr := fmt.Sscan(v, &minCnt); scanErr != nil || minCnt < 1 {
			return 0, 0, fmt.Errorf("%w: MinCount must be a positive integer", ErrInvalidParameter)
		}
	}

	maxCnt := minCnt
	if v := vals.Get("MaxCount"); v != "" {
		if _, scanErr := fmt.Sscan(v, &maxCnt); scanErr != nil || maxCnt < 1 {
			return 0, 0, fmt.Errorf("%w: MaxCount must be a positive integer", ErrInvalidParameter)
		}
	}

	if maxCnt < minCnt {
		return 0, 0, fmt.Errorf("%w: MaxCount must be greater than or equal to MinCount", ErrInvalidParameter)
	}

	return minCnt, maxCnt, nil
}

// validateSecurityGroupIDs parses SecurityGroupId.N from vals and verifies each exists.
// Returns an error if any ID is not found.
func (h *Handler) validateSecurityGroupIDs(vals url.Values) ([]string, error) {
	sgIDs := parseMemberList(vals, "SecurityGroupId")
	if len(sgIDs) == 0 {
		return nil, nil
	}

	existing := h.Backend.DescribeSecurityGroups(sgIDs)
	if len(existing) != len(sgIDs) {
		return nil, fmt.Errorf("%w: one or more SecurityGroupId values not found", ErrSecurityGroupNotFound)
	}

	return sgIDs, nil
}

// parseEC2Filters parses Filter.N.Name / Filter.N.Value.M from EC2 form values.
// Returns a map of filter name → list of accepted values (OR semantics per AWS).
func parseEC2Filters(vals url.Values) map[string][]string {
	filters := make(map[string][]string)

	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}

		var values []string
		for j := 1; ; j++ {
			v := vals.Get(fmt.Sprintf("Filter.%d.Value.%d", i, j))
			if v == "" {
				break
			}

			values = append(values, v)
		}

		if len(values) > 0 {
			filters[name] = values
		}
	}

	return filters
}

// applyInstanceFilters returns only instances matching all filters (AND across filters,
// OR within each filter's values). Supports the following filter names:
//
//   - instance-state-name (already handled by backend but re-applied for multi-value)
//   - image-id
//   - vpc-id
//   - subnet-id
//   - instance-type
//   - key-name
//   - private-ip-address
//   - ip-address (public IP)
//   - tag:<key>
func applyInstanceFilters(instances []*Instance, filters map[string][]string, b Backend) []*Instance {
	if len(filters) == 0 {
		return instances
	}

	out := instances[:0:0]

instanceLoop:
	for _, inst := range instances {
		for name, values := range filters {
			if !instanceMatchesFilter(inst, name, values, b) {
				continue instanceLoop
			}
		}

		out = append(out, inst)
	}

	return out
}

// instanceMatchesFilter returns true if the instance matches any value in the filter.
func instanceMatchesFilter(inst *Instance, filterName string, values []string, b Backend) bool {
	switch filterName {
	case "instance-state-name":
		return anyEqual(inst.State.Name, values)
	case "image-id":
		return anyEqual(inst.ImageID, values)
	case filterKeyVPCID:
		return anyEqual(inst.VPCID, values)
	case filterKeySubnetID:
		return anyEqual(inst.SubnetID, values)
	case "instance-type":
		return anyEqual(inst.InstanceType, values)
	case "key-name":
		return anyEqual(inst.KeyName, values)
	case "private-ip-address":
		return anyEqual(inst.PrivateIP, values)
	case "ip-address":
		return anyEqual(inst.PublicIPAddress, values)
	default:
		if tagKey, ok := strings.CutPrefix(filterName, "tag:"); ok {
			tags := b.TagsForResource(inst.ID)
			tagVal, exists := tags[tagKey]

			if !exists {
				return false
			}

			return slices.Contains(values, tagVal)
		}
	}

	// Unknown filters: pass through (lenient, per common mock behaviour).
	return true
}

// anyEqual returns true if target equals any element in vals.
func anyEqual(target string, vals []string) bool {
	return slices.Contains(vals, target)
}

// applySecurityGroupFilters filters security groups by named EC2 filter values.
// Supported filter names: vpc-id, group-name, group-id.
func applySecurityGroupFilters(groups []*SecurityGroup, filters map[string][]string) []*SecurityGroup {
	if len(filters) == 0 {
		return groups
	}

	out := groups[:0:0]

groupLoop:
	for _, sg := range groups {
		for name, values := range filters {
			if !sgMatchesFilter(sg, name, values) {
				continue groupLoop
			}
		}

		out = append(out, sg)
	}

	return out
}

// sgMatchesFilter returns true if the security group matches any value in the filter.
func sgMatchesFilter(sg *SecurityGroup, filterName string, values []string) bool {
	switch filterName {
	case filterKeyVPCID:
		return anyEqual(sg.VPCID, values)
	case "group-name":
		return anyEqual(sg.Name, values)
	case "group-id":
		return anyEqual(sg.ID, values)
	}

	// Unknown filters: pass through (lenient).
	return true
}
