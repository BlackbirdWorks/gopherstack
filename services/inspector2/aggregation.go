package inspector2

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

const (
	aggPageKey = "\x00pageKey"

	sortByAll               = "ALL"
	sortByCritical          = "CRITICAL"
	sortByHigh              = "HIGH"
	sortByAffectedImages    = "AFFECTED_IMAGES"
	sortByAffectedInstances = "AFFECTED_INSTANCES"
	sortByNetworkFindings   = "NETWORK_FINDINGS"

	yesValue = "YES"

	attrRepository   = "repository"
	attrFunctionName = "functionName"
	filterResourceID = "resourceIds"
	filterRepos      = "repositories"
	keyProjectNames  = "projectNames"
)

// aggBucket is one aggregation group: findings sharing an account and group key.
type aggBucket struct {
	counts    map[string]int64
	attrs     map[string]string
	tags      map[string]string
	resources map[string]struct{}
	account   string
	key       string
	list      []string
	exploit   int64
	fix       int64
	network   int64
}

// aggContribution is one group a finding contributes to.
type aggContribution struct {
	attrs    map[string]string
	tags     map[string]string
	key      string
	resource string
	list     []string
}

// aggSpec describes how one AggregationType groups, filters, sorts and renders findings.
type aggSpec struct {
	extract     func(f *Finding) []aggContribution
	render      func(bk *aggBucket) map[string]any
	filters     map[string]func(bk *aggBucket) string
	mapFilters  map[string]struct{}
	listFilters map[string]struct{}
	extraSorts  map[string]func(bk *aggBucket) int64
	member      string
	scalars     bool
}

func attr(name string) func(bk *aggBucket) string {
	return func(bk *aggBucket) string { return bk.attrs[name] }
}

func bucketKey(bk *aggBucket) string { return bk.key }

func resourceExtract(
	resourceType string,
	detailsOf func(r FindingResource) aggContribution,
) func(f *Finding) []aggContribution {
	return func(f *Finding) []aggContribution {
		var out []aggContribution

		for _, r := range f.Resources {
			if r.Type != resourceType || r.ID == "" {
				continue
			}

			var c aggContribution
			if detailsOf != nil {
				c = detailsOf(r)
			}

			c.key, c.resource = r.ID, r.ID
			out = append(out, c)
		}

		return out
	}
}

func packagesOf(f *Finding) []VulnerablePackage {
	if f.PackageVulnerabilityDetails == nil {
		return nil
	}

	return f.PackageVulnerabilityDetails.VulnerablePackages
}

func firstResourceID(f *Finding, resourceType string) (string, string) {
	for _, r := range f.Resources {
		if r.Type == resourceType {
			return r.ID, r.Repository
		}
	}

	return "", ""
}

func accountOnly(*Finding) []aggContribution { return []aggContribution{{}} }

func titleExtract(f *Finding) []aggContribution {
	if f.Title == "" {
		return nil
	}

	c := aggContribution{key: f.Title}
	if f.PackageVulnerabilityDetails != nil {
		c.attrs = map[string]string{"vulnerabilityId": f.PackageVulnerabilityDetails.VulnerabilityID}
	}

	return []aggContribution{c}
}

func packageExtract(f *Finding) []aggContribution {
	var out []aggContribution

	for _, p := range packagesOf(f) {
		if p.Name != "" {
			out = append(out, aggContribution{key: p.Name})
		}
	}

	return out
}

func imageLayerExtract(f *Finding) []aggContribution {
	imageID, repo := firstResourceID(f, findingResourceTypeECRContainerImg)

	var out []aggContribution

	for _, p := range packagesOf(f) {
		if p.SourceLayerHash == "" {
			continue
		}

		out = append(out, aggContribution{
			key:   p.SourceLayerHash + "\x00" + imageID,
			attrs: map[string]string{"layerHash": p.SourceLayerHash, keyResourceID: imageID, attrRepository: repo},
		})
	}

	return out
}

func lambdaLayerExtract(f *Finding) []aggContribution {
	fnID, _ := firstResourceID(f, findingResourceTypeLambdaFunction)
	fnName := ""

	for _, r := range f.Resources {
		if r.Type == findingResourceTypeLambdaFunction {
			fnName = r.FunctionName

			break
		}
	}

	var out []aggContribution

	for _, p := range packagesOf(f) {
		if p.SourceLambdaLayerArn == "" {
			continue
		}

		out = append(out, aggContribution{
			key:   p.SourceLambdaLayerArn + "\x00" + fnID,
			attrs: map[string]string{"layerArn": p.SourceLambdaLayerArn, keyResourceID: fnID, attrFunctionName: fnName},
		})
	}

	return out
}

func amiExtract(f *Finding) []aggContribution {
	var out []aggContribution

	for _, r := range f.Resources {
		if r.Type == findingResourceTypeEC2Instance && r.ImageID != "" {
			out = append(out, aggContribution{key: r.ImageID, resource: r.ID})
		}
	}

	return out
}

func repositoryExtract(f *Finding) []aggContribution {
	var images []string

	for _, r := range f.Resources {
		if r.Type == findingResourceTypeECRContainerImg {
			images = append(images, r.ID)
		}
	}

	var out []aggContribution

	for _, r := range f.Resources {
		if r.Type != findingResourceTypeECRRepository || r.ID == "" {
			continue
		}

		out = append(out, aggContribution{key: r.ID, resource: strings.Join(images, "\x00")})
	}

	return out
}

func sev(bk *aggBucket) map[string]any { return severityCountsWire(bk.counts) }

func baseRender(member string, fields func(bk *aggBucket) map[string]any) func(bk *aggBucket) map[string]any {
	return func(bk *aggBucket) map[string]any {
		m := fields(bk)
		m[keyAccountID] = bk.account
		m[keySeverityCounts] = sev(bk)

		return map[string]any{member: m}
	}
}

func aggSpecs() map[string]aggSpec {
	out := accountScopedSpecs()
	maps.Copy(out, resourceSpecs())
	maps.Copy(out, derivedSpecs())

	return out
}

func accountScopedSpecs() map[string]aggSpec {
	return map[string]aggSpec{
		aggregationTypeAccount: {
			member: "accountAggregation", extract: accountOnly, scalars: true,
			render: baseRender("accountAggregation", func(bk *aggBucket) map[string]any {
				return map[string]any{"exploitAvailableCount": bk.exploit, "fixAvailableCount": bk.fix}
			}),
		},
		aggregationTypeFindingType: {
			member: "findingTypeAggregation", extract: accountOnly, scalars: true,
			render: baseRender("findingTypeAggregation", func(bk *aggBucket) map[string]any {
				return map[string]any{"exploitAvailableCount": bk.exploit, "fixAvailableCount": bk.fix}
			}),
		},
		aggregationTypeTitle: {
			member: "titleAggregation", extract: titleExtract, scalars: true,
			filters: map[string]func(*aggBucket) string{
				"titles":           bucketKey,
				"vulnerabilityIds": attr("vulnerabilityId"),
			},
			render: baseRender("titleAggregation", func(bk *aggBucket) map[string]any {
				m := map[string]any{"title": bk.key}
				if v := bk.attrs["vulnerabilityId"]; v != "" {
					m["vulnerabilityId"] = v
				}

				return m
			}),
		},
	}
}

func resourceSpecs() map[string]aggSpec {
	return map[string]aggSpec{
		aggregationTypeRepository: {
			member: "repositoryAggregation", extract: repositoryExtract,
			filters: map[string]func(*aggBucket) string{filterRepos: bucketKey},
			extraSorts: map[string]func(*aggBucket) int64{
				sortByAffectedImages: func(bk *aggBucket) int64 { return int64(len(bk.resources)) },
			},
			render: baseRender("repositoryAggregation", func(bk *aggBucket) map[string]any {
				return map[string]any{attrRepository: bk.key, "affectedImages": int64(len(bk.resources))}
			}),
		},
		aggregationTypeAwsEc2Instance: {
			member: "ec2InstanceAggregation",
			extract: resourceExtract(findingResourceTypeEC2Instance, func(r FindingResource) aggContribution {
				return aggContribution{attrs: map[string]string{"ami": r.ImageID, "os": r.Platform}, tags: r.Tags}
			}),
			filters: map[string]func(*aggBucket) string{
				"instanceIds": bucketKey, "amis": attr("ami"), "operatingSystems": attr("os"),
			},
			mapFilters: map[string]struct{}{"instanceTags": {}},
			extraSorts: map[string]func(*aggBucket) int64{
				sortByNetworkFindings: func(bk *aggBucket) int64 { return bk.network },
			},
			render: baseRender("ec2InstanceAggregation", func(bk *aggBucket) map[string]any {
				m := map[string]any{"instanceId": bk.key, "networkFindings": bk.network}
				putNonEmpty(m, "ami", bk.attrs["ami"])
				putNonEmpty(m, "operatingSystem", bk.attrs["os"])

				if len(bk.tags) > 0 {
					m["instanceTags"] = bk.tags
				}

				return m
			}),
		},
		aggregationTypeAwsEcrContainer: {
			member: "awsEcrContainerAggregation",
			extract: resourceExtract(findingResourceTypeECRContainerImg, func(r FindingResource) aggContribution {
				return aggContribution{
					attrs: map[string]string{attrRepository: r.Repository, "arch": r.Architecture, "sha": r.ImageHash},
					list:  r.ImageTags,
				}
			}),
			filters: map[string]func(*aggBucket) string{
				filterResourceID: bucketKey, filterRepos: attr(attrRepository),
				"architectures": attr("arch"), "imageShas": attr("sha"),
			},
			listFilters: map[string]struct{}{"imageTags": {}},
			render: baseRender("awsEcrContainerAggregation", func(bk *aggBucket) map[string]any {
				m := map[string]any{keyResourceID: bk.key}
				putNonEmpty(m, attrRepository, bk.attrs[attrRepository])
				putNonEmpty(m, "architecture", bk.attrs["arch"])
				putNonEmpty(m, "imageSha", bk.attrs["sha"])

				if len(bk.list) > 0 {
					m["imageTags"] = bk.list
				}

				return m
			}),
		},
		aggregationTypeAwsLambda: {
			member: "lambdaFunctionAggregation",
			extract: resourceExtract(findingResourceTypeLambdaFunction, func(r FindingResource) aggContribution {
				return aggContribution{
					attrs: map[string]string{attrFunctionName: r.FunctionName, "runtime": r.Runtime},
					tags:  r.Tags,
				}
			}),
			filters: map[string]func(*aggBucket) string{
				filterResourceID: bucketKey, "functionNames": attr(attrFunctionName), "runtimes": attr("runtime"),
			},
			mapFilters: map[string]struct{}{"functionTags": {}},
			render: baseRender("lambdaFunctionAggregation", func(bk *aggBucket) map[string]any {
				m := map[string]any{keyResourceID: bk.key}
				putNonEmpty(m, attrFunctionName, bk.attrs[attrFunctionName])
				putNonEmpty(m, "runtime", bk.attrs["runtime"])

				if len(bk.tags) > 0 {
					m["lambdaTags"] = bk.tags
				}

				return m
			}),
		},
		aggregationTypeCodeRepository: {
			member: "codeRepositoryAggregation", extract: resourceExtract(findingResourceTypeCodeRepository, nil),
			filters: map[string]func(*aggBucket) string{keyProjectNames: bucketKey, filterResourceID: bucketKey},
			// projectNames is the only identifier Finding.Resources carries for a code repository.
			render: baseRender("codeRepositoryAggregation", func(bk *aggBucket) map[string]any {
				return map[string]any{keyProjectNames: bk.key}
			}),
		},
	}
}

func derivedSpecs() map[string]aggSpec {
	return map[string]aggSpec{
		aggregationTypePackage: {
			member: "packageAggregation", extract: packageExtract,
			filters: map[string]func(*aggBucket) string{"packageNames": bucketKey},
			render: baseRender("packageAggregation", func(bk *aggBucket) map[string]any {
				return map[string]any{"packageName": bk.key}
			}),
		},
		aggregationTypeImageLayer: {
			member: "imageLayerAggregation", extract: imageLayerExtract,
			filters: map[string]func(*aggBucket) string{
				"layerHashes": attr(
					"layerHash",
				), filterResourceID: attr(keyResourceID), filterRepos: attr(attrRepository),
			},
			render: baseRender("imageLayerAggregation", func(bk *aggBucket) map[string]any {
				m := map[string]any{"layerHash": bk.attrs["layerHash"]}
				putNonEmpty(m, keyResourceID, bk.attrs[keyResourceID])
				putNonEmpty(m, attrRepository, bk.attrs[attrRepository])

				return m
			}),
		},
		aggregationTypeLambdaLayer: {
			member: "lambdaLayerAggregation", extract: lambdaLayerExtract,
			filters: map[string]func(*aggBucket) string{
				"layerArns": attr(
					"layerArn",
				), filterResourceID: attr(keyResourceID), "functionNames": attr(attrFunctionName),
			},
			render: baseRender("lambdaLayerAggregation", func(bk *aggBucket) map[string]any {
				m := map[string]any{"layerArn": bk.attrs["layerArn"]}
				putNonEmpty(m, keyResourceID, bk.attrs[keyResourceID])
				putNonEmpty(m, attrFunctionName, bk.attrs[attrFunctionName])

				return m
			}),
		},
		aggregationTypeAmi: {
			member: "amiAggregation", extract: amiExtract,
			filters: map[string]func(*aggBucket) string{"amis": bucketKey},
			extraSorts: map[string]func(*aggBucket) int64{
				sortByAffectedInstances: func(bk *aggBucket) int64 { return int64(len(bk.resources)) },
			},
			render: baseRender("amiAggregation", func(bk *aggBucket) map[string]any {
				return map[string]any{"ami": bk.key, "affectedInstances": int64(len(bk.resources))}
			}),
		},
	}
}

func putNonEmpty(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}

// aggregationRequestMember returns the single union member of an aggregationRequest for the given type.
func aggregationRequestMember(req map[string]any, member string) map[string]any {
	m, _ := req[member].(map[string]any)

	return m
}

// aggStringFilters decodes a request StringFilter list.
func aggStringFilters(m map[string]any, field string) []stringFilter {
	return extractStringFilters(m, field)
}

func (b *InMemoryBackend) aggregate(spec aggSpec, req map[string]any, accounts []stringFilter) []*aggBucket {
	member := aggregationRequestMember(req, spec.member)

	var findingType, resourceType string
	if spec.scalars {
		findingType, _ = member["findingType"].(string)
		resourceType, _ = member["resourceType"].(string)
	}

	b.mu.RLock("ListFindingAggregations")
	defer b.mu.RUnlock()

	buckets := map[string]*aggBucket{}

	b.findings.Range(func(sf *storedFinding) bool {
		f := &sf.Finding
		if matchStringFilters(accounts, f.AccountID) &&
			(findingType == "" || f.Type == findingType) &&
			(resourceType == "" || hasResourceType(f, resourceType)) {
			addToBuckets(buckets, spec.extract(f), f)
		}

		return true
	})

	out := make([]*aggBucket, 0, len(buckets))

	for _, bk := range buckets {
		if bucketMatches(spec, member, bk) {
			out = append(out, bk)
		}
	}

	return out
}

// addToBuckets counts f once in every bucket it contributes to.
func addToBuckets(buckets map[string]*aggBucket, contributions []aggContribution, f *Finding) {
	counted := map[string]bool{}

	for _, c := range contributions {
		id := f.AccountID + "\x00" + c.key

		bk, ok := buckets[id]
		if !ok {
			bk = &aggBucket{
				account: f.AccountID, key: c.key, attrs: map[string]string{},
				counts: map[string]int64{}, resources: map[string]struct{}{},
			}
			buckets[id] = bk
		}

		bk.merge(c)

		if !counted[id] {
			counted[id] = true
			bk.count(f)
		}
	}
}

// merge folds a contribution's descriptive attributes and resources into the bucket.
func (bk *aggBucket) merge(c aggContribution) {
	for k, v := range c.attrs {
		if v != "" {
			bk.attrs[k] = v
		}
	}

	if len(c.tags) > 0 {
		bk.tags = c.tags
	}

	if len(c.list) > 0 {
		bk.list = c.list
	}

	for res := range strings.SplitSeq(c.resource, "\x00") {
		if res != "" {
			bk.resources[res] = struct{}{}
		}
	}
}

func hasResourceType(f *Finding, resourceType string) bool {
	return slices.ContainsFunc(f.Resources, func(r FindingResource) bool { return r.Type == resourceType })
}

func (bk *aggBucket) count(f *Finding) {
	bk.counts[f.Severity.Label]++

	if f.ExploitAvailable == yesValue {
		bk.exploit++
	}

	if f.FixAvailable == yesValue {
		bk.fix++
	}

	if f.Type == "NETWORK_REACHABILITY" {
		bk.network++
	}
}

func bucketMatches(spec aggSpec, member map[string]any, bk *aggBucket) bool {
	for field, value := range spec.filters {
		if !matchStringFilters(aggStringFilters(member, field), value(bk)) {
			return false
		}
	}

	for field := range spec.mapFilters {
		if !matchMapFilters(extractMapFilters(member, field), bk.tags) {
			return false
		}
	}

	for field := range spec.listFilters {
		if !matchAnyTag(aggStringFilters(member, field), bk.list) {
			return false
		}
	}

	return true
}

func (spec aggSpec) sortValue(sortBy string) (func(bk *aggBucket) int64, bool) {
	switch sortBy {
	case sortByAll:
		return func(bk *aggBucket) int64 { return severityTotal(bk.counts) }, true
	case sortByCritical:
		return func(bk *aggBucket) int64 { return bk.counts[severityCritical] }, true
	case sortByHigh:
		return func(bk *aggBucket) int64 { return bk.counts[severityHigh] }, true
	}

	fn, ok := spec.extraSorts[sortBy]

	return fn, ok
}

func severityTotal(counts map[string]int64) int64 {
	var total int64
	for _, n := range counts {
		total += n
	}

	return total
}

// ListFindingAggregations returns aggregated finding counts for every account.
func (b *InMemoryBackend) ListFindingAggregations(aggregationType string, req map[string]any) (map[string]any, error) {
	return b.ListFindingAggregationsForAccounts(aggregationType, req, nil)
}

// ListFindingAggregationsForAccounts groups findings by aggregationType, narrowed by the optional accountIds
// StringFilter list and the per-type aggregationRequest member (filters and SortBy/SortOrder).
// VM_INSTANCE, CONTAINER_IMAGE and SERVERLESS_FUNCTION are multi-cloud types with no findings to aggregate.
func (b *InMemoryBackend) ListFindingAggregationsForAccounts(
	aggregationType string, req map[string]any, accountIDs []stringFilter,
) (map[string]any, error) {
	if aggregationType == "" {
		aggregationType = aggregationTypeAccount
	}

	spec, ok := aggSpecs()[aggregationType]
	if !ok {
		return emptyFindingAggregations(aggregationType), nil
	}

	member := aggregationRequestMember(req, spec.member)
	sortBy, _ := member["sortBy"].(string)
	sortOrder, _ := member["sortOrder"].(string)

	if !validCisSortOrder(sortOrder) {
		return nil, fmt.Errorf("%w: sortOrder must be ASC or DESC, got %q", ErrValidation, sortOrder)
	}

	valueOf, sortOK := spec.sortValue(sortBy)
	if sortBy != "" && !sortOK {
		return nil, fmt.Errorf("%w: unsupported sortBy %q for %s", ErrValidation, sortBy, aggregationType)
	}

	buckets := b.aggregate(spec, req, accountIDs)
	if len(buckets) == 0 {
		return emptyFindingAggregations(aggregationType), nil
	}

	sign := 1
	if sortOrder == cisSortDesc {
		sign = -1
	}

	slices.SortFunc(buckets, func(x, y *aggBucket) int {
		if valueOf != nil {
			if c := sign * cmpInt64(valueOf(x), valueOf(y)); c != 0 {
				return c
			}
		}

		return strings.Compare(x.account+"\x00"+x.key, y.account+"\x00"+y.key)
	})

	responses := make([]map[string]any, 0, len(buckets))

	for _, bk := range buckets {
		row := spec.render(bk)
		row[aggPageKey] = bk.account + "|" + bk.key
		responses = append(responses, row)
	}

	return map[string]any{keyAggregationType: aggregationType, keyResponses: responses}, nil
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
