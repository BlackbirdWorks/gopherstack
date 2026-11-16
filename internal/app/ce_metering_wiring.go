package app

import (
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	budgetsbackend "github.com/blackbirdworks/gopherstack/services/budgets"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	pricingbackend "github.com/blackbirdworks/gopherstack/services/pricing"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	ceServiceEC2        = "Amazon Elastic Compute Cloud - Compute"
	ceServiceEC2Other   = "EC2 - Other"
	ceServiceS3         = "Amazon Simple Storage Service"
	ceServiceLambda     = "AWS Lambda"
	ceServiceDynamoDB   = "Amazon DynamoDB"
	priceEC2            = "AmazonEC2"
	priceS3             = "AmazonS3"
	priceLambda         = "AWSLambda"
	priceDynamoDB       = "AmazonDynamoDB"
	attrUsageType       = "usagetype"
	attrOperationName   = "operation"
	unitHours           = "Hrs"
	unitGBMonth         = "GB-Mo"
	unitRequests        = "Requests"
	bytesPerGB          = 1 << 30
	millisPerSecond     = 1000.0
	mbPerGB             = 1024.0
	lambdaARMArch       = "arm64"
	ddbReadUnits        = "ReadRequestUnits"
	ddbWriteUnits       = "WriteRequestUnits"
	ddbOperation        = "PayPerRequestThroughput"
	s3Tier1UsageType    = "Requests-Tier1"
	s3Tier2UsageType    = "Requests-Tier2"
	s3StandardClass     = "STANDARD"
	ebsUsagePrefix      = "EBS:VolumeUsage"
	ebsVolumeAvailable  = "available"
	ebsVolumeInUse      = "in-use"
	ec2RunInstancesOp   = "RunInstances"
	ec2RunningHoursName = "EC2: Running Hours"
)

// wireCostMetering connects the emulated services that have billing dimensions to the Cost Explorer ledger.
func wireCostMetering(byName map[string]service.Registerable, resourceLevel bool) {
	ceH, ok := byName["Ce"].(*cebackend.Handler)
	if !ok || ceH.Backend == nil {
		return
	}

	ce := ceH.Backend
	ce.SetResourceLevelData(resourceLevel)
	ce.SetPriceLookup(pricingbackend.OnDemandRate)

	if h, hok := byName["EC2"].(*ec2backend.Handler); hok {
		ce.AddHoldingSource(func() []cebackend.Holding { return ec2Holdings(h) })

		if b, bok := byName["Budgets"].(*budgetsbackend.Handler); bok {
			b.Backend.SetCommitmentSource(
				budgetsRICommitments{
					ce:           ce,
					reservations: func() []*ec2backend.ReservedInstance { return ec2Reservations(h) },
				},
			)
		}
	}

	if h, hok := byName["S3"].(*s3backend.S3Handler); hok {
		meterS3(ce, h)
	}

	if h, hok := byName["Lambda"].(*lambdabackend.Handler); hok {
		h.SetInvocationObserver(func(u lambdabackend.InvocationUsage) { recordLambda(ce, u) })
	}

	if h, hok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler); hok {
		if db, dbOK := h.Backend.(*ddbbackend.InMemoryDB); dbOK {
			db.EnableCapacityMetering()
			ce.AddUsageSource(func() []cebackend.Usage { return ddbUsage(db) })
		}
	}
}

func zoneRegion(az string) string {
	return strings.TrimRight(az, "abcdefghijklmnopqrstuvwxyz")
}

func ec2Holdings(h *ec2backend.Handler) []cebackend.Holding {
	var out []cebackend.Holding

	for _, bk := range h.RegionBackends() {
		mem, ok := bk.(*ec2backend.InMemoryBackend)
		if !ok {
			continue
		}

		out = append(out, instanceHoldings(mem)...)
		out = append(out, volumeHoldings(mem)...)
	}

	return out
}

func instanceHoldings(mem *ec2backend.InMemoryBackend) []cebackend.Holding {
	instances := mem.DescribeInstances(nil, "running")
	ids := make([]string, len(instances))

	for i, inst := range instances {
		ids[i] = inst.ID
	}

	tagged := mem.TagsForResources(ids)
	out := make([]cebackend.Holding, 0, len(instances))

	for _, inst := range instances {
		usage := "BoxUsage:" + inst.InstanceType
		az := inst.Placement.AvailabilityZone

		out = append(out, cebackend.Holding{Usage: cebackend.Usage{
			Service: ceServiceEC2, PriceService: priceEC2, UsageType: usage, Operation: ec2RunInstancesOp,
			Region: zoneRegion(az), AvailabilityZone: az, InstanceType: inst.InstanceType, ResourceID: inst.ID,
			ProductFamily: "Compute Instance", UsageTypeGroup: ec2RunningHoursName, Unit: unitHours,
			PriceMatch: map[string]string{attrUsageType: usage, attrOperationName: ec2RunInstancesOp},
			Tags:       tagged[inst.ID], Quantity: 1,
		}})
	}

	return out
}

func ebsUsageType(volumeType string) string {
	switch volumeType {
	case "standard":
		return ebsUsagePrefix
	case "io1", "io2":
		return ebsUsagePrefix + ".piops"
	default:
		return ebsUsagePrefix + "." + volumeType
	}
}

func volumeHoldings(mem *ec2backend.InMemoryBackend) []cebackend.Holding {
	volumes := mem.DescribeVolumes(nil)
	ids := make([]string, len(volumes))

	for i, v := range volumes {
		ids[i] = v.ID
	}

	tagged := mem.TagsForResources(ids)
	out := make([]cebackend.Holding, 0, len(volumes))

	for _, v := range volumes {
		if v.State != ebsVolumeAvailable && v.State != ebsVolumeInUse {
			continue
		}

		out = append(out, cebackend.Holding{Usage: cebackend.Usage{
			Service:          ceServiceEC2Other,
			PriceService:     priceEC2,
			UsageType:        ebsUsageType(v.VolumeType),
			Operation:        "CreateVolume",
			Region:           zoneRegion(v.AZ),
			AvailabilityZone: v.AZ,
			ResourceID:       v.ID,
			ProductFamily:    "Storage",
			Unit:             unitGBMonth,
			PriceMatch: map[string]string{
				"volumeApiName": v.VolumeType,
				attrUsageType:   "EBS:VolumeUsage." + v.VolumeType,
			},
			Tags:     tagged[v.ID],
			Quantity: cebackend.Hourly(float64(v.Size)),
		}})
	}

	return out
}

type s3ClassBilling struct{ usageType, operation, group string }

func s3StorageBilling(class string) s3ClassBilling {
	switch class {
	case "STANDARD_IA":
		return s3ClassBilling{
			"TimedStorage-SIA-ByteHrs",
			"StandardIAStorage",
			"S3: Storage - Standard Infrequent Access",
		}
	case "ONEZONE_IA":
		return s3ClassBilling{
			"TimedStorage-ZIA-ByteHrs",
			"OneZoneIAStorage",
			"S3: Storage - One Zone Infrequent Access",
		}
	case "GLACIER":
		return s3ClassBilling{"TimedStorage-GlacierByteHrs", "GlacierStorage", ""}
	case "INTELLIGENT_TIERING":
		return s3ClassBilling{"TimedStorage-INT-FA-ByteHrs", "IntelligentTieringFAStorage", ""}
	case "DEEP_ARCHIVE":
		return s3ClassBilling{"TimedStorage-GDA-ByteHrs", "DeepArchiveStorage", ""}
	case "REDUCED_REDUNDANCY":
		return s3ClassBilling{"TimedStorage-RRS-ByteHrs", "ReducedRedundancyStorage", ""}
	default:
		return s3ClassBilling{"TimedStorage-ByteHrs", "StandardStorage", "S3: Storage - Standard"}
	}
}

func meterS3(ce *cebackend.InMemoryBackend, h *s3backend.S3Handler) {
	if mem, ok := h.Backend.(*s3backend.InMemoryBackend); ok {
		ce.AddHoldingSource(func() []cebackend.Holding { return s3Holdings(mem) })

		m := &s3RequestMeter{mem: mem, lock: lockmetrics.New("ce-s3-requests"), resourceLevel: ce.ResourceLevelData}
		h.SetRequestObserver(m.observe)
		ce.AddUsageSource(m.drain)
	}
}

func s3Holdings(mem *s3backend.InMemoryBackend) []cebackend.Holding {
	held := mem.StorageHoldings()
	out := make([]cebackend.Holding, 0, len(held))

	for _, s := range held {
		bill := s3StorageBilling(s.StorageClass)

		out = append(out, cebackend.Holding{Usage: cebackend.Usage{
			Service: ceServiceS3, PriceService: priceS3, UsageType: bill.usageType, Operation: bill.operation,
			Region: s.Region, ResourceID: s.Bucket, ProductFamily: "Storage", UsageTypeGroup: bill.group,
			Unit: unitGBMonth, PriceMatch: map[string]string{attrUsageType: bill.usageType}, Tags: s.Tags,
			Quantity: cebackend.Hourly(float64(s.Bytes) / bytesPerGB),
		}})
	}

	return out
}

func recordLambda(ce *cebackend.InMemoryBackend, u lambdabackend.InvocationUsage) {
	base := cebackend.Usage{
		Service: ceServiceLambda, PriceService: priceLambda, Operation: "Invoke", Region: u.Region, Account: u.Account,
		ResourceID: u.FunctionARN, ProductFamily: "Serverless", Tags: u.Tags,
	}

	req := base
	req.UsageType, req.UsageTypeGroup, req.Unit, req.Quantity = "Request", "Lambda: Requests", unitRequests, 1
	req.PriceMatch = map[string]string{attrUsageType: "Request"}
	ce.RecordUsage(req)

	ms := float64(u.Duration.Truncate(time.Millisecond)) / float64(time.Millisecond)
	if u.Duration%time.Millisecond != 0 {
		ms++
	}

	if ms <= 0 {
		return
	}

	dur := base
	dur.UsageType, dur.UsageTypeGroup, dur.Unit = "Lambda-GB-Second", "Lambda: Duration", "Lambda-GB-Second"

	if u.Architecture == lambdaARMArch {
		dur.UsageType = "Lambda-GB-Second-ARM"
	}

	dur.PriceMatch = map[string]string{attrUsageType: dur.UsageType}
	dur.Quantity = float64(u.MemoryMB) / mbPerGB * ms / millisPerSecond
	ce.RecordUsage(dur)
}

func ddbUsage(db *ddbbackend.InMemoryDB) []cebackend.Usage {
	drained := db.DrainCapacityUsage()
	out := make([]cebackend.Usage, 0, len(drained))

	for _, c := range drained {
		for _, line := range []struct {
			unit string
			qty  float64
		}{{ddbReadUnits, c.ReadUnits}, {ddbWriteUnits, c.WriteUnits}} {
			if line.qty <= 0 {
				continue
			}

			out = append(out, cebackend.Usage{
				Service: ceServiceDynamoDB, PriceService: priceDynamoDB, UsageType: line.unit, Operation: ddbOperation,
				Region: c.Region, ResourceID: c.TableARN, ProductFamily: "Amazon DynamoDB PayPerRequest Throughput",
				Unit: line.unit, PriceMatch: map[string]string{attrUsageType: line.unit}, Tags: c.Tags,
				Quantity: line.qty,
			})
		}
	}

	return out
}

// budgetsRICommitments derives RI utilization and coverage from EC2 reservations and metered instance hours.
type budgetsRICommitments struct {
	ce           *cebackend.InMemoryBackend
	reservations func() []*ec2backend.ReservedInstance
}

func ec2Reservations(h *ec2backend.Handler) []*ec2backend.ReservedInstance {
	var out []*ec2backend.ReservedInstance

	for _, bk := range h.RegionBackends() {
		if mem, ok := bk.(*ec2backend.InMemoryBackend); ok {
			out = append(out, mem.DescribeReservedInstances(nil)...)
		}
	}

	return out
}

type riPoolKey struct{ instanceType, az string }

func (s budgetsRICommitments) Commitment(
	start, end time.Time, budgetType string, filters map[string][]string,
) (float64, bool) {
	pool, used := s.usedHours(start, end, filters)
	purchased, covered := 0.0, 0.0

	for _, ri := range s.reservations() {
		if ri.State != "active" || !riMatches(ri, filters) {
			continue
		}

		hours := overlapHours(ri.Start, ri.End, start, end) * float64(ri.InstanceCount)
		purchased += hours
		covered += takeFromPool(pool, ri, hours)
	}

	if purchased == 0 {
		return 0, false
	}

	if budgetType == "RI_COVERAGE" {
		if used == 0 {
			return 0, true
		}

		return covered / used * percent, true
	}

	return covered / purchased * percent, true
}

const percent = 100.0

func (s budgetsRICommitments) usedHours(
	start, end time.Time, filters map[string][]string,
) (map[riPoolKey]float64, float64) {
	pool := map[riPoolKey]float64{}
	total := 0.0

	for _, e := range s.ce.MeteredEntries(start.Format(dateLayout), end.AddDate(0, 0, 1).Format(dateLayout)) {
		if e.Service != ceServiceEC2 || !strings.HasPrefix(e.UsageType, "BoxUsage:") {
			continue
		}

		if !filterAllows(filters["InstanceType"], e.InstanceType) || !filterAllows(filters["AZ"], e.AvailabilityZone) {
			continue
		}

		pool[riPoolKey{e.InstanceType, e.AvailabilityZone}] += e.UsageQuantity
		total += e.UsageQuantity
	}

	return pool, total
}

func filterAllows(allowed []string, v string) bool {
	return len(allowed) == 0 || slices.Contains(allowed, v)
}

func riMatches(ri *ec2backend.ReservedInstance, filters map[string][]string) bool {
	return filterAllows(filters["InstanceType"], ri.InstanceType) && filterAllows(filters["AZ"], ri.AvailabilityZone)
}

func overlapHours(riStart, riEnd, start, end time.Time) float64 {
	from, to := riStart, riEnd

	if start.After(from) {
		from = start
	}

	if end.Before(to) {
		to = end
	}

	if !to.After(from) {
		return 0
	}

	return to.Sub(from).Hours()
}

// takeFromPool consumes up to want instance hours of the RI's type from the usage pool: its own zone for a zonal RI,
// any zone for a regional one.
func takeFromPool(pool map[riPoolKey]float64, ri *ec2backend.ReservedInstance, want float64) float64 {
	keys := make([]riPoolKey, 0, len(pool))

	for k := range pool {
		if k.instanceType == ri.InstanceType && (ri.AvailabilityZone == "" || k.az == ri.AvailabilityZone) {
			keys = append(keys, k)
		}
	}

	slices.SortFunc(keys, func(a, b riPoolKey) int { return strings.Compare(a.az, b.az) })

	taken := 0.0

	for _, k := range keys {
		n := min(want-taken, pool[k])
		pool[k] -= n
		taken += n
	}

	return taken
}
