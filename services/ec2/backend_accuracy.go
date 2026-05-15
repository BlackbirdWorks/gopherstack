package ec2

import (
	"fmt"
	"hash/fnv"
	"math"
	"time"
)

// SpotPriceRecord represents a single spot price history data point.
type SpotPriceRecord struct {
	Timestamp          time.Time
	InstanceType       string
	AvailabilityZone   string
	ProductDescription string
	SpotPrice          string
}

// Instance attribute names accepted by ModifyInstanceAttribute / SetInstanceAttribute.
// Mirror AWS EC2 attribute naming (lowerCamelCase).
const (
	attrInstanceType                      = "instanceType"
	attrKernel                            = "kernel"
	attrRamdisk                           = "ramdisk"
	attrUserData                          = "userData"
	attrEnaSupport                        = "enaSupport"
	attrSriovNetSupport                   = "sriovNetSupport"
	attrDisableAPIStop                    = "disableApiStop"
	attrDisableAPITermination             = "disableApiTermination"
	attrEBSOptimized                      = "ebsOptimized"
	attrInstanceInitiatedShutdownBehavior = "instanceInitiatedShutdownBehavior"

	instanceTypeT3Micro = "t3.micro"
	instanceTypeT3Small = "t3.small"
	instanceTypeC5XL    = "c5.xlarge"
)

// stoppedRequiredAttrs lists attributes that require a stopped instance when
// set via ModifyInstanceAttribute (not RunInstances initial setup).
//
//nolint:gochecknoglobals // lookup set
var stoppedRequiredAttrs = map[string]bool{
	attrInstanceType: true,
	attrKernel:       true,
	attrRamdisk:      true,
}

// SetInstanceAttribute persists a modifiable attribute on an instance.
// Attributes requiring stopped state (instanceType, userData, etc.) reject
// requests against running instances with ErrInvalidInstanceState.
func (b *InMemoryBackend) SetInstanceAttribute(instanceID, attribute, value string) error {
	b.mu.Lock("SetInstanceAttribute")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if stoppedRequiredAttrs[attribute] && inst.State != StateStopped {
		return fmt.Errorf("%w: instance %s must be in the stopped state to modify %s",
			ErrInvalidInstanceState, instanceID, attribute)
	}

	switch attribute {
	case attrUserData:
		// AWS stores userData base64-encoded; accept both encoded and raw.
		inst.UserData = value
	case attrInstanceType:
		inst.InstanceType = value
	case attrEnaSupport:
		inst.EnaSupport = value == ec2BooleanTrue
	case attrSriovNetSupport:
		inst.SriovNetSupport = value
	case attrDisableAPITermination, attrDisableAPIStop, attrEBSOptimized,
		"sourceDestCheck", attrInstanceInitiatedShutdownBehavior,
		"groupSet", "blockDeviceMapping":
		// accepted but not modelled beyond acknowledgment
	default:
		return fmt.Errorf("%w: unsupported attribute %q", ErrInvalidParameter, attribute)
	}

	return nil
}

// SetVolumeEncryption marks a volume as encrypted and records its KMS key ID.
// If encrypted is false the KMS key ID is cleared. If encrypted is true and
// kmsKeyID is empty an AWS-managed key ("aws/ebs") is implied.
func (b *InMemoryBackend) SetVolumeEncryption(
	volumeID string,
	encrypted bool,
	kmsKeyID string,
) error {
	b.mu.Lock("SetVolumeEncryption")
	defer b.mu.Unlock()

	vol, ok := b.volumes[volumeID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, volumeID)
	}

	vol.Encrypted = encrypted

	if encrypted {
		if kmsKeyID == "" {
			kmsKeyID = "alias/aws/ebs"
		}

		vol.KmsKeyId = kmsKeyID
	} else {
		vol.KmsKeyId = ""
	}

	return nil
}

// spotPriceBaseTable holds per-instance-type baseline prices in USD/hr.
// Values are approximate AWS on-demand prices used as a seed for spot history.
//
//nolint:gochecknoglobals,mnd // lookup table of AWS published prices
var spotPriceBaseTable = map[string]float64{
	"t2.micro":                   0.0116,
	"t2.small":                   0.023,
	"t2.medium":                  0.0464,
	"t2.large":                   0.0928,
	instanceTypeT3Micro:          0.0104,
	instanceTypeT3Small:          0.0208,
	"t3.medium":                  0.0416,
	"t3.large":                   0.0832,
	"t3.xlarge":                  0.1664,
	"t3.2xlarge":                 0.3328,
	spotFleetDefaultInstanceType: 0.096,
	"m5.xlarge":                  0.192,
	"m5.2xlarge":                 0.384,
	"m5.4xlarge":                 0.768,
	"c5.large":                   0.085,
	instanceTypeC5XL:             0.17,
	"c5.2xlarge":                 0.34,
	"r5.large":                   0.126,
	"r5.xlarge":                  0.252,
	"r5.2xlarge":                 0.504,
	"p3.2xlarge":                 3.06,
	"p3.8xlarge":                 12.24,
	"g4dn.xlarge":                0.526,
}

// deterministicSpotPrice returns a stable spot price for (instanceType, az, product)
// that varies predictably without randomness, keeping tests deterministic.
// Price is ~30–70% of the on-demand base.
func deterministicSpotPrice(instanceType, az, product string) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(instanceType + "|" + az + "|" + product))
	ratio := 0.30 + float64(h.Sum32()%1000)/2500.0 // 0.30 – 0.70
	base, ok := spotPriceBaseTable[instanceType]

	if !ok {
		base = 0.10
	}

	// Round to 4 decimal places for realism.
	return math.Round(base*ratio*10000) / 10000
}

// GenerateSpotPriceHistory produces a deterministic slice of spot price records
// for the given filters. startTime defaults to 24 h ago if zero.
// Each (instanceType, az, product) combination gets one record per hour in the window.
func GenerateSpotPriceHistory(
	instanceTypes, azs, products []string,
	startTime time.Time,
	region string,
) []SpotPriceRecord {
	if startTime.IsZero() {
		startTime = time.Now().UTC().Add(-24 * time.Hour)
	}

	endTime := time.Now().UTC()

	if len(instanceTypes) == 0 {
		instanceTypes = []string{
			instanceTypeT3Micro,
			instanceTypeT3Small,
			spotFleetDefaultInstanceType,
			instanceTypeC5XL,
		}
	}

	if len(azs) == 0 {
		azs = []string{region + "a", region + "b", region + "c"}
	}

	if len(products) == 0 {
		products = []string{"Linux/UNIX"}
	}

	var records []SpotPriceRecord

	for _, it := range instanceTypes {
		for _, az := range azs {
			for _, prod := range products {
				price := deterministicSpotPrice(it, az, prod)
				// One price point per 6-hour bucket in the window.
				for ts := startTime; ts.Before(endTime); ts = ts.Add(6 * time.Hour) {
					records = append(records, SpotPriceRecord{
						InstanceType:       it,
						AvailabilityZone:   az,
						ProductDescription: prod,
						SpotPrice:          fmt.Sprintf("%.4f", price),
						Timestamp:          ts,
					})
				}
			}
		}
	}

	return records
}
