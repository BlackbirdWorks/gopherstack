package ec2

// instanceTypeSpec is one static catalog entry backing DescribeInstanceTypes,
// DescribeInstanceTypeOfferings, and GetInstanceTypesFromInstanceRequirements.
// Every populated field is sourced from AWS's published instance-type
// documentation (cited per family block below) or the pinned aws-sdk-go-v2
// wire shape (services/ec2/PARITY.md, 2026-09-11 entry, carries the full
// source list). A member left at its zero value is omitted from the wire
// response by the handler rather than fabricated -- see PARITY.md for the
// list of InstanceTypeInfo members this catalog never populates for any
// entry (DedicatedHostsSupported, AutoRecoverySupported,
// HibernationSupported, EbsInfo, FpgaInfo, InferenceAcceleratorInfo,
// PlacementGroupInfo, NitroEnclavesSupport, NitroTpmSupport,
// SupportedBootModes, RebootMigrationSupport, MediaAcceleratorInfo,
// NeuronInfo, PhcSupport, NetworkCards/EfaInfo/bandwidth-weighting detail).
type instanceTypeSpec struct {
	gpu            *gpuSpec
	storage        *instanceStorageSpec
	hypervisor     string
	networkPerf    string
	arch           string
	memoryMiB      int64
	vcpus          int32
	maxENI         int32
	ipv4PerENI     int32
	threadsPerCore int32
	cores          int32
	currentGen     bool
	freeTier       bool
	burstable      bool
}

type instanceStorageSpec struct {
	diskType   string
	diskSizeGB int64
	totalGB    int64
	diskCount  int32
}

type gpuSpec struct {
	name          string
	manufacturer  string
	count         int32
	memoryMiBEach int32
}

// instanceTypeCatalog is the static instance-type attribute table. Populated
// families/sizes and their source page are documented at each block; numbers
// not sourced from a verified page are left as zero values on the struct
// (see instanceTypeSpec doc) rather than invented.
//
//nolint:mnd,gochecknoglobals // static sourced spec table, not magic numbers; package-level lookup data
var instanceTypeCatalog = map[string]instanceTypeSpec{
	// ---- T2 (burstable, previous generation, x86_64, Xen) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html (General purpose
	// instances -- T2 family table). Per-size network performance and
	// core/thread split are not broken out per size on this page for T2
	// (only "Variable"/burstable network is documented) -- left unverified.
	"t2.nano": {vcpus: 1, memoryMiB: 512, arch: archX8664, hypervisor: hypervisorXen, burstable: true},
	"t2.micro": {
		vcpus:      1,
		memoryMiB:  1024,
		arch:       archX8664,
		hypervisor: hypervisorXen,
		burstable:  true,
		freeTier:   true,
	},
	"t2.small":   {vcpus: 1, memoryMiB: 2048, arch: archX8664, hypervisor: hypervisorXen, burstable: true},
	"t2.medium":  {vcpus: 2, memoryMiB: 4096, arch: archX8664, hypervisor: hypervisorXen, burstable: true},
	"t2.large":   {vcpus: 2, memoryMiB: 8192, arch: archX8664, hypervisor: hypervisorXen, burstable: true},
	"t2.xlarge":  {vcpus: 4, memoryMiB: 16384, arch: archX8664, hypervisor: hypervisorXen, burstable: true},
	"t2.2xlarge": {vcpus: 8, memoryMiB: 32768, arch: archX8664, hypervisor: hypervisorXen, burstable: true},

	// ---- T3 (burstable, current generation, Intel x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html T3 table: all
	// sizes share a 5 Gbps burst cap, so AWS's networkPerformance string is
	// "Up to 5 Gigabit" across the family. T3 is 1 core/2 threads below
	// xlarge, matching its documented hyperthreaded Intel/AMD vCPU layout.
	"t3.nano": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      512,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3.micro": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      1024,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		freeTier:       true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3.small": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      2048,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3.medium": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      4096,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},

	// ---- T3a (burstable, current generation, AMD x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html T3a table: same
	// vCPU/memory/bandwidth shape as T3.
	"t3a.nano": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      512,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3a.micro": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      1024,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3a.small": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      2048,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3a.medium": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      4096,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3a.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3a.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t3a.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},

	// ---- T4g (burstable, current generation, AWS Graviton2 arm64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html T4g table.
	// Graviton has no SMT, so cores == vCPUs and threadsPerCore == 1.
	"t4g.nano": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      512,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t4g.micro": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      1024,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t4g.small": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      2048,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t4g.medium": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      4096,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t4g.large": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      8192,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t4g.xlarge": {
		vcpus:          4,
		cores:          4,
		threadsPerCore: 1,
		memoryMiB:      16384,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},
	"t4g.2xlarge": {
		vcpus:          8,
		cores:          8,
		threadsPerCore: 1,
		memoryMiB:      32768,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		burstable:      true,
		networkPerf:    netUpTo5Gigabit,
	},

	// ---- M5 (general purpose, previous generation, Intel x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html M5 table.
	"m5.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      65536,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5.8xlarge": {
		vcpus:          32,
		cores:          16,
		threadsPerCore: 2,
		memoryMiB:      131072,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    net10Gigabit,
	},
	"m5.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      196608,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    net12Gigabit,
	},

	// ---- M5a (general purpose, previous generation, AMD x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html M5a table.
	"m5a.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5a.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5a.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5a.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      65536,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5a.8xlarge": {
		vcpus:          32,
		cores:          16,
		threadsPerCore: 2,
		memoryMiB:      131072,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"m5a.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      196608,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    net10Gigabit,
	},

	// ---- M6i (general purpose, current generation, Intel x86_64 Ice Lake, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html M6i table.
	"m6i.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m6i.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m6i.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m6i.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      65536,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m6i.8xlarge": {
		vcpus:          32,
		cores:          16,
		threadsPerCore: 2,
		memoryMiB:      131072,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net12_5Gigabit,
	},
	"m6i.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      196608,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net18_75Gigabit,
	},

	// ---- M6g (general purpose, current generation, AWS Graviton2 arm64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html M6g table.
	"m6g.medium": {
		vcpus:          1,
		cores:          1,
		threadsPerCore: 1,
		memoryMiB:      4096,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"m6g.large": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      8192,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"m6g.xlarge": {
		vcpus:          4,
		cores:          4,
		threadsPerCore: 1,
		memoryMiB:      16384,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"m6g.2xlarge": {
		vcpus:          8,
		cores:          8,
		threadsPerCore: 1,
		memoryMiB:      32768,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"m6g.4xlarge": {
		vcpus:          16,
		cores:          16,
		threadsPerCore: 1,
		memoryMiB:      65536,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"m6g.8xlarge": {
		vcpus:          32,
		cores:          32,
		threadsPerCore: 1,
		memoryMiB:      131072,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net12Gigabit,
	},

	// ---- M7i (general purpose, current generation, Intel x86_64 Sapphire Rapids, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html M7i table.
	"m7i.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m7i.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m7i.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m7i.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      65536,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m7i.8xlarge": {
		vcpus:          32,
		cores:          16,
		threadsPerCore: 2,
		memoryMiB:      131072,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net12_5Gigabit,
	},
	"m7i.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      196608,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net18_75Gigabit,
	},

	// ---- M7g (general purpose, current generation, AWS Graviton3 arm64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/gp.html M7g table.
	"m7g.medium": {
		vcpus:          1,
		cores:          1,
		threadsPerCore: 1,
		memoryMiB:      4096,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m7g.large": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      8192,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m7g.xlarge": {
		vcpus:          4,
		cores:          4,
		threadsPerCore: 1,
		memoryMiB:      16384,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"m7g.2xlarge": {
		vcpus:          8,
		cores:          8,
		threadsPerCore: 1,
		memoryMiB:      32768,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo15Gigabit,
	},
	"m7g.4xlarge": {
		vcpus:          16,
		cores:          16,
		threadsPerCore: 1,
		memoryMiB:      65536,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo15Gigabit,
	},
	"m7g.8xlarge": {
		vcpus:          32,
		cores:          32,
		threadsPerCore: 1,
		memoryMiB:      131072,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    "15 Gigabit",
	},

	// ---- C5 (compute optimized, previous generation, Intel x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/co.html C5 table.
	"c5.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      4096,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"c5.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"c5.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"c5.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"c5.9xlarge": {
		vcpus:          36,
		cores:          18,
		threadsPerCore: 2,
		memoryMiB:      73728,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    net12Gigabit,
	},
	"c5.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      98304,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    net12Gigabit,
	},

	// ---- C6i (compute optimized, current generation, Intel x86_64 Ice Lake, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/co.html C6i table.
	"c6i.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      4096,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"c6i.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      8192,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"c6i.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"c6i.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"c6i.8xlarge": {
		vcpus:          32,
		cores:          16,
		threadsPerCore: 2,
		memoryMiB:      65536,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net12_5Gigabit,
	},
	"c6i.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      98304,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net18_75Gigabit,
	},

	// ---- C6g (compute optimized, current generation, AWS Graviton2 arm64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/co.html C6g table.
	"c6g.large": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      4096,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"c6g.xlarge": {
		vcpus:          4,
		cores:          4,
		threadsPerCore: 1,
		memoryMiB:      8192,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"c6g.2xlarge": {
		vcpus:          8,
		cores:          8,
		threadsPerCore: 1,
		memoryMiB:      16384,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"c6g.4xlarge": {
		vcpus:          16,
		cores:          16,
		threadsPerCore: 1,
		memoryMiB:      32768,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"c6g.8xlarge": {
		vcpus:          32,
		cores:          32,
		threadsPerCore: 1,
		memoryMiB:      65536,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net12Gigabit,
	},
	"c6g.12xlarge": {
		vcpus:          48,
		cores:          48,
		threadsPerCore: 1,
		memoryMiB:      98304,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    "20 Gigabit",
	},

	// ---- C7g (compute optimized, current generation, AWS Graviton3 arm64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/co.html C7g table.
	"c7g.large": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      4096,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"c7g.xlarge": {
		vcpus:          4,
		cores:          4,
		threadsPerCore: 1,
		memoryMiB:      8192,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"c7g.2xlarge": {
		vcpus:          8,
		cores:          8,
		threadsPerCore: 1,
		memoryMiB:      16384,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo15Gigabit,
	},
	"c7g.4xlarge": {
		vcpus:          16,
		cores:          16,
		threadsPerCore: 1,
		memoryMiB:      32768,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo15Gigabit,
	},
	"c7g.8xlarge": {
		vcpus:          32,
		cores:          32,
		threadsPerCore: 1,
		memoryMiB:      65536,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    "15 Gigabit",
	},
	"c7g.12xlarge": {
		vcpus:          48,
		cores:          48,
		threadsPerCore: 1,
		memoryMiB:      98304,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    "22.5 Gigabit",
	},

	// ---- R5 (memory optimized, previous generation, Intel x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/mo.html R5 table.
	"r5.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"r5.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"r5.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      65536,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"r5.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      131072,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    netUpTo10Gigabit,
	},
	"r5.8xlarge": {
		vcpus:          32,
		cores:          16,
		threadsPerCore: 2,
		memoryMiB:      262144,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    net10Gigabit,
	},
	"r5.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      393216,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		networkPerf:    net12Gigabit,
	},

	// ---- R6i (memory optimized, current generation, Intel x86_64 Ice Lake, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/mo.html R6i table.
	"r6i.large": {
		vcpus:          2,
		cores:          1,
		threadsPerCore: 2,
		memoryMiB:      16384,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"r6i.xlarge": {
		vcpus:          4,
		cores:          2,
		threadsPerCore: 2,
		memoryMiB:      32768,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"r6i.2xlarge": {
		vcpus:          8,
		cores:          4,
		threadsPerCore: 2,
		memoryMiB:      65536,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"r6i.4xlarge": {
		vcpus:          16,
		cores:          8,
		threadsPerCore: 2,
		memoryMiB:      131072,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo12_5Gigabit,
	},
	"r6i.8xlarge": {
		vcpus:          32,
		cores:          16,
		threadsPerCore: 2,
		memoryMiB:      262144,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net12_5Gigabit,
	},
	"r6i.12xlarge": {
		vcpus:          48,
		cores:          24,
		threadsPerCore: 2,
		memoryMiB:      393216,
		arch:           archX8664,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net18_75Gigabit,
	},

	// ---- R6g (memory optimized, current generation, AWS Graviton2 arm64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/mo.html R6g table.
	"r6g.medium": {
		vcpus:          1,
		cores:          1,
		threadsPerCore: 1,
		memoryMiB:      8192,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"r6g.large": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      16384,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"r6g.xlarge": {
		vcpus:          4,
		cores:          4,
		threadsPerCore: 1,
		memoryMiB:      32768,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"r6g.2xlarge": {
		vcpus:          8,
		cores:          8,
		threadsPerCore: 1,
		memoryMiB:      65536,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"r6g.4xlarge": {
		vcpus:          16,
		cores:          16,
		threadsPerCore: 1,
		memoryMiB:      131072,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    netUpTo10Gigabit,
	},
	"r6g.8xlarge": {
		vcpus:          32,
		cores:          32,
		threadsPerCore: 1,
		memoryMiB:      262144,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
		networkPerf:    net12Gigabit,
	},

	// ---- R7g (memory optimized, current generation, AWS Graviton3 arm64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/mo.html R7g table
	// (vCPU/memory only -- the fetched network-performance row for R7g
	// disagreed with the pattern the same page shows for M7g/C7g Graviton3
	// siblings, so networkPerf is left unverified here rather than risking
	// an invented number).
	"r7g.medium": {
		vcpus:          1,
		cores:          1,
		threadsPerCore: 1,
		memoryMiB:      8192,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
	},
	"r7g.large": {
		vcpus:          2,
		cores:          2,
		threadsPerCore: 1,
		memoryMiB:      16384,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
	},
	"r7g.xlarge": {
		vcpus:          4,
		cores:          4,
		threadsPerCore: 1,
		memoryMiB:      32768,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
	},
	"r7g.2xlarge": {
		vcpus:          8,
		cores:          8,
		threadsPerCore: 1,
		memoryMiB:      65536,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
	},
	"r7g.4xlarge": {
		vcpus:          16,
		cores:          16,
		threadsPerCore: 1,
		memoryMiB:      131072,
		arch:           archArm64,
		hypervisor:     hypervisorNitro,
		currentGen:     true,
	},

	// ---- I3 (storage optimized, current generation, Intel x86_64, Xen) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/so.html I3 table. I3
	// predates the Nitro hypervisor (non-metal sizes run on Xen).
	"i3.large": {
		vcpus: 2, cores: 1, threadsPerCore: 2, memoryMiB: 15616, arch: archX8664, hypervisor: hypervisorXen,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 3, ipv4PerENI: 10,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 475, diskType: diskTypeSSD, totalGB: 475},
	},
	"i3.xlarge": {
		vcpus: 4, cores: 2, threadsPerCore: 2, memoryMiB: 31232, arch: archX8664, hypervisor: hypervisorXen,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 4, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 950, diskType: diskTypeSSD, totalGB: 950},
	},
	"i3.2xlarge": {
		vcpus: 8, cores: 4, threadsPerCore: 2, memoryMiB: 62464, arch: archX8664, hypervisor: hypervisorXen,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 4, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 1900, diskType: diskTypeSSD, totalGB: 1900},
	},
	"i3.4xlarge": {
		vcpus: 16, cores: 8, threadsPerCore: 2, memoryMiB: 124928, arch: archX8664, hypervisor: hypervisorXen,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 8, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 2, diskSizeGB: 1900, diskType: diskTypeSSD, totalGB: 3800},
	},

	// ---- I4i (storage optimized, current generation, Intel x86_64 Ice Lake, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/so.html I4i table.
	"i4i.large": {
		vcpus: 2, cores: 1, threadsPerCore: 2, memoryMiB: 16384, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 3, ipv4PerENI: 10,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 468, diskType: diskTypeSSD, totalGB: 468},
	},
	"i4i.xlarge": {
		vcpus: 4, cores: 2, threadsPerCore: 2, memoryMiB: 32768, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 4, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 937, diskType: diskTypeSSD, totalGB: 937},
	},
	"i4i.2xlarge": {
		vcpus: 8, cores: 4, threadsPerCore: 2, memoryMiB: 65536, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: "Up to 12 Gigabit", maxENI: 4, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 1875, diskType: diskTypeSSD, totalGB: 1875},
	},
	"i4i.4xlarge": {
		vcpus: 16, cores: 8, threadsPerCore: 2, memoryMiB: 131072, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo25Gigabit, maxENI: 8, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 3750, diskType: diskTypeSSD, totalGB: 3750},
	},

	// ---- G4dn (accelerated computing, NVIDIA T4 GPU, current generation, Intel x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/ac.html G4dn table.
	"g4dn.xlarge": {
		vcpus: 4, cores: 2, threadsPerCore: 2, memoryMiB: 16384, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo25Gigabit, maxENI: 3, ipv4PerENI: 10,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 125, diskType: diskTypeSSD, totalGB: 125},
		gpu:     &gpuSpec{count: 1, name: "T4", manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 16384},
	},
	"g4dn.2xlarge": {
		vcpus: 8, cores: 4, threadsPerCore: 2, memoryMiB: 32768, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo25Gigabit, maxENI: 3, ipv4PerENI: 10,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 225, diskType: diskTypeSSD, totalGB: 225},
		gpu:     &gpuSpec{count: 1, name: "T4", manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 16384},
	},
	"g4dn.4xlarge": {
		vcpus: 16, cores: 8, threadsPerCore: 2, memoryMiB: 65536, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo25Gigabit, maxENI: 3, ipv4PerENI: 10,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 225, diskType: diskTypeSSD, totalGB: 225},
		gpu:     &gpuSpec{count: 1, name: "T4", manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 16384},
	},

	// ---- G5 (accelerated computing, NVIDIA A10G GPU, current generation, AMD x86_64, Nitro) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/ac.html G5 table.
	"g5.xlarge": {
		vcpus: 4, cores: 2, threadsPerCore: 2, memoryMiB: 16384, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 4, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 250, diskType: diskTypeSSD, totalGB: 250},
		gpu:     &gpuSpec{count: 1, name: gpuNameA10G, manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 22528},
	},
	"g5.2xlarge": {
		vcpus: 8, cores: 4, threadsPerCore: 2, memoryMiB: 32768, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo10Gigabit, maxENI: 4, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 450, diskType: diskTypeSSD, totalGB: 450},
		gpu:     &gpuSpec{count: 1, name: gpuNameA10G, manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 22528},
	},
	"g5.4xlarge": {
		vcpus: 16, cores: 8, threadsPerCore: 2, memoryMiB: 65536, arch: archX8664, hypervisor: hypervisorNitro,
		currentGen: true, networkPerf: netUpTo25Gigabit, maxENI: 8, ipv4PerENI: 15,
		storage: &instanceStorageSpec{diskCount: 1, diskSizeGB: 600, diskType: diskTypeSSD, totalGB: 600},
		gpu:     &gpuSpec{count: 1, name: gpuNameA10G, manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 22528},
	},

	// ---- P3 (accelerated computing, NVIDIA V100 GPU, previous generation, Intel x86_64, Xen) ----
	// docs.aws.amazon.com/ec2/latest/instancetypes/pg.html P3 table. P3
	// has no local instance storage ("Instance store not supported").
	"p3.2xlarge": {
		vcpus: 8, cores: 4, threadsPerCore: 2, memoryMiB: 62464, arch: archX8664, hypervisor: hypervisorXen,
		networkPerf: netUpTo10Gigabit, maxENI: 4, ipv4PerENI: 15,
		gpu: &gpuSpec{count: 1, name: gpuNameV100, manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 16384},
	},
	"p3.8xlarge": {
		vcpus: 32, cores: 16, threadsPerCore: 2, memoryMiB: 249856, arch: archX8664, hypervisor: hypervisorXen,
		networkPerf: net10Gigabit, maxENI: 8, ipv4PerENI: 30,
		gpu: &gpuSpec{count: 4, name: gpuNameV100, manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 16384},
	},
	"p3.16xlarge": {
		vcpus: 64, cores: 32, threadsPerCore: 2, memoryMiB: 499712, arch: archX8664, hypervisor: hypervisorXen,
		networkPerf: "25 Gigabit", maxENI: 8, ipv4PerENI: 30,
		gpu: &gpuSpec{count: 8, name: gpuNameV100, manufacturer: gpuManufacturerNVIDIA, memoryMiBEach: 16384},
	},
}

// Wire-value constants shared by the catalog and the DescribeInstanceTypes /
// GetInstanceTypesFromInstanceRequirements handlers. Values are the literal
// enum strings from aws-sdk-go-v2/service/ec2/types/enums.go (ArchitectureType,
// InstanceTypeHypervisor, DiskType).
const (
	// archX8664 is declared in store.go (shared with the AMI/instance
	// architecture wire code); reused here rather than redeclared.
	archArm64 = "arm64"

	hypervisorNitro = "nitro"
	hypervisorXen   = "xen"

	diskTypeSSD = "ssd"

	gpuManufacturerNVIDIA = "NVIDIA"

	netUpTo5Gigabit    = "Up to 5 Gigabit"
	netUpTo10Gigabit   = "Up to 10 Gigabit"
	netUpTo12_5Gigabit = "Up to 12.5 Gigabit"
	net12_5Gigabit     = "12.5 Gigabit"
	net18_75Gigabit    = "18.75 Gigabit"
	netUpTo15Gigabit   = "Up to 15 Gigabit"
	netUpTo25Gigabit   = "Up to 25 Gigabit"
	net10Gigabit       = "10 Gigabit"
	net12Gigabit       = "12 Gigabit"

	gpuNameA10G = "A10G"
	gpuNameV100 = "V100"
)
