// Command xmlitemwrap finds a specific, mechanically-detectable AWS
// query/XML wire-shape bug: a plain string list emitted with structure
// wrapped around each element instead of the real flat
// <someSet><item>value</item></someSet> shape.
//
// gopherstack-6flj's hand sweep of ec2 (the largest query/XML service in
// this repo) found this same mistake five separate times (commits
// 3337c961d, b430921d9): a Go field declared as a slice of a struct whose
// only real member is itself tagged `xml:"item"` or `xml:"item,omitempty"`,
// rather than a slice of the scalar the SDK actually deserializes. ec2 is
// EC2-Query (`awsEc2query_`), which names its repeated list element "item";
// the classic AWS Query protocol (`awsAwsquery_`, 14 more services per
// services/_PROTOCOLS.md -- rds, sns, iam, autoscaling, cloudformation,
// ...) names the same repeated element "member" instead (confirmed against
// sns@v1.42.4 and rds@v1.124.1's deserializers.go, both switching on
// strings.EqualFold("member", t.Name.Local)) -- this tool treats "item" and
// "member" as equally valid sentinel names, since the same mistake is
// equally possible under either convention. Two concrete shapes recur:
//
//   - DOUBLE-WRAP: the slice field's own tag is a sentinel name (or
//     "...>"+sentinel) and its element struct's single member is ALSO
//     tagged with a sentinel name -- `<item><item>value</item></item>` on
//     the wire. This decodes to a real aws-sdk-go-v2 client as
//     "deserialization failed ... expected value for item element, got
//     xml.StartElement" -- a hard failure, not a silent drop.
//   - NAMED-CHILD: the slice field's own tag is a sentinel name but its
//     element struct's single member is tagged with some OTHER name --
//     `<item><instanceId>i-123</instanceId></item>` instead of plain
//     `<item>i-123</item>`. Same hard decode failure.
//
// Both shapes are structurally identical to "declare the wrapper one level
// too deep." A list-of-object shape where the element struct has more than
// one real member is a different, often genuinely correct, AWS shape (e.g.
// TagSet's Key/Value pairs) and is never flagged.
//
// CONFIDENCE. A double-wrap hit is always reported CONFIDENT: item-in-item
// is never a real AWS shape -- no query/XML deserializer in this repo's
// pinned SDKs ever nests a literal <item> under <item>.
//
// A named-child hit is ALWAYS reported NEEDS REVIEW, never confident. A
// "Set"/"List"-suffixed wrapper name was tried as a confidence signal and
// rejected after checking this tool's own repo-wide named-child findings
// against ec2@v1.319.1/deserializers.go by hand: it fires identically on a
// real confirmed bug (RunScheduledInstances' InstanceIDSet, commit
// 3337c961d, single member "instanceId") and on a genuinely correct AWS
// shape with the exact same structure (GetInstanceTypesFromInstanceRequirements'
// InstanceTypeSet, whose real element type
// types.InstanceTypeInfoFromInstanceRequirements has exactly one member,
// InstanceType). ec2 turns out to declare many real single- and
// under-implemented multi-member object-list types this way
// (types.AttributeValue, types.IpamOperatingRegion, types.PoolCidrBlock,
// types.UnsuccessfulItem, types.CapacityReservationGroup,
// types.SnapshotRecycleBinInfo, ...) -- none of which decode-crash a real
// client, unlike the confirmed bugs. There is no purely-syntactic signal
// that separates them; only reading the pinned SDK's own deserializer for
// that element type does. DescribeVpcEndpointServicePermissions's
// AllowedPrincipals is the same story: single member tagged "principal",
// not "item", but a genuinely correct partial rendering of the real
// two-member types.AllowedPrincipal (Principal + PrincipalType).
//
// This is an AST-based structural scan (go/parser + go/ast + reflect
// struct-tag parsing), not a regex: gopherstack-4xr5 is a regex bug of
// exactly this kind, missed by a prior auditor that tried to read struct
// tags with a pattern instead of the parser.
//
// Usage:
//
//	go run ./cmd/xmlitemwrap                   # report to stdout
//	go run ./cmd/xmlitemwrap -json out.json     # also write full finding list as JSON
//
// Exit codes: 0 no confident findings (needs-review hits may still be
// printed), 1 a run error (can't resolve the repo root, can't parse a
// file), 2 at least one confident finding -- gates CI once trusted.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const (
	exitClean      = 0
	exitRunError   = 1
	exitConfidence = 2
)

func main() {
	jsonOut := flag.String("json", "", "write the full finding list to this path as JSON")
	flag.Parse()

	root, err := repoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(exitRunError)
	}

	findings, err := scanServices(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(exitRunError)
	}

	if *jsonOut != "" {
		if werr := writeJSON(*jsonOut, findings); werr != nil {
			fmt.Fprintln(os.Stderr, "write json:", werr)
			os.Exit(exitRunError)
		}
	}

	printReport(findings)
	os.Exit(exitCode(findings))
}

func exitCode(findings []finding) int {
	for _, f := range findings {
		if f.Confident {
			return exitConfidence
		}
	}

	return exitClean
}

func repoRoot() (string, error) {
	out, err := exec.CommandContext(context.Background(), "go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}
