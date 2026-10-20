package replicator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

const latestVersion = "$LATEST"

var errVersionNotReplicated = errors.New("version was not replicated")

type fnCopy struct {
	cfg  *lambdatypes.FunctionConfiguration
	vpc  *lambdatypes.VpcConfig
	name string
	zip  []byte
}

func (r *run) copyExtras(ctx context.Context, f fnCopy, dstExists bool) {
	vm := r.copyVersions(ctx, f, dstExists)
	r.copyAliases(ctx, f.name, vm, dstExists)
	r.copyEventSourceMappings(ctx, f.name)
}

func listVersions(ctx context.Context, c *lambda.Client, name string) ([]lambdatypes.FunctionConfiguration, error) {
	var out []lambdatypes.FunctionConfiguration

	pager := lambda.NewListVersionsByFunctionPaginator(c, &lambda.ListVersionsByFunctionInput{FunctionName: &name})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list versions: %w", err)
		}

		for _, v := range page.Versions {
			if aws.ToString(v.Version) != latestVersion {
				out = append(out, v)
			}
		}
	}

	slices.SortFunc(out, func(a, b lambdatypes.FunctionConfiguration) int {
		x, _ := strconv.Atoi(aws.ToString(a.Version))
		y, _ := strconv.Atoi(aws.ToString(b.Version))

		return x - y
	})

	return out, nil
}

// copyVersions republishes missing source versions in order and returns the source-to-target
// version map. Versions pair by position, so numbering gaps in the source stay idempotent.
func (r *run) copyVersions(ctx context.Context, f fnCopy, dstExists bool) map[string]string {
	vm := map[string]string{latestVersion: latestVersion}

	src, err := listVersions(ctx, r.src.lambda, f.name)
	if err != nil {
		r.fail(TypeLambda, f.name+":versions", err)

		return vm
	}

	var dst []lambdatypes.FunctionConfiguration

	if dstExists {
		if dst, err = listVersions(ctx, r.dst.lambda, f.name); err != nil {
			r.fail(TypeLambda, f.name+":versions", err)

			return vm
		}
	}

	for i := range min(len(src), len(dst)) {
		vm[aws.ToString(src[i].Version)] = aws.ToString(dst[i].Version)
	}

	if len(dst) >= len(src) {
		return vm
	}

	r.publishPending(ctx, f, src[len(dst):], vm)

	return vm
}

func (r *run) publishPending(
	ctx context.Context, f fnCopy, pending []lambdatypes.FunctionConfiguration, vm map[string]string,
) {
	for _, v := range pending {
		sv := aws.ToString(v.Version)
		id := f.name + ":" + sv

		if r.cfg.DryRun {
			vm[sv] = sv
			r.add(TypeLambda, id, ActionCreated, "")

			continue
		}

		dv, note, err := r.publishVersion(ctx, f, sv)
		if err != nil {
			r.fail(TypeLambda, id, err)

			break
		}

		vm[sv] = dv

		if dv != sv {
			note = strings.TrimSpace(note + " published as version " + dv + " (version numbers differ from the source)")
		}

		reason := note

		r.add(TypeLambda, id, ActionCreated, reason)
	}

	if r.cfg.DryRun {
		return
	}

	if err := r.updateFunction(ctx, f); err != nil {
		r.fail(TypeLambda, f.name+":"+latestVersion, fmt.Errorf("restore $LATEST: %w", err))
	}
}

// publishVersion points the target's $LATEST at a source version's code and configuration
// and publishes it; the caller restores $LATEST afterwards. A source that does not expose
// version code (Gopherstack) falls back to the $LATEST code, which the note reports.
func (r *run) publishVersion(ctx context.Context, f fnCopy, version string) (string, string, error) {
	name, note := f.name, ""

	got, err := r.src.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: &name, Qualifier: &version})
	if err != nil {
		return "", "", fmt.Errorf("get version: %w", err)
	}

	zip := f.zip

	if got.Code != nil && aws.ToString(got.Code.Location) != "" {
		if zip, err = r.downloadCode(ctx, aws.ToString(got.Code.Location)); err != nil {
			return "", "", err
		}
	} else {
		note = "source does not expose version code; $LATEST code used."
	}

	vf := fnCopy{name: name, cfg: got.Configuration, zip: zip, vpc: r.vpcFor(ctx, got.Configuration)}

	if err = r.updateFunction(ctx, vf); err != nil {
		return "", "", err
	}

	out, err := r.dst.lambda.PublishVersion(ctx, &lambda.PublishVersionInput{
		FunctionName: &name, Description: got.Configuration.Description,
	})
	if err != nil {
		return "", "", fmt.Errorf("publish version: %w", err)
	}

	return aws.ToString(out.Version), note, nil
}

func (r *run) copyAliases(ctx context.Context, name string, vm map[string]string, dstExists bool) {
	pager := lambda.NewListAliasesPaginator(r.src.lambda, &lambda.ListAliasesInput{FunctionName: &name})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.fail(TypeLambda, name+":aliases", fmt.Errorf("list aliases: %w", err))

			return
		}

		for i := range page.Aliases {
			r.copyAlias(ctx, name, &page.Aliases[i], vm, dstExists)
		}
	}
}

func (r *run) copyAlias(
	ctx context.Context, name string, a *lambdatypes.AliasConfiguration, vm map[string]string, dstExists bool,
) {
	id := name + ":" + aws.ToString(a.Name)

	version, ok := vm[aws.ToString(a.FunctionVersion)]
	if !ok {
		r.add(TypeLambda, id, ActionFailed, "version "+aws.ToString(a.FunctionVersion)+" was not replicated")

		return
	}

	routing, err := mapRouting(a.RoutingConfig, vm)
	if err != nil {
		r.fail(TypeLambda, id, err)

		return
	}

	exists := false

	if dstExists {
		_, gerr := r.dst.lambda.GetAlias(ctx, &lambda.GetAliasInput{FunctionName: &name, Name: a.Name})
		if exists, gerr = existsErr(gerr); gerr != nil {
			r.fail(TypeLambda, id, gerr)

			return
		}
	}

	if r.cfg.DryRun {
		r.add(TypeLambda, id, upsert(exists), "")

		return
	}

	if exists {
		_, err = r.dst.lambda.UpdateAlias(ctx, &lambda.UpdateAliasInput{
			FunctionName: &name, Name: a.Name, FunctionVersion: &version,
			Description: a.Description, RoutingConfig: routing,
		})
	} else {
		_, err = r.dst.lambda.CreateAlias(ctx, &lambda.CreateAliasInput{
			FunctionName: &name, Name: a.Name, FunctionVersion: &version,
			Description: a.Description, RoutingConfig: routing,
		})
	}

	r.done(TypeLambda, id, upsert(exists), err)
}

func mapRouting(
	rc *lambdatypes.AliasRoutingConfiguration, vm map[string]string,
) (*lambdatypes.AliasRoutingConfiguration, error) {
	out := map[string]float64{}
	if rc == nil {
		return &lambdatypes.AliasRoutingConfiguration{AdditionalVersionWeights: out}, nil
	}

	for v, w := range rc.AdditionalVersionWeights {
		dv, ok := vm[v]
		if !ok {
			return nil, fmt.Errorf("%w: routing version %s", errVersionNotReplicated, v)
		}

		out[dv] = w
	}

	return &lambdatypes.AliasRoutingConfiguration{AdditionalVersionWeights: out}, nil
}
