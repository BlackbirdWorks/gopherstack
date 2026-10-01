package asl

import (
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/recolabs/gnata"
)

var (
	sfnEnvOnce sync.Once                //nolint:gochecknoglobals // lazy shared environment
	sfnEnv     *gnata.CustomEnvironment //nolint:gochecknoglobals // lazy shared environment
)

func intrinsicFunc(fn func([]any) (any, error)) gnata.CustomFunc {
	return func(args []any, _ any) (any, error) { return fn(args) }
}

// sfnJSONataEnv holds the AWS-provided $partition, $range, $hash, $random,
// $uuid and $parse; $eval is disabled, as on AWS.
func sfnJSONataEnv() *gnata.CustomEnvironment {
	sfnEnvOnce.Do(func() {
		sfnEnv = gnata.NewCustomEnvironment(map[string]gnata.CustomFunc{
			"partition": intrinsicFunc(intrinsicArrayPartition),
			"range":     intrinsicFunc(intrinsicArrayRange),
			"hash":      intrinsicFunc(intrinsicHash),
			"uuid":      intrinsicFunc(intrinsicUUID),
			"random":    randomFunc,
			"parse":     intrinsicFunc(intrinsicStringToJSON),
			"eval":      func([]any, any) (any, error) { return nil, errJSONataEvalBanned },
		})
	})

	return sfnEnv
}

// randomFunc is $random([seed]): n in [0,1); the same seed yields the same n.
func randomFunc(args []any, _ any) (any, error) {
	if len(args) == 0 {
		return rand.Float64(), nil //nolint:gosec // non-cryptographic per spec
	}

	if len(args) > 1 {
		return nil, fmt.Errorf("%w: $random takes at most one argument", errJSONataEval)
	}

	seed, ok := toFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("%w: $random seed must be a number", errJSONataEval)
	}

	return rand.New(rand.NewPCG(uint64(int64(seed)), 0)).Float64(), nil //nolint:gosec // seeded by design
}
