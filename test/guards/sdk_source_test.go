package guards_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	sdkModPrefix = "github.com/aws/aws-sdk-go-v2/service/"
	smithyJSONRe = `smithy-go/encoding/json"`
)

var errSDKModule = errors.New("sdk module unresolved")

// sdkData is parsed once per test binary from the SDK versions pinned in go.mod.
type sdkData struct {
	shapes sdkShapes
	keys   map[string]map[string]bool
}

type sdkLoader struct {
	err  error
	data sdkData
	once sync.Once
}

//nolint:gochecknoglobals // SDK sources are parsed once per test binary
var sdkCache sdkLoader

func loadSDK(t *testing.T) sdkData {
	t.Helper()

	sdkCache.once.Do(func() { sdkCache.data, sdkCache.err = buildSDKData() })
	require.NoError(t, sdkCache.err)

	return sdkCache.data
}

type sdkModule struct{ pkg, version string }

func pinnedSDKModules() ([]sdkModule, error) {
	gomod, err := os.ReadFile("go.mod")
	if err != nil {
		return nil, err
	}

	var mods []sdkModule

	for _, m := range sdkModRe.FindAllStringSubmatch(string(gomod), -1) {
		mods = append(mods, sdkModule{pkg: m[1], version: m[2]})
	}

	return mods, nil
}

// downloadSDKDirs resolves the exact pinned version of each module, downloading it when missing.
func downloadSDKDirs(mods []sdkModule) (map[string]string, error) {
	args := make([]string, 0, 3+len(mods))
	args = append(args, "mod", "download", "-json")
	for _, m := range mods {
		args = append(args, sdkModPrefix+m.pkg+"@"+m.version)
	}

	var stderr bytes.Buffer

	cmd := exec.Command("go", args...)
	cmd.Stderr = &stderr
	out, runErr := cmd.Output()

	dirs := map[string]string{}
	dec := json.NewDecoder(bytes.NewReader(out))

	for dec.More() {
		var r struct {
			Path    string `json:"Path"`
			Version string `json:"Version"`
			Dir     string `json:"Dir"`
			Error   string `json:"Error"`
		}
		if err := dec.Decode(&r); err != nil {
			return nil, err
		}

		if r.Error != "" || r.Dir == "" {
			return nil, fmt.Errorf("%w: %s@%s: %s", errSDKModule, r.Path, r.Version, r.Error)
		}

		dirs[strings.TrimPrefix(r.Path, sdkModPrefix)] = r.Dir
	}

	if runErr != nil {
		return nil, fmt.Errorf("%w: go mod download: %w: %s", errSDKModule, runErr, stderr.String())
	}

	for _, m := range mods {
		if dirs[m.pkg] == "" {
			return nil, fmt.Errorf("%w: %s@%s not downloaded", errSDKModule, m.pkg, m.version)
		}
	}

	return dirs, nil
}

func buildSDKData() (sdkData, error) {
	mods, err := pinnedSDKModules()
	if err != nil {
		return sdkData{}, err
	}

	dirs, err := downloadSDKDirs(mods)
	if err != nil {
		return sdkData{}, err
	}

	d := sdkData{
		shapes: sdkShapes{awsjson: map[string]map[string]map[tagShape]bool{}, all: map[string]bool{}},
		keys:   map[string]map[string]bool{},
	}

	sort.Slice(mods, func(i, j int) bool { return mods[i].pkg < mods[j].pkg })

	var mu sync.Mutex

	var wg sync.WaitGroup

	sem := make(chan struct{}, 4)

	for _, m := range mods {
		d.shapes.all[m.pkg] = true

		wg.Add(1)

		sem <- struct{}{}

		go func() {
			defer func() { <-sem; wg.Done() }()

			shapes, keys := readSDKPackage(dirs[m.pkg])
			if shapes == nil {
				return
			}

			mu.Lock()
			d.shapes.awsjson[m.pkg] = shapes
			if keys != nil {
				d.keys[m.pkg] = keys
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	return d, nil
}

var (
	schemaMemberRe = regexp.MustCompile(`(?m)^\s*\w+ = \w+\.AddMember\("(\w+)", (\w+)\)$`)
	schemaKindRe   = regexp.MustCompile(
		`(?m)^var (\w+) = smithy\.NewSchema\(smithy\.ShapeID\{[^}]*\}, smithy\.ShapeType(\w+),`,
	)
	schemaAWSJSON  = regexp.MustCompile(`protocol/awsjson"`)
	schemaRESTJSON = regexp.MustCompile(`protocol/restjson"`)
)

// readSDKPackage returns the awsjson tag shapes (nil when the package has no
// readable serde source) and the JSON wire keys (nil for non-JSON protocols).
// SDK versions use either serializers.go or the newer schemas/schemas.go layout.
func readSDKPackage(dir string) (map[string]map[tagShape]bool, map[string]bool) {
	if ser, err := os.ReadFile(filepath.Join(dir, "serializers.go")); err == nil {
		des, _ := os.ReadFile(filepath.Join(dir, "deserializers.go"))

		var keys map[string]bool

		if strings.Contains(string(ser), smithyJSONRe) {
			keys = map[string]bool{}
			addKeys(keys, ser)
			addKeys(keys, des)
		}

		return sdkAwsjsonTagShapes(ser), keys
	}

	sch, err := os.ReadFile(filepath.Join(dir, "schemas", "schemas.go"))
	if err != nil {
		return nil, nil
	}

	client, _ := os.ReadFile(filepath.Join(dir, "api_client.go"))

	isAWSJSON := schemaAWSJSON.Match(client)
	if !isAWSJSON && !schemaRESTJSON.Match(client) {
		return map[string]map[tagShape]bool{}, nil
	}

	shapes, keys := schemaShapesAndKeys(string(sch))
	if !isAWSJSON {
		shapes = map[string]map[tagShape]bool{}
	}

	return shapes, keys
}

func schemaShapesAndKeys(text string) (map[string]map[tagShape]bool, map[string]bool) {
	kinds := map[string]tagShape{}

	for _, m := range schemaKindRe.FindAllStringSubmatch(text, -1) {
		switch m[2] {
		case "List":
			kinds[m[1]] = shapeList
		case "Map":
			kinds[m[1]] = shapeMap
		}
	}

	shapes := map[string]map[tagShape]bool{}
	keys := map[string]bool{}

	for _, m := range schemaMemberRe.FindAllStringSubmatch(text, -1) {
		name, target := m[1], m[2]
		if target == "smithyprelude.Unit" {
			continue
		}

		keys[name] = true

		low := strings.ToLower(name)
		if !strings.Contains(low, "tag") {
			continue
		}

		shape, ok := kinds[target]
		if !ok {
			shape = shapeOther
		}

		if isTagKey(name) || shapes[name] == nil {
			if shapes[name] == nil {
				shapes[name] = map[tagShape]bool{}
			}

			shapes[name][shape] = true
		}
	}

	return shapes, keys
}
