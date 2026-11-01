package gcpartifactregistry_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/rpmpack"
	"github.com/stretchr/testify/require"
)

func tarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)

	for name, body := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}))
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func arMember(name string, body []byte) []byte {
	hdr := fmt.Sprintf("%-16s%-12d%-6d%-6d%-8s%-10d`\n", name+"/", 0, 0, 0, "100644", len(body))
	out := append([]byte(hdr), body...)

	if len(body)%2 == 1 {
		out = append(out, '\n')
	}

	return out
}

// makeDeb builds a binary package in the Debian ar layout.
func makeDeb(t *testing.T, name, version, arch string) []byte {
	t.Helper()

	control := "Package: " + name + "\nVersion: " + version + "\nArchitecture: " + arch +
		"\nMaintainer: Test <t@example.com>\nInstalled-Size: 12\nDepends: libc6\nSection: utils\nPriority: optional\n" +
		"Description: " + name + " test package\n long description line\n"

	out := []byte("!<arch>\n")
	out = append(out, arMember("debian-binary", []byte("2.0\n"))...)
	out = append(out, arMember("control.tar.gz", tarGz(t, map[string]string{"./control": control}))...)
	out = append(out, arMember("data.tar.gz", tarGz(t, map[string]string{"./usr/bin/" + name: "#!/bin/sh\n"}))...)

	return out
}

func makeRPM(t *testing.T, name, version, release, arch string) []byte {
	t.Helper()

	r, err := rpmpack.NewRPM(rpmpack.RPMMetaData{
		Name: name, Version: version, Release: release, Arch: arch, Summary: name + " summary",
		Description: name + " description", Licence: "MIT", URL: "https://example.com", Packager: "Test",
		Requires: mustRelations(t, "bash >= 4.0"), Provides: mustRelations(t, name+" = "+version+"-"+release),
	})
	require.NoError(t, err)
	r.AddFile(rpmpack.RPMFile{Name: "/usr/bin/" + name, Body: []byte("#!/bin/sh\n"), Mode: 0o755})

	var buf bytes.Buffer
	require.NoError(t, r.Write(&buf))

	return buf.Bytes()
}

func mustRelations(t *testing.T, specs ...string) rpmpack.Relations {
	t.Helper()

	rels := rpmpack.Relations{}
	for _, s := range specs {
		require.NoError(t, rels.Set(s))
	}

	return rels
}

func fieldOf(stanza, field string) string {
	for line := range strings.SplitSeq(stanza, "\n") {
		if v, ok := strings.CutPrefix(line, field+": "); ok {
			return v
		}
	}

	return ""
}

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }
