package gcpartifactregistry

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

const (
	rpmSenseLess    = 0x02
	rpmSenseGreater = 0x04
	rpmSenseEqual   = 0x08
	rpmSensePre     = 0x200 | 0x400
	yumDir          = "Packages/"
)

func rpmFileName(m *rpmMeta) string {
	return m.strs[tagName] + "-" + m.strs[tagVersion] + "-" + m.strs[tagRelease] + "." + m.strs[tagArch] + ".rpm"
}

func yumArtifactPB(ref repoRef, m *rpmMeta) *artifactregistrypb.YumArtifact {
	return &artifactregistrypb.YumArtifact{
		Name:        ref.full() + "/yumArtifacts/" + rpmFileName(m),
		PackageName: m.strs[tagName], PackageType: artifactregistrypb.YumArtifact_BINARY,
		Architecture: m.strs[tagArch],
	}
}

func (b *InMemoryBackend) addRPM(ref repoRef, data []byte) (*artifactregistrypb.YumArtifact, error) {
	m, err := parseRPM(data)
	if err != nil {
		return nil, errInvalid("The file is not a valid .rpm: %v", err)
	}

	ver := m.strs[tagVersion] + "-" + m.strs[tagRelease]
	if e, ok := m.ints[tagEpoch]; ok {
		ver = strconv.Itoa(int(e)) + ":" + ver
	}

	err = b.putPackageFile(ref, pkgFile{
		pkgID: m.strs[tagName], display: m.strs[tagName], version: ver, fileID: yumDir + rpmFileName(m), data: data,
	}, true)
	if err != nil {
		return nil, err
	}

	return yumArtifactPB(ref, m), nil
}

func storeYum(b *InMemoryBackend, parent string, _, data []byte) (*longrunningpb.Operation, error) {
	ref, err := b.requireFormat(parent, artifactregistrypb.Repository_YUM)
	if err != nil {
		return nil, err
	}

	art, err := b.addRPM(ref, data)
	if err != nil {
		return nil, err
	}

	return b.doneAny(ref.parent(), "UploadYumArtifactResponse", art)
}

type rpmEntry struct {
	file *genFile
	meta *rpmMeta
	path string
	pkg  string
}

func (b *InMemoryBackend) rpmEntries(rc repoCtx) []rpmEntry {
	var out []rpmEntry

	for _, id := range b.packageIDs(rc.ref) {
		for _, v := range b.versionsOfPackage(rc.ref, id) {
			for _, nf := range v.files {
				if m, err := parseRPM(nf.file.Data); err == nil {
					out = append(out, rpmEntry{pkg: id, path: nf.id, file: nf.file, meta: m})
				}
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })

	return out
}

type xmlEntry struct {
	Name  string `xml:"name,attr"`
	Flags string `xml:"flags,attr,omitempty"`
	Epoch string `xml:"epoch,attr,omitempty"`
	Ver   string `xml:"ver,attr,omitempty"`
	Rel   string `xml:"rel,attr,omitempty"`
}

func flagName(f uint32) string {
	switch f & (rpmSenseLess | rpmSenseGreater | rpmSenseEqual) {
	case rpmSenseLess:
		return "LT"
	case rpmSenseGreater:
		return "GT"
	case rpmSenseEqual:
		return "EQ"
	case rpmSenseLess | rpmSenseEqual:
		return "LE"
	case rpmSenseGreater | rpmSenseEqual:
		return "GE"
	default:
		return ""
	}
}

func depEntries(deps []rpmDep, skipRpmlib bool) []xmlEntry {
	var out []xmlEntry

	for _, d := range deps {
		if skipRpmlib && strings.HasPrefix(d.name, "rpmlib(") {
			continue
		}

		e := xmlEntry{Name: d.name, Flags: flagName(d.flags)}
		if e.Flags != "" && d.ver != "" {
			e.Epoch = "0"
			e.Ver, e.Rel, _ = strings.Cut(d.ver, "-")
		}

		out = append(out, e)
	}

	return out
}

type xmlChecksum struct {
	Type  string `xml:"type,attr"`
	PkgID string `xml:"pkgid,attr,omitempty"`
	Value string `xml:",chardata"`
}

type xmlPrimaryPkg struct {
	Version  xmlVersion  `xml:"version"`
	Checksum xmlChecksum `xml:"checksum"`
	XMLName  xml.Name    `xml:"package"`
	Summary  string      `xml:"summary"`
	Arch     string      `xml:"arch"`
	Name     string      `xml:"name"`
	Type     string      `xml:"type,attr"`
	Desc     string      `xml:"description"`
	Packager string      `xml:"packager"`
	URL      string      `xml:"url"`
	Location struct {
		Href string `xml:"href,attr"`
	} `xml:"location"`
	Format xmlFormat `xml:"format"`
	Time   struct {
		File  int64 `xml:"file,attr"`
		Build int64 `xml:"build,attr"`
	} `xml:"time"`
	Size struct {
		Package   int    `xml:"package,attr"`
		Installed uint32 `xml:"installed,attr"`
		Archive   uint32 `xml:"archive,attr"`
	} `xml:"size"`
}

type xmlVersion struct {
	Epoch string `xml:"epoch,attr"`
	Ver   string `xml:"ver,attr"`
	Rel   string `xml:"rel,attr"`
}

type xmlFormat struct {
	License    string     `xml:"rpm:license"`
	Vendor     string     `xml:"rpm:vendor"`
	Group      string     `xml:"rpm:group"`
	BuildHost  string     `xml:"rpm:buildhost"`
	SourceRPM  string     `xml:"rpm:sourcerpm"`
	Provides   []xmlEntry `xml:"rpm:provides>rpm:entry"`
	Requires   []xmlEntry `xml:"rpm:requires>rpm:entry"`
	HeaderSpan struct {
		Start int `xml:"start,attr"`
		End   int `xml:"end,attr"`
	} `xml:"rpm:header-range"`
}

func (e rpmEntry) pkgID() string {
	sum := sha256.Sum256(e.file.Data)

	return hex.EncodeToString(sum[:])
}

func (e rpmEntry) epoch() string { return strconv.Itoa(int(e.meta.ints[tagEpoch])) }

func (e rpmEntry) primary() xmlPrimaryPkg {
	m := e.meta
	p := xmlPrimaryPkg{Type: "rpm", Name: m.strs[tagName], Arch: m.strs[tagArch]}
	p.Version = xmlVersion{Epoch: e.epoch(), Ver: m.strs[tagVersion], Rel: m.strs[tagRelease]}
	p.Checksum = xmlChecksum{Type: "sha256", PkgID: "YES", Value: e.pkgID()}
	p.Summary, p.Desc, p.Packager, p.URL = m.strs[tagSummary], m.strs[tagDescription], m.strs[tagPackager], m.strs[tagURL]
	p.Time.File, p.Time.Build = e.file.Updated.Unix(), int64(m.ints[tagBuildTime])
	p.Size.Package, p.Size.Installed, p.Size.Archive = len(e.file.Data), m.ints[tagSize], m.ints[tagSize]
	p.Location.Href = e.path
	p.Format = xmlFormat{
		License: m.strs[tagLicense], Vendor: m.strs[tagVendor], Group: m.strs[tagGroup],
		BuildHost: m.strs[tagBuildHost], SourceRPM: m.strs[tagSourceRPM],
		Provides: depEntries(m.provides, false), Requires: depEntries(m.requires, true),
	}
	p.Format.HeaderSpan.Start, p.Format.HeaderSpan.End = m.hdrStart, m.hdrEnd

	return p
}

const xmlHeader = `<?xml version="1.0" encoding="UTF-8"?>` + "\n"

func marshalIndent(v any) []byte {
	out, _ := xml.MarshalIndent(v, "", "  ")

	return out
}

func primaryXML(entries []rpmEntry) []byte {
	var buf bytes.Buffer

	buf.WriteString(xmlHeader)
	buf.WriteString(`<metadata xmlns="http://linux.duke.edu/metadata/common" ` +
		`xmlns:rpm="http://linux.duke.edu/metadata/rpm" packages="` + strconv.Itoa(len(entries)) + "\">\n")

	for _, e := range entries {
		raw := string(marshalIndent(e.primary()))
		buf.WriteString(raw + "\n")
	}

	buf.WriteString("</metadata>\n")

	return buf.Bytes()
}

func filelistsXML(entries []rpmEntry) []byte {
	var buf bytes.Buffer

	buf.WriteString(xmlHeader)
	buf.WriteString(`<filelists xmlns="http://linux.duke.edu/metadata/filelists" packages="` +
		strconv.Itoa(len(entries)) + "\">\n")

	for _, e := range entries {
		buf.WriteString(`<package pkgid="` + e.pkgID() + `" name="` + e.meta.strs[tagName] + `" arch="` +
			e.meta.strs[tagArch] + `"><version epoch="` + e.epoch() + `" ver="` + e.meta.strs[tagVersion] +
			`" rel="` + e.meta.strs[tagRelease] + "\"/></package>\n")
	}

	buf.WriteString("</filelists>\n")

	return buf.Bytes()
}

func otherXML(entries []rpmEntry) []byte {
	out := bytes.ReplaceAll(filelistsXML(entries), []byte("filelists"), []byte("otherdata"))

	return bytes.ReplaceAll(out, []byte("metadata/otherdata"), []byte("metadata/other"))
}

type repodataFile struct {
	typ  string
	name string
	gz   []byte
	raw  []byte
}

func (f repodataFile) href() string {
	sum := sha256.Sum256(f.gz)

	return "repodata/" + hex.EncodeToString(sum[:]) + "-" + f.name + ".xml.gz"
}

func repodata(entries []rpmEntry) []repodataFile {
	mk := func(typ, name string, raw []byte) repodataFile {
		return repodataFile{typ: typ, name: name, raw: raw, gz: gzipBytes(raw)}
	}

	return []repodataFile{
		mk("primary", "primary", primaryXML(entries)),
		mk("filelists", "filelists", filelistsXML(entries)),
		mk("other", "other", otherXML(entries)),
	}
}

func repomdXML(files []repodataFile, ts time.Time) []byte {
	var sb strings.Builder

	sb.WriteString(xmlHeader)
	sb.WriteString(
		`<repomd xmlns="http://linux.duke.edu/metadata/repo" xmlns:rpm="http://linux.duke.edu/metadata/rpm">` +
			"\n  <revision>" + strconv.FormatInt(ts.Unix(), 10) + "</revision>\n",
	)

	for _, f := range files {
		gz, raw := sha256.Sum256(f.gz), sha256.Sum256(f.raw)

		sb.WriteString(`  <data type="` + f.typ + "\">\n")
		sb.WriteString(`    <checksum type="sha256">` + hex.EncodeToString(gz[:]) + "</checksum>\n")
		sb.WriteString(`    <open-checksum type="sha256">` + hex.EncodeToString(raw[:]) + "</open-checksum>\n")
		sb.WriteString(`    <location href="` + f.href() + "\"/>\n")
		sb.WriteString("    <timestamp>" + strconv.FormatInt(ts.Unix(), 10) + "</timestamp>\n")
		sb.WriteString("    <size>" + strconv.Itoa(len(f.gz)) + "</size>\n")
		sb.WriteString("    <open-size>" + strconv.Itoa(len(f.raw)) + "</open-size>\n  </data>\n")
	}

	sb.WriteString("</repomd>\n")

	return []byte(sb.String())
}

func (b *InMemoryBackend) serveYum(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) {
	if !isRead(r) {
		writePlain(w, http.StatusMethodNotAllowed, "Yum repositories are populated with the upload and import APIs.")

		return
	}

	entries := b.rpmEntries(rc)

	switch {
	case strings.HasPrefix(p, "repodata/"):
		serveRepodata(w, entries, p, b.now())
	case strings.HasPrefix(p, yumDir):
		for _, e := range entries {
			if e.path != p {
				continue
			}

			if b.packageDenied(r.Context(), rc, e.pkg) {
				writePlain(w, http.StatusForbidden, "A download rule denies access to this package.")

				return
			}

			writeBytes(w, "application/x-rpm", e.file.Data)

			return
		}

		writePlain(w, http.StatusNotFound, "Not found: "+p)
	default:
		writePlain(w, http.StatusNotFound, "Not found: "+p)
	}
}

func serveRepodata(w http.ResponseWriter, entries []rpmEntry, p string, now time.Time) {
	files := repodata(entries)

	var newest time.Time

	for _, e := range entries {
		if e.file.Updated.After(newest) {
			newest = e.file.Updated
		}
	}

	if newest.IsZero() {
		newest = now
	}

	if p == "repodata/repomd.xml" {
		writeBytes(w, "application/xml", repomdXML(files, newest))

		return
	}

	for _, f := range files {
		if p == f.href() {
			writeBytes(w, "application/gzip", f.gz)

			return
		}
	}

	writePlain(w, http.StatusNotFound, "Not found: "+p)
}
