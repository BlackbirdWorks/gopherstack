package main

import (
	"os"
	"regexp"
	"strings"
)

const (
	corpusFields    = 10
	requestFields   = 8
	host            = "localhost:4566"
	uaFmt           = "User-Agent:aws-sdk-go-v2/1.41.0 ua/2.1 os/linux lang/go#1.26 api/%s#1.0.0"
	cborHeader      = "Smithy-Protocol:rpc-v2-cbor"
	maxPlaceChars   = 6
	methodGet       = "GET"
	methodPost      = "POST"
	json10          = "application/x-amz-json-1.0"
	json11          = "application/x-amz-json-1.1"
	scopeExecuteAPI = "execute-api"
	keyPath         = "/mybucket/key.txt"
	bucketSegs      = 2
)

// row is one corpus request; note tags its origin and is not written out.
type row struct {
	method, host, uri, auth, ctype, target, body, headers, note string
}

func (r row) key() string {
	return strings.Join([]string{r.method, r.host, r.uri, r.auth, r.ctype, r.target, r.body, r.headers}, "\t")
}

func (r row) pkg() string { return r.note[:strings.IndexByte(r.note, ':')] }

var (
	reSigning  = regexp.MustCompile(`SetSigV4SigningName\(&props, "([^"]+)"\)`)
	reOpSplit  = regexp.MustCompile(`func \(m \*\w+_serializeOp(\w+)\) HandleSerialize`)
	reTarget   = regexp.MustCompile(`SetHeader\("X-Amz-Target"\)\.String\("([^"]+)"\)`)
	reCType    = regexp.MustCompile(`SetHeader\("Content-Type"\)\.String\("([^"]+)"\)`)
	reAction   = regexp.MustCompile(`Key\("Action"\)\.String\("([^"]+)"\)`)
	reVersion  = regexp.MustCompile(`Key\("Version"\)\.String\("([^"]+)"\)`)
	reSplitURI = regexp.MustCompile(`SplitURI\("([^"]*)"\)`)
	reMethod   = regexp.MustCompile(`request\.Method = "(\w+)"`)
	reURLPath  = regexp.MustCompile(`req\.URL\.Path = "([^"]+)"`)
	reProtocol = regexp.MustCompile(`options\.Protocol = (\w+)\.(\w+)\(schemas\.(\w+)\)`)
	reSchemaOp = regexp.MustCompile(`(?s)var (\w+) = smithy\.NewSchema\(smithy\.ShapeID\{\s*Namespace:\s*"[^"]*",` +
		`\s*Name:\s*"(\w+)",\s*\}, smithy\.ShapeTypeOperation, 0(.*?)\)\n\n`)
	reHTTPTrait = regexp.MustCompile(`Method: "(\w+)",\s*URI:\s*"([^"]*)"`)
	rePlaceName = regexp.MustCompile(`\{([^}]+)\}`)
	reNonWord   = regexp.MustCompile(`[^0-9A-Za-z_]`)
)

func collectSDKRows() ([]row, error) {
	dirs, err := sdkDirs()
	if err != nil {
		return nil, err
	}

	var rows []row

	for _, d := range dirs {
		rows = append(rows, pkgRows(d[0], d[1])...)
	}

	return rows, nil
}

func readFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return string(b)
}

func signingName(name, dir string) string {
	if m := reSigning.FindStringSubmatch(readFile(dir + "/auth.go")); m != nil {
		return m[1]
	}

	return name
}

func pkgRows(name, dir string) []row {
	sign := signingName(name, dir)

	if src := readFile(dir + "/serializers.go"); src != "" {
		return serializerRows(name, sign, src)
	}

	schemas := readFile(dir + "/schemas/schemas.go")
	if schemas == "" {
		return nil
	}

	return schemaRows(name, sign, readFile(dir+"/api_client.go"), schemas)
}

func substPlaceholders(p string) string {
	return rePlaceName.ReplaceAllStringFunc(p, func(m string) string {
		k := m[1 : len(m)-1]
		if strings.HasSuffix(k, "+") {
			return "a/b"
		}

		w := strings.ToLower(reNonWord.ReplaceAllString(k, ""))
		if len(w) > maxPlaceChars {
			w = w[:maxPlaceChars]
		}

		return "x" + w
	})
}

func restURI(raw string, ensureSlash bool) string {
	p, q, hasQ := strings.Cut(raw, "?")
	p = substPlaceholders(p)

	if ensureSlash && !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	if hasQ {
		return p + "?" + q
	}

	return p
}

func serializerRows(name, sign, src string) []row {
	idx := reOpSplit.FindAllStringSubmatchIndex(src, -1)

	var rows []row

	for i, m := range idx {
		end := len(src)
		if i+1 < len(idx) {
			end = idx[i+1][0]
		}

		op, body := src[m[2]:m[3]], src[m[1]:end]
		if r, ok := serializerRow(name+":"+op, sign, body); ok {
			rows = append(rows, r)
		}
	}

	return rows
}

func serializerRow(note, sign, body string) (row, bool) {
	t := reTarget.FindStringSubmatch(body)
	a := reAction.FindStringSubmatch(body)
	v := reVersion.FindStringSubmatch(body)
	cp := reURLPath.FindStringSubmatch(body)
	u := reSplitURI.FindStringSubmatch(body)
	me := reMethod.FindStringSubmatch(body)

	switch {
	case t != nil:
		ct := json10
		if m := reCType.FindStringSubmatch(body); m != nil {
			ct = m[1]
		}

		return row{methodPost, host, "/", sign, ct, t[1], "{}", "", note}, true
	case a != nil && v != nil:
		return row{
			methodPost, host, "/", sign, "application/x-www-form-urlencoded", "",
			"Action=" + a[1] + "&Version=" + v[1], "", note,
		}, true
	case strings.Contains(body, "rpc-v2-cbor") && cp != nil:
		return row{methodPost, host, cp[1], sign, "application/cbor", "", "", cborHeader, note}, true
	case u != nil && me != nil:
		return row{me[1], host, restURI(u[1], true), sign, "", "", "", "", note}, true
	}

	return row{}, false
}

func schemaRows(name, sign, apiClient, schemas string) []row {
	pm := reProtocol.FindStringSubmatch(apiClient)
	if pm == nil {
		return nil
	}

	var rows []row

	for _, om := range reSchemaOp.FindAllStringSubmatch(schemas+"\n\n", -1) {
		note := name + ":" + om[2]

		switch pm[1] {
		case "awsjson":
			ct := json11
			if pm[2] == "New10" {
				ct = json10
			}

			rows = append(rows, row{methodPost, host, "/", sign, ct, pm[3] + "." + om[2], "{}", "", note})
		case "rpcv2":
			uri := "/service/" + pm[3] + "/operation/" + om[2]
			rows = append(rows, row{methodPost, host, uri, sign, "application/cbor", "", "", cborHeader, note})
		default:
			if hm := reHTTPTrait.FindStringSubmatch(om[3]); hm != nil {
				rows = append(rows, row{hm[1], host, restURI(hm[2], false), sign, "", "", "", "", note})
			}
		}
	}

	return rows
}
