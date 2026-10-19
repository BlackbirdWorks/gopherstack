package gcpstorage

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/labstack/echo/v5"
)

type objectsJSON struct {
	Kind          string       `json:"kind"`
	NextPageToken string       `json:"nextPageToken,omitempty"`
	Prefixes      []string     `json:"prefixes,omitempty"`
	Items         []objectJSON `json:"items"`
}

func (h *Handler) listObjects(c *echo.Context, rt route) error {
	q := c.QueryParams()
	limit, _ := strconv.Atoi(q.Get("maxResults"))

	res, err := h.Backend.ListObjects(rt.bucket, ListOptions{
		Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"),
		StartOffset: q.Get("startOffset"), EndOffset: q.Get("endOffset"),
		PageToken: q.Get("pageToken"), MaxResults: limit,
		Versions:                 q.Get("versions") == "true",
		IncludeTrailingDelimiter: q.Get("includeTrailingDelimiter") == "true",
	})
	if err != nil {
		return err
	}

	base := baseURL(c.Request())
	out := objectsJSON{
		Kind: "storage#objects", NextPageToken: res.Next, Prefixes: res.Prefixes,
		Items: make([]objectJSON, 0, len(res.Items)),
	}

	for _, o := range res.Items {
		out.Items = append(out.Items, objectResource(base, o))
	}

	return c.JSON(http.StatusOK, out)
}

func objectParams(q url.Values) (int64, Preconds, error) {
	gen, apiErr := parseGeneration(q, "generation")
	if apiErr != nil {
		return 0, Preconds{}, apiErr
	}

	pre, apiErr := parsePreconds(q, "if")
	if apiErr != nil {
		return 0, Preconds{}, apiErr
	}

	return gen, pre, nil
}

func (h *Handler) getObject(c *echo.Context, rt route) error {
	q := c.QueryParams()

	gen, pre, err := objectParams(q)
	if err != nil {
		return err
	}

	o, err := h.Backend.GetObject(rt.bucket, rt.object, gen, pre)
	if err != nil {
		return err
	}

	if q.Get("alt") == "media" {
		serveObject(c, o)

		return nil
	}

	return c.JSON(http.StatusOK, objectResource(baseURL(c.Request()), o))
}

func (h *Handler) downloadObject(c *echo.Context, rt route) error {
	gen, pre, err := objectParams(c.QueryParams())
	if err != nil {
		return err
	}

	o, err := h.Backend.GetObject(rt.bucket, rt.object, gen, pre)
	if err != nil {
		return err
	}

	serveObject(c, o)

	return nil
}

func (h *Handler) updateObject(c *echo.Context, rt route) error {
	gen, pre, err := objectParams(c.QueryParams())
	if err != nil {
		return err
	}

	p, err := readPatch(c)
	if err != nil {
		return err
	}

	o, err := h.Backend.UpdateObject(rt.bucket, rt.object, gen, p, c.Request().Method == http.MethodPut, pre)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, objectResource(baseURL(c.Request()), o))
}

func (h *Handler) deleteObject(c *echo.Context, rt route) error {
	gen, pre, err := objectParams(c.QueryParams())
	if err != nil {
		return err
	}

	if delErr := h.Backend.DeleteObject(rt.bucket, rt.object, gen, pre); delErr != nil {
		return delErr
	}

	return c.NoContent(http.StatusNoContent)
}

func hasMetadataKeys(p Patch) bool {
	for _, k := range []string{
		keyContentType, keyContentEncoding, keyContentDisposition, keyContentLanguage, keyCacheControl,
		keyMetadata, keyCustomTime, keyStorageClass,
	} {
		if _, ok := p[k]; ok {
			return true
		}
	}

	return false
}

type rewriteJSON struct {
	Kind                string     `json:"kind"`
	TotalBytesRewritten string     `json:"totalBytesRewritten"`
	ObjectSize          string     `json:"objectSize"`
	Resource            objectJSON `json:"resource"`
	Done                bool       `json:"done"`
}

// copyObject serves copyTo and rewriteTo; both complete in one call.
func (h *Handler) copyObject(c *echo.Context, rt route) error {
	q := c.QueryParams()

	srcGen, apiErr := parseGeneration(q, "sourceGeneration")
	if apiErr != nil {
		return apiErr
	}

	dstPre, apiErr := parsePreconds(q, "if")
	if apiErr != nil {
		return apiErr
	}

	srcPre, apiErr := parsePreconds(q, "ifSource")
	if apiErr != nil {
		return apiErr
	}

	p, err := readPatch(c)
	if err != nil {
		return err
	}

	req := CopyRequest{
		SrcBucket: rt.bucket, SrcName: rt.object, SrcGeneration: srcGen,
		DstBucket: rt.dstBucket, DstName: rt.dstObject, DstPre: dstPre, SrcPre: srcPre,
	}
	if hasMetadataKeys(p) {
		req.Meta = p
	}

	o, err := h.Backend.CopyObject(req)
	if err != nil {
		return err
	}

	res := objectResource(baseURL(c.Request()), o)
	if rt.op == opCopyObject {
		return c.JSON(http.StatusOK, res)
	}

	size := strconv.FormatInt(o.Size(), 10)

	return c.JSON(http.StatusOK, rewriteJSON{
		Kind: "storage#rewriteResponse", TotalBytesRewritten: size, ObjectSize: size, Done: true, Resource: res,
	})
}

type composeRequest struct {
	Destination   Patch `json:"destination"`
	SourceObjects []struct {
		Name          string `json:"name"`
		Generation    string `json:"generation"`
		Preconditions struct {
			IfGenerationMatch string `json:"ifGenerationMatch"`
		} `json:"objectPreconditions"`
	} `json:"sourceObjects"`
}

func (h *Handler) composeObject(c *echo.Context, rt route) error {
	pre, apiErr := parsePreconds(c.QueryParams(), "if")
	if apiErr != nil {
		return apiErr
	}

	body, err := readBody(c)
	if err != nil {
		return err
	}

	var req composeRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return errInvalid("Invalid JSON payload received.")
	}

	sources := make([]ComposeSource, 0, len(req.SourceObjects))

	for _, s := range req.SourceObjects {
		cs := ComposeSource{Name: s.Name}
		if s.Generation != "" {
			if cs.Generation, apiErr = parseGeneration(url.Values{"g": {s.Generation}}, "g"); apiErr != nil {
				return apiErr
			}
		}

		if v := s.Preconditions.IfGenerationMatch; v != "" {
			if cs.Pre, apiErr = parsePreconds(url.Values{"ifGenerationMatch": {v}}, "if"); apiErr != nil {
				return apiErr
			}
		}

		sources = append(sources, cs)
	}

	meta := req.Destination
	if meta == nil {
		meta = Patch{}
	}

	o, err := h.Backend.ComposeObject(rt.bucket, rt.object, sources, meta, pre)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, objectResource(baseURL(c.Request()), o))
}
