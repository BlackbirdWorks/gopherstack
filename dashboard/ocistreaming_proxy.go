package dashboard

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	streambackend "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

const (
	ociStreamingPrefix   = ociProxyPrefix + "/streaming"
	ociStreamDefaultRead = 20
	ociStreamMaxRead     = 200
	cursorAtOffset       = "AT_OFFSET"
)

type ociStreamBytes struct {
	Text   string `json:"text,omitempty"`
	Base64 string `json:"base64,omitempty"`
	IsText bool   `json:"isText"`
}

type ociStreamMessage struct {
	Timestamp time.Time      `json:"timestamp"`
	Partition string         `json:"partition"`
	Key       ociStreamBytes `json:"key"`
	Value     ociStreamBytes `json:"value"`
	Offset    int64          `json:"offset"`
}

type ociStreamingAPI struct {
	b *streambackend.InMemoryBackend
}

// registerOCIStreamingRoutes serves the Streaming page from the backend in-process.
func (h *DashboardHandler) registerOCIStreamingRoutes() {
	api := &ociStreamingAPI{}
	if h.config.OCIStreamingOps != nil {
		api.b = h.config.OCIStreamingOps.Backend
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(api.b != nil, "OCI Streaming", next)
	}

	r := h.SubRouter
	p := ociStreamingPrefix
	r.Add(http.MethodGet, p+"/pools", guard(api.pools))
	r.Add(http.MethodGet, p+"/streams", guard(api.streams))
	r.Add(http.MethodGet, p+"/streams/:stream", guard(api.stream))
	r.Add(http.MethodGet, p+"/streams/:stream/partitions", guard(api.partitions))
	r.Add(http.MethodGet, p+"/streams/:stream/messages", guard(api.messages))
}

func (a *ociStreamingAPI) pools(c *echo.Context) error {
	items, err := ociAcross(a.b.Compartments(), func(comp string) ([]any, error) {
		return ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListPools(comp, "", "", "", streambackend.ListOpts{Page: token, Limit: ociListLimit})

			return anyPage(pg.Data, pg.Next), perr
		})
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"pools": items})
}

func (a *ociStreamingAPI) streams(c *echo.Context) error {
	pool := c.QueryParam("poolId")

	items, err := ociAcross(a.b.Compartments(), func(comp string) ([]any, error) {
		return ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListStreams(
				comp,
				pool,
				"",
				"",
				"",
				"",
				streambackend.ListOpts{Page: token, Limit: ociListLimit},
			)

			return anyPage(pg.Data, pg.Next), perr
		})
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"streams": items})
}

func (a *ociStreamingAPI) stream(c *echo.Context) error {
	s, _, err := a.b.GetStream(c.Param("stream"), "")
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, s)
}

func (a *ociStreamingAPI) partitions(c *echo.Context) error {
	ranges, err := a.b.PartitionRanges(c.Param("stream"))
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"partitions": ranges})
}

func ociStreamBytesOf(raw []byte) ociStreamBytes {
	if utf8.Valid(raw) {
		return ociStreamBytes{Text: string(raw), IsText: true}
	}

	return ociStreamBytes{Base64: base64.StdEncoding.EncodeToString(raw)}
}

// messages reads from a partition through a server-side cursor; with no offset it returns the newest messages.
func (a *ociStreamingAPI) messages(c *echo.Context) error {
	stream, partition := c.Param("stream"), c.QueryParam("partition")
	if partition == "" {
		partition = "0"
	}

	limit, _ := strconv.Atoi(c.QueryParam("limit"))
	if limit <= 0 || limit > ociStreamMaxRead {
		limit = ociStreamDefaultRead
	}

	ranges, err := a.b.PartitionRanges(stream)
	if err != nil {
		return ociFail(c, err)
	}

	var window *streambackend.PartitionRange

	for i := range ranges {
		if ranges[i].Partition == partition {
			window = &ranges[i]
		}
	}

	if window == nil {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "partition "+partition+" does not exist")
	}

	start := max(window.Base, window.End-int64(limit))

	if raw := c.QueryParam("offset"); raw != "" {
		off, perr := strconv.ParseInt(raw, 10, 64)
		if perr != nil || off < 0 {
			return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid offset")
		}

		start = max(off, window.Base)
	}

	cur, err := a.b.CreateCursor(
		stream,
		streambackend.CreateCursorIn{Type: cursorAtOffset, Partition: partition, Offset: &start},
	)
	if err != nil {
		return ociFail(c, err)
	}

	msgs, _, err := a.b.GetMessages(stream, cur.Value, limit)
	if err != nil {
		return ociFail(c, err)
	}

	items := make([]ociStreamMessage, 0, len(msgs))
	for _, m := range msgs {
		items = append(items, ociStreamMessage{
			Timestamp: m.Timestamp, Partition: m.Partition, Offset: m.Offset,
			Key: ociStreamBytesOf(m.Key), Value: ociStreamBytesOf(m.Value),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"messages": items, "base": window.Base, "end": window.End, aciPxStart: start,
	})
}
