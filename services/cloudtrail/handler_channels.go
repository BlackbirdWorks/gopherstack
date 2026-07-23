package cloudtrail

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultChannelsPageSize is the ListChannels page size used when the caller
// omits MaxResults.
const defaultChannelsPageSize = 1000

// --- CreateChannel ---

type createChannelBody struct {
	Name         string        `json:"Name"`
	Source       string        `json:"Source"`
	Destinations []Destination `json:"Destinations"`
	Tags         []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleCreateChannel(c *echo.Context, body []byte) error {
	var in createChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		kv[tag.Key] = tag.Value
	}

	ch, err := h.Backend.CreateChannel(in.Name, in.Source, in.Destinations, kv)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelArn:   ch.ChannelARN,
		keyName:         ch.Name,
		keySource:       ch.Source,
		keyDestinations: ch.Destinations,
	})
}

// --- DeleteChannel ---

type deleteChannelBody struct {
	Channel string `json:"Channel"`
}

func (h *Handler) handleDeleteChannel(c *echo.Context, body []byte) error {
	var in deleteChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteChannel(in.Channel); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- GetChannel ---

type getChannelBody struct {
	Channel string `json:"Channel"`
}

func (h *Handler) handleGetChannel(c *echo.Context, body []byte) error {
	var in getChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	ch, err := h.Backend.GetChannel(in.Channel)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelArn:   ch.ChannelARN,
		keyName:         ch.Name,
		keySource:       ch.Source,
		keyDestinations: ch.Destinations,
	})
}

// --- UpdateChannel ---

type updateChannelBody struct {
	Channel      string        `json:"Channel"`
	Name         string        `json:"Name"`
	Destinations []Destination `json:"Destinations"`
}

func (h *Handler) handleUpdateChannel(c *echo.Context, body []byte) error {
	var in updateChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	ch, err := h.Backend.UpdateChannel(in.Channel, in.Name, in.Destinations)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelArn:   ch.ChannelARN,
		keyName:         ch.Name,
		keySource:       ch.Source,
		keyDestinations: ch.Destinations,
	})
}

// --- ListChannels ---

type listChannelsBody struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

func (h *Handler) handleListChannels(c *echo.Context, body []byte) error {
	var in listChannelsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}

	list := h.Backend.ListChannels()
	p := page.New(list, in.NextToken, in.MaxResults, defaultChannelsPageSize)

	items := make([]map[string]any, 0, len(p.Data))
	for _, ch := range p.Data {
		items = append(items, map[string]any{
			keyChannelArn: ch.ChannelARN,
			keyName:       ch.Name,
		})
	}

	resp := map[string]any{"Channels": items}
	if p.Next != "" {
		resp["NextToken"] = p.Next
	}

	return c.JSON(http.StatusOK, resp)
}
