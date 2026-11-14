package fixture

import (
	"encoding/json/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type putRuleInput struct {
	Tags []tag  `json:"Tags"`
	Name string `json:"Name"`
}

func decode(body []byte) (*putRuleInput, error) {
	var in putRuleInput

	return &in, json.Unmarshal(body, &in, jsonx.V1())
}

type tag struct{ Key, Value string }
