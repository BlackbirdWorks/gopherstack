package iot

import (
	"encoding/base64"
)

type userProp struct{ key, val string }

// mqttProps are the MQTT5 publish properties of the triggering message.
type mqttProps struct {
	contentType   string
	responseTopic string
	correlation   []byte
	user          []userProp
	utf8          bool
}

func propFuncs() map[string]funcDef {
	return map[string]funcDef{
		"get_mqtt_property":   {impl: getMQTTPropertyFunc},
		"get_user_properties": {impl: getUserPropertiesFunc},
		"principal":           {impl: func(c *sqlCtx, _ []any) any { return optString(c.msg.principal) }},
		"sourceip": {
			since2016: true,
			impl:      func(c *sqlCtx, _ []any) any { return optString(c.msg.sourceIP) },
		},
		"traceid": {impl: func(c *sqlCtx, _ []any) any { return optString(c.msg.traceID) }},
	}
}

func optString(s string) any {
	if s == "" {
		return sqlUndefined{}
	}

	return s
}

// getMQTTPropertyFunc returns one MQTT5 header per the documented argument table.
func getMQTTPropertyFunc(c *sqlCtx, args []any) any {
	name, _ := strArg(args, 0)
	p := c.msg.props

	if p == nil {
		return sqlUndefined{}
	}

	switch name {
	case "format_indicator":
		if p.utf8 {
			return "UTF8_DATA"
		}

		return "UNSPECIFIED_BYTES"
	case "content_type":
		return optString(p.contentType)
	case "response_topic":
		return optString(p.responseTopic)
	case "correlation_data":
		if len(p.correlation) > 0 {
			return base64.StdEncoding.EncodeToString(p.correlation)
		}
	}

	return sqlUndefined{}
}

// getUserPropertiesFunc returns the values for a key, or every key-value pair without an argument.
func getUserPropertiesFunc(c *sqlCtx, args []any) any {
	p := c.msg.props
	if p == nil {
		return sqlUndefined{}
	}

	if len(args) == 0 {
		out := make([]any, 0, len(p.user))

		for _, u := range p.user {
			o := newSQLObject()
			o.set(u.key, u.val)
			out = append(out, o)
		}

		return out
	}

	key, _ := strArg(args, 0)
	out := []any{}

	for _, u := range p.user {
		if u.key == key {
			out = append(out, u.val)
		}
	}

	if len(out) == 0 {
		return sqlUndefined{}
	}

	return out
}
