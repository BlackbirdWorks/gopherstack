package appsync

import (
	"maps"
	"strconv"
)

// ddbPlain converts DynamoDB AttributeValue JSON ({"S":"x"}, {"N":"1"}, ...)
// to plain JSON values, as AppSync does for DynamoDB resolver results.
func ddbPlain(v any) any {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return v
	}

	for tag, val := range m {
		switch tag {
		case "S", "B":
			return val
		case "N":
			return ddbNumber(val)
		case "BOOL":
			return val
		case "NULL":
			return nil
		case "L":
			return ddbPlainList(val, false)
		case "SS", "BS":
			return ddbPlainList(val, false)
		case "NS":
			return ddbPlainList(val, true)
		case "M":
			return ddbPlainMap(val)
		}
	}

	return v
}

func ddbNumber(val any) any {
	s, ok := val.(string)
	if !ok {
		return val
	}

	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}

	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	return s
}

func ddbPlainList(val any, numbers bool) any {
	items, ok := val.([]any)
	if !ok {
		return val
	}

	out := make([]any, len(items))
	for i, it := range items {
		if numbers {
			out[i] = ddbNumber(it)
		} else {
			out[i] = ddbPlain(it)
		}
	}

	return out
}

func ddbPlainMap(val any) any {
	m, ok := val.(map[string]any)
	if !ok {
		return val
	}

	return ddbPlainItem(m)
}

// ddbPlainItem converts a typed DynamoDB item to a plain map.
func ddbPlainItem(item map[string]any) map[string]any {
	out := make(map[string]any, len(item))
	for k, v := range item {
		out[k] = ddbPlain(v)
	}

	return out
}

// ddbPutItem builds the item a PutItem request writes: key merged with
// attributeValues, falling back to the legacy "item" member.
func ddbPutItem(request map[string]any) map[string]any {
	key, _ := request["key"].(map[string]any)
	attrs, _ := request["attributeValues"].(map[string]any)

	if item, ok := request["item"].(map[string]any); ok && attrs == nil {
		return item
	}

	item := make(map[string]any, len(key)+len(attrs))
	maps.Copy(item, key)
	maps.Copy(item, attrs)

	return item
}
