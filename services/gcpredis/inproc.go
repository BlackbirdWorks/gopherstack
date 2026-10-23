package gcpredis

import "cloud.google.com/go/redis/apiv1/redispb"

// API exposes the CloudRedis server for in-process callers (dashboard proxy).
func (h *Handler) API() redispb.CloudRedisServer { return &server{b: h.Backend} }
