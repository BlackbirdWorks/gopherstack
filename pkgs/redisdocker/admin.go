package redisdocker

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Target addresses one node's control endpoint.
type Target struct {
	TLS      *tls.Config
	Addr     string
	Username string
	Password string
}

// Admin sends commands to one node.
type Admin interface {
	Do(ctx context.Context, args ...any) (any, error)
	Close() error
}

// Dialer opens an Admin for t; it does not connect until the first command.
type Dialer func(t Target) Admin

type goRedisAdmin struct{ c *redis.Client }

func dialRedis(t Target) Admin {
	return &goRedisAdmin{c: redis.NewClient(&redis.Options{
		Addr: t.Addr, Username: t.Username, Password: t.Password, TLSConfig: t.TLS,
		Protocol: 2, DisableIdentity: true, MaxRetries: -1,
	})}
}

func (a *goRedisAdmin) Do(ctx context.Context, args ...any) (any, error) {
	res, err := a.c.Do(ctx, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis %v: %w", args[0], err)
	}

	return res, nil
}

func (a *goRedisAdmin) Close() error { return a.c.Close() }

func asString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprint(v)
	}
}

// infoField returns the value of key in INFO/CLUSTER INFO style "key:value" lines.
func infoField(text, key string) string {
	for _, line := range strings.Split(text, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok && k == key {
			return v
		}
	}

	return ""
}
