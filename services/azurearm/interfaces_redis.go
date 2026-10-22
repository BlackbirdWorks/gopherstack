package azurearm

// RedisSpec is the part of a cache's configuration the Redis servers act on.
type RedisSpec struct {
	SKU              string
	MinTLS           string
	EnableNonSSLPort bool
	Persistence      bool
	AuthNotRequired  bool
}

// RedisEndpoint is where a cache's servers listen; Port is 0 while the plaintext port is disabled.
type RedisEndpoint struct {
	Host    string
	Port    int
	SSLPort int
}

// RedisKeys are a cache's access keys.
type RedisKeys struct {
	Primary   string
	Secondary string
}

// Redis is the seam the Microsoft.Cache resource provider uses to run real Redis servers for its caches. id is the
// lower-cased ARM resource ID. Primitive types only, so this package never imports services/azureredis.
type Redis interface {
	Ensure(id string, spec RedisSpec) (RedisEndpoint, error)
	Endpoint(id string) (RedisEndpoint, bool)
	Delete(id string) error
	Keys(id string) (RedisKeys, error)
	RegenerateKey(id, keyType string) (RedisKeys, error)
	Reboot(id, rebootType string) error
	Import(id, format string, files []string) error
	Export(id, format, prefix, container string) error
}

type noopRedis struct{}

func (noopRedis) Ensure(string, RedisSpec) (RedisEndpoint, error) {
	return RedisEndpoint{}, ErrRedisUnavailable
}
func (noopRedis) Endpoint(string) (RedisEndpoint, bool) { return RedisEndpoint{}, false }
func (noopRedis) Delete(string) error                   { return nil }
func (noopRedis) Keys(string) (RedisKeys, error)        { return RedisKeys{}, ErrRedisUnavailable }
func (noopRedis) RegenerateKey(string, string) (RedisKeys, error) {
	return RedisKeys{}, ErrRedisUnavailable
}
func (noopRedis) Reboot(string, string) error                 { return ErrRedisUnavailable }
func (noopRedis) Import(string, string, []string) error       { return ErrRedisUnavailable }
func (noopRedis) Export(string, string, string, string) error { return ErrRedisUnavailable }

var _ Redis = noopRedis{}

// SetRedis gives the registered Microsoft.Cache provider (if any) a real data-plane adapter, replacing its no-op
// default.
func (r *Registry) SetRedis(rd Redis) {
	p, ok := r.ProviderNamed(namespaceMicrosoftCache)
	if !ok {
		return
	}

	if rp, rpOk := p.(*RedisProvider); rpOk {
		rp.dataPlane = rd
	}
}
