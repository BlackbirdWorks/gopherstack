package kafka

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// ScramSecretPrefix is the name prefix MSK requires of secrets associated with a cluster.
	ScramSecretPrefix = "AmazonMSK_"

	scramIterations    = 4096
	scramSaltLen       = 24
	scramMechSHA512    = 2
	scramInvalidSecret = "InvalidSecret"
	scramSecretMissing = "SecretNotFound"
)

// ErrScramSecretNotFound is returned by a ScramSecretResolver when the secret does not exist.
var ErrScramSecretNotFound = errors.New("secret not found")

var errScramSecretShape = errors.New("secret must be JSON with username and password string keys")

// ScramCredential is the username/password pair stored in an AmazonMSK_ secret.
type ScramCredential struct {
	Username string
	Password string
	// SecretName is the Secrets Manager name; MSK requires the AmazonMSK_ prefix.
	SecretName string
}

// ScramSecretResolver reads SCRAM credentials from the emulator's Secrets Manager.
type ScramSecretResolver interface {
	ResolveScramSecret(ctx context.Context, secretArn string) (ScramCredential, error)
}

// ScramUser is one SCRAM-SHA-512 user to create on a broker.
type ScramUser struct {
	Username string
	Password string
}

// ScramAdmin applies SCRAM user changes to the broker reachable at addr.
type ScramAdmin func(ctx context.Context, addr string, upsert []ScramUser, remove []string) error

// SetScramSecretResolver registers the Secrets Manager reader used to validate and sync SCRAM secrets.
func (b *InMemoryBackend) SetScramSecretResolver(r ScramSecretResolver) {
	b.mu.Lock("SetScramSecretResolver")
	defer b.mu.Unlock()

	b.scramSecretSource = r
}

// validateScramSecrets splits arns into usable secrets and per-secret errors (docker mode with a resolver only).
func (b *InMemoryBackend) validateScramSecrets(ctx context.Context, arns []string) ([]string, []ScramSecretError) {
	b.mu.RLock("validateScramSecrets")
	src, managed := b.scramSecretSource, b.engine != nil
	b.mu.RUnlock()

	if src == nil || !managed {
		return arns, nil
	}

	var (
		ok   []string
		errs []ScramSecretError
	)

	for _, a := range arns {
		cred, err := src.ResolveScramSecret(ctx, a)

		switch {
		case errors.Is(err, ErrScramSecretNotFound):
			errs = append(
				errs,
				ScramSecretError{SecretArn: a, ErrorCode: scramSecretMissing, ErrorMessage: err.Error()},
			)
		case err != nil:
			errs = append(
				errs,
				ScramSecretError{SecretArn: a, ErrorCode: scramInvalidSecret, ErrorMessage: err.Error()},
			)
		case !strings.HasPrefix(cred.SecretName, ScramSecretPrefix):
			errs = append(errs, ScramSecretError{
				SecretArn: a, ErrorCode: scramInvalidSecret,
				ErrorMessage: "secret name must start with " + ScramSecretPrefix,
			})
		default:
			ok = append(ok, a)
		}
	}

	return ok, errs
}

// syncScram makes the broker's SCRAM users match the cluster's associated secrets.
func (b *InMemoryBackend) syncScram(ctx context.Context, clusterArn string) {
	b.mu.RLock("syncScram")
	lb, live := b.managedBrokerLocked(clusterArn)
	src := b.scramSecretSource

	if !live || src == nil || lb.adminAddr == "" || !lb.plan.scram {
		b.mu.RUnlock()

		return
	}

	desired := append([]string(nil), b.scramSecrets[clusterArn]...)
	admin, addr, gate := b.engine.cfg.ScramAdmin, lb.adminAddr, lb.syncGate
	b.mu.RUnlock()

	gate <- struct{}{}

	defer func() { <-gate }()

	b.mu.RLock("syncScramApplied")
	applied := make(map[string]string, len(lb.scramApplied))
	maps.Copy(applied, lb.scramApplied)
	b.mu.RUnlock()

	upsert, next := collectScramUsers(ctx, src, desired)

	var remove []string

	for arn, user := range applied {
		if _, still := next[arn]; !still && !userInSet(next, user) {
			remove = append(remove, user)
		}
	}

	if len(upsert)+len(remove) == 0 {
		return
	}

	if err := admin(ctx, addr, upsert, remove); err != nil {
		logger.Load(ctx).WarnContext(ctx, "kafka: SCRAM user sync failed", "error", err)

		return
	}

	b.mu.Lock("syncScramRecord")
	defer b.mu.Unlock()

	if cur, ok := b.managedBrokerLocked(clusterArn); ok && cur == lb {
		lb.scramApplied = next
	}
}

func collectScramUsers(ctx context.Context, src ScramSecretResolver, arns []string) ([]ScramUser, map[string]string) {
	users := make([]ScramUser, 0, len(arns))
	next := make(map[string]string, len(arns))

	for _, a := range arns {
		cred, err := src.ResolveScramSecret(ctx, a)
		if err != nil || cred.Username == "" || cred.Password == "" {
			logger.Load(ctx).WarnContext(ctx, "kafka: skipping unreadable SCRAM secret", "secret", a)

			continue
		}

		users = append(users, ScramUser{Username: cred.Username, Password: cred.Password})
		next[a] = cred.Username
	}

	return users, next
}

func userInSet(set map[string]string, user string) bool {
	for _, u := range set {
		if u == user {
			return true
		}
	}

	return false
}

// franzScramAdmin applies SCRAM-SHA-512 credential changes through the Kafka admin API.
func franzScramAdmin(ctx context.Context, addr string, upsert []ScramUser, remove []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}

	defer cl.Close()

	req := kmsg.NewPtrAlterUserSCRAMCredentialsRequest()

	for _, u := range upsert {
		salt := make([]byte, scramSaltLen)
		if _, err = rand.Read(salt); err != nil {
			return fmt.Errorf("scram salt: %w", err)
		}

		salted, kerr := pbkdf2.Key(sha512.New, u.Password, salt, scramIterations, sha512.Size)
		if kerr != nil {
			return fmt.Errorf("scram salted password: %w", kerr)
		}

		up := kmsg.NewAlterUserSCRAMCredentialsRequestUpsertion()
		up.Name, up.Mechanism, up.Iterations = u.Username, scramMechSHA512, scramIterations
		up.Salt, up.SaltedPassword = salt, salted
		req.Upsertions = append(req.Upsertions, up)
	}

	for _, name := range remove {
		del := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
		del.Name, del.Mechanism = name, scramMechSHA512
		req.Deletions = append(req.Deletions, del)
	}

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return fmt.Errorf("alter scram credentials: %w", err)
	}

	for _, r := range resp.Results {
		if r.ErrorCode != 0 {
			msg := ""
			if r.ErrorMessage != nil {
				msg = *r.ErrorMessage
			}

			return fmt.Errorf("%w: user %s: code %d %s", errScramAlter, r.User, r.ErrorCode, msg)
		}
	}

	return nil
}

var errScramAlter = errors.New("alter scram credentials rejected")

// ParseScramSecret decodes the {"username","password"} JSON stored in an MSK secret.
func ParseScramSecret(secretString string) (ScramCredential, error) {
	var v struct {
		Username *string `json:"username"`
		Password *string `json:"password"`
	}

	if err := json.Unmarshal([]byte(secretString), &v); err != nil || v.Username == nil || v.Password == nil ||
		*v.Username == "" || *v.Password == "" {
		return ScramCredential{}, errScramSecretShape
	}

	return ScramCredential{Username: *v.Username, Password: *v.Password}, nil
}
