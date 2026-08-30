package acmpca

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreatePermission creates a permission on the given CA.
func (b *InMemoryBackend) CreatePermission(
	ctx context.Context,
	caARN string,
	principal string,
	sourceAccount string,
	actions []string,
) (*Permission, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn", ErrInvalidArn); err != nil {
		return nil, err
	}

	if principal == "" {
		return nil, fmt.Errorf("%w: Principal is required", ErrInvalidArgs)
	}

	// Per aws-sdk-go-v2's CreatePermissionInput.Principal doc comment: "At this
	// time, the only valid principal is acm.amazonaws.com." Real AWS rejects
	// anything else; gopherstack previously accepted any string.
	if principal != acmServicePrincipal {
		return nil, fmt.Errorf("%w: Principal must be %s", ErrInvalidArgs, acmServicePrincipal)
	}

	if len(actions) == 0 {
		return nil, fmt.Errorf("%w: Actions is required", ErrInvalidArgs)
	}

	for _, action := range actions {
		switch action {
		case actionIssueCertificate, actionGetCertificate, actionListPermissions:
		default:
			return nil, fmt.Errorf("%w: unsupported action %s", ErrInvalidArgs, action)
		}
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreatePermission")
	defer b.mu.Unlock()

	if _, ok := b.caGet(region, caARN); !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if _, exists := b.permissionGet(region, permissionKey(caARN, principal, sourceAccount)); exists {
		return nil, fmt.Errorf(
			"%w: permission for principal %s already exists on CA %s", ErrPermissionAlreadyExists, principal, caARN,
		)
	}

	permission := &Permission{
		CreatedAt:               time.Now().UTC(),
		Actions:                 append([]string(nil), actions...),
		CertificateAuthorityArn: caARN,
		Principal:               principal,
		SourceAccount:           sourceAccount,
		region:                  region,
	}
	b.permissionPut(permission)

	cp := copyPermission(permission)

	return &cp, nil
}

// DeletePermission deletes a permission on the given CA.
func (b *InMemoryBackend) DeletePermission(ctx context.Context, caARN, principal, sourceAccount string) error {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn", ErrInvalidArn); err != nil {
		return err
	}

	if err := validateRequiredParameter(principal, "Principal", ErrInvalidArgs); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePermission")
	defer b.mu.Unlock()

	if _, ok := b.caGet(region, caARN); !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	key := permissionKey(caARN, principal, sourceAccount)
	if _, ok := b.permissionGet(region, key); !ok {
		return fmt.Errorf("%w: permission for principal %s not found", ErrPermissionNotFound, principal)
	}

	b.permissionDelete(region, key)

	return nil
}

// ListPermissions lists permissions on the given CA.
func (b *InMemoryBackend) ListPermissions(
	ctx context.Context, caARN, nextToken string, maxItems int,
) (page.Page[Permission], error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn", ErrInvalidArn); err != nil {
		return page.Page[Permission]{}, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	if _, ok := b.caGet(region, caARN); !ok {
		return page.Page[Permission]{}, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	permissionsForCA := b.permissionsForCA(region, caARN)
	perms := make([]Permission, 0, len(permissionsForCA))
	for _, perm := range permissionsForCA {
		perms = append(perms, copyPermission(perm))
	}

	sort.Slice(perms, func(i, j int) bool {
		if perms[i].Principal == perms[j].Principal {
			return perms[i].SourceAccount < perms[j].SourceAccount
		}

		return perms[i].Principal < perms[j].Principal
	})

	return page.New(perms, nextToken, maxItems, defaultMaxItems), nil
}

func copyPermission(permission *Permission) Permission {
	cp := *permission
	cp.Actions = append([]string(nil), permission.Actions...)

	return cp
}

func permissionKey(caARN, principal, sourceAccount string) string {
	// url.QueryEscape does not emit pipe characters, so "|" remains a safe separator.
	return strings.Join([]string{
		url.QueryEscape(caARN),
		url.QueryEscape(principal),
		url.QueryEscape(sourceAccount),
	}, "|")
}
