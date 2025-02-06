package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// AuthorizationKind is returned by (*Authorization).Kind().
const AuthorizationKind = "authorization"

// ErrUnableToCreateToken sanitized error message for all errors when a user cannot create a token
var ErrUnableToCreateToken = &errors.Error{
	Msg:  "unable to create token",
	Code: errors.EInvalid,
}

// Authorization is an authorization. 🎉
type Authorization struct {
	ID          platform.ID  `json:"id"`
	Token       string       `json:"token,omitempty"`
	HashedToken string       `json:"hashedToken,omitempty"`
	Status      Status       `json:"status"`
	Description string       `json:"description"`
	OrgID       platform.ID  `json:"orgID"`
	UserID      platform.ID  `json:"userID,omitempty"`
	Permissions []Permission `json:"permissions"`
	CRUDLog
}

// AuthorizationUpdate is the authorization update request.
type AuthorizationUpdate struct {
	Status      *Status `json:"status,omitempty"`
	Description *string `json:"description,omitempty"`
}

// Valid ensures that the authorization is valid.
func (a *Authorization) Valid() error {
	for _, p := range a.Permissions {
		if p.Resource.OrgID != nil && *p.Resource.OrgID != a.OrgID {
			return &errors.Error{
				Msg:  fmt.Sprintf("permission %s is not for org id %s", p, a.OrgID),
				Code: errors.EInvalid,
			}
		}
	}

	return nil
}

// PermissionSet returns the set of permissions associated with the Authorization.
func (a *Authorization) PermissionSet() (PermissionSet, error) {
	if !a.IsActive() {
		return nil, &errors.Error{
			Code: errors.EUnauthorized,
			Msg:  "token is inactive",
		}
	}

	return a.Permissions, nil
}

// IsActive is a stub for idpe.
func IsActive(a *Authorization) bool {
	return a.IsActive()
}

// IsActive returns true if the authorization active.
func (a *Authorization) IsActive() bool {
	return a.Status == Active
}

// GetUserID returns the user id.
func (a *Authorization) GetUserID() platform.ID {
	return a.UserID
}

// Kind returns session and is used for auditing.
func (a *Authorization) Kind() string { return AuthorizationKind }

// Identifier returns the authorizations ID and is used for auditing.
func (a *Authorization) Identifier() platform.ID { return a.ID }

// auth service op
const (
	OpFindAuthorizationByID    = "FindAuthorizationByID"
	OpFindAuthorizationByToken = "FindAuthorizationByToken"
	OpFindAuthorizations       = "FindAuthorizations"
	OpCreateAuthorization      = "CreateAuthorization"
	OpUpdateAuthorization      = "UpdateAuthorization"
	OpDeleteAuthorization      = "DeleteAuthorization"
)

// AuthorizationService represents a service for managing authorization data.
type AuthorizationService interface {
	// Returns a single authorization by ID.
	FindAuthorizationByID(ctx context.Context, id platform.ID) (*Authorization, error)

	// Returns a single authorization by Token.
	FindAuthorizationByToken(ctx context.Context, t string) (*Authorization, error)

	// Returns a list of authorizations that match filter and the total count of matching authorizations.
	// Additional options provide pagination & sorting.
	FindAuthorizations(ctx context.Context, filter AuthorizationFilter, opt ...FindOptions) ([]*Authorization, int, error)

	// Creates a new authorization and sets a.Token and a.UserID with the new identifier.
	CreateAuthorization(ctx context.Context, a *Authorization) error

	// UpdateAuthorization updates the status and description if available.
	UpdateAuthorization(ctx context.Context, id platform.ID, upd *AuthorizationUpdate) (*Authorization, error)

	// Removes a authorization by token.
	DeleteAuthorization(ctx context.Context, id platform.ID) error
}

// AuthorizationFilter represents a set of filter that restrict the returned results.
type AuthorizationFilter struct {
	Token *string
	ID    *platform.ID

	UserID *platform.ID
	User   *string

	OrgID *platform.ID
	Org   *string
}
