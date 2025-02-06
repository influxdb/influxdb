package authorization

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

// TenantService is used to look up the Organization and User for an Authorization
type TenantService interface {
	FindOrganizationByID(ctx context.Context, id platform.ID) (*influxdb.Organization, error)
	FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error)
	FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error)
	FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error)
	FindBucketByID(ctx context.Context, id platform.ID) (*influxdb.Bucket, error)
}

type PasswordService interface {
	SetPassword(ctx context.Context, id platform.ID, password string) error
}

type AuthHandler struct {
	chi.Router
	api           *kithttp.API
	log           *zap.Logger
	authSvc       influxdb.AuthorizationService
	passwordSvc   PasswordService
	tenantService TenantService
}

// NewHTTPAuthHandler constructs a new http server.
func NewHTTPAuthHandler(log *zap.Logger, authService influxdb.AuthorizationService, passwordService PasswordService, tenantService TenantService) *AuthHandler {
	h := &AuthHandler{
		api:           kithttp.NewAPI(kithttp.WithLog(log)),
		log:           log,
		authSvc:       authService,
		passwordSvc:   passwordService,
		tenantService: tenantService,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	r.Route("/", func(r chi.Router) {
		r.Post("/", h.handlePostAuthorization)
		r.Get("/", h.handleGetAuthorizations)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.handleGetAuthorization)
			r.Patch("/", h.handleUpdateAuthorization)
			r.Delete("/", h.handleDeleteAuthorization)
			r.Post("/password", h.handlePostUserPassword)
		})
	})

	h.Router = r
	return h
}

const prefixAuthorization = "/private/legacy/authorizations"

func (h *AuthHandler) Prefix() string {
	return prefixAuthorization
}

// handlePostAuthorization is the HTTP handler for the POST prefixAuthorization route.
func (h *AuthHandler) handlePostAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	a, err := decodePostAuthorizationRequest(ctx, r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	user, err := getAuthorizedUser(r, h.tenantService)
	if err != nil {
		h.api.Err(w, r, influxdb.ErrUnableToCreateToken)
		return
	}

	userID := user.ID
	if a.UserID != nil && a.UserID.Valid() {
		userID = *a.UserID
	}

	auth := a.toInfluxdb(userID)

	if err := h.authSvc.CreateAuthorization(ctx, auth); err != nil {
		h.api.Err(w, r, err)
		return
	}

	perms, err := h.newPermissionsResponse(ctx, auth.Permissions)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Auth created ", zap.String("auth", fmt.Sprint(auth)))

	resp, err := h.newAuthResponse(ctx, auth, perms)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusCreated, resp)
}

func getAuthorizedUser(r *http.Request, ts TenantService) (*influxdb.User, error) {
	ctx := r.Context()

	a, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}

	return ts.FindUserByID(ctx, a.GetUserID())
}

type postAuthorizationRequest struct {
	Token       string                `json:"token"`
	Status      influxdb.Status       `json:"status"`
	OrgID       platform.ID           `json:"orgID"`
	UserID      *platform.ID          `json:"userID,omitempty"`
	Description string                `json:"description"`
	Permissions []influxdb.Permission `json:"permissions"`
}

type authResponse struct {
	ID          platform.ID          `json:"id"`
	Token       string               `json:"token"`
	Status      influxdb.Status      `json:"status"`
	Description string               `json:"description"`
	OrgID       platform.ID          `json:"orgID"`
	Org         string               `json:"org"`
	UserID      platform.ID          `json:"userID"`
	User        string               `json:"user"`
	Permissions []permissionResponse `json:"permissions"`
	Links       map[string]string    `json:"links"`
	CreatedAt   time.Time            `json:"createdAt"`
	UpdatedAt   time.Time            `json:"updatedAt"`
}

// In the future, we would like only the service layer to look up the user and org to see if they are valid
// but for now we need to look up the User and Org here because the API expects the response
// to have the names of the Org and User
func (h *AuthHandler) newAuthResponse(ctx context.Context, a *influxdb.Authorization, ps []permissionResponse) (*authResponse, error) {
	org, err := h.tenantService.FindOrganizationByID(ctx, a.OrgID)
	if err != nil {
		h.log.Info("Failed to get org", zap.String("handler", "getAuthorizations"), zap.String("orgID", a.OrgID.String()), zap.Error(err))
		return nil, err
	}
	user, err := h.tenantService.FindUserByID(ctx, a.UserID)
	if err != nil {
		h.log.Info("Failed to get user", zap.String("userID", a.UserID.String()), zap.Error(err))
		return nil, err
	}
	res := &authResponse{
		ID:          a.ID,
		Token:       a.Token,
		Status:      a.Status,
		Description: a.Description,
		OrgID:       a.OrgID,
		UserID:      a.UserID,
		User:        user.Name,
		Org:         org.Name,
		Permissions: ps,
		Links: map[string]string{
			"self": fmt.Sprintf(prefixAuthorization+"/%s", a.ID),
			"user": fmt.Sprintf("/api/v2/users/%s", a.UserID),
		},
		CreatedAt: a.CreatedAt,
		UpdatedAt: a.UpdatedAt,
	}

	return res, nil
}

func (p *postAuthorizationRequest) toInfluxdb(userID platform.ID) *influxdb.Authorization {
	t := &influxdb.Authorization{
		OrgID:       p.OrgID,
		Token:       p.Token,
		Status:      p.Status,
		Description: p.Description,
		Permissions: p.Permissions,
		UserID:      userID,
	}

	return t
}

func (a *authResponse) toInfluxdb() *influxdb.Authorization {
	res := &influxdb.Authorization{
		ID:          a.ID,
		Token:       a.Token,
		Status:      a.Status,
		Description: a.Description,
		OrgID:       a.OrgID,
		UserID:      a.UserID,
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: a.CreatedAt,
			UpdatedAt: a.UpdatedAt,
		},
	}
	for _, p := range a.Permissions {
		res.Permissions = append(res.Permissions, influxdb.Permission{Action: p.Action, Resource: p.Resource.Resource})
	}
	return res
}

type authsResponse struct {
	Links map[string]string `json:"links"`
	Auths []*authResponse   `json:"authorizations"`
}

func newAuthsResponse(as []*authResponse) *authsResponse {
	return &authsResponse{
		// TODO(desa): update links to include paging and filter information
		Links: map[string]string{
			"self": prefixAuthorization,
		},
		Auths: as,
	}
}

func newPostAuthorizationRequest(a *influxdb.Authorization) (*postAuthorizationRequest, error) {
	res := &postAuthorizationRequest{
		OrgID:       a.OrgID,
		Description: a.Description,
		Permissions: a.Permissions,
		Token:       a.Token,
		Status:      a.Status,
	}

	if a.UserID.Valid() {
		res.UserID = &a.UserID
	}

	res.SetDefaults()

	return res, res.Validate()
}

func (p *postAuthorizationRequest) SetDefaults() {
	if p.Status == "" {
		p.Status = influxdb.Active
	}
}

func (p *postAuthorizationRequest) Validate() error {
	if len(p.Permissions) == 0 {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "authorization must include permissions",
		}
	}

	for _, perm := range p.Permissions {
		if err := perm.Valid(); err != nil {
			return &errors.Error{
				Err: err,
			}
		}
	}

	if !p.OrgID.Valid() {
		return &errors.Error{
			Err:  platform.ErrInvalidID,
			Code: errors.EInvalid,
			Msg:  "org id required",
		}
	}

	if p.Status == "" {
		p.Status = influxdb.Active
	}

	if err := p.Status.Valid(); err != nil {
		return err
	}

	if p.Token == "" {
		return &errors.Error{
			Msg:  "token required for v1 user authorization type",
			Code: errors.EInvalid,
		}
	}

	return nil
}

type permissionResponse struct {
	Action   influxdb.Action  `json:"action"`
	Resource resourceResponse `json:"resource"`
}

type resourceResponse struct {
	influxdb.Resource
	Name         string `json:"name,omitempty"`
	Organization string `json:"org,omitempty"`
}

func (h *AuthHandler) newPermissionsResponse(ctx context.Context, ps []influxdb.Permission) ([]permissionResponse, error) {
	res := make([]permissionResponse, len(ps))
	for i, p := range ps {
		res[i] = permissionResponse{
			Action: p.Action,
			Resource: resourceResponse{
				Resource: p.Resource,
			},
		}

		if p.Resource.ID != nil {
			name, err := h.getNameForResource(ctx, p.Resource.Type, *p.Resource.ID)
			if errors.ErrorCode(err) == errors.ENotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			res[i].Resource.Name = name
		}

		if p.Resource.OrgID != nil {
			name, err := h.getNameForResource(ctx, influxdb.OrgsResourceType, *p.Resource.OrgID)
			if errors.ErrorCode(err) == errors.ENotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			res[i].Resource.Organization = name
		}
	}
	return res, nil
}

func (h *AuthHandler) getNameForResource(ctx context.Context, resource influxdb.ResourceType, id platform.ID) (string, error) {
	if err := resource.Valid(); err != nil {
		return "", err
	}

	if ok := id.Valid(); !ok {
		return "", platform.ErrInvalidID
	}

	switch resource {
	case influxdb.BucketsResourceType:
		r, err := h.tenantService.FindBucketByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case influxdb.OrgsResourceType:
		r, err := h.tenantService.FindOrganizationByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case influxdb.UsersResourceType:
		r, err := h.tenantService.FindUserByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	}

	return "", nil
}

func decodePostAuthorizationRequest(ctx context.Context, r *http.Request) (*postAuthorizationRequest, error) {
	a := &postAuthorizationRequest{}
	if err := json.NewDecoder(r.Body).Decode(a); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "invalid json structure",
			Err:  err,
		}
	}

	a.SetDefaults()

	return a, a.Validate()
}

// handleGetAuthorizations is the HTTP handler for the GET prefixAuthorization route.
func (h *AuthHandler) handleGetAuthorizations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetAuthorizationsRequest(ctx, r)
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "getAuthorizations"), zap.Error(err))
		h.api.Err(w, r, err)
		return
	}

	opts := influxdb.FindOptions{}
	as, _, err := h.authSvc.FindAuthorizations(ctx, req.filter, opts)

	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	f := req.filter
	// If the user or org name was provided, look up the ID first
	if f.User != nil {
		u, err := h.tenantService.FindUser(ctx, influxdb.UserFilter{Name: f.User})
		if err != nil {
			h.api.Err(w, r, err)
			return
		}
		f.UserID = &u.ID
	}

	if f.Org != nil {
		o, err := h.tenantService.FindOrganization(ctx, influxdb.OrganizationFilter{Name: f.Org})
		if err != nil {
			h.api.Err(w, r, err)
			return
		}
		f.OrgID = &o.ID
	}

	auths := make([]*authResponse, 0, len(as))
	for _, a := range as {
		ps, err := h.newPermissionsResponse(ctx, a.Permissions)
		if err != nil {
			h.api.Err(w, r, err)
			return
		}

		resp, err := h.newAuthResponse(ctx, a, ps)
		if err != nil {
			h.log.Info("Failed to create auth response", zap.String("handler", "getAuthorizations"))
			continue
		}
		auths = append(auths, resp)
	}

	h.log.Debug("Auths retrieved ", zap.String("auths", fmt.Sprint(auths)))

	h.api.Respond(w, r, http.StatusOK, newAuthsResponse(auths))
}

type getAuthorizationsRequest struct {
	filter influxdb.AuthorizationFilter
}

func decodeGetAuthorizationsRequest(ctx context.Context, r *http.Request) (*getAuthorizationsRequest, error) {
	qp := r.URL.Query()

	req := &getAuthorizationsRequest{}

	userID := qp.Get("userID")
	if userID != "" {
		id, err := platform.IDFromString(userID)
		if err != nil {
			return nil, err
		}
		req.filter.UserID = id
	}

	user := qp.Get("user")
	if user != "" {
		req.filter.User = &user
	}

	orgID := qp.Get("orgID")
	if orgID != "" {
		id, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.OrgID = id
	}

	org := qp.Get("org")
	if org != "" {
		req.filter.Org = &org
	}

	authID := qp.Get("id")
	if authID != "" {
		id, err := platform.IDFromString(authID)
		if err != nil {
			return nil, err
		}
		req.filter.ID = id
	}

	token := qp.Get("token")
	if token != "" {
		req.filter.Token = &token
	}

	return req, nil
}

func (h *AuthHandler) handleGetAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "getAuthorization"), zap.Error(err))
		h.api.Err(w, r, err)
		return
	}

	a, err := h.authSvc.FindAuthorizationByID(ctx, *id)
	if err != nil {
		// Don't log here, it should already be handled by the service
		h.api.Err(w, r, err)
		return
	}

	ps, err := h.newPermissionsResponse(ctx, a.Permissions)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Auth retrieved ", zap.String("auth", fmt.Sprint(a)))

	resp, err := h.newAuthResponse(ctx, a, ps)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, resp)
}

// handleUpdateAuthorization is the HTTP handler for the PATCH /api/v2/authorizations/:id route that updates the authorization's status and desc.
func (h *AuthHandler) handleUpdateAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeUpdateAuthorizationRequest(ctx, r)
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "updateAuthorization"), zap.Error(err))
		h.api.Err(w, r, err)
		return
	}

	a, err := h.authSvc.FindAuthorizationByID(ctx, req.ID)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	a, err = h.authSvc.UpdateAuthorization(ctx, a.ID, req.AuthorizationUpdate)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	ps, err := h.newPermissionsResponse(ctx, a.Permissions)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Auth updated", zap.String("auth", fmt.Sprint(a)))

	resp, err := h.newAuthResponse(ctx, a, ps)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, resp)
}

type updateAuthorizationRequest struct {
	ID platform.ID
	*influxdb.AuthorizationUpdate
}

func decodeUpdateAuthorizationRequest(ctx context.Context, r *http.Request) (*updateAuthorizationRequest, error) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		return nil, err
	}

	upd := &influxdb.AuthorizationUpdate{}
	if err := json.NewDecoder(r.Body).Decode(upd); err != nil {
		return nil, err
	}

	return &updateAuthorizationRequest{
		ID:                  *id,
		AuthorizationUpdate: upd,
	}, nil
}

// handleDeleteAuthorization is the HTTP handler for the DELETE prefixAuthorization/:id route.
func (h *AuthHandler) handleDeleteAuthorization(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "deleteAuthorization"), zap.Error(err))
		h.api.Err(w, r, err)
		return
	}

	if err := h.authSvc.DeleteAuthorization(r.Context(), *id); err != nil {
		// Don't log here, it should already be handled by the service
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Auth deleted", zap.String("authID", fmt.Sprint(id)))

	w.WriteHeader(http.StatusNoContent)
}

// password APIs

type passwordSetRequest struct {
	Password string `json:"password"`
}

// handlePutPassword is the HTTP handler for the PUT /private/legacy/authorizations/:id/password
func (h *AuthHandler) handlePostUserPassword(w http.ResponseWriter, r *http.Request) {
	var body passwordSetRequest
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		})
		return
	}

	param := chi.URLParam(r, "id")
	authID, err := platform.IDFromString(param)
	if err != nil {
		h.api.Err(w, r, &errors.Error{
			Msg: "invalid authorization ID provided in route",
		})
		return
	}

	err = h.passwordSvc.SetPassword(r.Context(), *authID, body.Password)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
