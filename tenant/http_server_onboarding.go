package tenant

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

// OnboardHandler represents an HTTP API handler for users.
type OnboardHandler struct {
	chi.Router
	api           *kithttp.API
	log           *zap.Logger
	onboardingSvc influxdb.OnboardingService
}

const (
	prefixOnboard = "/api/v2/setup"
)

// NewHTTPOnboardHandler constructs a new http server.
func NewHTTPOnboardHandler(log *zap.Logger, onboardSvc influxdb.OnboardingService) *OnboardHandler {
	svr := &OnboardHandler{
		api:           kithttp.NewAPI(kithttp.WithLog(log)),
		log:           log,
		onboardingSvc: onboardSvc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	// RESTy routes for "articles" resource
	r.Route("/", func(r chi.Router) {
		r.Post("/", svr.handleInitialOnboardRequest)
		r.Get("/", svr.handleIsOnboarding)

	})

	svr.Router = r
	return svr
}

func (h *OnboardHandler) Prefix() string {
	return prefixOnboard
}

type isOnboardingResponse struct {
	Allowed bool `json:"allowed"`
}

// isOnboarding is the HTTP handler for the POST /api/v2/setup route.
func (h *OnboardHandler) handleIsOnboarding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	result, err := h.onboardingSvc.IsOnboarding(ctx)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Onboarding eligibility check finished", zap.String("result", fmt.Sprint(result)))

	h.api.Respond(w, r, http.StatusOK, isOnboardingResponse{result})
}

// handleInitialOnboardRequest is the HTTP handler for the GET /api/v2/setup route.
func (h *OnboardHandler) handleInitialOnboardRequest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &influxdb.OnboardingRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		h.api.Err(w, r, err)
		return
	}
	results, err := h.onboardingSvc.OnboardInitialUser(ctx, req)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Onboarding setup completed", zap.String("results", fmt.Sprint(results)))

	h.api.Respond(w, r, http.StatusCreated, NewOnboardingResponse(results))
}

type onboardingResponse struct {
	User         *influxdb.UserResponse `json:"user"`
	Bucket       *bucketResponse        `json:"bucket"`
	Organization orgResponse            `json:"org"`
	Auth         *authResponse          `json:"auth"`
}

func NewOnboardingResponse(results *influxdb.OnboardingResults) *onboardingResponse {
	return &onboardingResponse{
		User:         newUserResponse(results.User),
		Bucket:       NewBucketResponse(results.Bucket),
		Organization: newOrgResponse(*results.Org),
		Auth:         newAuthResponse(results.Auth),
	}
}

type authResponse struct {
	influxdb.Authorization
	Links map[string]string `json:"links"`
}

func newAuthResponse(a *influxdb.Authorization) *authResponse {
	if a == nil {
		return nil
	}

	res := &authResponse{
		Authorization: *a,
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/authorizations/%s", a.ID),
			"user": fmt.Sprintf("/api/v2/users/%s", a.UserID),
		},
	}
	return res
}

func (a *authResponse) toPlatform() *influxdb.Authorization {
	res := &influxdb.Authorization{
		ID:          a.ID,
		Token:       a.Token,
		HashedToken: a.HashedToken,
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
		res.Permissions = append(res.Permissions, influxdb.Permission{Action: p.Action, Resource: p.Resource})
	}
	return res
}
