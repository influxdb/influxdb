package tenant

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

// BucketHandler represents an HTTP API handler for users.
type BucketHandler struct {
	chi.Router
	api       *kithttp.API
	log       *zap.Logger
	bucketSvc influxdb.BucketService
	labelSvc  influxdb.LabelService // we may need this for now but we dont want it permanently
}

const (
	prefixBuckets = "/api/v2/buckets"
)

// NewHTTPBucketHandler constructs a new http server.
func NewHTTPBucketHandler(log *zap.Logger, bucketSvc influxdb.BucketService, labelSvc influxdb.LabelService, urmHandler, labelHandler http.Handler) *BucketHandler {
	svr := &BucketHandler{
		api:       kithttp.NewAPI(kithttp.WithLog(log)),
		log:       log,
		bucketSvc: bucketSvc,
		labelSvc:  labelSvc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	// RESTy routes for "articles" resource
	r.Route("/", func(r chi.Router) {
		r.Post("/", svr.handlePostBucket)
		r.Get("/", svr.handleGetBuckets)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", svr.handleGetBucket)
			r.Patch("/", svr.handlePatchBucket)
			r.Delete("/", svr.handleDeleteBucket)

			// mount embedded resources
			mountableRouter := r.With(kithttp.ValidResource(svr.api, svr.lookupOrgByBucketID))
			mountableRouter.Mount("/members", urmHandler)
			mountableRouter.Mount("/owners", urmHandler)
			mountableRouter.Mount("/labels", labelHandler)
		})
	})

	svr.Router = r
	return svr
}

func (h *BucketHandler) Prefix() string {
	return prefixBuckets
}

// bucket is used for serialization/deserialization with duration string syntax.
type bucket struct {
	ID                  platform.ID     `json:"id,omitempty"`
	OrgID               platform.ID     `json:"orgID,omitempty"`
	Type                string          `json:"type"`
	Description         string          `json:"description,omitempty"`
	Name                string          `json:"name"`
	RetentionPolicyName string          `json:"rp,omitempty"` // This to support v1 sources
	RetentionRules      []retentionRule `json:"retentionRules"`
	influxdb.CRUDLog
}

// retentionRule is the retention rule action for a bucket.
type retentionRule struct {
	Type                      string `json:"type"`
	EverySeconds              int64  `json:"everySeconds"`
	ShardGroupDurationSeconds int64  `json:"shardGroupDurationSeconds"`
}

func (b *bucket) toInfluxDB() *influxdb.Bucket {
	if b == nil {
		return nil
	}

	var rpDuration time.Duration // zero value implies infinite retention policy
	var sgDuration time.Duration // zero value implies the server should pick a value

	// Only support a single retention period for the moment
	if len(b.RetentionRules) > 0 {
		rpDuration = time.Duration(b.RetentionRules[0].EverySeconds) * time.Second
		sgDuration = time.Duration(b.RetentionRules[0].ShardGroupDurationSeconds) * time.Second
	}

	return &influxdb.Bucket{
		ID:                  b.ID,
		OrgID:               b.OrgID,
		Type:                influxdb.ParseBucketType(b.Type),
		Description:         b.Description,
		Name:                b.Name,
		RetentionPolicyName: b.RetentionPolicyName,
		RetentionPeriod:     rpDuration,
		ShardGroupDuration:  sgDuration,
		CRUDLog:             b.CRUDLog,
	}
}

func newBucket(pb *influxdb.Bucket) *bucket {
	if pb == nil {
		return nil
	}

	bkt := bucket{
		ID:                  pb.ID,
		OrgID:               pb.OrgID,
		Type:                pb.Type.String(),
		Name:                pb.Name,
		Description:         pb.Description,
		RetentionPolicyName: pb.RetentionPolicyName,
		RetentionRules:      []retentionRule{},
		CRUDLog:             pb.CRUDLog,
	}

	// Only append a retention rule if the user wants to explicitly set
	// a parameter on the rule.
	//
	// This is for backwards-compatibility with older versions of the API,
	// which didn't support setting shard-group durations and used an empty
	// array of rules to represent infinite retention.
	if pb.RetentionPeriod > 0 || pb.ShardGroupDuration > 0 {
		bkt.RetentionRules = append(bkt.RetentionRules, retentionRule{
			Type:                      "expire",
			EverySeconds:              int64(pb.RetentionPeriod.Round(time.Second) / time.Second),
			ShardGroupDurationSeconds: int64(pb.ShardGroupDuration.Round(time.Second) / time.Second),
		})
	}

	return &bkt
}

type retentionRuleUpdate struct {
	Type                      string `json:"type"`
	EverySeconds              *int64 `json:"everySeconds"`
	ShardGroupDurationSeconds *int64 `json:"shardGroupDurationSeconds"`
}

// bucketUpdate is used for serialization/deserialization with retention rules.
type bucketUpdate struct {
	Name           *string               `json:"name,omitempty"`
	Description    *string               `json:"description,omitempty"`
	RetentionRules []retentionRuleUpdate `json:"retentionRules,omitempty"`
}

func (b *bucketUpdate) OK() error {
	if len(b.RetentionRules) > 1 {
		return &errors.Error{
			Code: errors.EUnprocessableEntity,
			Msg:  "buckets cannot have more than one retention rule at this time",
		}
	}

	if len(b.RetentionRules) > 0 {
		rule := b.RetentionRules[0]
		if rule.EverySeconds != nil && *rule.EverySeconds < 0 {
			return &errors.Error{
				Code: errors.EUnprocessableEntity,
				Msg:  "expiration seconds cannot be negative",
			}
		}
		if rule.ShardGroupDurationSeconds != nil && *rule.ShardGroupDurationSeconds < 0 {
			return &errors.Error{
				Code: errors.EUnprocessableEntity,
				Msg:  "shard-group duration seconds cannot be negative",
			}
		}
	}

	return nil
}

func (b *bucketUpdate) toInfluxDB() *influxdb.BucketUpdate {
	if b == nil {
		return nil
	}

	upd := influxdb.BucketUpdate{
		Name:        b.Name,
		Description: b.Description,
	}

	// For now, only use a single retention rule.
	if len(b.RetentionRules) > 0 {
		rule := b.RetentionRules[0]
		if rule.EverySeconds != nil {
			rp := time.Duration(*rule.EverySeconds) * time.Second
			upd.RetentionPeriod = &rp
		}
		if rule.ShardGroupDurationSeconds != nil {
			sgd := time.Duration(*rule.ShardGroupDurationSeconds) * time.Second
			upd.ShardGroupDuration = &sgd
		}
	} else if len(b.RetentionRules) == 0 {
		rp := time.Duration(0)
		upd.RetentionPeriod = &rp
		upd.ShardGroupDuration = &rp
	}

	return &upd
}

func newBucketUpdate(pb *influxdb.BucketUpdate) *bucketUpdate {
	if pb == nil {
		return nil
	}

	up := &bucketUpdate{
		Name:           pb.Name,
		Description:    pb.Description,
		RetentionRules: []retentionRuleUpdate{},
	}

	if pb.RetentionPeriod == nil && pb.ShardGroupDuration == nil {
		return up
	}

	rule := retentionRuleUpdate{Type: "expire"}

	if pb.RetentionPeriod != nil {
		rp := int64((*pb.RetentionPeriod).Round(time.Second) / time.Second)
		rule.EverySeconds = &rp
	}
	if pb.ShardGroupDuration != nil {
		sgd := int64((*pb.ShardGroupDuration).Round(time.Second) / time.Second)
		rule.ShardGroupDurationSeconds = &sgd
	}

	up.RetentionRules = append(up.RetentionRules, rule)
	return up
}

type bucketResponse struct {
	bucket
	Links  map[string]string `json:"links"`
	Labels []influxdb.Label  `json:"labels"`
}

func NewBucketResponse(b *influxdb.Bucket, labels ...*influxdb.Label) *bucketResponse {
	res := &bucketResponse{
		Links: map[string]string{
			"self":    fmt.Sprintf("/api/v2/buckets/%s", b.ID),
			"org":     fmt.Sprintf("/api/v2/orgs/%s", b.OrgID),
			"members": fmt.Sprintf("/api/v2/buckets/%s/members", b.ID),
			"owners":  fmt.Sprintf("/api/v2/buckets/%s/owners", b.ID),
			"labels":  fmt.Sprintf("/api/v2/buckets/%s/labels", b.ID),
			"write":   fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", b.OrgID, b.ID),
		},
		bucket: *newBucket(b),
		Labels: []influxdb.Label{},
	}
	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

type bucketsResponse struct {
	Links   *influxdb.PagingLinks `json:"links"`
	Buckets []*bucketResponse     `json:"buckets"`
}

func newBucketsResponse(ctx context.Context, opts influxdb.FindOptions, f influxdb.BucketFilter, bs []*influxdb.Bucket, labelSvc influxdb.LabelService) *bucketsResponse {
	rs := make([]*bucketResponse, 0, len(bs))
	for _, b := range bs {
		var labels []*influxdb.Label
		if labelSvc != nil { // allow for no label svc
			labels, _ = labelSvc.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: b.ID, ResourceType: influxdb.BucketsResourceType})
		}
		rs = append(rs, NewBucketResponse(b, labels...))
	}
	return &bucketsResponse{
		Links:   influxdb.NewPagingLinks(prefixBuckets, opts, f, len(bs)),
		Buckets: rs,
	}
}

// handlePostBucket is the HTTP handler for the POST /api/v2/buckets route.
func (h *BucketHandler) handlePostBucket(w http.ResponseWriter, r *http.Request) {
	var b postBucketRequest
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}
	if err := b.OK(); err != nil {
		h.api.Err(w, r, err)
		return
	}

	bucket := b.toInfluxDB()

	if err := h.bucketSvc.CreateBucket(r.Context(), bucket); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Bucket created", zap.String("bucket", fmt.Sprint(bucket)))

	h.api.Respond(w, r, http.StatusCreated, NewBucketResponse(bucket))
}

type postBucketRequest struct {
	OrgID               platform.ID     `json:"orgID,omitempty"`
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	RetentionPolicyName string          `json:"rp,omitempty"` // This to support v1 sources
	RetentionRules      []retentionRule `json:"retentionRules"`
}

func (b *postBucketRequest) OK() error {
	if !b.OrgID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "organization id must be provided",
		}
	}

	if len(b.RetentionRules) > 1 {
		return &errors.Error{
			Code: errors.EUnprocessableEntity,
			Msg:  "buckets cannot have more than one retention rule at this time",
		}
	}

	if len(b.RetentionRules) > 0 {
		rule := b.RetentionRules[0]

		if rule.EverySeconds < 0 {
			return &errors.Error{
				Code: errors.EUnprocessableEntity,
				Msg:  "expiration seconds cannot be negative",
			}
		}
		if rule.ShardGroupDurationSeconds < 0 {
			return &errors.Error{
				Code: errors.EUnprocessableEntity,
				Msg:  "shard-group duration seconds cannot be negative",
			}
		}
	}

	return nil
}

func (b postBucketRequest) toInfluxDB() *influxdb.Bucket {
	// Only support a single retention period for the moment
	var rpDur time.Duration
	var sgDur time.Duration
	if len(b.RetentionRules) > 0 {
		rule := b.RetentionRules[0]
		rpDur = time.Duration(rule.EverySeconds) * time.Second
		sgDur = time.Duration(rule.ShardGroupDurationSeconds) * time.Second
	}

	return &influxdb.Bucket{
		OrgID:               b.OrgID,
		Description:         b.Description,
		Name:                b.Name,
		Type:                influxdb.BucketTypeUser,
		RetentionPolicyName: b.RetentionPolicyName,
		RetentionPeriod:     rpDur,
		ShardGroupDuration:  sgDur,
	}
}

// handleGetBucket is the HTTP handler for the GET /api/v2/buckets/:id route.
func (h *BucketHandler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	b, err := h.bucketSvc.FindBucketByID(ctx, *id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket retrieved", zap.String("bucket", fmt.Sprint(b)))
	var labels []*influxdb.Label
	if h.labelSvc != nil { // allow for no label svc
		labels, _ = h.labelSvc.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: b.ID, ResourceType: influxdb.BucketsResourceType})
	}

	h.api.Respond(w, r, http.StatusOK, NewBucketResponse(b, labels...))
}

// handleDeleteBucket is the HTTP handler for the DELETE /api/v2/buckets/:id route.
func (h *BucketHandler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.bucketSvc.DeleteBucket(r.Context(), *id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket deleted", zap.String("bucketID", id.String()))

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

// handleGetBuckets is the HTTP handler for the GET /api/v2/buckets route.
func (h *BucketHandler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	bucketsRequest, err := decodeGetBucketsRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	bs, _, err := h.bucketSvc.FindBuckets(r.Context(), bucketsRequest.filter, bucketsRequest.opts)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Buckets retrieved", zap.String("buckets", fmt.Sprint(bs)))

	h.api.Respond(w, r, http.StatusOK, newBucketsResponse(r.Context(), bucketsRequest.opts, bucketsRequest.filter, bs, h.labelSvc))
}

type getBucketsRequest struct {
	filter influxdb.BucketFilter
	opts   influxdb.FindOptions
}

func decodeGetBucketsRequest(r *http.Request) (*getBucketsRequest, error) {
	qp := r.URL.Query()
	req := &getBucketsRequest{}

	opts, err := influxdb.DecodeFindOptions(r)
	if err != nil {
		return nil, err
	}

	req.opts = *opts

	if orgID := qp.Get("orgID"); orgID != "" {
		id, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.OrganizationID = id
	}

	if org := qp.Get("org"); org != "" {
		req.filter.Org = &org
	}

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	if bucketID := qp.Get("id"); bucketID != "" {
		id, err := platform.IDFromString(bucketID)
		if err != nil {
			return nil, err
		}
		req.filter.ID = id
	}

	return req, nil
}

// handlePatchBucket is the HTTP handler for the PATCH /api/v2/buckets route.
func (h *BucketHandler) handlePatchBucket(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	var reqBody bucketUpdate
	if err := h.api.DecodeJSON(r.Body, &reqBody); err != nil {
		h.api.Err(w, r, err)
		return
	}

	if reqBody.Name != nil {
		b, err := h.bucketSvc.FindBucketByID(r.Context(), *id)
		if err != nil {
			h.api.Err(w, r, err)
			return
		}
		b.Name = *reqBody.Name
	}

	b, err := h.bucketSvc.UpdateBucket(r.Context(), *id, *reqBody.toInfluxDB())
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket updated", zap.String("bucket", fmt.Sprint(b)))

	h.api.Respond(w, r, http.StatusOK, NewBucketResponse(b))
}

func (h *BucketHandler) lookupOrgByBucketID(ctx context.Context, id platform.ID) (platform.ID, error) {
	b, err := h.bucketSvc.FindBucketByID(ctx, id)
	if err != nil {
		return 0, err
	}
	return b.OrgID, nil
}
