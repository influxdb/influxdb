package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/http/metric"
	"go.uber.org/zap"

	"github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/tsdb"
)

// WriteBackend is all services and associated parameters required to construct
// the WriteHandler.
type WriteBackend struct {
	influxdb.HTTPErrorHandler
	log                *zap.Logger
	WriteEventRecorder metric.EventRecorder

	PointsWriter        storage.PointsWriter
	BucketService       influxdb.BucketService
	OrganizationService influxdb.OrganizationService
}

// NewWriteBackend returns a new instance of WriteBackend.
func NewWriteBackend(log *zap.Logger, b *APIBackend) *WriteBackend {
	return &WriteBackend{
		HTTPErrorHandler:   b.HTTPErrorHandler,
		log:                log,
		WriteEventRecorder: b.WriteEventRecorder,

		PointsWriter:        b.PointsWriter,
		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
	}
}

// WriteHandler receives line protocol and sends to a publish function.
type WriteHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	log *zap.Logger

	BucketService       influxdb.BucketService
	OrganizationService influxdb.OrganizationService

	PointsWriter storage.PointsWriter

	EventRecorder metric.EventRecorder
}

const (
	writePath            = "/api/v2/write"
	errInvalidGzipHeader = "gzipped HTTP body contains an invalid header"
	errInvalidPrecision  = "invalid precision; valid precision units are ns, us, ms, and s"
)

// NewWriteHandler creates a new handler at /api/v2/write to receive line protocol.
func NewWriteHandler(log *zap.Logger, b *WriteBackend) *WriteHandler {
	h := &WriteHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		PointsWriter:        b.PointsWriter,
		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
		EventRecorder:       b.WriteEventRecorder,
	}

	h.HandlerFunc("POST", writePath, h.handleWrite)
	return h
}

func (h *WriteHandler) handleWrite(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "WriteHandler")
	defer span.Finish()

	ctx := r.Context()
	defer r.Body.Close()

	// TODO(desa): I really don't like how we're recording the usage metrics here
	// Ideally this will be moved when we solve https://github.com/influxdata/influxdb/issues/13403
	var orgID influxdb.ID
	var requestBytes int
	sw := newStatusResponseWriter(w)
	w = sw
	defer func() {
		h.EventRecorder.Record(ctx, metric.Event{
			OrgID:         orgID,
			Endpoint:      r.URL.Path, // This should be sufficient for the time being as it should only be single endpoint.
			RequestBytes:  requestBytes,
			ResponseBytes: sw.responseBytes,
			Status:        sw.code(),
		})
	}()

	in := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		var err error
		in, err = gzip.NewReader(r.Body)
		if err != nil {
			h.HandleHTTPError(ctx, &influxdb.Error{
				Code: influxdb.EInvalid,
				Op:   "http/handleWrite",
				Msg:  errInvalidGzipHeader,
				Err:  err,
			}, w)
			return
		}
		defer in.Close()
	}

	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	req, err := decodeWriteRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	log := h.log.With(zap.String("org", req.Org), zap.String("bucket", req.Bucket))

	var org *influxdb.Organization
	org, err = queryOrganization(ctx, r, h.OrganizationService)
	if err != nil {
		log.Info("Failed to find organization", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	orgID = org.ID
	span.LogKV("org_id", orgID)

	var bucket *influxdb.Bucket
	if id, err := influxdb.IDFromString(req.Bucket); err == nil {
		// Decoded ID successfully. Make sure it's a real bucket.
		b, err := h.BucketService.FindBucket(ctx, influxdb.BucketFilter{
			OrganizationID: &org.ID,
			ID:             id,
		})
		if err == nil {
			bucket = b
		} else if influxdb.ErrorCode(err) != influxdb.ENotFound {
			h.HandleHTTPError(ctx, err, w)
			return
		}
	}

	if bucket == nil {
		b, err := h.BucketService.FindBucket(ctx, influxdb.BucketFilter{
			OrganizationID: &org.ID,
			Name:           &req.Bucket,
		})
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		bucket = b
	}
	span.LogKV("bucket_id", bucket.ID)

	p, err := influxdb.NewPermissionAtID(bucket.ID, influxdb.WriteAction, influxdb.BucketsResourceType, org.ID)
	if err != nil {
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInternal,
			Op:   "http/handleWrite",
			Msg:  fmt.Sprintf("unable to create permission for bucket: %v", err),
			Err:  err,
		}, w)
		return
	}

	if !a.Allowed(*p) {
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EForbidden,
			Op:   "http/handleWrite",
			Msg:  "insufficient permissions for write",
		}, w)
		return
	}

	// TODO(jeff): we should be publishing with the org and bucket instead of
	// parsing, rewriting, and publishing, but the interface isn't quite there yet.
	// be sure to remove this when it is there!
	span, _ = tracing.StartSpanFromContextWithOperationName(ctx, "read request body")
	data, err := ioutil.ReadAll(in)
	span.LogKV("request_bytes", len(data))
	span.Finish()
	if err != nil {
		log.Error("Error reading body", zap.Error(err))
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInternal,
			Op:   "http/handleWrite",
			Msg:  fmt.Sprintf("unable to read data: %v", err),
			Err:  err,
		}, w)
		return
	}

	requestBytes = len(data)
	if requestBytes == 0 {
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   "http/handleWrite",
			Msg:  "writing requires points",
		}, w)
		return
	}

	span, _ = tracing.StartSpanFromContextWithOperationName(ctx, "encoding and parsing")
	encoded := tsdb.EncodeName(org.ID, bucket.ID)
	mm := models.EscapeMeasurement(encoded[:])
	points, err := models.ParsePointsWithPrecision(data, mm, time.Now(), req.Precision)
	span.LogKV("values_total", len(points))
	span.Finish()
	if err != nil {
		log.Error("Error parsing points", zap.Error(err))
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  err.Error(),
		}, w)
		return
	}

	if err := h.PointsWriter.WritePoints(ctx, points); err != nil {
		log.Error("Error writing points", zap.Error(err))
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInternal,
			Op:   "http/handleWrite",
			Msg:  "unexpected error writing points to database",
			Err:  err,
		}, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func decodeWriteRequest(ctx context.Context, r *http.Request) (*postWriteRequest, error) {
	qp := r.URL.Query()
	p := qp.Get("precision")
	if p == "" {
		p = "ns"
	}

	if !models.ValidPrecision(p) {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   "http/decodeWriteRequest",
			Msg:  errInvalidPrecision,
		}
	}

	return &postWriteRequest{
		Bucket:    qp.Get("bucket"),
		Org:       qp.Get("org"),
		Precision: p,
	}, nil
}

type postWriteRequest struct {
	Org       string
	Bucket    string
	Precision string
}

// WriteService sends data over HTTP to influxdb via line protocol.
type WriteService struct {
	Addr               string
	Token              string
	Precision          string
	InsecureSkipVerify bool
}

var _ influxdb.WriteService = (*WriteService)(nil)

func (s *WriteService) Write(ctx context.Context, orgID, bucketID influxdb.ID, r io.Reader) error {
	precision := s.Precision
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   "http/Write",
			Msg:  errInvalidPrecision,
		}
	}

	u, err := NewURL(s.Addr, writePath)
	if err != nil {
		return err
	}

	r, err = compressWithGzip(r)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	SetToken(s.Token, req)

	org, err := orgID.Encode()
	if err != nil {
		return err
	}

	bucket, err := bucketID.Encode()
	if err != nil {
		return err
	}

	params := req.URL.Query()
	params.Set("org", string(org))
	params.Set("bucket", string(bucket))
	params.Set("precision", string(precision))
	req.URL.RawQuery = params.Encode()

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp)
}

func compressWithGzip(data io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)
	var err error

	go func() {
		_, err = io.Copy(gw, data)
		gw.Close()
		pw.Close()
	}()

	return pr, err
}
