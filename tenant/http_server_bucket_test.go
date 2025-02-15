package tenant_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-chi/chi"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func initBucketHttpService(f itesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	t.Helper()

	s := itesting.NewTestInmemStore(t)

	store := tenant.NewStore(s)
	if f.IDGenerator != nil {
		store.IDGen = f.IDGenerator
	}

	if f.OrgIDs != nil {
		store.OrgIDGen = f.OrgIDs
	}

	if f.BucketIDs != nil {
		store.BucketIDGen = f.BucketIDs
	}

	ctx := context.Background()

	// go direct to storage for test data
	if err := s.Update(ctx, func(tx kv.Tx) error {
		for _, o := range f.Organizations {
			if err := store.CreateOrg(tx.Context(), tx, o); err != nil {
				return err
			}
		}

		for _, b := range f.Buckets {
			if err := store.CreateBucket(tx.Context(), tx, b); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to seed data: %s", err)
	}

	handler := tenant.NewHTTPBucketHandler(zaptest.NewLogger(t), tenant.NewService(store), nil, nil, nil)
	r := chi.NewRouter()
	r.Mount(handler.Prefix(), handler)
	server := httptest.NewServer(r)
	httpClient, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	client := tenant.BucketClientService{
		Client: httpClient,
	}

	return &client, "http_tenant", server.Close
}

func TestHTTPBucketService(t *testing.T) {
	itesting.BucketService(initBucketHttpService, t)
}

const idOne = platform.ID(iota + 1)

func TestHTTPBucketService_InvalidRetention(t *testing.T) {
	type args struct {
		name          string
		id            platform.ID
		retention     int
		shardDuration int
		description   *string
	}
	type wants struct {
		err    error
		bucket *influxdb.Bucket
	}

	tests := []struct {
		name   string
		fields itesting.BucketFields
		args   args
		wants  wants
	}{
		{
			name: "update with negative retention",
			fields: itesting.BucketFields{
				OrgIDs:        mock.NewIncrementingIDGenerator(idOne),
				BucketIDs:     mock.NewIncrementingIDGenerator(idOne),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						// ID(1)
						Name: "theorg",
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						// ID(1)
						OrgID:              idOne,
						Name:               "bucket1",
						RetentionPeriod:    humanize.Day,
						ShardGroupDuration: time.Hour,
					},
					{
						// ID(2)
						OrgID: idOne,
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:        idOne,
				retention: -1,
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EUnprocessableEntity,
					Msg:  "expiration seconds cannot be negative",
				},
			},
		},
		{
			name: "update with negative shard-group duration",
			fields: itesting.BucketFields{
				OrgIDs:        mock.NewIncrementingIDGenerator(idOne),
				BucketIDs:     mock.NewIncrementingIDGenerator(idOne),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						// ID(1)
						Name: "theorg",
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						// ID(1)
						OrgID:              idOne,
						Name:               "bucket1",
						RetentionPeriod:    humanize.Day,
						ShardGroupDuration: time.Hour,
					},
					{
						// ID(2)
						OrgID: idOne,
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:            idOne,
				shardDuration: -1,
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EUnprocessableEntity,
					Msg:  "shard-group duration seconds cannot be negative",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := initBucketHttpService(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := influxdb.BucketUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.retention != 0 {
				d := time.Duration(tt.args.retention) * time.Minute
				upd.RetentionPeriod = &d
			}
			if tt.args.shardDuration != 0 {
				d := time.Duration(tt.args.shardDuration) * time.Minute
				upd.ShardGroupDuration = &d
			}

			upd.Description = tt.args.description

			bucket, err := s.UpdateBucket(ctx, tt.args.id, upd)
			itesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("bucket is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestRetentionNever(t *testing.T) {
	id := mock.NewStaticIDGenerator(idOne)
	orgID := mock.NewStaticIDGenerator(idOne)
	bucket := &influxdb.Bucket{
		ID:                  id.ID(),
		OrgID:               orgID.ID(),
		Type:                influxdb.BucketTypeUser,
		Name:                "bucket",
		Description:         "words",
		RetentionPolicyName: "",
		RetentionPeriod:     time.Duration(60) * time.Minute,
		ShardGroupDuration:  time.Duration(24) * time.Hour,
	}
	store := tenant.NewStore(itesting.NewTestInmemStore(t))
	ctx := context.Background()
	err := store.Update(ctx, func(tx kv.Tx) error {
		if err := store.CreateBucket(tx.Context(), tx, bucket); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed data: %s", err)
	}

	client, closeServer := initHttpClient(t, store)
	defer closeServer()

	var b *influxdb.Bucket
	err = store.View(ctx, func(tx kv.Tx) error {
		b, err = store.GetBucket(tx.Context(), tx, bucket.ID)
		if err != nil {
			return err
		}
		return nil
	})
	bucketUpdate := map[string]any{
		"type":           "user",
		"orgID":          b.OrgID,
		"id":             b.ID,
		"name":           b.Name,
		"retentionRules": []map[string]any{},
	}
	err = client.PatchJSON(bucketUpdate, fmt.Sprintf("/api/v2/buckets/%s", b.ID.String())).Do(ctx)
	if err != nil {
		t.Fatalf("failed to patch bucket: %s", err)
	}

	err = store.View(ctx, func(tx kv.Tx) error {
		b, err = store.GetBucket(tx.Context(), tx, bucket.ID)
		if err != nil {
			return err
		}
		assert.Equal(t, b.RetentionPeriod, time.Duration(0))
		assert.Equal(t, b.ShardGroupDuration, time.Duration(0))
		return nil
	})
	if err != nil {
		t.Fatalf("failed to seed data: %s", err)
	}
}

func initHttpClient(t *testing.T, store *tenant.Store) (*httpc.Client, func()) {
	handler := tenant.NewHTTPBucketHandler(zaptest.NewLogger(t), tenant.NewService(store), nil, nil, nil)
	r := chi.NewRouter()
	r.Mount(handler.Prefix(), handler)
	server := httptest.NewServer(r)

	client, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	return client, server.Close
}
