package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
)

func newBucketServiceWithOneBucket(bucket platform.Bucket) platform.BucketService {
	bs := mock.NewBucketService()
	bs.FindBucketFn = func(ctx context.Context, bucketFilter platform.BucketFilter) (*platform.Bucket, error) {
		if *bucketFilter.Name == bucket.Name {
			return &bucket, nil
		}

		return nil, errors.New("unknown bucket")
	}

	return bs
}

func TestPreAuthorizer_PreAuthorize(t *testing.T) {
	// TODO(adam) add this test back when BucketsAccessed is restored for the from function
	// https://github.com/influxdata/flux/issues/114
	t.Skip("https://github.com/influxdata/flux/issues/114")
	ctx := context.Background()
	now := time.Now().UTC()

	q := `from(bucket:"my_bucket") |> range(start:-2h) |> yield()`
	spec, err := flux.Compile(ctx, q, now)
	if err != nil {
		t.Fatalf("Error compiling query: %v", err)
	}

	// Try to pre-authorize with bucket service with no buckets
	// and no authorization
	auth := &platform.Authorization{Status: platform.Active}
	emptyBucketService := mock.NewBucketService()
	preAuthorizer := query.NewPreAuthorizer(emptyBucketService)

	err = preAuthorizer.PreAuthorize(ctx, spec, auth)
	if diagnostic := cmp.Diff("Bucket service returned nil bucket", err.Error()); diagnostic != "" {
		t.Errorf("Authorize message mismatch: -want/+got:\n%v", diagnostic)
	}

	// Try to authorize with a bucket service that knows about one bucket
	// (still no authorization)
	id, _ := platform.IDFromString("deadbeefdeadbeef")
	bucketService := newBucketServiceWithOneBucket(platform.Bucket{
		Name: "my_bucket",
		ID:   *id,
	})

	preAuthorizer = query.NewPreAuthorizer(bucketService)
	err = preAuthorizer.PreAuthorize(ctx, spec, auth)
	if diagnostic := cmp.Diff(`No read permission for bucket: "my_bucket"`, err.Error()); diagnostic != "" {
		t.Errorf("Authorize message mismatch: -want/+got:\n%v", diagnostic)
	}

	orgID := platform.ID(1)
	p, err := platform.NewPermissionAtID(*id, platform.ReadAction, platform.BucketsResourceType, orgID)
	if err != nil {
		t.Fatalf("Error creating read bucket permission query: %v", err)
	}
	// Try to authorize with read permission on bucket
	auth = &platform.Authorization{
		Status:      platform.Active,
		Permissions: []platform.Permission{*p},
	}

	err = preAuthorizer.PreAuthorize(ctx, spec, auth)
	if err != nil {
		t.Errorf("Expected successful authorization, but got error: \"%v\"", err.Error())
	}
}

func TestPreAuthorizer_RequiredPermissions(t *testing.T) {
	ctx := context.Background()

	i := inmem.NewService()

	o := platform.Organization{Name: "o"}
	if err := i.CreateOrganization(ctx, &o); err != nil {
		t.Fatal(err)
	}
	bFrom := platform.Bucket{Name: "b-from", OrganizationID: o.ID}
	if err := i.CreateBucket(ctx, &bFrom); err != nil {
		t.Fatal(err)
	}
	bTo := platform.Bucket{Name: "b-to", OrganizationID: o.ID}
	if err := i.CreateBucket(ctx, &bTo); err != nil {
		t.Fatal(err)
	}

	const script = `from(bucket:"b-from") |> range(start:-1m) |> to(bucket:"b-to", org:"o")`
	spec, err := flux.Compile(ctx, script, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	preAuthorizer := query.NewPreAuthorizer(i)
	perms, err := preAuthorizer.RequiredPermissions(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}

	pWrite, err := platform.NewPermissionAtID(bTo.ID, platform.WriteAction, platform.BucketsResourceType, o.ID)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("WARNING: this test does not validate permissions on the 'from' bucket. Please update after https://github.com/influxdata/flux/issues/114.")

	exp := []platform.Permission{*pWrite}
	if diff := cmp.Diff(exp, perms); diff != "" {
		t.Fatalf("unexpected permissions: %s", diff)
	}
}
