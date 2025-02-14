package all

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

func Test_(t *testing.T) {
	// Token hashing isn't applicable to this test because we never migrate far enough up.
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	ts := newService(t, ctx, 2, false)

	taskBucket := []byte("tasksv1")
	id := "05da585043e02000"
	// create a task that has auth set and no ownerID
	err := ts.Store.Update(context.Background(), func(tx kv.Tx) error {
		b, err := tx.Bucket(taskBucket)
		if err != nil {
			t.Fatal(err)
		}
		taskBody := fmt.Sprintf(`{"id":"05da585043e02000","type":"system","orgID":"05d3ae3492c9c000","org":"whos","authorizationID":"%s","name":"asdf","status":"active","flux":"option v = {\n  bucket: \"bucks\",\n  timeRangeStart: -1h,\n  timeRangeStop: now()\n}\n\noption task = { \n  name: \"asdf\",\n  every: 5m,\n}\n\nfrom(bucket: \"_monitoring\")\n  |\u003e range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |\u003e filter(fn: (r) =\u003e r[\"_measurement\"] == \"boltdb_reads_total\")\n  |\u003e filter(fn: (r) =\u003e r[\"_field\"] == \"counter\")\n  |\u003e to(bucket: \"bucks\", org: \"whos\")","every":"5m","latestCompleted":"2020-06-16T17:01:26.083319Z","latestScheduled":"2020-06-16T17:01:26.083319Z","lastRunStatus":"success","createdAt":"2020-06-15T19:10:29Z","updatedAt":"0001-01-01T00:00:00Z"}`, ts.Auth.ID.String())
		err = b.Put([]byte(id), []byte(taskBody))

		if err != nil {
			t.Fatal(err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = Migration0003_TaskOwnerIDUpMigration.Up(context.Background(), ts.Store)
	if err != nil {
		t.Fatal(err)
	}

	idType, _ := platform.IDFromString(id)
	task, err := ts.Service.FindTaskByID(context.Background(), *idType)
	if err != nil {
		t.Fatal(err)
	}
	if task.OwnerID != ts.User.ID {
		t.Fatal("failed to fill in ownerID")
	}

	// create a task that has no auth or owner id but a urm exists
	err = ts.Store.Update(context.Background(), func(tx kv.Tx) error {
		b, err := tx.Bucket([]byte("tasksv1"))
		if err != nil {
			t.Fatal(err)
		}
		taskBody := fmt.Sprintf(`{"id":"05da585043e02000","type":"system","orgID":"%s","org":"whos","name":"asdf","status":"active","flux":"option v = {\n  bucket: \"bucks\",\n  timeRangeStart: -1h,\n  timeRangeStop: now()\n}\n\noption task = { \n  name: \"asdf\",\n  every: 5m,\n}\n\nfrom(bucket: \"_monitoring\")\n  |\u003e range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |\u003e filter(fn: (r) =\u003e r[\"_measurement\"] == \"boltdb_reads_total\")\n  |\u003e filter(fn: (r) =\u003e r[\"_field\"] == \"counter\")\n  |\u003e to(bucket: \"bucks\", org: \"whos\")","every":"5m","latestCompleted":"2020-06-16T17:01:26.083319Z","latestScheduled":"2020-06-16T17:01:26.083319Z","lastRunStatus":"success","createdAt":"2020-06-15T19:10:29Z","updatedAt":"0001-01-01T00:00:00Z"}`, ts.Org.ID.String())
		err = b.Put([]byte(id), []byte(taskBody))
		if err != nil {
			t.Fatal(err)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = Migration0003_TaskOwnerIDUpMigration.Up(context.Background(), ts.Store)
	if err != nil {
		t.Fatal(err)
	}

	task, err = ts.Service.FindTaskByID(context.Background(), *idType)
	if err != nil {
		t.Fatal(err)
	}
	if task.OwnerID != ts.User.ID {
		t.Fatal("failed to fill in ownerID")
	}
}
