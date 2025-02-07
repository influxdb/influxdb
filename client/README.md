# InfluxDB Client

[![GoDoc](https://godoc.org/github.com/influxdata/influxdb?status.svg)](http://godoc.org/github.com/influxdata/influxdb1-client/v2)

## Description

**NOTE:** The Go client library now has a "v2" version, with the old version
being deprecated. The new version can be imported at
`import "github.com/influxdata/influxdb/client/v2"`. It is not backwards-compatible.

A Go client library written and maintained by the **InfluxDB** team.
This package provides convenience functions to read and write time series data.
It uses the HTTP protocol to communicate with your **InfluxDB** cluster.


## Getting Started

### Connecting To Your Database

Connecting to an **InfluxDB** database is straightforward. You will need a host
name, a port and the cluster user credentials if applicable. The default port is
8086. You can customize these settings to your specific installation via the
**InfluxDB** configuration file.

Though not necessary for experimentation, you may want to create a new user
and authenticate the connection to your database.

For more information please check out the
[Admin Docs](https://docs.influxdata.com/influxdb/latest/administration/).

For the impatient, you can create a new admin user _bubba_ by firing off the
[InfluxDB CLI](https://github.com/influxdata/influxdb/blob/master/cmd/influx/main.go).

```shell
influx
> create user bubba with password 'bumblebeetuna'
> grant all privileges to bubba
```

And now for good measure set the credentials in you shell environment.
In the example below we will use $INFLUX_USER and $INFLUX_PWD

Now with the administrivia out of the way, let's connect to our database.

NOTE: If you've opted out of creating a user, you can omit Username and Password in
the configuration below.

```go
package main

import (
	"log"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

const (
	MyDB = "square_holes"
	username = "bubba"
	password = "bumblebeetuna"
)


func main() {
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "s",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}

	pt, err := client.NewPoint("cpu_usage", tags, fields, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		log.Fatal(err)
	}

	// Close client resources
	if err := c.Close(); err != nil {
    		log.Fatal(err)
	}
}

```

### Inserting Data

Time series data aka *points* are written to the database using batch inserts.
The mechanism is to create one or more points and then create a batch aka
*batch points* and write these to a given database and series. A series is a
combination of a measurement (time/values) and a set of tags.

In this sample we will create a batch of a 1,000 points. Each point has a time and
a single value as well as 2 tags indicating a shape and color. We write these points
to a database called _square_holes_ using a measurement named _shapes_.

NOTE: You can specify a RetentionPolicy as part of the batch points. If not
provided InfluxDB will use the database _default_ retention policy.

```go

func writePoints(clnt client.Client) {
	sampleSize := 1000

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "systemstats",
		Precision: "us",
	})
	if err != nil {
		log.Fatal(err)
	}

    rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < sampleSize; i++ {
		regions := []string{"us-west1", "us-west2", "us-west3", "us-east1"}
		tags := map[string]string{
			"cpu":    "cpu-total",
			"host":   fmt.Sprintf("host%d", rand.Intn(1000)),
			"region": regions[rand.Intn(len(regions))],
		}

		idle := rand.Float64() * 100.0
		fields := map[string]interface{}{
			"idle": idle,
			"busy": 100.0 - idle,
		}

		pt, err := client.NewPoint(
			"cpu_usage",
			tags,
			fields,
			time.Now(),
		)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)
	}

	if err := clnt.Write(bp); err != nil {
		log.Fatal(err)
	}
}
```

#### Uint64 Support

The `uint64` data type is supported if your server is version `1.4.0` or
greater. To write a data point as an unsigned integer, you must insert
the point as `uint64`. You cannot use `uint` or any of the other
derivatives because previous versions of the client have supported
writing those types as an integer.

### Querying Data

One nice advantage of using **InfluxDB** the ability to query your data using familiar
SQL constructs. In this example we can create a convenience function to query the database
as follows:

```go
// queryDB convenience function to query the database
func queryDB(clnt client.Client, cmd string, params map[string]interface{}) (res []client.Result, err error) {
	q := client.Query{
		Command:    cmd,
		Database:   MyDB,
		Parameters: params,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}
```

#### Parameter support

The special parameters such as `client.Identifier` and `client.IntegerValue` are only supported in version 1.8 or greater.
Versions before 1.8 only support literal types like `string`, `int64`, `float64`, and `bool`.
The below queries cannot use parameters in versions before 1.8 and will have to use `fmt.Sprintf` to construct the query instead.
Constructing queries using `fmt.Sprintf` is unsafe and vulnerable to query injection if they are constructed with user data.

#### Creating a Database

```go
_, err := queryDB(clnt, "CREATE DATABASE $db", client.Params{
	"db": client.Identifier(MyDB),
})
if err != nil {
	log.Fatal(err)
}
```

#### Count Records

```go
q := "SELECT count($field) FROM $m"
res, err := queryDB(clnt, q, client.Params{
	"field": client.Identifier("value"),
	"m":     client.Identifier(MyMeasurement),
})
if err != nil {
	log.Fatal(err)
}
count := res[0].Series[0].Values[0][1]
log.Printf("Found a total of %v records\n", count)
```

#### Find the last 10 _shapes_ records

```go
q := "SELECT * FROM $m LIMIT $n"
res, err = queryDB(clnt, q, client.Params{
	"m": client.Identifier(MyMeasurement),
	"n": 10,
})
if err != nil {
	log.Fatal(err)
}

for i, row := range res[0].Series[0].Values {
	t, err := time.Parse(time.RFC3339, row[0].(string))
	if err != nil {
		log.Fatal(err)
	}
	val := row[1].(string)
	log.Printf("[%2d] %s: %s\n", i, t.Format(time.Stamp), val)
}
```

### Using the UDP Client

The **InfluxDB** client also supports writing over UDP.

```go
func WriteUDP() {
	// Make client
	// PayloadSize is default value(512)
	c, err := client.NewUDPClient(client.UDPConfig{Addr: "localhost:8089"})
	if err != nil {
		panic(err.Error())
	}

	// Create a new point batch
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Precision: "s",
	})

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}
	pt, err := client.NewPoint("cpu_usage", tags, fields, time.Now())
	if err != nil {
		panic(err.Error())
	}
	bp.AddPoint(pt)

	// Write the batch
	c.Write(bp)
}
```

### Point Splitting

The UDP client now supports splitting single points that exceed the configured
payload size. The logic for processing each point is listed here, starting with
an empty payload.

1. If adding the point to the current (non-empty) payload would exceed the
   configured size, send the current payload. Otherwise, add it to the current
   payload.
1. If the point is smaller than the configured size, add it to the payload.
1. If the point has no timestamp, just try to send the entire point as a single
   UDP payload, and process the next point.
1. Since the point has a timestamp, re-use the existing measurement name,
   tagset, and timestamp and create multiple new points by splitting up the
   fields. The per-point length will be kept close to the configured size,
   staying under it if possible. This does mean that one large field, maybe a
   long string, could be sent as a larger-than-configured payload.

The above logic attempts to respect configured payload sizes, but not sacrifice
any data integrity. Points without a timestamp can't be split, as that may
cause fields to have differing timestamps when processed by the server.

## Go Docs

Please refer to
[http://godoc.org/github.com/influxdata/influxdb/client/v2](http://godoc.org/github.com/influxdata/influxdb1-client/v2)
for documentation.

## See Also

You can also examine how the client library is used by the
[InfluxDB CLI](https://github.com/influxdata/influxdb/blob/master/cmd/influx/main.go).
