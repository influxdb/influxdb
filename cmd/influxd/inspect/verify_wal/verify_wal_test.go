package verify_wal

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

type testInfo struct {
	t           *testing.T
	path        string
	expectedOut string
	expectErr   bool
	withStdErr  bool
}

func TestVerifies_InvalidFileType(t *testing.T) {
	path := t.TempDir()

	f, err := os.CreateTemp(path, "verifywaltest*"+".txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	runCommand(testInfo{
		t:           t,
		path:        path,
		expectedOut: "no WAL files found in directory",
		expectErr:   true,
	})
}

func TestVerifies_InvalidNotDir(t *testing.T) {
	_, file := newTempWALInvalid(t, true)

	runCommand(testInfo{
		t:           t,
		path:        file.Name(),
		expectedOut: "is not a directory",
		expectErr:   true,
	})
}

func TestVerifies_InvalidEmptyFile(t *testing.T) {
	path, _ := newTempWALInvalid(t, true)

	runCommand(testInfo{
		t:           t,
		path:        path,
		expectedOut: "no WAL entries found",
		withStdErr:  true,
	})
}

func TestVerifies_Invalid(t *testing.T) {
	path, _ := newTempWALInvalid(t, false)

	runCommand(testInfo{
		t:           t,
		path:        path,
		expectedOut: "corrupt entry found at position",
		withStdErr:  true,
	})
}

func TestVerifies_Valid(t *testing.T) {
	path := newTempWALValid(t)

	runCommand(testInfo{
		t:           t,
		path:        path,
		expectedOut: "clean",
		withStdErr:  true,
	})
}

func runCommand(args testInfo) {
	verify := NewVerifyWALCommand()
	verify.SetArgs([]string{"--wal-path", args.path, "--verbose"})

	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetErr(b)
	if args.withStdErr {
		verify.SetErr(b)
	}

	if args.expectErr {
		require.Error(args.t, verify.Execute())
	} else {
		require.NoError(args.t, verify.Execute())
	}

	out, err := io.ReadAll(b)
	require.NoError(args.t, err)
	require.Contains(args.t, string(out), args.expectedOut)
}

func newTempWALValid(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	w := tsm1.NewWAL(dir, 0, 0, tsdb.EngineTags{})
	require.NoError(t, w.Open())
	t.Cleanup(func() {
		require.NoError(t, w.Close())
	})

	p1 := tsm1.NewValue(1, 1.1)
	p2 := tsm1.NewValue(1, int64(1))
	p3 := tsm1.NewValue(1, true)
	p4 := tsm1.NewValue(1, "string")
	p5 := tsm1.NewValue(1, ^uint64(0))

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#float":    {p1},
		"cpu,host=A#!~#int":      {p2},
		"cpu,host=A#!~#bool":     {p3},
		"cpu,host=A#!~#string":   {p4},
		"cpu,host=A#!~#unsigned": {p5},
	}

	_, err := w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	return dir
}

func newTempWALInvalid(t *testing.T, empty bool) (string, *os.File) {
	t.Helper()

	dir := t.TempDir()

	file, err := os.CreateTemp(dir, "verifywaltest*."+tsm1.WALFileExtension)
	require.NoError(t, err)
	t.Cleanup(func() { file.Close() })

	if !empty {
		written, err := file.Write([]byte("foobar"))
		require.NoError(t, err)
		require.Equal(t, 6, written)
	}

	return dir, file
}
