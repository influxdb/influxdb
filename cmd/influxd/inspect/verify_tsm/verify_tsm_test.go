package verify_tsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

func TestInvalidChecksum(t *testing.T) {
	path := newChecksumTest(t, true)
	defer os.RemoveAll(path)

	verify := NewTSMVerifyCommand()
	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetErr(b)
	verify.SetArgs([]string{"--engine-path", path})
	require.NoError(t, verify.Execute())

	out, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), "Broken Blocks: 1 / 1")
}

func TestValidChecksum(t *testing.T) {
	path := newChecksumTest(t, false)
	defer os.RemoveAll(path)

	verify := NewTSMVerifyCommand()
	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetErr(b)
	verify.SetArgs([]string{"--engine-path", path})
	require.NoError(t, verify.Execute())

	out, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), "Broken Blocks: 0 / 1")
}

func TestInvalidUTF8(t *testing.T) {
	path := newUTFTest(t, true)
	defer os.RemoveAll(path)

	verify := NewTSMVerifyCommand()
	verify.SetOut(bytes.NewBufferString(""))
	verify.SetArgs([]string{"--engine-path", path, "--check-utf8"})
	require.Error(t, verify.Execute())
}

func TestValidUTF8(t *testing.T) {
	path := newUTFTest(t, false)
	defer os.RemoveAll(path)

	verify := NewTSMVerifyCommand()
	b := bytes.NewBufferString("")
	verify.SetOut(b)
	verify.SetErr(b)
	verify.SetArgs([]string{"--engine-path", path, "--check-utf8"})
	require.NoError(t, verify.Execute())

	out, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), "Invalid Keys: 0 / 1")
}

func newUTFTest(t *testing.T, withError bool) string {
	t.Helper()

	dir := t.TempDir()

	f, err := os.CreateTemp(dir, "verifytsmtest*"+"."+tsm1.TSMFileExtension)
	require.NoError(t, err)

	w, err := tsm1.NewTSMWriter(f)
	require.NoError(t, err)
	defer w.Close()

	values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	require.NoError(t, w.Write([]byte("cpu"), values))

	if withError {
		require.NoError(t, binary.Write(f, binary.BigEndian, []byte("foobar\n")))
	}

	require.NoError(t, w.WriteIndex())

	return dir
}

func newChecksumTest(t *testing.T, withError bool) string {
	t.Helper()

	dir := t.TempDir()

	f, err := os.CreateTemp(dir, "verifytsmtest*"+"."+tsm1.TSMFileExtension)
	require.NoError(t, err)

	w, err := tsm1.NewTSMWriter(f)
	require.NoError(t, err)

	values := []tsm1.Value{tsm1.NewValue(0, "entry")}
	require.NoError(t, w.Write([]byte("cpu"), values))

	require.NoError(t, w.WriteIndex())
	w.Close()

	if withError {
		fh, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
		require.NoError(t, err)
		defer fh.Close()

		written, err := fh.WriteAt([]byte("foob"), 5)
		require.Equal(t, 4, written)
		require.NoError(t, err)
	}

	return dir
}
