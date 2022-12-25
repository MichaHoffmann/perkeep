//go:build linux || darwin
// +build linux darwin

/*
Copyright 2013 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fuse

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"perkeep.org/pkg/test"

	"github.com/hanwen/go-fuse/v2/posixtest"
)

var w *test.World

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		log.Println("Skipping FUSE tests in short mode")
		os.Exit(0)
	}
	if os.Getenv("SKIP_FUSE_TESTS") != "" {
		log.Println("Skipping FUSE tests because 'SKIP_FUSE_TESTS' is set")
		os.Exit(0)
	}
	if !(runtime.GOOS == "darwin" || runtime.GOOS == "linux") {
		log.Printf("Skipping FUSE tests on %s", runtime.GOOS)
		os.Exit(0)
	}

	var err error
	if w, err = test.NewWorld(); err != nil {
		log.Fatal(err)
	}
	if err = w.Start(); err != nil {
		log.Fatal(err)
	}
	defer w.Stop()

	m.Run()
}

func TestPosix(t *testing.T) {
	for k := range map[string]func(*testing.T, string){
		"AppendWrite":          posixtest.AppendWrite,
		"DirectIO":             posixtest.DirectIO,
		"ReadDirPicksUpCreate": posixtest.ReadDirPicksUpCreate,
		"FdLeak":               posixtest.FdLeak,
		"RenameOpenDir":        posixtest.RenameOpenDir,
		"SymlinkReadlink":      posixtest.SymlinkReadlink,
	} {
		f := posixtest.All[k]
		n := k
		t.Run(n, func(tt *testing.T) {
			inEmptyRoot(tt, func(env *mountEnv, rootDir string) {
				f(tt, rootDir)
			})
		})
	}
}

func TestTruncateFile(t *testing.T) {
	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		f, err := ioutil.TempFile(rootDir, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteString("hello world from test"); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Logf("error closing file %s: %s", f.Name(), err)
		}

		const truncateAt = 6

		f, err = os.OpenFile(f.Name(), os.O_RDWR, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.Truncate(truncateAt); err != nil {
			t.Fatal(err)
		}
		if stat, err := f.Stat(); err != nil {
			t.Fatal(err)
		} else if stat.Size() != truncateAt {
			t.Fatalf("file size = %d, want %d", stat.Size(), truncateAt)
		}

		if _, err = f.WriteAt([]byte("perkeep"), truncateAt); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Logf("error closing file %s: %s", f.Name(), err)
		}

		got, err := ioutil.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		if want := "hello perkeep"; string(got) != want {
			t.Fatalf("file content = %q, want %q", got, want)
		}
	})
}

func TestTruncateFileOnOpen(t *testing.T) {
	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		f, err := ioutil.TempFile(rootDir, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteString("hello"); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Logf("error closing file %s: %s", f.Name(), err)
		}

		f, err = os.OpenFile(f.Name(), os.O_RDWR|os.O_TRUNC, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if stat, err := f.Stat(); err != nil {
			t.Fatal(err)
		} else if stat.Size() != 0 {
			t.Fatalf("file size = %d, want %d", stat.Size(), 0)
		}

		if _, err = f.Write([]byte("perkeep")); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Logf("error closing file %s: %s", f.Name(), err)
		}

		got, err := ioutil.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		if want := "perkeep"; string(got) != want {
			t.Fatalf("file content = %q, want %q", got, want)
		}
	})
}

func TestMutable(t *testing.T) {
	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		filename := filepath.Join(rootDir, "x")
		f, err := os.Create(filename)
		if err != nil {
			t.Fatalf("error creating: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Logf("error closing file %s: %s", f.Name(), err)
		}
		fi, err := os.Stat(filename)
		if err != nil {
			t.Errorf("stat error: %v", err)
		} else if !fi.Mode().IsRegular() || fi.Size() != 0 {
			t.Errorf("stat of roots/r/x = %v size %d; want a %d byte regular file", fi.Mode(), fi.Size(), 0)
		}

		for _, str := range []string{"foo, ", "bar\n", "another line.\n"} {
			f, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				t.Fatalf("error opening file: %v", err)
			}
			if _, err := f.Write([]byte(str)); err != nil {
				t.Fatalf("error appending %q to %s: %v", str, filename, err)
			}
			if err := f.Close(); err != nil {
				t.Logf("error closing file %s: %s", f.Name(), err)
			}
		}
		slurp, err := ioutil.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}

		const want = "foo, bar\nanother line.\n"
		fi, err = os.Stat(filename)
		if err != nil {
			t.Errorf("stat error: %v", err)
		} else if !fi.Mode().IsRegular() || fi.Size() != int64(len(want)) {
			t.Errorf("stat of roots/r/x = %v size %d; want a %d byte regular file", fi.Mode(), fi.Size(), len(want))
		}
		if got := string(slurp); got != want {
			t.Fatalf("contents = %q; want %q", got, want)
		}

		if err := os.Remove(filename); err != nil {
			t.Fatal(err)
		}

		if _, err := os.Stat(filename); !os.IsNotExist(err) {
			t.Fatalf("expected file to be gone; got stat err = %v instead", err)
		}
	})
}

func TestReadDir(t *testing.T) {
	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		for _, suffix := range []string{"a", "b", "c"} {
			filename := filepath.Join(rootDir, suffix)
			if err := os.MkdirAll(filename, os.ModeDir); err != nil {
				t.Fatal(err)
			}
		}
		dentries, err := os.ReadDir(rootDir)
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, len(dentries))
		for i := range dentries {
			names[i] = dentries[i].Name()
		}
		sort.Strings(names)
		want := []string{"a", "b", "c"}
		if !reflect.DeepEqual(names, want) {
			t.Errorf("read dir = %q; want %q", names, want)
		}

		for _, suffix := range []string{"a", "b", "c"} {
			filename := filepath.Join(rootDir, suffix)
			if _, err := os.Stat(filename); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestDifferentWriteTypes(t *testing.T) {
	// shortenString reduces any run of 5 or more identical bytes to "x{17}".
	// "hello" => "hello"
	// "fooooooooooooooooo" => "fo{17}"
	shortenString := func(v string) string {
		var buf bytes.Buffer
		var last byte
		var run int
		flush := func() {
			switch {
			case run == 0:
			case run < 5:
				for i := 0; i < run; i++ {
					buf.WriteByte(last)
				}
			default:
				buf.WriteByte(last)
				fmt.Fprintf(&buf, "{%d}", run)
			}
			run = 0
		}
		for i := 0; i < len(v); i++ {
			b := v[i]
			if b != last {
				flush()
			}
			last = b
			run++
		}
		flush()
		return buf.String()
	}

	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		filename := filepath.Join(rootDir, "big")

		writes := []struct {
			name     string
			flag     int
			write    []byte // if non-nil, Write is called
			writeAt  []byte // if non-nil, WriteAt is used
			writePos int64  // writeAt position
			want     string // shortenString of remaining file
		}{
			{
				name:  "write 8k of a",
				flag:  os.O_RDWR | os.O_CREATE | os.O_TRUNC,
				write: bytes.Repeat([]byte("a"), 8<<10),
				want:  "a{8192}",
			},
			{
				name:     "writeAt HI at offset 10",
				flag:     os.O_RDWR,
				writeAt:  []byte("HI"),
				writePos: 10,
				want:     "a{10}HIa{8180}",
			},
			{
				name:  "append single C",
				flag:  os.O_WRONLY | os.O_APPEND,
				write: []byte("C"),
				want:  "a{10}HIa{8180}C",
			},
			{
				name:  "append 8k of b",
				flag:  os.O_WRONLY | os.O_APPEND,
				write: bytes.Repeat([]byte("b"), 8<<10),
				want:  "a{10}HIa{8180}Cb{8192}",
			},
		}

		for _, wr := range writes {
			f, err := os.OpenFile(filename, wr.flag, 0644)
			if err != nil {
				t.Fatalf("error opening file %s: %v", wr.name, err)
			}
			if wr.write != nil {
				if n, err := f.Write(wr.write); err != nil || n != len(wr.write) {
					t.Fatalf("error writing %s: (%v, %v); want (%d, nil)", wr.name, n, err, len(wr.write))
				}
			}
			if wr.writeAt != nil {
				if n, err := f.WriteAt(wr.writeAt, wr.writePos); err != nil || n != len(wr.writeAt) {
					t.Fatalf("error writing at %s: (%v, %v); want (%d, nil)", wr.name, n, err, len(wr.writeAt))
				}
			}
			if err := f.Close(); err != nil {
				t.Logf("error closing file %s: %s", f.Name(), err)
			}

			slurp, err := ioutil.ReadFile(filename)
			if err != nil {
				t.Fatalf("error reading file %s: %v", wr.name, err)
			}
			if got := shortenString(string(slurp)); got != wr.want {
				t.Fatalf("file %s: %q; want %q", wr.name, got, wr.want)
			}

		}

		if err := os.Remove(filename); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRename(t *testing.T) {
	statStr := func(name string) string {
		fi, err := os.Stat(name)
		if os.IsNotExist(err) {
			return "ENOENT"
		}
		if err != nil {
			return "err=" + err.Error()
		}
		return fmt.Sprintf("file %v, size %d", fi.Mode(), fi.Size())
	}

	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		name1 := filepath.Join(rootDir, "1")
		name2 := filepath.Join(rootDir, "2")
		subdir := filepath.Join(rootDir, "dir")
		name3 := filepath.Join(subdir, "3")

		contents := []byte("Some file contents")
		const gone = "ENOENT"
		const reg = "file -rw-------, size 18"

		if err := ioutil.WriteFile(name1, contents, 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(subdir, 0755); err != nil {
			t.Fatal(err)
		}

		if got, want := statStr(name1), reg; got != want {
			t.Errorf("name1 = %q; want %q", got, want)
		}
		if err := os.Rename(name1, name2); err != nil {
			t.Fatal(err)
		}
		if got, want := statStr(name1), gone; got != want {
			t.Errorf("name1 = %q; want %q", got, want)
		}
		if got, want := statStr(name2), reg; got != want {
			t.Errorf("name2 = %q; want %q", got, want)
		}

		// Moving to a different directory.
		if err := os.Rename(name2, name3); err != nil {
			t.Fatal(err)
		}
		if got, want := statStr(name2), gone; got != want {
			t.Errorf("name2 = %q; want %q", got, want)
		}
		if got, want := statStr(name3), reg; got != want {
			t.Errorf("name3 = %q; want %q", got, want)
		}
	})
}

func TestSymlink(t *testing.T) {
	var suffix string
	var link string
	const target = "../../some-target" // arbitrary string. some-target is fake.
	check := func() {
		fi, err := os.Lstat(link)
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			t.Errorf("Mode = %v; want Symlink bit set", fi.Mode())
		}
		got, err := os.Readlink(link)
		if err != nil {
			t.Fatalf("Readlink: %v", err)
		}
		if got != target {
			t.Errorf("ReadLink = %q; want %q", got, target)
		}
	}
	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		// Save for second test:
		link = filepath.Join(rootDir, "some-link")
		suffix = strings.TrimPrefix(link, env.mountPoint)

		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
		check()
	})
	pkmountTest(t, func(env *mountEnv) {
		link = env.mountPoint + suffix
		check()
	})
}

func TestXattr(t *testing.T) {
	setxattr := func(path, attr string, data []byte, flags int) error {
		return ignoringEINTR(func() error {
			return syscall.Setxattr(path, attr, data, flags)
		})
	}
	getxattr := func(path, attr string, data []byte) (sz int, err error) {
		err = ignoringEINTR(func() error {
			sz, err = syscall.Getxattr(path, attr, data)
			return err
		})
		return
	}
	listxattr := func(path string, data []byte) (sz int, err error) {
		err = ignoringEINTR(func() error {
			sz, err = syscall.Listxattr(path, data)
			return err
		})
		return
	}
	removexattr := func(path, attr string) (err error) {
		return ignoringEINTR(func() error {
			return syscall.Removexattr(path, attr)
		})
	}
	parseXattrList := func(from []byte) map[string]bool {
		attrNames := bytes.Split(from, []byte{0})
		m := map[string]bool{}
		for _, nm := range attrNames {
			if len(nm) == 0 {
				continue
			}
			m[string(nm)] = true
		}
		return m
	}

	inEmptyRoot(t, func(env *mountEnv, rootDir string) {
		name1 := filepath.Join(rootDir, "1")
		attr1 := "attr1"
		attr2 := "attr2"
		attr3 := "attr3"

		contents := []byte("Some file contents")
		if err := ioutil.WriteFile(name1, contents, 0644); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 8192)
		// list empty
		n, err := listxattr(name1, buf)
		if err != nil {
			t.Errorf("error in initial listxattr: %v", err)
		}
		if n != 0 {
			t.Errorf("expected zero-length xattr list, got %q", buf[:n])
		}

		// get missing
		n, err = getxattr(name1, attr1, buf)
		if err == nil {
			t.Errorf("expected error getting non-existent xattr, got %q", buf[:n])
		}

		// Set (two different attributes)
		err = setxattr(name1, attr1, []byte("hello1"), 0)
		if err != nil {
			t.Fatalf("error setting xattr: %v", err)
		}
		err = setxattr(name1, attr2, []byte("hello2"), 0)
		if err != nil {
			t.Fatalf("error setting xattr: %v", err)
		}
		// Alternate value for first attribute
		err = setxattr(name1, attr1, []byte("hello1a"), 0)
		if err != nil {
			t.Fatalf("error setting xattr: %v", err)
		}

		// list attrs
		n, err = listxattr(name1, buf)
		if err != nil {
			t.Errorf("error in initial listxattr: %v", err)
		}
		m := parseXattrList(buf[:n])
		if !(len(m) == 2 && m[attr1] && m[attr2]) {
			t.Errorf("missing an attribute: %q", buf[:n])
		}

		// Remove attr
		err = removexattr(name1, attr2)
		if err != nil {
			t.Errorf("failed to remove attr: %v", err)
		}

		// List attrs
		n, err = listxattr(name1, buf)
		if err != nil {
			t.Errorf("error in initial listxattr: %v", err)
		}
		m = parseXattrList(buf[:n])
		if !(len(m) == 1 && m[attr1]) {
			t.Errorf("missing an attribute: %q", buf[:n])
		}

		// Get remaining attr
		n, err = getxattr(name1, attr1, buf)
		if err != nil {
			t.Errorf("error getting attr1: %v", err)
		}
		if string(buf[:n]) != "hello1a" {
			t.Logf("expected hello1a, got %q", buf[:n])
		}

		// Check short buffer errors
		buf = make([]byte, 4)
		err = setxattr(name1, attr3, []byte("hello1a"), 0)
		if err != nil {
			t.Fatalf("error setting xattr: %v", err)
		}
		if _, err = getxattr(name1, attr3, buf); err != syscall.ERANGE {
			t.Errorf("expected syscall.ERANGE on short buffer, got: %v", err)
		}
		if _, err = listxattr(name1, buf); err != syscall.ERANGE {
			t.Errorf("expected syscall.ERANGE on short buffer, got: %v", err)
		}

		// Check setxattr flag errors
		if err = setxattr(name1, attr1, []byte("x"), unix.XATTR_CREATE); err != syscall.EEXIST {
			t.Errorf("expected syscall.EEXIST when setting existing xattr with XATTR_CREATE flag, got: %v", err)
		}
		if err = setxattr(name1, "non-existing", []byte("x"), unix.XATTR_REPLACE); err != syscall.ENODATA {
			t.Errorf("expected syscall.ENODATA when setting existing xattr with XATTR_REPLACE flag, got: %v", err)
		}
	})
}

type mountEnv struct {
	t          *testing.T
	mountPoint string
}

func exclusiveTestDir(t *testing.T) string {
	return fmt.Sprintf("%s-%d", url.PathEscape(t.Name()), time.Now().Unix())
}

func inEmptyRoot(t *testing.T, fn func(env *mountEnv, dir string)) {
	pkmountTest(t, func(env *mountEnv) {
		dir := filepath.Join(env.mountPoint, "roots", exclusiveTestDir(t))
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to make roots/r dir: %v", err)
		}
		fi, err := os.Stat(dir)
		if err != nil || !fi.IsDir() {
			t.Fatalf("Stat of %s dir = %v, %v; want a directory", dir, fi, err)
		}
		fn(env, dir)
	})
}

func pkmountTest(t *testing.T, fn func(env *mountEnv)) {
	if err := w.Ping(); err != nil {
		t.Fatal(err)
	}
	mountPoint := t.TempDir()
	mount := w.CmdWithEnv(
		"pk-mountng",
		os.Environ(),
		"--debug=true",
		mountPoint,
	)
	mount.Stderr = testLogWriter{t}
	if err := mount.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command("umount", "-l", mountPoint).Run(); err != nil {
			t.Logf("unmounting failed: %s", err)
		}
		if !test.WaitFor(func() bool { return !mounted(mountPoint) }, 5*time.Second, 100*time.Millisecond) {
			t.Fatalf("timed out waiting for %s to be unmounted", mountPoint)
		}
	}()

	if !test.WaitFor(func() bool { return mounted(mountPoint) }, 5*time.Second, 100*time.Millisecond) {
		t.Fatalf("timed out waiting for %s to be mounted", mountPoint)
	}
	fn(&mountEnv{
		t:          t,
		mountPoint: mountPoint,
	})
}

func mounted(dir string) bool {
	out, err := exec.Command("df", dir).CombinedOutput()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "pk-fuse") && strings.Contains(string(out), dir)
}

// https://cs.opensource.google/go/go/+/refs/tags/go1.18.3:src/os/file_posix.go;drc=635b1244aa7671bcd665613680f527452cac7555;l=243
// some code to deal with EINTR on the application side
func ignoringEINTR(fn func() error) error {
	for {
		err := fn()
		if err != syscall.EINTR {
			return err
		}
	}
}

type testLogWriter struct {
	t *testing.T
}

func (tl testLogWriter) Write(p []byte) (n int, err error) {
	tl.t.Helper()
	tl.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
