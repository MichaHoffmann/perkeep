//go:build darwin
// +build darwin

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
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTextEdit(t *testing.T) {
	inEmptyRoot(t, func(env *mountEnv, testDir string) {
		var (
			testFile = filepath.Join(testDir, "some-text-file.txt")
			content1 = []byte("Some text content.")
			content2 = []byte("Some replacement content.")
		)
		if err := ioutil.WriteFile(testFile, content1, 0644); err != nil {
			t.Fatal(err)
		}

		cmd := exec.Command("osascript")
		script := fmt.Sprintf(`
tell application "TextEdit"
	activate
	open POSIX file %q
	tell front document
		set paragraph 1 to %q as text
		save
		close
	end tell
end tell
`, testFile, content2)
		cmd.Stdin = strings.NewReader(script)

		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("error running AppleScript: %v, %s", err, out)
		} else {
			t.Logf("AppleScript said: %q", out)
		}

		fi, err := os.Stat(testFile)
		if err != nil {
			t.Errorf("stat = %v, %v", fi, err)
		} else if fi.Size() != int64(len(content2)) {
			t.Errorf("stat size = %d; want %d", fi.Size(), len(content2))
		}
		slurp, err := ioutil.ReadFile(testFile)
		if err != nil {
			t.Fatalf("readFile: %v", err)
		}
		if !bytes.Equal(slurp, content2) {
			t.Errorf("file = %q; want %q", slurp, content2)
		}
	})
}

func TestFinderCopy(t *testing.T) {
	inEmptyRoot(t, func(env *mountEnv, destDir string) {
		f, err := ioutil.TempFile("", "finder-copy-file")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		want := []byte("Some data for Finder to copy.")
		if _, err := f.Write(want); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Logf("error closing file %s: %s", f.Name(), err)
		}

		cmd := exec.Command("osascript")
		script := fmt.Sprintf(`
tell application "Finder"
  copy file POSIX file %q to folder POSIX file %q
end tell
`, f.Name(), destDir)
		cmd.Stdin = strings.NewReader(script)

		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("error running AppleScript: %v, %s", err, out)
		} else {
			t.Logf("AppleScript said: %q", out)
		}

		destFile := filepath.Join(destDir, filepath.Base(f.Name()))
		fi, err := os.Stat(destFile)
		if err != nil {
			t.Errorf("stat = %v, %v", fi, err)
		}
		if fi.Size() != int64(len(want)) {
			t.Errorf("dest stat size = %d; want %d", fi.Size(), len(want))
		}
		slurp, err := ioutil.ReadFile(destFile)
		if err != nil {
			t.Fatalf("readFile: %v", err)
		}
		if !bytes.Equal(slurp, want) {
			t.Errorf("dest file = %q; want %q", slurp, want)
		}
	})
}
