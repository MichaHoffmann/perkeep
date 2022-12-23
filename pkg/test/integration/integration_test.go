/*
Copyright 2018 The Perkeep Authors

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

package integration

import (
	"flag"
	"log"
	"os"
	"testing"

	"perkeep.org/internal/testhooks"
	"perkeep.org/pkg/test"
)

func init() {
	testhooks.SetUseSHA1(true)
}

var w *test.World

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		log.Println("Skipping integration tests in short mode")
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

	os.Exit(m.Run())
}
