/*
Copyright 2013 The Perkeep Authors.

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

// This file adds the "mountng" subcommand to devcam, to run pk-mountng against the dev server.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"perkeep.org/internal/osutil"
	"perkeep.org/pkg/cmdmain"
)

type mountNgCmd struct {
	// start of flag vars
	altkey bool
	path   string
	port   string
	tls    bool
	debug  bool
	// end of flag vars

	env *Env
}

func init() {
	cmdmain.RegisterMode("mountng", func(flags *flag.FlagSet) cmdmain.CommandRunner {
		cmd := &mountNgCmd{
			env: NewCopyEnv(),
		}
		flags.BoolVar(&cmd.altkey, "altkey", false, "Use different gpg key and password from the server's.")
		flags.BoolVar(&cmd.tls, "tls", false, "Use TLS.")
		flags.StringVar(&cmd.path, "path", "/", "Optional URL prefix path.")
		flags.StringVar(&cmd.port, "port", "3179", "Port perkeep is listening on.")
		flags.BoolVar(&cmd.debug, "debug", false, "print debugging messages.")
		return cmd
	})
}

func (c *mountNgCmd) Usage() {
	fmt.Fprintf(cmdmain.Stderr, "Usage: devcam mount [mount_opts] [<root-blobref>|<share URL>]\n")
}

func (c *mountNgCmd) Examples() []string {
	return []string{
		"",
		"http://localhost:3169/share/<blobref>",
	}
}

func (c *mountNgCmd) Describe() string {
	return "run pk-mountng in dev mode."
}

func (c *mountNgCmd) RunCommand(args []string) error {
	err := c.checkFlags(args)
	if err != nil {
		return cmdmain.UsageError(fmt.Sprint(err))
	}
	if !*noBuild {
		if err := build(filepath.Join("cmd", "pk-mountng")); err != nil {
			return fmt.Errorf("Could not build pk-mountng: %v", err)
		}
	}
	c.env.SetCamdevVars(c.altkey)
	// wipeCacheDir needs to be called after SetCamdevVars, because that is
	// where CAMLI_CACHE_DIR is defined.
	if *wipeCache {
		c.env.wipeCacheDir()
	}

	tryUnmount(mountpoint)
	if err := os.Mkdir(mountpoint, 0700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("Could not make mount point: %v", err)
	}

	blobserver := "http://localhost:" + c.port + c.path
	if c.tls {
		blobserver = strings.Replace(blobserver, "http://", "https://", 1)
	}

	cmdBin, err := osutil.LookPathGopath("pk-mountng")
	if err != nil {
		return err
	}
	cmdArgs := []string{
		"-debug=" + strconv.FormatBool(c.debug),
		"-server=" + blobserver,
	}
	cmdArgs = append(cmdArgs, args...)
	cmdArgs = append(cmdArgs, mountpoint)
	fmt.Printf("pk-mount running with mountpoint %v. Press 'q' <enter> or ctrl-c to shut down.\n", mountpoint)
	return runExec(cmdBin, cmdArgs, c.env)
}

func (c *mountNgCmd) checkFlags(args []string) error {
	if _, err := strconv.ParseInt(c.port, 0, 0); err != nil {
		return fmt.Errorf("Invalid -port value: %q", c.port)
	}
	return nil
}
