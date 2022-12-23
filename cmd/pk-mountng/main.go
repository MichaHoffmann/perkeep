package main

import (
	"flag"
	"log"
	"time"

	pkclient "perkeep.org/pkg/client"
	pkfuse "perkeep.org/pkg/fuse"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	debug = flag.Bool("debug", false, "print debug messages from fuse")

	attrTimeout     = 5 * time.Second
	entryTimeout    = 5 * time.Second
	negativeTimeout = 30 * time.Second
)

func main() {
	pkclient.AddFlags()
	flag.Parse()

	mountpoint := flag.Arg(0)

	client, err := pkclient.New()
	if err != nil {
		log.Panic(err)
	}
	pkfs := pkfuse.NewPkFS(client)
	server, err := fs.Mount(mountpoint, pkfs.Root(), &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:         *debug,
			FsName:        "pk-fuse",
			Name:          "pk",
			DisableXAttrs: true,
		},
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negativeTimeout,
	})
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Mounted on %s", mountpoint)
	log.Printf("Unmount by calling 'fusermount -u %s'", mountpoint)

	server.Wait()
}
