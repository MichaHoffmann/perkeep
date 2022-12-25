package main

import (
	"flag"
	"log"
	"os"
	"time"

	"perkeep.org/internal/lru"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/fuse"

	fsv2 "github.com/hanwen/go-fuse/v2/fs"
	fusev2 "github.com/hanwen/go-fuse/v2/fuse"
)

var (
	debug           = flag.Bool("debug", false, "print debug messages from fuse")
	attrTimeout     = flag.Duration("attr-timeout", 5*time.Second, "attr timeout")
	entryTimeout    = flag.Duration("entry-timeout", 5*time.Second, "entry timeout")
	negativeTimeout = flag.Duration("negative-timeout", 30*time.Second, "negative timeout")
)

func init() {
	client.AddFlags()
}

func main() {
	flag.Parse()

	client, err := client.New()
	if err != nil {
		log.Panic(err)
	}
	client.SetHaveCache(&memHaveCache{m: lru.New(1024 * 1024)})
	pkfs := fuse.NewPkFS(client)

	mountpoint := flag.Arg(0)
	server, err := fsv2.Mount(mountpoint, pkfs.Root(), &fsv2.Options{
		MountOptions: fusev2.MountOptions{
			Debug:          *debug,
			FsName:         "pk-fuse",
			Name:           "pk",
			RememberInodes: true,
		},
		AttrTimeout:     attrTimeout,
		EntryTimeout:    entryTimeout,
		NegativeTimeout: negativeTimeout,
		GID:             uint32(os.Getgid()),
		UID:             uint32(os.Getuid()),
	})
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Mounted on %s", mountpoint)
	log.Printf("Unmount by calling 'fusermount -u %s'", mountpoint)

	server.Wait()
}

type memHaveCache struct {
	m *lru.Cache
}

func (mc *memHaveCache) StatBlobCache(br blob.Ref) (uint32, bool) {
	v, ok := mc.m.Get(br.String())
	if !ok {
		return 0, ok
	}
	return v.(uint32), ok
}
func (mc *memHaveCache) NoteBlobExists(br blob.Ref, size uint32) {
	mc.m.Add(br.String(), size)
}
