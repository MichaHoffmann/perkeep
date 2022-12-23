//go:build linux || darwin
// +build linux darwin

/*
Copyright 2011 The Perkeep Authors

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

package fuse // import "perkeep.org/pkg/fuse"

import (
	"context"
	"os"
	"strings"
	"syscall"
	"time"

	"go4.org/syncutil"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/search"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type pkRootsNode struct {
	fs.Inode

	pk *PkFS
	at time.Time
}

var (
	_ = (fs.InodeEmbedder)((*pkRootsNode)(nil))
	_ = (fs.NodeReaddirer)((*pkRootsNode)(nil))
	_ = (fs.NodeLookuper)((*pkRootsNode)(nil))
	_ = (fs.NodeGetattrer)((*pkRootsNode)(nil))
	_ = (fs.NodeMkdirer)((*pkRootsNode)(nil))
)

func (n *pkRootsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	query := &search.SearchQuery{
		Describe: &search.DescribeRequest{Depth: 1},
		Constraint: &search.Constraint{
			Permanode: &search.PermanodeConstraint{
				Attr:         "camliRoot",
				ValueMatches: &search.StringConstraint{Empty: false},
			},
		},
	}
	results := make([]*search.SearchResult, 0)
	for {
		result, err := n.pk.client.Query(ctx, query)
		if err != nil {
			return nil, errnoFromErr(err)
		}
		results = append(results, result)
		if result.Continue == "" {
			break
		}
		query.Continue = result.Continue
	}

	dirEntryList := make([]fuse.DirEntry, 0)
	for _, result := range results {
		for _, wi := range result.Blobs {
			pn := wi.Blob
			db := result.Describe.Meta.Get(pn)
			if db == nil || db.Permanode == nil {
				continue
			}
			name := db.Permanode.Attr.Get("camliRoot")
			if name == "" {
				continue
			}
			dirEntryList = append(dirEntryList, fuse.DirEntry{
				Name: name,
				Ino:  pn.Sum64(),
			})
		}
	}
	return fs.NewListDirStream(dirEntryList), 0
}

func (n *pkRootsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	query := &search.SearchQuery{
		Limit: 1,
		Describe: &search.DescribeRequest{
			Depth: 1,
		},
		Constraint: &search.Constraint{
			Permanode: &search.PermanodeConstraint{
				Attr:  "camliRoot",
				Value: name,
			},
		},
	}
	results, err := n.pk.client.Query(ctx, query)
	if err != nil {
		return nil, errnoFromErr(err)
	}
	if len(results.Blobs) == 0 {
		return nil, syscall.ENOENT
	}

	blob := results.Blobs[0].Blob
	meta := results.Describe.Meta.Get(blob)
	if meta == nil || meta.Permanode == nil {
		return nil, syscall.ENOENT
	}

	if results.Continue != "" && !strings.Contains(results.Continue, blob.String()) {
		return nil, syscall.ENOENT
	}

	return n.NewInode(ctx, &pkNode{
		pk: n.pk,
		br: blob,
	}, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  blob.Sum64(),
	}), 0
}

func (n *pkRootsNode) isReadOnly() bool {
	return !n.at.IsZero()
}

func (n *pkRootsNode) dirMode() os.FileMode {
	if n.isReadOnly() {
		return 0500
	}
	return 0700
}

func (n *pkRootsNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	return 0
}

func (n *pkRootsNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.isReadOnly() {
		return nil, syscall.EPERM
	}
	pr, err := n.pk.client.UploadNewPermanode(ctx)
	if err != nil {
		return nil, errnoFromErr(err)
	}

	var grp syncutil.Group
	grp.Go(func() (err error) {
		claim := schema.NewSetAttributeClaim(pr.BlobRef, "camliRoot", name)
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return
	})
	grp.Go(func() (err error) {
		claim := schema.NewSetAttributeClaim(pr.BlobRef, "title", name)
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return
	})
	if err := grp.Err(); err != nil {
		return nil, errnoFromErr(err)
	}

	return n.NewInode(ctx, &pkNode{
		pk: n.pk,
		br: pr.BlobRef,
	}, fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  pr.BlobRef.Sum64(),
	}), 0
}
