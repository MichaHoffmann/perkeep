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
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"go4.org/syncutil"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/search"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	logger = log.New(os.Stderr, "PerkeepFS: ", log.LstdFlags)
)

type PkFS struct {
	// TODO: retry retriable networking issues
	client *client.Client
}

func NewPkFS(client *client.Client) *PkFS {
	return &PkFS{
		client: client,
	}
}

func (pk *PkFS) Root() fs.InodeEmbedder {
	return &pkRoot{pk: pk}
}

func (pk *PkFS) fetchSchemaMeta(ctx context.Context, br blob.Ref) (*schema.Blob, error) {
	rc, _, err := pk.client.Fetch(ctx, br)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	blob, err := schema.BlobFromReader(br, rc)
	if err != nil {
		return nil, os.ErrInvalid
	}
	if blob.Type() == "" {
		return nil, os.ErrInvalid
	}
	return blob, nil

}

type pkNode struct {
	fs.Inode

	pk *PkFS
	br blob.Ref

	name string

	mu sync.RWMutex

	// file
	cbr     blob.Ref
	content []byte
	refs    uint
	lw      time.Time
	lf      time.Time

	// symlink
	target []byte
}

func isDir(attr url.Values) bool {
	if attr.Get("camliNodeType") == string(schema.TypeDirectory) {
		return true
	}
	for k := range attr {
		if strings.HasPrefix(k, "camliPath:") {
			return true
		}
	}
	return false
}

func isFile(attr url.Values) bool {
	return attr.Get("camliContent") != ""
}

func isSymlink(attr url.Values) bool {
	return attr.Get("camliSymlinkTarget") != ""
}

var (
	_ = (fs.InodeEmbedder)((*pkNode)(nil))
	_ = (fs.NodeReaddirer)((*pkNode)(nil))
	_ = (fs.NodeCreater)((*pkNode)(nil))
	_ = (fs.NodeLookuper)((*pkNode)(nil))
	_ = (fs.NodeOpener)((*pkNode)(nil))
	_ = (fs.NodeUnlinker)((*pkNode)(nil))
	_ = (fs.NodeSymlinker)((*pkNode)(nil))
	_ = (fs.NodeReadlinker)((*pkNode)(nil))
	_ = (fs.NodeGetattrer)((*pkNode)(nil))
	_ = (fs.NodeSetattrer)((*pkNode)(nil))
	_ = (fs.NodeMkdirer)((*pkNode)(nil))
	_ = (fs.NodeRenamer)((*pkNode)(nil))
)

// For Readdir and Lookup
func describeRequest(br blob.Ref) *search.DescribeRequest {
	return &search.DescribeRequest{
		Depth:   2,
		BlobRef: br,
		Rules:   []*search.DescribeRule{{Attrs: []string{"camliPath:*", "camliContent"}}},
	}
}

func (n *pkNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	res, err := n.pk.client.Describe(ctx, describeRequest(n.br))
	if err != nil {
		return nil, errnoFromErr(err)
	}

	db := res.Meta.Get(n.br)
	if db == nil {
		return nil, syscall.EIO
	}

	dirEntries := make([]fuse.DirEntry, 0)
	for k, v := range db.Permanode.Attr {
		if !strings.HasPrefix(k, "camliPath:") || len(v) < 1 {
			continue
		}
		name := strings.TrimPrefix(k, "camliPath:")

		cr := res.Meta.Get(blob.ParseOrZero(v[0]))
		if cr == nil || cr.Permanode == nil {
			continue
		}
		var mode uint32
		switch {
		case isFile(cr.Permanode.Attr):
			mode = syscall.S_IFREG
		case isDir(cr.Permanode.Attr):
			mode = syscall.S_IFDIR
		case isSymlink(cr.Permanode.Attr):
			mode = syscall.S_IFLNK
		default:
			continue
		}
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: name,
			Ino:  cr.BlobRef.Sum64(),
			Mode: mode,
		})
	}
	return fs.NewListDirStream(dirEntries), 0
}

func (n *pkNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if child := n.GetChild(name); child != nil {
		return child, 0
	}

	res, err := n.pk.client.Describe(ctx, describeRequest(n.br))
	if err != nil {
		return nil, errnoFromErr(err)
	}

	db := res.Meta.Get(n.br)
	if db == nil {
		return nil, syscall.EIO
	}

	mb := blob.ParseOrZero(db.Permanode.Attr.Get("camliPath:" + name))
	if !mb.Valid() {
		return nil, syscall.ENOENT
	}
	mdb := res.Meta.Get(mb)

	child := &pkNode{
		pk:   n.pk,
		br:   mdb.BlobRef,
		name: name,
		refs: 1,
	}

	var mode uint32
	switch {
	case isFile(mdb.Permanode.Attr):
		mode = syscall.S_IFREG
		child.cbr = blob.ParseOrZero(mdb.Permanode.Attr.Get("camliContent"))
		sc, err := n.pk.fetchSchemaMeta(ctx, child.cbr)
		if err != nil {
			return nil, errnoFromErr(err)
		}
		child.content = make([]byte, sc.PartsSize())
	case isDir(mdb.Permanode.Attr):
		mode = syscall.S_IFDIR
	case isSymlink(mdb.Permanode.Attr):
		mode = syscall.S_IFLNK
		child.target = []byte(mdb.Permanode.Attr.Get("camliSymlinkTarget"))
	default:
		return nil, syscall.EIO
	}
	child.populateAttrOut(&out.Attr)

	return n.NewInode(ctx, child, fs.StableAttr{Mode: mode, Ino: child.br.Sum64()}), 0
}

func (n *pkNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	pr, err := n.pk.client.UploadNewPermanode(ctx)
	if err != nil {
		return nil, nil, 0, errnoFromErr(err)
	}

	br, err := schema.WriteFileFromReader(ctx, n.pk.client, n.name, bytes.NewReader(nil))
	if err != nil {
		return nil, nil, 0, errnoFromErr(err)
	}

	var grp syncutil.Group
	grp.Go(func() error {
		claim := schema.NewSetAttributeClaim(n.br, "camliPath:"+name, pr.BlobRef.String())
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return err
	})
	grp.Go(func() error {
		claim := schema.NewSetAttributeClaim(pr.BlobRef, "camliContent", br.String())
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return err
	})
	if err := grp.Err(); err != nil {
		return nil, nil, 0, errnoFromErr(err)
	}

	child := &pkNode{
		pk:   n.pk,
		br:   pr.BlobRef,
		name: name,
		refs: 1,
	}
	fh := &pkFileView{
		n:     child,
		flags: flags,
	}
	child.populateAttrOut(&out.Attr)

	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG, Ino: child.br.Sum64()}), fh, fuse.FOPEN_DIRECT_IO, 0
}

func (n *pkNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.content == nil {
		if n.cbr.Valid() {
			r, err := schema.NewFileReader(ctx, n.pk.client, n.cbr)
			if err != nil {
				return nil, 0, errnoFromErr(err)
			}
			content, err := io.ReadAll(r)
			if err != nil {
				return nil, 0, errnoFromErr(err)
			}
			n.content = content
		}
	}
	n.refs++
	return &pkFileView{n: n, flags: flags}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *pkNode) Unlink(ctx context.Context, name string) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	child := n.GetChild(name)
	if child == nil {
		return syscall.ENOENT
	}
	childBr := child.Operations().(*pkNode).br

	claim := schema.NewDeleteClaim(childBr)
	_, err := n.pk.client.UploadAndSignBlob(ctx, claim)
	if err != nil {
		return errnoFromErr(err)
	}

	claim = schema.NewDelAttributeClaim(n.br, "camliPath:"+name, "")
	_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
	if err != nil {
		return errnoFromErr(err)
	}

	return 0
}

func (n *pkNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	pr, err := n.pk.client.UploadNewPermanode(ctx)
	if err != nil {
		return nil, errnoFromErr(err)
	}

	var grp syncutil.Group
	grp.Go(func() error {
		claim := schema.NewSetAttributeClaim(n.br, "camliPath:"+name, pr.BlobRef.String())
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return err
	})
	grp.Go(func() error {
		claim := schema.NewSetAttributeClaim(pr.BlobRef, "camliSymlinkTarget", target)
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return err
	})
	if err := grp.Err(); err != nil {
		return nil, errnoFromErr(err)
	}

	child := &pkNode{
		pk:     n.pk,
		br:     pr.BlobRef,
		name:   name,
		target: []byte(target),
	}
	child.populateAttrOut(&out.Attr)

	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFLNK, Ino: child.br.Sum64()}), 0
}

func (n *pkNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.target, 0
}

func (n *pkNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.populateAttrOutUnlocked(&out.Attr)
	return 0
}

func (n *pkNode) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	if sz, ok := in.GetSize(); ok {
		n.resizeUnlocked(sz)
	}
	n.populateAttrOutUnlocked(&out.Attr)
	n.lw = time.Now()
	return 0
}

func (n *pkNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	pr, err := n.pk.client.UploadNewPermanode(ctx)
	if err != nil {
		return nil, errnoFromErr(err)
	}

	var grp syncutil.Group
	grp.Go(func() error {
		claim := schema.NewSetAttributeClaim(n.br, "camliPath:"+name, pr.BlobRef.String())
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return err
	})
	grp.Go(func() error {
		claim := schema.NewSetAttributeClaim(pr.BlobRef, "title", name)
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return err
	})
	grp.Go(func() error {
		claim := schema.NewSetAttributeClaim(pr.BlobRef, "camliNodeType", "directory")
		_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
		return err
	})
	if name == ".DS_Store" {
		grp.Go(func() (err error) {
			claim := schema.NewSetAttributeClaim(pr.BlobRef, "camliDefVis", "hide")
			_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
			return
		})
	}
	if err := grp.Err(); err != nil {
		return nil, errnoFromErr(err)
	}

	child := &pkNode{
		pk:   n.pk,
		br:   pr.BlobRef,
		name: name,
	}
	child.populateAttrOut(&out.Attr)

	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: child.br.Sum64()}), 0
}

func (n *pkNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.Unlink(ctx, name)
}

func (n *pkNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	child := n.GetChild(name)
	if child == nil {
		return syscall.ENOENT
	}
	childBr := child.Operations().(*pkNode).br

	nn := newParent.(*pkNode)
	claim := schema.NewSetAttributeClaim(nn.br, "camliPath:"+newName, childBr.String())
	_, err := n.pk.client.UploadAndSignBlob(ctx, claim)
	if err != nil {
		return errnoFromErr(err)
	}

	var grp syncutil.Group
	grp.Go(func() (err error) {
		delClaim := schema.NewDelAttributeClaim(n.br, "camliPath:"+name, "")
		_, err = n.pk.client.UploadAndSignBlob(ctx, delClaim)
		return
	})
	if n.Mode() == syscall.S_IFDIR {
		grp.Go(func() (err error) {
			claim := schema.NewSetAttributeClaim(childBr, "title", newName)
			_, err = n.pk.client.UploadAndSignBlob(ctx, claim)
			return
		})
	}
	if err := grp.Err(); err != nil {
		return errnoFromErr(err)
	}
	return 0
}

type pkFileView struct {
	n *pkNode

	flags uint32
}

var (
	_ = (fs.FileReader)((*pkFileView)(nil))
	_ = (fs.FileWriter)((*pkFileView)(nil))
	_ = (fs.FileFlusher)((*pkFileView)(nil))
)

func (fh *pkFileView) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.n.mu.Lock()
	defer fh.n.mu.Unlock()

	if fh.flags&uint32(os.O_WRONLY) > 0 {
		return nil, syscall.EPERM
	}

	end := int(off) + len(dest)
	if end > len(fh.n.content) {
		end = len(fh.n.content)
	}
	return fuse.ReadResultData(fh.n.content[off:end]), 0
}

func (fh *pkFileView) Write(_ context.Context, buf []byte, off int64) (uint32, syscall.Errno) {
	fh.n.mu.Lock()
	defer fh.n.mu.Unlock()

	if fh.flags&uint32(os.O_RDONLY) > 0 {
		return 0, syscall.EPERM
	}
	if fh.flags&uint32(os.O_APPEND) > 0 {
		off = int64(len(fh.n.content))
	}

	sz := int64(len(buf))
	if off+sz > int64(len(fh.n.content)) {
		fh.n.resizeUnlocked(uint64(off + sz))
	}

	copy(fh.n.content[off:], buf)
	fh.n.lw = time.Now()
	return uint32(sz), 0
}

func (fh *pkFileView) Flush(ctx context.Context) syscall.Errno {
	fh.n.mu.Lock()
	defer fh.n.mu.Unlock()

	if !fh.n.lw.IsZero() && fh.n.lw.Before(fh.n.lf) {
		return 0
	}

	br, err := schema.WriteFileFromReader(ctx, fh.n.pk.client, fh.n.name, bytes.NewReader(fh.n.content))
	if err != nil {
		return errnoFromErr(err)
	}
	claim := schema.NewSetAttributeClaim(fh.n.br, "camliContent", br.String())
	if _, err := fh.n.pk.client.UploadAndSignBlob(ctx, claim); err != nil {
		return errnoFromErr(err)
	}
	fh.n.cbr = br
	fh.n.lf = time.Now()
	return 0
}

func (n *pkNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	return 0
}

func (n *pkNode) resizeUnlocked(sz uint64) {
	if sz > uint64(cap(n.content)) {
		buf := make([]byte, sz)
		copy(buf, n.content)
		n.content = buf
	} else {
		n.content = n.content[:sz]
	}
}

func (n *pkNode) populateAttrOutUnlocked(out *fuse.Attr) {
	out.Size = uint64(len(n.content))
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.Nlink = 1

	switch n.Mode() {
	case syscall.S_IFREG, syscall.S_IFLNK:
		out.Mode = 0600
	case syscall.S_IFDIR:
		out.Mode = 0700
	}
}

func (n *pkNode) populateAttrOut(out *fuse.Attr) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.populateAttrOutUnlocked(out)
}

func errnoFromErr(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	logger.Println("perkeep error: ", err)
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	} else {
		return syscall.EIO
	}
}
