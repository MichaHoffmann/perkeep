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
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

type pkRoot struct {
	fs.Inode

	pk *PkFS
}

var (
	_ = (fs.NodeOnAdder)((*pkRoot)(nil))
	_ = (fs.InodeEmbedder)((*pkRoot)(nil))
)

// The root populates the tree in its OnAdd method
func (pkr *pkRoot) OnAdd(ctx context.Context) {

	// add WELCOME.txt
	pkr.addWelcomeNode(ctx)
	pkr.addRootsNode(ctx)
}

func (pkr *pkRoot) addWelcomeNode(ctx context.Context) {
	const (
		welcomeName    = "WELCOME.txt"
		welcomeMessage = `
Welcome to PerkeepFS.

More information is available in the pk-mount documentation.

See https://perkeep.org/cmd/pk-mount/ , or run 'go doc perkeep.org/cmd/pk-mount'.
`
	)
	p := &pkr.Inode

	c := p.NewPersistentInode(ctx, &fs.MemRegularFile{Data: []byte(welcomeMessage)}, fs.StableAttr{})
	if !p.AddChild(welcomeName, c, false) {
		logger.Printf("OnAdd[root]: unable to add welcome node")
	}
}

func (pkr *pkRoot) addRootsNode(ctx context.Context) {
	const (
		rootsName = "roots"
	)
	p := &pkr.Inode

	c := p.NewPersistentInode(ctx, &pkRootsNode{pk: pkr.pk}, fs.StableAttr{Mode: syscall.S_IFDIR})
	if !p.AddChild(rootsName, c, false) {
		logger.Printf("OnAdd[root]: unable to add roots node")
	}
}
