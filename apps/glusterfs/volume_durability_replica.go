//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package glusterfs

import (
	"encoding/gob"

	"github.com/heketi/heketi/executors"
	"github.com/heketi/heketi/pkg/glusterfs/api"
)

func init() {
	// Volume Entry has VolumeDurability interface as a member.
	// Serialization tools need to know the types that satisfy this
	// interface. gob is used to serialize entries for db. Strictly
	// speaking, it is not required to store VolumeDurability member in db
	// as it can be recreated from volumeInfo. But removing it now would
	// break backward with db.
	gob.Register(&VolumeReplicaDurability{})
}

type VolumeReplicaDurability struct {
	api.ReplicaDurability
}

func NewVolumeReplicaDurability(r *api.ReplicaDurability) *VolumeReplicaDurability {
	v := &VolumeReplicaDurability{}
	v.Replica = r.Replica

	return v
}

func (r *VolumeReplicaDurability) SetDurability() {
	if r.Replica == 0 {
		r.Replica = DEFAULT_REPLICA
	}
}

func (r *VolumeReplicaDurability) BrickSizeGenerator(size uint64) func() (int, uint64, error) {

	seeds := []int{1, 3, 5, 7, 9, 11}
	return func() (int, uint64, error) {

		var brick_size uint64
		var num_sets int

		for {
			min_idx := 0
			num_sets = seeds[min_idx]
			for idx, v := range seeds {
				if v < num_sets {
					min_idx = idx
					num_sets = v
				}
			}
			seeds[min_idx] *= 2

			brick_size = size / uint64(num_sets)

			if brick_size < BrickMinSize {
				return 0, 0, ErrMinimumBrickSize
			} else if brick_size <= BrickMaxSize {
				break
			}
		}

		return num_sets, brick_size, nil
	}
}

func (r *VolumeReplicaDurability) MinVolumeSize() uint64 {
	return BrickMinSize
}

func (r *VolumeReplicaDurability) BricksInSet() int {
	return r.Replica
}

func (r *VolumeReplicaDurability) QuorumBrickCount() int {
	if r.BricksInSet() < 3 {
		return 1
	} else {
		return r.BricksInSet()/2 + 1
	}
}

func (r *VolumeReplicaDurability) SetExecutorVolumeRequest(v *executors.VolumeRequest) {
	v.Type = executors.DurabilityReplica
	v.Replica = r.Replica
}
