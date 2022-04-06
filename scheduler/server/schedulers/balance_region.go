// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	suitableStores := make([]*core.StoreInfo, 0)
	for _, store := range stores {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})
	lStores := len(suitableStores)
	var sourceRegion *core.RegionInfo
	var sourceStore *core.StoreInfo
	for i := 0; i < lStores-1; i += 1 { // Note@wy just need to get a random one, if failed in this time, coordinator will retry.
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(regions core.RegionsContainer) {
			sourceRegion = regions.RandomRegion(nil, nil)
		})
		if sourceRegion != nil {
			sourceStore = suitableStores[i]
			break
		}
		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(regions core.RegionsContainer) {
			sourceRegion = regions.RandomRegion(nil, nil)
		})
		if sourceRegion != nil {
			sourceStore = suitableStores[i]
			break
		}
		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(regions core.RegionsContainer) {
			sourceRegion = regions.RandomRegion(nil, nil)
		})
		if sourceRegion != nil {
			sourceStore = suitableStores[i]
			break
		}
	}
	if sourceRegion == nil {
		log.Debug("Don't find a suitable region to move, skip schedule.")
		return nil
	}
	regionStores := sourceRegion.GetStoreIds()
	if len(regionStores) != cluster.GetMaxReplicas() {
		log.Debug("Region %+v 's store number != cluster's maxreplicas, may need add or remove peer.")
		return nil
	}
	var targetStore *core.StoreInfo
	for i := lStores - 1; i >= 0; i -= 1 {
		if _, ok := regionStores[suitableStores[i].GetID()]; !ok {
			targetStore = suitableStores[i]
			break
		}
	}
	if targetStore == nil || targetStore == sourceStore {
		log.Debug("Don't find a suitable store to move to, skip schedule.")
		return nil
	}
	if 2*sourceRegion.GetApproximateSize() >= sourceStore.GetRegionSize()-targetStore.GetRegionSize() {
		log.Debug("Regionsize's diff less than 2 times region's approximatesize, skip schedule.")
		return nil
	}
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		log.Error("Store %+v fail to alloc new peer, err %+v.", targetStore.GetID(), err)
		return nil
	}
	op, err := operator.CreateMovePeerOperator("balance-region", cluster, sourceRegion, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.Id)
	if err != nil {
		log.Errorf("Fail to create new operator , err +%v.", err)
		return nil
	}
	return op
}
