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

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	filters      []filter.Filter
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
	s.filters = []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true}}
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
	sources := filter.SelectSourceStores(stores, s.filters, cluster)
	targets := filter.SelectTargetStores(stores, s.filters, cluster)
	if len(sources) == 0 || len(targets) == 0 {
		return nil
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})

	var source *core.StoreInfo
	var op *operator.Operator
	var callback func(rc core.RegionsContainer)
	callback = func(rc core.RegionsContainer) {
		if op != nil {
			return
		}

		region := rc.RandomRegion([]byte(""), []byte(""))
		if region == nil {
			return
		}

		if len(region.GetPeers()) < cluster.GetMaxReplicas() {
			return
		}

		storeIds := region.GetStoreIds()
		for _, target := range targets {
			if _, exist := storeIds[target.GetID()]; !exist {
				op = s.createOperator(cluster, region, source, target)
				return
			}
		}
	}

	for i := 0; i < len(sources); i++ {
		source = sources[i]
		sourceID := source.GetID()

		for j := 0; j < balanceRegionRetryLimit; j++ {
			cluster.GetPendingRegionsWithLock(sourceID, callback)
			if op != nil {
				return op
			}

			cluster.GetFollowersWithLock(sourceID, callback)
			if op != nil {
				return op
			}

			cluster.GetLeadersWithLock(sourceID, callback)
			if op != nil {
				return op
			}
		}

	}

	return nil
}

// 生成MovePeerOperator
// 如果源store与目标store的region size差值小于等于该 region 的 approximate size 的两倍，则不生成
func (s *balanceRegionScheduler) createOperator(cluster opt.Cluster, region *core.RegionInfo, source, target *core.StoreInfo) *operator.Operator {
	if source.GetRegionSize()-target.GetRegionSize() <= region.GetApproximateSize() {
		return nil
	}

	peer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		log.Debug("failed to AllocPeer",
			zap.String("scheduler", s.GetName()),
			zap.Uint64("region-id", region.GetID()),
			zap.Uint64("store-id", target.GetID()),
			zap.Error(err),
		)
		return nil
	}

	op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, source.GetID(), target.GetID(), peer.Id)
	if err != nil {
		log.Debug("failed to create MovePeerOperator",
			zap.String("scheduler", s.GetName()),
			zap.Uint64("region-id", region.GetID()),
			zap.Uint64("source-store-id", source.GetID()),
			zap.Uint64("target-store-id", target.GetID()),
			zap.Uint64("new-peer-id", peer.Id),
			zap.Error(err),
		)
		return nil
	}

	return op
}
