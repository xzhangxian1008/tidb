// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mock

import (
	"bytes"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/topsql/collector"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// TopSQLCollector uses for testing.
type TopSQLCollector struct {
	// sql_digest -> normalized SQL
	sqlMap map[string]string
	// plan_digest -> normalized plan
	planMap map[string]string
	// (sql + plan_digest) -> sql stats
	sqlStatsMap map[string]*collector.SQLCPUTimeRecord
	collectCnt  atomic.Int64
	sync.Mutex
}

// NewTopSQLCollector uses for testing.
func NewTopSQLCollector() *TopSQLCollector {
	return &TopSQLCollector{
		sqlMap:      make(map[string]string),
		planMap:     make(map[string]string),
		sqlStatsMap: make(map[string]*collector.SQLCPUTimeRecord),
	}
}

// Start implements TopSQLReporter interface.
func (*TopSQLCollector) Start() {}

// Collect uses for testing.
func (c *TopSQLCollector) Collect(stats []collector.SQLCPUTimeRecord) {
	defer c.collectCnt.Inc()
	if len(stats) == 0 {
		return
	}
	c.Lock()
	defer c.Unlock()
	for _, stmt := range stats {
		hash := c.hash(stmt)
		stats, ok := c.sqlStatsMap[hash]
		if !ok {
			stats = &collector.SQLCPUTimeRecord{
				SQLDigest:  stmt.SQLDigest,
				PlanDigest: stmt.PlanDigest,
			}
			c.sqlStatsMap[hash] = stats
		}
		stats.CPUTimeMs += stmt.CPUTimeMs
		logutil.BgLogger().Info("mock top sql collector collected sql",
			zap.String("sql", c.sqlMap[string(stmt.SQLDigest)]),
			zap.Bool("has-plan", len(c.planMap[string(stmt.PlanDigest)]) > 0))
	}
}

// BindProcessCPUTimeUpdater implements TopSQLReporter.
func (*TopSQLCollector) BindProcessCPUTimeUpdater(_ collector.ProcessCPUTimeUpdater) {}

// BindKeyspaceID implements TopSQLReporter.
func (c *TopSQLCollector) BindKeyspaceID(_ *uint32) {
}

// CollectStmtStatsMap implements stmtstats.Collector.
func (*TopSQLCollector) CollectStmtStatsMap(_ stmtstats.StatementStatsMap) {}

// GetSQLStatsBySQLWithRetry uses for testing.
func (c *TopSQLCollector) GetSQLStatsBySQLWithRetry(sql string, planIsNotNull bool) []*collector.SQLCPUTimeRecord {
	after := time.After(time.Second * 10)
	for {
		select {
		case <-after:
			return nil
		default:
		}
		stats := c.GetSQLStatsBySQL(sql, planIsNotNull)
		if len(stats) > 0 {
			return stats
		}
		c.WaitCollectCnt(1)
	}
}

// GetSQLStatsBySQL uses for testing.
func (c *TopSQLCollector) GetSQLStatsBySQL(sql string, planIsNotNull bool) []*collector.SQLCPUTimeRecord {
	stats := make([]*collector.SQLCPUTimeRecord, 0, 2)
	sqlDigest := GenSQLDigest(sql)
	c.Lock()
	for _, stmt := range c.sqlStatsMap {
		if bytes.Equal(stmt.SQLDigest, sqlDigest.Bytes()) {
			if planIsNotNull {
				plan := c.planMap[string(stmt.PlanDigest)]
				if len(plan) > 0 {
					stats = append(stats, stmt)
				}
			} else {
				stats = append(stats, stmt)
			}
		}
	}
	c.Unlock()
	return stats
}

// GetSQLCPUTimeBySQL uses for testing.
func (c *TopSQLCollector) GetSQLCPUTimeBySQL(sql string) uint32 {
	sqlDigest := GenSQLDigest(sql)
	cpuTime := uint32(0)
	c.Lock()
	for _, stmt := range c.sqlStatsMap {
		if bytes.Equal(stmt.SQLDigest, sqlDigest.Bytes()) {
			cpuTime += stmt.CPUTimeMs
		}
	}
	c.Unlock()
	return cpuTime
}

// GetSQL uses for testing.
func (c *TopSQLCollector) GetSQL(sqlDigest []byte) string {
	c.Lock()
	sql := c.sqlMap[string(sqlDigest)]
	c.Unlock()
	return sql
}

// GetPlan uses for testing.
func (c *TopSQLCollector) GetPlan(planDigest []byte) string {
	c.Lock()
	plan := c.planMap[string(planDigest)]
	c.Unlock()
	return plan
}

// RegisterSQL uses for testing.
func (c *TopSQLCollector) RegisterSQL(sqlDigest []byte, normalizedSQL string, _ bool) {
	digestStr := string(hack.String(sqlDigest))
	c.Lock()
	_, ok := c.sqlMap[digestStr]
	if !ok {
		c.sqlMap[digestStr] = normalizedSQL
	}
	c.Unlock()
}

// RegisterPlan uses for testing.
func (c *TopSQLCollector) RegisterPlan(planDigest []byte, normalizedPlan string, isLarge bool) {
	if isLarge {
		return
	}

	digestStr := string(hack.String(planDigest))
	c.Lock()
	_, ok := c.planMap[digestStr]
	if !ok {
		c.planMap[digestStr] = normalizedPlan
	}
	c.Unlock()
}

// WaitCollectCnt uses for testing.
func (c *TopSQLCollector) WaitCollectCnt(count int64) {
	timeout := time.After(time.Second * 10)
	end := c.collectCnt.Load() + count
	for {
		// Wait for reporter to collect sql stats count >= expected count
		if c.collectCnt.Load() >= end {
			return
		}
		select {
		case <-timeout:
			return
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// Reset cleans all collected data.
func (c *TopSQLCollector) Reset() {
	c.Lock()
	defer c.Unlock()
	c.sqlMap = make(map[string]string)
	c.planMap = make(map[string]string)
	c.sqlStatsMap = make(map[string]*collector.SQLCPUTimeRecord)
	c.collectCnt.Store(0)
}

// CollectCnt uses for testing.
func (c *TopSQLCollector) CollectCnt() int64 {
	return c.collectCnt.Load()
}

// Close implements the interface.
func (*TopSQLCollector) Close() {}

func (*TopSQLCollector) hash(stat collector.SQLCPUTimeRecord) string {
	return string(stat.SQLDigest) + string(stat.PlanDigest)
}

// GenSQLDigest uses for testing.
func GenSQLDigest(sql string) *parser.Digest {
	_, digest := parser.NormalizeDigest(sql)
	return digest
}
