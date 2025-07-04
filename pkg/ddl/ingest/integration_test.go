// Copyright 2023 PingCAP, Inc.
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

package ingest_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddIndexIngestGeneratedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	assertLastNDDLUseIngest := func(n int) {
		tk.MustExec("admin check table t;")
		rows := tk.MustQuery(fmt.Sprintf("admin show ddl jobs %d;", n)).Rows()
		require.Len(t, rows, n)
		for i := range n {
			//nolint: forcetypeassert
			jobTp := rows[i][12].(string)
			require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
		}
	}
	tk.MustExec("create table t (a int, b int, c int as (b+10), d int as (b+c), primary key (a) clustered);")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("alter table t add index idx(c);")
	tk.MustExec("alter table t add index idx1(c, a);")
	tk.MustExec("alter table t add index idx2(a);")
	tk.MustExec("alter table t add index idx3(d);")
	tk.MustExec("alter table t add index idx4(d, c);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 11 12", "2 2 12 14", "3 3 13 16"))
	assertLastNDDLUseIngest(5)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c char(10) as (concat(b, 'x')), d int, e char(20) as (c));")
	tk.MustExec("insert into t (a, b, d) values (1, '1', 1), (2, '2', 2), (3, '3', 3);")
	tk.MustExec("alter table t add index idx(c);")
	tk.MustExec("alter table t add index idx1(a, c);")
	tk.MustExec("alter table t add index idx2(c(7));")
	tk.MustExec("alter table t add index idx3(e(5));")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1x 1 1x", "2 2 2x 2 2x", "3 3 3x 3 3x"))
	assertLastNDDLUseIngest(4)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c tinyint, d int as (a + c), e bigint as (d - a), primary key(b, a) clustered);")
	tk.MustExec("insert into t (a, b, c) values (1, '1', 1), (2, '2', 2), (3, '3', 3);")
	tk.MustExec("alter table t add index idx(d);")
	tk.MustExec("alter table t add index idx1(b(2), d);")
	tk.MustExec("alter table t add index idx2(d, c);")
	tk.MustExec("alter table t add index idx3(e);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1 2 1", "2 2 2 4 2", "3 3 3 6 3"))
	assertLastNDDLUseIngest(4)
}

func TestIngestError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set global tidb_enable_dist_task = 0")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 4 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (50000) regions 5;").Check(testkit.Rows("4 1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCopSenderError", "1*return"))
	tk.MustExec("alter table t add index idx(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCopSenderError"))
	tk.MustExec("admin check table t;")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	//nolint: forcetypeassert
	jobTp := rows[0][12].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 4 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustQuery("split table t between (0) and (50000) regions 5;").Check(testkit.Rows("4 1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterError", "1*return"))
	tk.MustExec("alter table t add index idx(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterError"))
	tk.MustExec("admin check table t;")
	rows = tk.MustQuery("admin show ddl jobs 1;").Rows()
	//nolint: forcetypeassert
	jobTp = rows[0][12].(string)
	require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
}

func TestAddIndexIngestPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("set global tidb_enable_dist_task = 0")

	t.Run("Mock panic on scan record operator", func(t *testing.T) {
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/scanRecordExec", func(*model.DDLReorgMeta) {
			panic("mock panic")
		})
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int, b int, c int, d int, primary key (a) clustered);")
		tk.MustExec("insert into t (a, b, c, d) values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
		tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrReorgPanic)
	})

	t.Run("Mock panic on local engine writer", func(t *testing.T) {
		tk.MustExec("drop table if exists t;")
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockLocalWriterPanic", "return")
		tk.MustExec("create table t (a int, b int, c int, d int, primary key (a) clustered);")
		tk.MustExec("insert into t (a, b, c, d) values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);")
		tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrReorgPanic)
	})
}

func TestAddIndexSetInternalSessions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("set global tidb_enable_dist_task = 0;")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	expectInternalTS := []uint64{}
	actualInternalTS := []uint64{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackStartTS", func(startTS uint64) {
		expectInternalTS = append(expectInternalTS, startTS)
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/scanRecordExec", func(*model.DDLReorgMeta) {
		mgr := tk.Session().GetSessionManager()
		actualInternalTS = mgr.GetInternalSessionStartTSList()
	})
	tk.MustExec("alter table t add index idx(a);")
	require.Len(t, expectInternalTS, 1)
	for _, ts := range expectInternalTS {
		require.Contains(t, actualInternalTS, ts)
	}
}

func TestAddIndexIngestCancel(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3);")
	cancelled := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if cancelled {
			return
		}
		if job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
			idx := testutil.FindIdxInfo(dom, "test", "t", "idx")
			if idx == nil {
				return
			}
			if idx.BackfillState == model.BackfillStateRunning {
				tk2 := testkit.NewTestKit(t, store)
				rs, err := tk2.Exec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
				assert.NoError(t, err)
				assert.NoError(t, rs.Close())
				cancelled = true
			}
		}
	})
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrCancelledDDLJob)
	require.True(t, cancelled)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")
	cnt := ingest.LitDiskRoot.Count()
	require.Equal(t, 0, cnt)
}

func TestAddIndexGetChunkCancel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("create table t (a int primary key, b int);")
	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d);", i*10000, i*10000))
	}
	tk.MustExec("split table t between (0) and (1000000) regions 10;")
	jobID := int64(0)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if jobID == 0 && job.Type == model.ActionAddIndex {
			jobID = job.ID
		}
	})
	cancelled := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeGetChunk", func() {
		if !cancelled {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec(fmt.Sprintf("admin cancel ddl jobs %d", jobID))
			cancelled = true
		}
	})
	tk.MustGetErrCode("alter table t add index idx(b);", errno.ErrCancelledDDLJob)
	require.True(t, cancelled)
	tk.MustExec("admin check table t;")
}

func TestIngestPartitionRowCount(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec(`create table t (a int, b int, c int as (b+10), d int as (b+c),
		primary key (a) clustered) partition by range (a) (
    		partition p0 values less than (1),
    		partition p1 values less than (2),
    		partition p2 values less than MAXVALUE);`)
	tk.MustExec("insert into t (a, b) values (0, 0), (1, 1), (2, 2);")
	tk.MustExec("alter table t add index idx(d);")
	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	//nolint: forcetypeassert
	rowCount := rows[0][7].(string)
	require.Equal(t, "3", rowCount)
	tk.MustExec("admin check table t;")
}

func TestAddIndexIngestClientError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("CREATE TABLE t1 (f1 json);")
	tk.MustExec(`insert into t1(f1) values (cast("null" as json));`)
	tk.MustGetErrCode("create index i1 on t1((cast(f1 as unsigned array)));", errno.ErrInvalidJSONValueForFuncIndex)
}

func TestAddIndexCancelOnNoneState(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tkCancel := testkit.NewTestKit(t, store)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("use test")
	tk.MustExec(`create table t (c1 int, c2 int, c3 int)`)
	tk.MustExec("insert into t values(1, 1, 1);")

	first := true
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.SchemaState == model.StateNone && first {
			_, err := tkCancel.Exec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			assert.NoError(t, err)
			first = false
		}
	})
	tk.MustGetErrCode("alter table t add index idx1(c1)", errno.ErrCancelledDDLJob)
	cnt := ingest.LitDiskRoot.Count()
	require.Equal(t, 0, cnt)
}

func TestAddIndexIngestTimezone(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("SET time_zone = '-06:00';")
	tk.MustExec("create table t (`src` varchar(48),`t` timestamp,`timezone` varchar(100));")
	tk.MustExec("insert into t values('2000-07-29 23:15:30','2000-07-29 23:15:30','-6:00');")
	// Test Daylight time.
	tk.MustExec("insert into t values('1991-07-21 00:00:00','1991-07-21 00:00:00','-6:00');")
	tk.MustExec("alter table t add index idx(t);")
	tk.MustExec("admin check table t;")

	tk.MustExec("alter table t drop index idx;")
	tk.MustExec("SET time_zone = 'Asia/Shanghai';")
	tk.MustExec("insert into t values('2000-07-29 23:15:30','2000-07-29 23:15:30', '+8:00');")
	tk.MustExec("insert into t values('1991-07-21 00:00:00','1991-07-21 00:00:00','+8:00');")
	tk.MustExec("alter table t add index idx(t);")
	tk.MustExec("admin check table t;")
}

func TestAddIndexIngestMultiSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values(1, 1), (2, 2);")
	tk.MustExec("alter table t add index idx(a), add index idx_2(b);")
	tk.MustExec("admin check table t;")
	tk.MustExec("alter table t drop index idx, drop index idx_2;")
	tk.MustExec(`alter table t
		add unique index idx(a),
		add unique index idx_2(b, a),
		add unique index idx_3(b);`)
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table t;")
	tk.MustExec(`create table t (a int, b int, c int as (b+10), d int as (b+c),
		primary key (a) clustered) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than MAXVALUE);`)
	for i := range 30 {
		insertSQL := fmt.Sprintf("insert into t (a, b) values (%d, %d);", i, i)
		tk.MustExec(insertSQL)
	}
	tk.MustExec("alter table t add index idx_a(a), add index idx_ab(a, b), add index idx_d(d);")
	tk.MustExec("admin check table t;")
}

func TestAddIndexDuplicateMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk.MustExec("create table t(id int primary key, b int, k int);")
	tk.MustExec("insert into t values (1, 1, 1);")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	var errDML error
	var once sync.Once
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/ingest/afterMockWriterWriteRow", func() {
		once.Do(func() {
			_, errDML = tk1.Exec("insert into t values (2, 1, 2);")
		})
	})
	tk.MustGetErrMsg("alter table t add unique index idx(b);", "[kv:1062]Duplicate entry '1' for key 't.idx'")

	require.NoError(t, errDML)
	tk.MustExec("admin check table t;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 1 1", "2 1 2"))
}

func TestMultiSchemaAddIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	for _, createTableSQL := range []string{
		"create table t (a int, b int);",
		"create table t (a int, b int) PARTITION BY HASH (`a`) PARTITIONS 4;",
	} {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(createTableSQL)
		tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")

		first := true
		var tk2Err error
		ingest.MockExecAfterWriteRow = func() {
			if !first {
				return
			}
			_, tk2Err = tk2.Exec("insert into t values (4, 4), (5, 5);")
			first = false
		}
		tk.MustExec("alter table t add index idx1(a), add index idx2(b);")
		require.False(t, first)
		require.NoError(t, tk2Err)
		tk.MustExec("admin check table t;")
	}
}

func TestAddIndexIngestJobWriteConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int);")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("set global tidb_enable_dist_task = off;")

	injected := false
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/afterRunIngestReorgJob", func(job *model.Job, done bool) {
		if done && !injected {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			// Simulate write-conflict on ddl job table.
			updateSQL := fmt.Sprintf("update mysql.tidb_ddl_job set processing = 0 where job_id = %d", job.ID)
			tk2.MustExec(updateSQL)
			updateSQL = fmt.Sprintf("update mysql.tidb_ddl_job set processing = 1 where job_id = %d", job.ID)
			tk2.MustExec(updateSQL)
			injected = true
		}
	})
	rowCnt := atomic.Int32{}
	failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/ingest/onMockWriterWriteRow", func() {
		rowCnt.Add(1)
	})
	tk.MustExec("alter table t add index idx(b);")
	require.True(t, injected)
	// Write conflict error should not retry the whole job.
	require.Equal(t, 3, int(rowCnt.Load())) // it should not be 6
	tk.MustExec("admin check table t;")
}

func TestAddIndexIngestPartitionCheckpoint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_dist_task = off;")
	tk.MustExec("create table t (a int primary key, b int) partition by hash(a) partitions 4;")
	for i := range 20 {
		insertSQL := fmt.Sprintf("insert into t values (%d, %d)", i, i)
		tk.MustExec(insertSQL)
	}

	var jobID int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeDeliveryJob", func(job *model.Job) {
		jobID = job.ID
	})
	rowCnt := atomic.Int32{}
	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/ddl/ingest/onMockWriterWriteRow",
		func() {
			rowCnt.Add(1)
			if rowCnt.Load() == 10 {
				tk2 := testkit.NewTestKit(t, store)
				tk2.MustExec("use test")
				updateSQL := fmt.Sprintf("update mysql.tidb_ddl_job set processing = 0 where job_id = %d", jobID)
				tk2.MustExec(updateSQL)
				updateSQL = fmt.Sprintf("update mysql.tidb_ddl_job set processing = 1 where job_id = %d", jobID)
				tk2.MustExec(updateSQL)
			}
		})

	tk.MustExec("alter table t add index idx(b);")
	// It should resume to correct partition.
	require.Equal(t, 20, int(rowCnt.Load()))
	tk.MustExec("admin check table t;")
}

func TestAddGlobalIndexInIngest(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int) partition by hash(a) partitions 5")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3)")
	var i atomic.Int32
	i.Store(3)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/writeLocalExec", func(bool) {
		tk2 := testkit.NewTestKit(t, store)
		tmp := i.Add(1)
		_, err := tk2.Exec(fmt.Sprintf("insert into test.t values (%d, %d)", tmp, tmp))
		assert.Nil(t, err)
	})
	tk.MustExec("alter table t add index idx_1(b), add unique index idx_2(b) global")
	rsGlobalIndex := tk.MustQuery("select * from t use index(idx_2)").Sort()
	rsTable := tk.MustQuery("select * from t use index()").Sort()
	rsNormalIndex := tk.MustQuery("select * from t use index(idx_1)").Sort()
	num := len(rsGlobalIndex.Rows())
	require.Greater(t, num, 3)
	require.Equal(t, rsGlobalIndex.String(), rsTable.String())
	require.Equal(t, rsGlobalIndex.String(), rsNormalIndex.String())

	// for indexes have different columns
	tk.MustExec("alter table t add index idx_3(a), add unique index idx_4(b) global")
	rsGlobalIndex = tk.MustQuery("select * from t use index(idx_4)").Sort()
	rsTable = tk.MustQuery("select * from t use index()").Sort()
	rsNormalIndex = tk.MustQuery("select * from t use index(idx_3)").Sort()
	require.Greater(t, len(rsGlobalIndex.Rows()), num)
	require.Equal(t, rsGlobalIndex.String(), rsTable.String())
	require.Equal(t, rsGlobalIndex.String(), rsNormalIndex.String())

	// for all global indexes
	tk.MustExec("alter table t add unique index idx_5(b) global, add unique index idx_6(b) global")
	rsGlobalIndex1 := tk.MustQuery("select * from t use index(idx_6)").Sort()
	rsTable = tk.MustQuery("select * from t use index()").Sort()
	rsGlobalIndex2 := tk.MustQuery("select * from t use index(idx_5)").Sort()
	require.Greater(t, len(rsGlobalIndex1.Rows()), len(rsGlobalIndex.Rows()))
	require.Equal(t, rsGlobalIndex1.String(), rsTable.String())
	require.Equal(t, rsGlobalIndex1.String(), rsGlobalIndex2.String())

	// for non-unique global idnexes
	tk.MustExec("alter table t add index idx_7(b) global, add index idx_8(b) global")
	rsNonUniqueGlobalIndex1 := tk.MustQuery("select * from t use index(idx_7)").Sort()
	rsTable = tk.MustQuery("select * from t use index()").Sort()
	rsNonUniqueGlobalIndex2 := tk.MustQuery("select * from t use index(idx_8)").Sort()
	require.Greater(t, len(rsNonUniqueGlobalIndex1.Rows()), len(rsGlobalIndex.Rows()))
	require.Equal(t, rsNonUniqueGlobalIndex1.String(), rsTable.String())
	require.Equal(t, rsNonUniqueGlobalIndex1.String(), rsNonUniqueGlobalIndex2.String())
}

func TestAddGlobalIndexInIngestWithUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int) partition by hash(a) partitions 5")
	tk.MustExec("insert into t (a, b) values (1, 1), (2, 2), (3, 3)")
	var i atomic.Int32
	i.Store(3)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.State != model.JobStateSynced {
			tk2 := testkit.NewTestKit(t, store)
			tmp := i.Add(1)
			_, err := tk2.Exec(fmt.Sprintf("insert into test.t values (%d, %d)", tmp, tmp))
			assert.Nil(t, err)

			_, err = tk2.Exec(fmt.Sprintf("update test.t set b = b + 11, a = b where b = %d", tmp-1))
			assert.Nil(t, err)
		}
	})
	tk.MustExec("alter table t add unique index idx(b) global")
	rsGlobalIndex := tk.MustQuery("select *,_tidb_rowid from t use index(idx)").Sort()
	rsTable := tk.MustQuery("select *,_tidb_rowid from t use index()").Sort()
	require.Equal(t, rsGlobalIndex.String(), rsTable.String())
}

func TestAddIndexValidateRangesFailed(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int);")
	tk.MustExec("insert into t values (1, 1);")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/loadTableRangesNoRetry", "return")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/validateAndFillRangesErr", "2*return")
	tk.MustExec("alter table t add index idx(b);")
	tk.MustExec("admin check table t;")
}

func TestIndexChangeWithModifyColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (b int, c varchar(100) collate utf8mb4_unicode_ci)")
	tk.MustExec("insert t values (1, 'aa'), (2, 'bb'), (3, 'cc');")

	tkddl := testkit.NewTestKit(t, store)
	tkddl.MustExec("use test")

	var checkErr error

	var wg sync.WaitGroup
	wg.Add(1)

	runModifyColumn := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		switch job.SchemaState {
		case model.StateNone:
			if runModifyColumn {
				return
			}
			runModifyColumn = true
			go func() {
				_, checkErr = tkddl.Exec("alter table t modify column c varchar(120) default 'aaaaa' collate utf8mb4_general_ci first;")
				wg.Done()
			}()
		default:
			return
		}
	})

	tk.MustExec("alter table t add index idx(c);")
	wg.Wait()
	require.ErrorContains(t, checkErr, "when index is defined")
	tk.MustExec("admin check table t")
	tk.MustExec("delete from t;")
}
