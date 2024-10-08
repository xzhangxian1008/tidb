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

package stmtsummary

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestColumn(t *testing.T) {
	columns := []*model.ColumnInfo{
		{Name: pmodel.NewCIStr(ClusterTableInstanceColumnNameStr)},
		{Name: pmodel.NewCIStr(StmtTypeStr)},
		{Name: pmodel.NewCIStr(SchemaNameStr)},
		{Name: pmodel.NewCIStr(DigestStr)},
		{Name: pmodel.NewCIStr(DigestTextStr)},
		{Name: pmodel.NewCIStr(TableNamesStr)},
		{Name: pmodel.NewCIStr(IndexNamesStr)},
		{Name: pmodel.NewCIStr(SampleUserStr)},
		{Name: pmodel.NewCIStr(ExecCountStr)},
		{Name: pmodel.NewCIStr(SumLatencyStr)},
		{Name: pmodel.NewCIStr(MaxLatencyStr)},
		{Name: pmodel.NewCIStr(AvgTidbCPUTimeStr)},
		{Name: pmodel.NewCIStr(AvgTikvCPUTimeStr)},
	}
	factories := makeColumnFactories(columns)
	info := GenerateStmtExecInfo4Test("digest")
	record := NewStmtRecord(info)
	record.Add(info)
	for n, f := range factories {
		column := f(mockColumnInfo{}, record)
		switch columns[n].Name.O {
		case ClusterTableInstanceColumnNameStr:
			require.Equal(t, "instance_addr", column)
		case StmtTypeStr:
			require.Equal(t, record.StmtType, column)
		case SchemaNameStr:
			require.Equal(t, record.SchemaName, column)
		case DigestStr:
			require.Equal(t, record.Digest, column)
		case DigestTextStr:
			require.Equal(t, record.NormalizedSQL, column)
		case TableNamesStr:
			require.Equal(t, record.TableNames, column)
		case IndexNamesStr:
			require.Equal(t, strings.Join(record.IndexNames, ","), column)
		case SampleUserStr:
			require.Equal(t, info.User, column)
		case ExecCountStr:
			require.Equal(t, int64(1), column)
		case SumLatencyStr:
			require.Equal(t, int64(record.SumLatency), column)
		case MaxLatencyStr:
			require.Equal(t, int64(record.MaxLatency), column)
		case AvgTidbCPUTimeStr:
			require.Equal(t, int64(record.SumTidbCPU), column)
		case AvgTikvCPUTimeStr:
			require.Equal(t, int64(record.SumTikvCPU), column)
		}
	}
}

type mockColumnInfo struct{}

func (mockColumnInfo) getInstanceAddr() string {
	return "instance_addr"
}

func (mockColumnInfo) getTimeLocation() *time.Location {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	return loc
}
