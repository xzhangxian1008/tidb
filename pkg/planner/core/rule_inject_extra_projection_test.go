// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInjectProjBelowAggKeepsPBColumnRefsDistinct(t *testing.T) {
	ctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	inputCols := []*expression.Column{
		{Index: 0, RetType: intType},
		{Index: 1, RetType: intType},
		{Index: 2, RetType: intType},
	}
	child := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	child.SetSchema(expression.NewSchema(inputCols...))

	sumArg := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.Plus, intType, inputCols[2], expression.NewOne())
	aggFunc, err := aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)
	aggSchema := expression.NewSchema(&expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: aggFunc.RetTp})
	agg := (&physicalop.BasePhysicalAgg{
		AggFuncs:     []*aggregation.AggFuncDesc{aggFunc},
		GroupByItems: []expression.Expression{inputCols[0], inputCols[1]},
	}).InitForHash(ctx, &property.StatsInfo{RowCount: 1}, 0, aggSchema, &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}).(*physicalop.PhysicalHashAgg)
	agg.SetChildren(child)

	InjectProjBelowAgg(agg, agg.AggFuncs, agg.GroupByItems)
	proj, ok := agg.Children()[0].(*physicalop.PhysicalProjection)
	require.True(t, ok)
	require.Len(t, proj.Exprs, 3)
	require.Len(t, proj.Schema().Columns, 3)
	groupSchema, ok := agg.GroupByItems[0].(*expression.Column)
	require.True(t, ok)
	groupDigest, ok := agg.GroupByItems[1].(*expression.Column)
	require.True(t, ok)
	require.NotEqual(t, groupSchema.Index, groupDigest.Index)
}
