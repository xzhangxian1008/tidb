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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func createColumnByTypeAndLen(tp byte, cl uint32) *column.Info {
	return &column.Info{
		Schema:       "test",
		Table:        "dual",
		OrgTable:     "",
		Name:         "a",
		OrgName:      "a",
		ColumnLength: cl,
		Charset:      uint16(mysql.CharsetNameToID(charset.CharsetUTF8)),
		Flag:         uint16(mysql.UnsignedFlag),
		Decimal:      uint8(0),
		Type:         tp,
		DefaultValue: nil,
	}
}
func TestConvertColumnInfo(t *testing.T) {
	// Test "mysql.TypeBit", for: https://github.com/pingcap/tidb/issues/5405.
	ftb := types.NewFieldTypeBuilder()
	ftb.SetType(mysql.TypeBit).SetFlag(mysql.UnsignedFlag).SetFlen(1).SetCharset(charset.CharsetUTF8).SetCollate(charset.CollationUTF8)
	resultField := resolve.ResultField{
		Column: &model.ColumnInfo{
			Name:      ast.NewCIStr("a"),
			ID:        0,
			Offset:    0,
			FieldType: ftb.Build(),
			Comment:   "column a is the first column in table dual",
		},
		ColumnAsName: ast.NewCIStr("a"),
		TableAsName:  ast.NewCIStr("dual"),
		DBName:       ast.NewCIStr("test"),
	}
	colInfo := column.ConvertColumnInfo(&resultField)
	require.Equal(t, createColumnByTypeAndLen(mysql.TypeBit, 1), colInfo)

	// Test "mysql.TypeTiny", for: https://github.com/pingcap/tidb/issues/5405.
	ftpb := types.NewFieldTypeBuilder()
	ftpb.SetType(mysql.TypeTiny).SetFlag(mysql.UnsignedFlag).SetFlen(1).SetCharset(charset.CharsetUTF8).SetCollate(charset.CollationUTF8)
	resultField = resolve.ResultField{
		Column: &model.ColumnInfo{
			Name:      ast.NewCIStr("a"),
			ID:        0,
			Offset:    0,
			FieldType: ftpb.Build(),
			Comment:   "column a is the first column in table dual",
		},
		ColumnAsName: ast.NewCIStr("a"),
		TableAsName:  ast.NewCIStr("dual"),
		DBName:       ast.NewCIStr("test"),
	}
	colInfo = column.ConvertColumnInfo(&resultField)
	require.Equal(t, createColumnByTypeAndLen(mysql.TypeTiny, 1), colInfo)

	ftpb1 := types.NewFieldTypeBuilder()
	ftpb1.SetType(mysql.TypeYear).SetFlag(mysql.ZerofillFlag).SetFlen(4).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin)
	resultField = resolve.ResultField{
		Column: &model.ColumnInfo{
			Name:      ast.NewCIStr("a"),
			ID:        0,
			Offset:    0,
			FieldType: ftpb1.Build(),
			Comment:   "column a is the first column in table dual",
		},
		ColumnAsName: ast.NewCIStr("a"),
		TableAsName:  ast.NewCIStr("dual"),
		DBName:       ast.NewCIStr("test"),
	}
	colInfo = column.ConvertColumnInfo(&resultField)
	require.Equal(t, uint32(4), colInfo.ColumnLength)

	// Test unspecified length
	for _, tp := range []byte{
		mysql.TypeBit,
		mysql.TypeTiny,
		mysql.TypeShort,
		mysql.TypeLong,
		mysql.TypeLonglong,
		mysql.TypeFloat,
		mysql.TypeDouble,
		mysql.TypeNewDecimal,
		mysql.TypeDuration,
		mysql.TypeDate,
		mysql.TypeTimestamp,
		mysql.TypeDatetime,
		mysql.TypeYear,
		mysql.TypeString,
		mysql.TypeVarchar,
		mysql.TypeVarString,
		mysql.TypeTinyBlob,
		mysql.TypeBlob,
		mysql.TypeMediumBlob,
		mysql.TypeLongBlob,
		mysql.TypeJSON,
	} {
		ftb = types.NewFieldTypeBuilder()
		ftb.SetType(tp).SetFlen(types.UnspecifiedLength)
		resultField = resolve.ResultField{
			Column: &model.ColumnInfo{
				Name:      ast.NewCIStr("a"),
				ID:        0,
				Offset:    0,
				FieldType: ftb.Build(),
				Comment:   "column a is the first column in table dual",
			},
			ColumnAsName: ast.NewCIStr("a"),
			TableAsName:  ast.NewCIStr("dual"),
			DBName:       ast.NewCIStr("test"),
		}
		colInfo = column.ConvertColumnInfo(&resultField)
		expectedLen, _ := mysql.GetDefaultFieldLengthAndDecimal(tp)
		require.Equal(t, colInfo.ColumnLength, uint32(expectedLen))
		require.NotZero(t, colInfo.ColumnLength)
	}
}
