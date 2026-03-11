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

package chunk

import (
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/stretchr/testify/require"
)

func TestSerializeToBytesForOneColumn(t *testing.T) {
	typeCtx := types.DefaultStmtNoWarningContext
	dateValue, err := types.ParseDate(typeCtx, "2024-01-02")
	require.NoError(t, err)
	datetimeValue, err := types.ParseDatetime(typeCtx, "2024-01-02 03:04:05")
	require.NoError(t, err)
	timestampValue, err := types.ParseTimestamp(typeCtx, "2024-01-02 03:04:05")
	require.NoError(t, err)
	jsonValue, err := types.ParseBinaryJSONFromString(`{"k":"v","n":1}`)
	require.NoError(t, err)
	decimalValue := types.NewDecFromStringForTest("123.456")

	enumNameFT := types.NewFieldType(mysql.TypeEnum)
	enumNameFT.SetElems([]string{"aa", "bb", "cc"})
	enumAsIntFT := types.NewFieldType(mysql.TypeEnum)
	enumAsIntFT.SetElems([]string{"aa", "bb", "cc"})
	enumAsIntFT.SetFlag(mysql.EnumSetAsIntFlag)
	setFT := types.NewFieldType(mysql.TypeSet)
	setFT.SetElems([]string{"x", "y", "z"})

	testCases := []struct {
		name        string
		ft          *types.FieldType
		appendValue func(chk *Chunk, colIdx int)
		expected    func(r Row, colIdx int, collator collate.Collator) ([]byte, error)
	}{
		{
			name:        "TypeTiny",
			ft:          types.NewFieldType(mysql.TypeTiny),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendInt64(colIdx, -8) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return append([]byte(nil), r.c.columns[colIdx].GetRaw(r.idx)...), nil
			},
		},
		{
			name:        "TypeShort",
			ft:          types.NewFieldType(mysql.TypeShort),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendInt64(colIdx, -16) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return append([]byte(nil), r.c.columns[colIdx].GetRaw(r.idx)...), nil
			},
		},
		{
			name:        "TypeInt24",
			ft:          types.NewFieldType(mysql.TypeInt24),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendInt64(colIdx, -24) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return append([]byte(nil), r.c.columns[colIdx].GetRaw(r.idx)...), nil
			},
		},
		{
			name:        "TypeLong",
			ft:          types.NewFieldType(mysql.TypeLong),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendInt64(colIdx, -32) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return append([]byte(nil), r.c.columns[colIdx].GetRaw(r.idx)...), nil
			},
		},
		{
			name:        "TypeLonglong",
			ft:          types.NewFieldType(mysql.TypeLonglong),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendInt64(colIdx, -64) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return append([]byte(nil), r.c.columns[colIdx].GetRaw(r.idx)...), nil
			},
		},
		{
			name:        "TypeYear",
			ft:          types.NewFieldType(mysql.TypeYear),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendInt64(colIdx, 2024) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return append([]byte(nil), r.c.columns[colIdx].GetRaw(r.idx)...), nil
			},
		},
		{
			name: "TypeDuration",
			ft:   types.NewFieldType(mysql.TypeDuration),
			appendValue: func(chk *Chunk, colIdx int) {
				chk.AppendDuration(colIdx, types.Duration{Duration: -time.Second, Fsp: 0})
			},
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return append([]byte(nil), r.c.columns[colIdx].GetRaw(r.idx)...), nil
			},
		},
		{
			name: "TypeFloat",
			ft:   types.NewFieldType(mysql.TypeFloat),
			appendValue: func(chk *Chunk, colIdx int) {
				chk.AppendFloat32(colIdx, math.Float32frombits(1<<31))
			},
			expected: func(_ Row, _ int, _ collate.Collator) ([]byte, error) {
				return float64ToBytes(0), nil
			},
		},
		{
			name: "TypeDouble",
			ft:   types.NewFieldType(mysql.TypeDouble),
			appendValue: func(chk *Chunk, colIdx int) {
				chk.AppendFloat64(colIdx, math.Copysign(0, -1))
			},
			expected: func(_ Row, _ int, _ collate.Collator) ([]byte, error) {
				return float64ToBytes(0), nil
			},
		},
		{
			name:        "TypeVarchar",
			ft:          types.NewFieldType(mysql.TypeVarchar),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendString(colIdx, "varchar-value") },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				return collator.ImmutableKey(r.GetString(colIdx)), nil
			},
		},
		{
			name:        "TypeVarString",
			ft:          types.NewFieldType(mysql.TypeVarString),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendString(colIdx, "varstring-value") },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				return collator.ImmutableKey(r.GetString(colIdx)), nil
			},
		},
		{
			name:        "TypeString",
			ft:          types.NewFieldType(mysql.TypeString),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendString(colIdx, "string-value") },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				return collator.ImmutableKey(r.GetString(colIdx)), nil
			},
		},
		{
			name:        "TypeBlob",
			ft:          types.NewFieldType(mysql.TypeBlob),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendBytes(colIdx, []byte{0x62, 0x6c, 0x6f, 0x62, 0x00, 0x01}) },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				return collator.ImmutableKey(string(r.GetBytes(colIdx))), nil
			},
		},
		{
			name:        "TypeTinyBlob",
			ft:          types.NewFieldType(mysql.TypeTinyBlob),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendBytes(colIdx, []byte{0x74, 0x62, 0x6c, 0x6f, 0x62}) },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				return collator.ImmutableKey(string(r.GetBytes(colIdx))), nil
			},
		},
		{
			name:        "TypeMediumBlob",
			ft:          types.NewFieldType(mysql.TypeMediumBlob),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendBytes(colIdx, []byte("medium-blob")) },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				return collator.ImmutableKey(string(r.GetBytes(colIdx))), nil
			},
		},
		{
			name:        "TypeLongBlob",
			ft:          types.NewFieldType(mysql.TypeLongBlob),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendBytes(colIdx, []byte("long-blob")) },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				return collator.ImmutableKey(string(r.GetBytes(colIdx))), nil
			},
		},
		{
			name:        "TypeDate",
			ft:          types.NewFieldType(mysql.TypeDate),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendTime(colIdx, dateValue) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				v, err := r.GetTime(colIdx).ToPackedUint()
				if err != nil {
					return nil, err
				}
				return uint64ToBytes(v), nil
			},
		},
		{
			name:        "TypeDatetime",
			ft:          types.NewFieldType(mysql.TypeDatetime),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendTime(colIdx, datetimeValue) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				v, err := r.GetTime(colIdx).ToPackedUint()
				if err != nil {
					return nil, err
				}
				return uint64ToBytes(v), nil
			},
		},
		{
			name:        "TypeTimestamp",
			ft:          types.NewFieldType(mysql.TypeTimestamp),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendTime(colIdx, timestampValue) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				v, err := r.GetTime(colIdx).ToPackedUint()
				if err != nil {
					return nil, err
				}
				return uint64ToBytes(v), nil
			},
		},
		{
			name:        "TypeNewDecimal",
			ft:          types.NewFieldType(mysql.TypeNewDecimal),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendMyDecimal(colIdx, decimalValue) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return r.GetMyDecimal(colIdx).ToHashKey()
			},
		},
		{
			name:        "TypeEnumName",
			ft:          enumNameFT,
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendEnum(colIdx, types.Enum{Name: "bb", Value: 2}) },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				enum, err := types.ParseEnumValue(enumNameFT.GetElems(), r.GetEnum(colIdx).Value)
				if err != nil {
					return nil, err
				}
				return collator.ImmutableKey(enum.Name), nil
			},
		},
		{
			name:        "TypeEnumAsInt",
			ft:          enumAsIntFT,
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendEnum(colIdx, types.Enum{Name: "cc", Value: 3}) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return uint64ToBytes(r.GetEnum(colIdx).Value), nil
			},
		},
		{
			name:        "TypeSet",
			ft:          setFT,
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendSet(colIdx, types.Set{Name: "x,z", Value: 5}) },
			expected: func(r Row, colIdx int, collator collate.Collator) ([]byte, error) {
				set, err := types.ParseSetValue(setFT.GetElems(), r.GetSet(colIdx).Value)
				if err != nil {
					return nil, err
				}
				return collator.ImmutableKey(set.Name), nil
			},
		},
		{
			name:        "TypeBit",
			ft:          types.NewFieldType(mysql.TypeBit),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendBytes(colIdx, []byte{0x12, 0x34}) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				v, err := types.BinaryLiteral(r.GetBytes(colIdx)).ToInt(typeCtx)
				if err != nil {
					return nil, err
				}
				return uint64ToBytes(v), nil
			},
		},
		{
			name:        "TypeJSON",
			ft:          types.NewFieldType(mysql.TypeJSON),
			appendValue: func(chk *Chunk, colIdx int) { chk.AppendJSON(colIdx, jsonValue) },
			expected: func(r Row, colIdx int, _ collate.Collator) ([]byte, error) {
				return r.GetJSON(colIdx).HashValue(nil), nil
			},
		},
	}

	fieldTypes := make([]*types.FieldType, 0, len(testCases))
	for _, tc := range testCases {
		fieldTypes = append(fieldTypes, tc.ft)
	}
	chk := NewChunkWithCapacity(fieldTypes, 1)
	for colIdx, tc := range testCases {
		tc.appendValue(chk, colIdx)
	}

	row := chk.GetRow(0)
	collator := collate.GetBinaryCollator()
	for colIdx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := row.SerializeToBytesForOneColumn(typeCtx, tc.ft, colIdx, collator)
			require.NoError(t, err)

			expected, err := tc.expected(row, colIdx, collator)
			require.NoError(t, err)
			require.Equal(t, expected, got)
		})
	}
}

func TestSerializeToBytesForOneColumnNullPanic(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeLonglong)
	chk := NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	chk.AppendNull(0)
	row := chk.GetRow(0)

	require.Panics(t, func() {
		_, _ = row.SerializeToBytesForOneColumn(types.DefaultStmtNoWarningContext, ft, 0, collate.GetBinaryCollator())
	})
}

func uint64ToBytes(v uint64) []byte {
	return append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)...)
}

func float64ToBytes(v float64) []byte {
	return append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeFloat64)...)
}
