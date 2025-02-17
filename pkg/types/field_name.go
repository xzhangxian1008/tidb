// Copyright 2019 PingCAP, Inc.
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

package types

import (
	"bytes"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/size"
)

// FieldName records the names used for mysql protocol.
type FieldName struct {
	OrigTblName ast.CIStr
	OrigColName ast.CIStr
	DBName      ast.CIStr
	TblName     ast.CIStr
	ColName     ast.CIStr

	Hidden bool

	// NotExplicitUsable is used for mark whether a column can be explicit used in SQL.
	// update stmt can write `writeable` column implicitly but cannot use non-public columns explicit.
	// e.g. update t set a = 10 where b = 10; which `b` is in `writeOnly` state
	NotExplicitUsable bool

	Redundant bool
}

const emptyName = "EMPTY_NAME"

// String implements Stringer interface.
func (name *FieldName) String() string {
	if name.Hidden {
		return emptyName
	}
	bs := make([]byte, 0, len(name.DBName.L)+1+len(name.TblName.L)+1+len(name.ColName.L))
	builder := bytes.NewBuffer(bs)
	if name.DBName.L != "" {
		builder.WriteString(name.DBName.L + ".")
	}
	if name.TblName.L != "" {
		builder.WriteString(name.TblName.L + ".")
	}
	builder.WriteString(name.ColName.L)
	return builder.String()
}

// MemoryUsage return the memory usage of FieldName
func (name *FieldName) MemoryUsage() (sum int64) {
	if name == nil {
		return
	}

	sum = name.OrigTblName.MemoryUsage() + name.OrigColName.MemoryUsage() + name.DBName.MemoryUsage() +
		name.TblName.MemoryUsage() + name.ColName.MemoryUsage() + size.SizeOfBool*3
	return
}

// NameSlice is the slice of the *fieldName
type NameSlice []*FieldName

// Shallow is a shallow copy, only making a new slice.
func (s NameSlice) Shallow() NameSlice {
	ret := make(NameSlice, len(s))
	copy(ret, s)
	return ret
}

// EmptyName is to occupy the position in the name slice. If it's set, that column's name is hidden.
var EmptyName = &FieldName{Hidden: true}

// FindAstColName checks whether the given ast.ColumnName is appeared in this slice.
func (s NameSlice) FindAstColName(name *ast.ColumnName) bool {
	for _, fieldName := range s {
		if (name.Schema.L == "" || name.Schema.L == fieldName.DBName.L) &&
			(name.Table.L == "" || name.Table.L == fieldName.TblName.L) &&
			name.Name.L == fieldName.ColName.L {
			return true
		}
	}
	return false
}
