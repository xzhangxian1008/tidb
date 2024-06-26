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

package log

import (
	"strings"

	"go.uber.org/zap/zapcore"
)

var _ zapcore.Core = (*FilterCore)(nil)

// FilterCore is a zapcore.Core implementation, it filters log by path-qualified
// package name.
type FilterCore struct {
	zapcore.Core
	filters []string
}

// NewFilterCore returns a FilterCore, only logs under allowPackages will be written.
//
// Example, only write br's log and ignore any other, `NewFilterCore(core, "github.com/pingcap/tidb/br/")`.
// Note, must set AddCaller() to the logger.
func NewFilterCore(core zapcore.Core, allowPackages ...string) *FilterCore {
	return &FilterCore{
		Core:    core,
		filters: allowPackages,
	}
}

// With adds structured context to the Core.
func (f *FilterCore) With(fields []zapcore.Field) zapcore.Core {
	return &FilterCore{
		Core:    f.Core.With(fields),
		filters: f.filters,
	}
}

// Check overrides wrapper core.Check and adds itself to zapcore.CheckedEntry.
func (f *FilterCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if f.Enabled(entry.Level) {
		return ce.AddCore(entry, f)
	}
	return ce
}

// Write filters entry by checking if entry's Caller.Function matches filtered
// package path.
func (f *FilterCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	for i := range f.filters {
		// Caller.Function is a package path-qualified function name.
		if strings.Contains(entry.Caller.Function, f.filters[i]) {
			return f.Core.Write(entry, fields)
		}
	}
	return nil
}
