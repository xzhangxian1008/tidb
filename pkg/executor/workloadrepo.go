// Copyright 2024 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// TakeSnapshot is a hook from workload repo that may trigger manual snapshot.
var TakeSnapshot func(context.Context) error

// WorkloadRepoCreateExec indicates WorkloadRepoCreate executor.
type WorkloadRepoCreateExec struct {
	exec.BaseExecutor
}

// Next implements the Executor Next interface.
func (*WorkloadRepoCreateExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if TakeSnapshot != nil {
		return TakeSnapshot(ctx)
	}
	return nil
}
