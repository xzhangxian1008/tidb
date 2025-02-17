// Copyright 2022 PingCAP, Inc.
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

package staleread

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// enforce implement Processor interface
var _ Processor = &staleReadProcessor{}

// StalenessTSEvaluator is a function to get staleness ts
type StalenessTSEvaluator func(ctx context.Context, sctx sessionctx.Context) (uint64, error)

// Processor is an interface used to process stale read
type Processor interface {
	// IsStaleness indicates that whether we should use the staleness
	IsStaleness() bool
	// GetStalenessInfoSchema returns the information schema if it is stale read, otherwise returns nil
	GetStalenessInfoSchema() infoschema.InfoSchema
	// GetStalenessReadTS returns the ts if it is stale read, otherwise returns 0
	GetStalenessReadTS() uint64
	// GetStalenessTSEvaluatorForPrepare returns a function that will be used by prepare to evaluate ts
	GetStalenessTSEvaluatorForPrepare() StalenessTSEvaluator

	// OnSelectTable will be called when process table in select statement
	OnSelectTable(tn *ast.TableName) error
	// OnExecutePreparedStmt when process execute
	OnExecutePreparedStmt(preparedTSEvaluator StalenessTSEvaluator) error
}

type baseProcessor struct {
	ctx        context.Context
	sctx       sessionctx.Context
	txnManager sessiontxn.TxnManager

	evaluated   bool
	ts          uint64
	tsEvaluator StalenessTSEvaluator
	is          infoschema.InfoSchema
}

func (p *baseProcessor) init(ctx context.Context, sctx sessionctx.Context) {
	p.ctx = ctx
	p.sctx = sctx
	p.txnManager = sessiontxn.GetTxnManager(sctx)
}

func (p *baseProcessor) IsStaleness() bool {
	return p.ts != 0
}

func (p *baseProcessor) GetStalenessInfoSchema() infoschema.InfoSchema {
	return p.is
}

func (p *baseProcessor) GetStalenessReadTS() uint64 {
	return p.ts
}

func (p *baseProcessor) GetStalenessTSEvaluatorForPrepare() StalenessTSEvaluator {
	return p.tsEvaluator
}

func (p *baseProcessor) OnSelectTable(_ *ast.TableName) error {
	return errors.New("not supported")
}

func (p *baseProcessor) OnExecutePrepared(_ StalenessTSEvaluator) error {
	return errors.New("not supported")
}

func (p *baseProcessor) setAsNonStaleRead() error {
	return p.setEvaluatedValues(0, nil, nil)
}

func (p *baseProcessor) setEvaluatedTS(ts uint64) (err error) {
	is, err := GetSessionSnapshotInfoSchema(p.sctx, ts)
	if err != nil {
		return err
	}

	return p.setEvaluatedValues(ts, is, func(_ context.Context, sctx sessionctx.Context) (uint64, error) {
		return ts, nil
	})
}

// setEvaluatedTSWithoutEvaluator sets the ts, but not set the evaluator, so it doesn't affect prepare statement
func (p *baseProcessor) setEvaluatedTSWithoutEvaluator(ts uint64) (err error) {
	is, err := GetSessionSnapshotInfoSchema(p.sctx, ts)
	if err != nil {
		return err
	}

	return p.setEvaluatedValues(ts, is, nil)
}

func (p *baseProcessor) setEvaluatedEvaluator(evaluator StalenessTSEvaluator) error {
	ts, err := evaluator(p.ctx, p.sctx)
	if err != nil {
		return err
	}

	is, err := GetSessionSnapshotInfoSchema(p.sctx, ts)
	if err != nil {
		return err
	}

	return p.setEvaluatedValues(ts, is, evaluator)
}

func (p *baseProcessor) setEvaluatedValues(ts uint64, is infoschema.InfoSchema, tsEvaluator StalenessTSEvaluator) error {
	if p.evaluated {
		return errors.New("already evaluated")
	}

	p.ts = ts
	p.is = is
	p.evaluated = true
	p.tsEvaluator = tsEvaluator
	return nil
}

type staleReadProcessor struct {
	baseProcessor
	stmtTS uint64
}

// NewStaleReadProcessor creates a new stale read processor
func NewStaleReadProcessor(ctx context.Context, sctx sessionctx.Context) Processor {
	p := &staleReadProcessor{}
	p.init(ctx, sctx)
	return p
}

// OnSelectTable will be called when process table in select statement
func (p *staleReadProcessor) OnSelectTable(tn *ast.TableName) error {
	if p.sctx.GetSessionVars().InTxn() {
		if tn.AsOf != nil {
			return plannererrors.ErrAsOf.FastGenWithCause("as of timestamp can't be set in transaction.")
		}

		if !p.evaluated {
			return p.evaluateFromTxn()
		}
		return nil
	}

	// If `stmtAsOfTS` is not 0, it means we use 'select ... from xxx as of timestamp ...'
	evaluateTS := func(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
		return parseAndValidateAsOf(ctx, p.sctx, tn.AsOf)
	}
	stmtAsOfTS, err := evaluateTS(p.ctx, p.sctx)
	if err != nil {
		return err
	}

	if p.evaluated {
		// If the select statement is related to multi tables, we should guarantee that all tables use the same timestamp
		if p.stmtTS != stmtAsOfTS {
			return plannererrors.ErrAsOf.GenWithStack("can not set different time in the as of")
		}
		return nil
	}
	return p.evaluateFromStmtTSOrSysVariable(stmtAsOfTS, evaluateTS)
}

func (p *staleReadProcessor) OnExecutePreparedStmt(preparedTSEvaluator StalenessTSEvaluator) (err error) {
	if p.evaluated {
		return errors.New("already evaluated")
	}

	if p.sctx.GetSessionVars().InTxn() {
		if preparedTSEvaluator != nil {
			return plannererrors.ErrAsOf.FastGenWithCause("as of timestamp can't be set in transaction.")
		}
		return p.evaluateFromTxn()
	}

	var stmtTS uint64
	if preparedTSEvaluator != nil {
		// If the `preparedTSEvaluator` is not nil, it means the prepared statement is stale read
		if stmtTS, err = preparedTSEvaluator(p.ctx, p.sctx); err != nil {
			return err
		}
	}
	// When executing a prepared stmt, the stmtTS is calculated once and reused to avoid eval overhead,
	// note it only takes PlanCacheStmt.SnapshotTSEvaluator without overwriting it.
	// the evaluator will be re-calculated in next execution.
	return p.evaluateFromStmtTSOrSysVariable(stmtTS, nil)
}

func (p *staleReadProcessor) evaluateFromTxn() error {
	// sys variables should be ignored when in an explicit transaction
	if txnCtx := p.sctx.GetSessionVars().TxnCtx; txnCtx.IsStaleness {
		// It means we meet following case:
		// 1. `start transaction read only as of timestamp ts
		// 2. select or execute statement
		return p.setEvaluatedValues(
			txnCtx.StartTS,
			temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, txnCtx.InfoSchema.(infoschema.InfoSchema)),
			nil,
		)
	}

	// Don't consider external ts, but just set non-stale read directly,because stepping here means
	// when the transaction begins, the external ts read hasn't been turned on, but it was turned
	// on during the transaction. Ignore it to avoid unexpected stepping back.
	return p.setAsNonStaleRead()
}

func (p *staleReadProcessor) evaluateFromStmtTSOrSysVariable(stmtTS uint64, evaluator StalenessTSEvaluator) error {
	// If `txnReadTS` is not 0, it means  we meet following situation:
	// set transaction read only as of timestamp ...
	// select from table or execute prepared statement
	txnReadTS := p.sctx.GetSessionVars().TxnReadTS.UseTxnReadTS()
	if txnReadTS > 0 && stmtTS > 0 {
		// `as of` and `@@tx_read_ts` cannot be set in the same time
		return plannererrors.ErrAsOf.FastGenWithCause("can't use select as of while already set transaction as of")
	}

	if stmtTS > 0 {
		p.stmtTS = stmtTS
		if evaluator != nil {
			is, err := GetSessionSnapshotInfoSchema(p.sctx, stmtTS)
			if err != nil {
				return err
			}
			return p.setEvaluatedValues(stmtTS, is, evaluator)
		}
		return p.setEvaluatedTS(stmtTS)
	}

	if txnReadTS > 0 {
		return p.setEvaluatedTS(txnReadTS)
	}

	if evaluator := getTsEvaluatorFromReadStaleness(p.sctx); evaluator != nil {
		// If both txnReadTS and stmtAsOfTS is empty while the return of getTsEvaluatorFromReadStaleness is not nil, it means we meet following situation:
		// set @@tidb_read_staleness='-5';
		// select from table
		// Then the following select statement should be affected by the tidb_read_staleness in session.
		return p.setEvaluatedEvaluator(evaluator)
	}

	ts, err := getTSFromExternalTS(p.ctx, p.sctx)
	if err != nil {
		return plannererrors.ErrAsOf.FastGenWithCause(err.Error())
	}
	if ts > 0 {
		return p.setEvaluatedTSWithoutEvaluator(ts)
	}

	// Otherwise, it means we should not use stale read.
	return p.setAsNonStaleRead()
}

func parseAndValidateAsOf(ctx context.Context, sctx sessionctx.Context, asOf *ast.AsOfClause) (uint64, error) {
	if asOf == nil {
		return 0, nil
	}

	ts, err := CalculateAsOfTsExpr(ctx, sctx.GetPlanCtx(), asOf.TsExpr)
	if err != nil {
		return 0, err
	}

	if err = sessionctx.ValidateSnapshotReadTS(ctx, sctx.GetStore(), ts, true); err != nil {
		return 0, err
	}

	return ts, nil
}

func getTsEvaluatorFromReadStaleness(sctx sessionctx.Context) StalenessTSEvaluator {
	readStaleness := sctx.GetSessionVars().ReadStaleness
	if readStaleness == 0 {
		return nil
	}

	return func(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
		return CalculateTsWithReadStaleness(ctx, sctx, readStaleness)
	}
}

func getTSFromExternalTS(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
	if sctx.GetSessionVars().EnableExternalTSRead && !sctx.GetSessionVars().InRestrictedSQL {
		externalTimestamp, err := GetExternalTimestamp(ctx, sctx.GetSessionVars().StmtCtx)
		if err != nil {
			return 0, err
		}
		return externalTimestamp, nil
	}

	return 0, nil
}

// GetSessionSnapshotInfoSchema returns the session's information schema with specified ts
func GetSessionSnapshotInfoSchema(sctx sessionctx.Context, snapshotTS uint64) (infoschema.InfoSchema, error) {
	is, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return nil, err
	}
	return temptable.AttachLocalTemporaryTableInfoSchema(sctx, is), nil
}
