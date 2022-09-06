// Code generated by execgen; DO NOT EDIT.
// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// TODO(yuzefovich): add benchmarks.

// NewRowNumberOperator creates a new Operator that computes window function
// ROW_NUMBER. outputColIdx specifies in which coldata.Vec the operator should
// put its output (if there is no such column, a new column is appended).
func NewRowNumberOperator(args *WindowArgs) colexecop.Operator {
	input := colexecutils.NewVectorTypeEnforcer(
		args.MainAllocator, args.Input, types.Int, args.OutputColIdx)
	base := rowNumberBase{
		OneInputHelper:  colexecop.MakeOneInputHelper(input),
		allocator:       args.MainAllocator,
		outputColIdx:    args.OutputColIdx,
		partitionColIdx: args.PartitionColIdx,
	}
	if args.PartitionColIdx == -1 {
		return &rowNumberNoPartitionOp{base}
	}
	return &rowNumberWithPartitionOp{base}
}

// rowNumberBase extracts common fields and common initialization of two
// variations of row number operators. Note that it is not an operator itself
// and should not be used directly.
type rowNumberBase struct {
	colexecop.OneInputHelper
	allocator       *colmem.Allocator
	outputColIdx    int
	partitionColIdx int

	rowNumber int64
}

type rowNumberNoPartitionOp struct {
	rowNumberBase
}

var _ colexecop.Operator = &rowNumberNoPartitionOp{}

func (r *rowNumberNoPartitionOp) Next() coldata.Batch {
	batch := r.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	rowNumberVec := batch.ColVec(r.outputColIdx)
	rowNumberCol := rowNumberVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		for _, i := range sel[:n] {
			r.rowNumber++
			rowNumberCol[i] = r.rowNumber
		}
	} else {
		_ = rowNumberCol[n-1]
		for i := 0; i < n; i++ {
			r.rowNumber++
			//gcassert:bce
			rowNumberCol[i] = r.rowNumber
		}
	}
	return batch
}

type rowNumberWithPartitionOp struct {
	rowNumberBase
}

var _ colexecop.Operator = &rowNumberWithPartitionOp{}

func (r *rowNumberWithPartitionOp) Next() coldata.Batch {
	batch := r.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	rowNumberVec := batch.ColVec(r.outputColIdx)
	rowNumberCol := rowNumberVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		for _, i := range sel[:n] {
			if partitionCol[i] {
				r.rowNumber = 0
			}
			r.rowNumber++
			rowNumberCol[i] = r.rowNumber
		}
	} else {
		_ = partitionCol[n-1]
		_ = rowNumberCol[n-1]
		for i := 0; i < n; i++ {
			//gcassert:bce
			if partitionCol[i] {
				r.rowNumber = 0
			}
			r.rowNumber++
			//gcassert:bce
			rowNumberCol[i] = r.rowNumber
		}
	}
	return batch
}
