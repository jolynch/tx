package utils

import (
	"context"
	"runtime/trace"
)

// TraceTask is a runtime/trace task that exists only while tracing was on
// when it started. Its End is safe either way.
type TraceTask struct{ task *trace.Task }

// NewTraceTask starts a trace task under ctx when tracing is on, and otherwise
// returns ctx unchanged. trace.NewTask allocates a task and a context even
// with tracing off, which on per-file paths is several allocations per file.
// A task started while tracing is off is not traced if tracing begins before
// it ends.
func NewTraceTask(ctx context.Context, taskType string) (context.Context, TraceTask) {
	if !trace.IsEnabled() {
		return ctx, TraceTask{}
	}
	ctx, task := trace.NewTask(ctx, taskType)
	return ctx, TraceTask{task: task}
}

// End ends the task, if one was started.
func (t TraceTask) End() {
	if t.task != nil {
		t.task.End()
	}
}
