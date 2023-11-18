package mr

import "time"

type JobType uint8

const (
	MapJob JobType = iota + 1
	ReduceJob
	WaitJob
	ExitJob
)

type TaskState uint8

const (
	Idle TaskState = iota + 1
	InProgress
	Completed
)

// Task accept the FilePath as input,
// there are different TaskState in different working stages.
type Task struct {
	taskNumber int
	filePath   string
	state      TaskState
	startTime  time.Time
}
