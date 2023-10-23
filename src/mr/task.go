package mr

// JobType represents the
type JobType uint8

const (
	MapJob JobType = iota + 1
	ReduceJob
)

type TaskState uint8

const (
	Idle TaskState = iota + 1
	InProgress
	Completed
)

type Task struct {
	state TaskState
}
