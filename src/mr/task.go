package mr

type TaskState uint8

const (
	Idle TaskState = iota + 1
	InProgress
	Completed
)

type Task struct {
	state TaskState
}
