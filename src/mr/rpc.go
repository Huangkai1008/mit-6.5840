package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and request for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type HeartBeatRequest struct{}

// The HeartBeatReply is the request that coordinator to worker.
// It notifies the worker the JobType it should handle
// and the FilePath of input data.
// It returns NReduce as the number of partitions.
type HeartBeatReply struct {
	JobType    JobType
	FilePath   string
	NFiles     int
	NReduce    int
	TaskNumber int
}

func (h *HeartBeatReply) String() string {
	return fmt.Sprintf("Reply(JobType = %v, TaskNumber = %d)", h.JobType, h.TaskNumber)
}

type ReportRequest struct {
	TaskNumber int
}

type ReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
