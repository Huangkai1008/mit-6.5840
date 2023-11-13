package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type SchedulePhase uint8

const (
	MapPhase SchedulePhase = iota + 1
	ReducePhase
)

const MaxTaskRunInterval = 10 * time.Second

type Coordinator struct {
	files   []string
	phase   SchedulePhase
	tasks   []Task
	nReduce int

	heartBeatCh chan HeartBeatMessage
}

type HeartBeatMessage struct {
	reply *HeartBeatReply
	ok    chan struct{}
}

func (c *Coordinator) HeartBeat(args *HeartBeatRequest, reply *HeartBeatReply) error {
	message := HeartBeatMessage{
		reply: reply,
		ok:    make(chan struct{}),
	}

	c.heartBeatCh <- message

	<-message.ok
	return nil
}

func (c *Coordinator) schedule() {
	c.startMapPhase()
	for {
		select {
		case <-c.heartBeatCh:
		}
	}
}

func (c *Coordinator) startMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, len(c.files))
	for index, filePath := range c.files {
		c.tasks[index] = Task{
			filePath: filePath,
			state:    Idle,
		}
	}
}

func (c *Coordinator) AssignTask(reply *HeartBeatReply) {
	for id, task := range c.tasks {
		switch task.state {
		case Idle:
			task.state = InProgress
			task.startTime = time.Now()

			reply.taskNumber = id
			reply.nReduce = c.nReduce

			if c.phase == MapPhase {
				reply.jobType = MapJob
				reply.filePath = task.filePath
			} else {
				reply.jobType = ReduceJob
			}

		case InProgress:
			if time.Now().Sub(task.startTime) > MaxTaskRunInterval {
				task.startTime
			}

		case Completed:

		}
	}

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
//
// Coordinator start rpc server for Worker.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.server()
	return &c
}
