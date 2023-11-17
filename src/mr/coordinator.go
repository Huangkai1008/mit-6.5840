package mr

import (
	"log"
	"sync"
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
	FinishedPhase
)

const TimeoutCheckInterval = 5 * time.Second
const MaxTaskRunInterval = 10 * time.Second

type Coordinator struct {
	mu sync.Mutex

	files   []string
	nReduce int

	phase     SchedulePhase
	taskQueue chan *Task
	taskMeta  map[int]*Task

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

// MakeCoordinator returns a new coordinator.
//
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:     files,
		nReduce:   nReduce,
		phase:     MapPhase,
		taskQueue: make(chan *Task, max(len(files), nReduce)),
	}

	c.startMapPhase()

	c.server()

	go c.schedule()
	return &c
}

func (c *Coordinator) startMapPhase() {
	for index, filePath := range c.files {
		task := &Task{
			filePath: filePath,
			state:    Idle,
		}

		c.taskQueue <- task
		c.taskMeta[index] = task
	}
}

func (c *Coordinator) schedule() {
	c.startMapPhase()

	for {
		select {
		case message := <-c.heartBeatCh:
			switch c.phase {
			case MapPhase:

			case FinishedPhase:
				message.reply.jobType = ExitJob
			}
		}
	}
}

func (c *Coordinator) startTimer() {
	ticker := time.NewTicker(TimeoutCheckInterval)
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.phase == FinishedPhase {
				c.mu.Unlock()
				return
			}

			for _, task := range c.taskMeta {
				if task.state == InProgress && time.Now().Sub(task.startTime) > MaxTaskRunInterval {
					task.state = Idle
					c.taskQueue <- task
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) assignTask(reply *HeartBeatReply) {

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
				task.startTime = time.Now()
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}
