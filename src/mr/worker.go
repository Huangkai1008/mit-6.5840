package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"sync"
)
import "log"
import "hash/fnv"

// A KeyValue slice is the return of Map functions.
type KeyValue struct {
	Key   string
	Value string
}

// use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker function called by main/mrworker.go.
// It accepts a mapF function and a reduceF function.
func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	for {
		reply := heatBeat()
		log.Printf("Worker: receive coordinator's heatbeat %v \n", reply)

		switch reply.jobType {
		case MapJob:
			executeMapTask(mapF, reply)
		case ReduceJob:
			executeReduceTask(reduceF, reply)
		}
	}
}

// The worker heatBeat coordinator for task periodically.
func heatBeat() *HeartBeatReply {
	args := HeartBeatRequest{}
	reply := HeartBeatReply{}
	call("Coordinator.HeartBeat", &args, &reply)
	return &reply
}

func executeMapTask(mapF func(string, string) []KeyValue, reply *HeartBeatReply) {
	filename := reply.filePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v : %s", filename, err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v : %s", filename, err)
	}

	if err := file.Close(); err != nil {
		log.Fatalf("Cannot close %v : %s", filename, err)
	}

	kva := mapF(filename, string(content))

	// Reduce invocations are distributed by partitioning the intermediate key
	// space into R pieces using a partitioning function (e.g., hash(key) mod R)
	intermediates := make([][]KeyValue, reply.nReduce)
	for _, kv := range kva {
		index := iHash(kv.Key) % reply.nReduce
		intermediates[index] = append(intermediates[index], kv)
	}

	var wg sync.WaitGroup
	taskNumber := reply.taskNumber
	for index, intermediate := range intermediates {
		wg.Add(1)

		go func(index int, intermediate []KeyValue) {
			defer wg.Done()

			fileName := nameOfMapResultFile(taskNumber, index)

			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("Cannot encode json %v: %s", kv.Key, err)
				}
			}

			if err := atomicCommitFile(fileName, &buf); err != nil {
				log.Fatalf("Cannot commit map result file %s: %s", fileName, err)
			}

		}(index, intermediate)
	}
	wg.Wait()

	// Report to coordinator after the map task finished.
	report(taskNumber)
}

func executeReduceTask(reduceF func(string, []string) string, reply *HeartBeatReply) {
	var wg sync.WaitGroup
	var kva []KeyValue
	for index := 0; index < reply.nMap; index++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			fileName := nameOfMapResultFile(index, reply.taskNumber)
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("Cannot open %s : %s", fileName, err)
			}

			defer func() {
				if err := file.Close(); err != nil {
					log.Fatalf("Cannot close %s : %s", fileName, err)
				}
			}()

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}

		}(index)
	}
}

// According to the hint of lab1, I use `mr-X-Y` as the name of intermediate files
// where X is the Map task number, and Y is the reduce task number.
func nameOfMapResultFile(mapTaskNumber int, reduceTaskNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceTaskNumber)
}

// The output of the X'th reduce task in the file `mr-out-X`.
func nameOfReduceResultFile(reduceTaskNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskNumber)
}

func atomicCommitFile(filename string, r io.Reader) (err error) {
	tmpFile, err := os.CreateTemp(os.TempDir(), "mr-tmp-")
	if err != nil {
		log.Fatalf("cannot create temporary file: %s", err)
	}

	tmpFileName := tmpFile.Name()

	log.Printf("temporary file %s has created", tmpFileName)

	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()

	defer func() {
		_ = tmpFile.Close()
	}()

	// Write data to temporary file.
	if _, err := io.Copy(tmpFile, r); err != nil {
		return fmt.Errorf("cannot write data to temporary file: %s", err)
	}

	if err := os.Rename(tmpFileName, filename); err != nil {
		return fmt.Errorf("cannot rename temporary file: %s to %s: %s", tmpFileName, filename, err)
	}

	return nil
}

func report(taskNumber int) {
	args := ReportRequest{
		taskNumber: taskNumber,
	}

	reply := ReportReply{}
	call("Coordinator.Report", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
