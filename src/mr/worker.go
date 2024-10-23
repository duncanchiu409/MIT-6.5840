package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func (kv *KeyValue) WriteJsonFile(enc *json.Encoder) {
	err := enc.Encode(kv)
	if err != nil {
		log.Fatalf("write File failed : %v", err)
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CallGetTask() (*GetTaskReply, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply, nil
	} else {
		return nil, errors.New("GetTask failed")
	}
}

func executeMTask(taskId int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot read %v : %v", fileName, err)
	}

	kva := mapf(fileName, string(content))
	kvmap := map[int][]KeyValue{}
	for _, kv := range kva {
		rNumber := ihash(kv.Key) % nReduce
		_, ok := kvmap[rNumber]
		if !ok {
			kvmap[rNumber] = []KeyValue{kv}
		} else {
			kvmap[rNumber] = append(kvmap[rNumber], kv)
		}
	}

	for i := 0; i < nReduce; i++ {
		file, err := os.Create(fmt.Sprintf("mr-%v-%v", taskId, i))
		if err != nil {
			log.Fatalf("file creation failed: %v", err)
		}

		kvs, ok := kvmap[i]
		if ok {
			enc := json.NewEncoder(file)
			for _, kv := range kvs {
				kv.WriteJsonFile(enc)
			}
		}

		file.Close()
	}
}

func executeRTask(taskId int, reducef func(string, []string) string) {
	filesPath, err := WalkDir("./", taskId)
	if err != nil {
		log.Fatalf("walk dir failed %v", err)
	}
	kvs := []KeyValue{}
	for _, file := range filesPath {
		fileReader, err := os.Open(file)
		if err != nil {
			log.Fatalf("file reading failed %v", err)
		}

		decoder := json.NewDecoder(fileReader)
		for {
			kv := KeyValue{}
			if decoder.Decode(&kv) != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		err = fileReader.Close()
		if err != nil {
			log.Fatalf("file reader close failed %v", err)
		}
	}

	oFile, err := os.Create(fmt.Sprintf("mr-out-%d", taskId))
	if err != nil {
		log.Fatalf("output file creation failed :%v", err)
	}

	slices.SortFunc(kvs, func(a KeyValue, b KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		fmt.Fprintf(oFile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
}

func WalkDir(root string, rNumber int) ([]string, error) {
	files := []string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err1 error) error {
		if d.IsDir() {
			return nil
		}
		match, err := regexp.Match(fmt.Sprintf(`mr-\d-%d`, rNumber), []byte(path))
		if err != nil {
			return err
		}
		if match {
			files = append(files, path)
			return nil
		}
		return nil
	})
	if err != nil {
		return files, err
	}
	return files, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		reply, err := CallGetTask()
		if err != nil {
			break
		}
		switch reply.TaskType {
		case mapTaskType:
			executeMTask(reply.TaskId, reply.FileName, reply.NReduce, mapf)
			CallUpdateTaskStatus(reply.TaskId, reply.TaskType)
		case reduceTaskType:
			executeRTask(reply.TaskId, reducef)
			CallUpdateTaskStatus(reply.TaskId, reply.TaskType)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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

func CallUpdateTaskStatus(taskId int, taskType TaskType) error {
	args := UpdateTaskStatusArgs{
		TaskId:   taskId,
		TaskType: taskType,
	}
	reply := UpdateTaskStatusReply{}

	ok := call("Coordinator.UpdateTaskStatus", &args, &reply)
	if !ok {
		return errors.New("call failed")
	} else {
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// log.Println(err)
	return false
}
