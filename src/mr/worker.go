package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "time"
import "os"
import "io/ioutil"
import "sort"
import "syscall"

var alive = true


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	log.Print("Start worker")

	for alive {
		reply := &JobRequestReply{}
		call("Coordinator.AllocateJob", &JobRequestArgs{}, reply)
		
		if reply.Job != nil {
			jsonData, _ := json.Marshal(reply.Job)
			log.Print("Receive Job: Job id: ", string(jsonData))
			if reply.Job.JobType == MapJob {
				doMap(reply.Job, mapf)
			} else {
				doReduce(reply.Job, reducef)
			}

			notifyJobDoneArgs := &NotifyJobDoneArgs{
				Job: reply.Job,
			}
			call("Coordinator.NotifyJobDone", notifyJobDoneArgs, &NotifyJobDoneReply{})
		} else {
			log.Print("Receive empty job, wait for few seconds")
			time.Sleep(time.Second * 5)
		}
	}
}

func checkAndCreateFile(filename string) (bool, *os.File) {
	lockF, _ := os.Open("lock")
	defer lockF.Close()

	if err := syscall.Flock(int(lockF.Fd()), syscall.LOCK_EX); err != nil {
        log.Println("Add ex lock failed", err)
    }
	if _, err := os.Stat(filename); err == nil {
		log.Println("Target file exists", filename)
		return true, nil
	}
	f, _ := os.Create(filename)
	if err := syscall.Flock(int(lockF.Fd()), syscall.LOCK_UN); err != nil {
        log.Println("Unlock lock failed", err)
    }
	return false, f
}

func doMap(job *Job, mapf func(string, string) []KeyValue) {
	filename := job.HandleFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Cannot open file %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, job.NReducer)
	for _, kv := range kva {
		intermediate[ihash(kv.Key) % job.NReducer] = append(intermediate[ihash(kv.Key) % job.NReducer], kv)
	}
	for reducer_num, _ := range intermediate {
		tmp_file_name := fmt.Sprintf("mr-tmp-%d-%d", job.JobId, reducer_num)
		existed, tmp_file := checkAndCreateFile(tmp_file_name)
		if existed {
			log.Printf("Map file has been created, file: %v", tmp_file_name)
			continue
		}
		enc := json.NewEncoder(tmp_file)
		for _, kv := range intermediate[reducer_num] {
			enc.Encode(kv)
		}
		tmp_file.Close()
	}
}

func doReduce(job *Job, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, filename := range job.HandleFiles {
		mapResult, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Failed to open map result file, err: %v", err)
		}
		dec := json.NewDecoder(mapResult)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		mapResult.Close()
	}
	sort.Sort(ByKey(intermediate))

	outputs := []*KeyValue{}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		outputs = append(outputs, &KeyValue{
			Key: intermediate[i].Key,
			Value: output,
		})

		i = j
	}

	output_filename := fmt.Sprintf("mr-out-%d", job.JobId)
	existed, output_file := checkAndCreateFile(output_filename)

	if existed {
		log.Printf("Reduce file has been created, file: %v", output_filename)
		return
	}

	for _, kva := range outputs {
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(output_file, "%v %v\n", kva.Key, kva.Value)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}
