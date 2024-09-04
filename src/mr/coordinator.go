package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "io/ioutil"
import "strconv"
import "strings"
import "time"

var mutex sync.Mutex

const (
	MapJob = "MapJob"
	ReduceJob = "ReduceJob"

	JobStatusPending = "Pending"
	JobStatusRunning = "Running"
	JobStatusDone = "Done"

	PhaseMap = "PhaseMap"
	PhaseReduce = "PhaseReduce"
	PhaseDone = "PhaseDone"
)

type Job struct {
	JobId int
	JobType string
	JobStatus string
	HandleFiles []string
	NReducer int
	StartTime time.Time
}


type Coordinator struct {
	// Your definitions here.
	MapChannel chan *Job
	ReduceChannel chan *Job
	Phase string
	MapFiles []string
	NReducer int
	AllJobs map[int]*Job
}

var jobId int

func generateJobId() int {
	jobId++
	return jobId
}

func (c *Coordinator) checkAllDone() bool {
	done := true
	for _, job := range c.AllJobs {
		if job.JobStatus != JobStatusDone {
			done = false
			break
		}
	}
	return done
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateJob(args *JobRequestArgs, reply *JobRequestReply) error {

	mutex.Lock()
	defer mutex.Unlock()

	log.Print("Receive an allocate job request")
	
	if c.Phase == PhaseMap {
		if len(c.MapChannel) > 0 {
			job := <- c.MapChannel
			job.JobStatus = JobStatusRunning
			job.StartTime = time.Now()
			reply.Job = job
		} else {
			if c.checkAllDone() {
				c.Phase = PhaseReduce
				c.addReduceJobs()
			}
		}
	} else if c.Phase == PhaseReduce {
		if len(c.ReduceChannel) > 0 {
			job := <- c.ReduceChannel
			job.JobStatus = JobStatusRunning
			job.StartTime = time.Now()
			reply.Job = job
		} else {
			if c.checkAllDone() {
				c.Phase = PhaseDone
			}
		}
	}

	return nil
}

func (c *Coordinator) NotifyJobDone(args *NotifyJobDoneArgs, reply *NotifyJobDoneReply) error {
	
	mutex.Lock()
	defer mutex.Unlock()

	log.Print("Notify Job Done, job id: ", args.Job.JobId)

	c.AllJobs[args.Job.JobId].JobStatus = JobStatusDone
	return nil
}

func (c *Coordinator) addMapJobs() {
	for _, file := range c.MapFiles {
		job := &Job {
			JobId: generateJobId(),
			JobType: MapJob,
			JobStatus: JobStatusPending,
			HandleFiles: []string{file},
			NReducer: c.NReducer,
		}
		c.AllJobs[job.JobId] = job
		c.MapChannel <- job
	}
}

func (c *Coordinator) addReduceJobs() {
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)

	for reducer_num:=0; reducer_num < c.NReducer; reducer_num++ {
		handleFiles := []string{}
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reducer_num)) {
				handleFiles = append(handleFiles, file.Name())
			}
		}
		job := &Job {
			JobId: generateJobId(),
			JobType: ReduceJob,
			JobStatus: JobStatusPending,
			HandleFiles: handleFiles,
			NReducer: c.NReducer,
		}
		c.AllJobs[job.JobId] = job
		c.ReduceChannel <- job
	}
}

func (c *Coordinator) recoverFailedJobs() {
	for {
		mutex.Lock()
		current_time := time.Now()
		for _, job := range c.AllJobs {
			if job.JobStatus == JobStatusRunning {
				if current_time.Sub(job.StartTime) > 10 * time.Second {
					log.Printf("Recover a failed job, job id: %v, job type: %v", job.JobId, job.JobType)
					job.JobStatus = JobStatusPending
					switch job.JobType {
					case MapJob:
						c.MapChannel <- job
					case ReduceJob:
						c.ReduceChannel <- job
					}
				}
			}
		}
		mutex.Unlock()
		time.Sleep(1 * time.Second)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.Phase == PhaseDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase: PhaseMap,
		MapFiles: files,
		NReducer: nReduce,
		MapChannel: make(chan *Job, len(files)),
		ReduceChannel: make(chan *Job, nReduce),
		AllJobs: make(map[int]*Job, len(files) + nReduce),
	}

	c.addMapJobs()

	os.Create("lock")

	go c.recoverFailedJobs()


	c.server()
	return &c
}
