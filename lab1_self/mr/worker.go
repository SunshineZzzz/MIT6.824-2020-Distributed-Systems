package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"

// for sorting by key.
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int { 
	return len(a) 
}
func (a ByKey) Swap(i, j int) { 
	a[i], a[j] = a[j], a[i] 
}
func (a ByKey) Less(i, j int) bool { 
	return a[i].Key < a[j].Key 
}

// workerid
var gId uint64

// Map函数返回KeyValue类型的切片
type KeyValue struct {
	Key string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by MapPhase.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go调用该函数
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 你的worker代码
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)

	// 注释掉下面的行，可以发送一个RPC请求到Master
	// CallExample()
	reply := CallHello()
	gId = reply.Id
	for {
		// 获取任务
		reply := CallRequireTask()
		log.Printf("%d require task %v\n", gId, reply)
		if reply.IsDone {
			break
		}
		if reply.PoolDone {
			time.Sleep(time.Second)
			continue
		}
		// 执行任务
		ok, err := todoTask(reply.Task, mapf, reducef)
		if !ok {
			log.Printf("%d todoTask err: %s\n", gId, err)
		}
		// 报告任务
		CallReportTask(reply.Task.Index, ok)
	}
	return
}

// 实例，如何发送RPC请求到master
func CallExample() {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}

	call("Master.Example", &args, &reply)
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// helllo
func CallHello() HelloReply {
	args := HelloArgs{}
	reply := HelloReply{}
	if ok := call("Master.HelloRPC", &args, &reply); !ok {
		log.Fatal("Call Master.HelloRPC failed")
	}
	log.Printf("hello %v\n", reply)
	return reply
}

// require task
func CallRequireTask() ReqTaskReply {
	args := ReqTaskArgs{}
	args.Id = gId
	reply := ReqTaskReply{}
	if ok := call("Master.RequireTaskRPC", &args, &reply); !ok {
		log.Fatalf("%d require task failed", gId)
	}
	log.Printf("%d get task %v\n", gId, reply)
	return reply
}

// report task
func CallReportTask(index uint64, success bool) RepTaskReply {
	args := RepTaskArgs{}
	args.Id = gId
	args.Index = index
	args.IsDone = success
	reply := RepTaskReply{}
	if ok := call("Master.ReportTaskRPC", &args, &reply); !ok {
		log.Fatalf("%d report task failed", gId)
	}
	log.Printf("%d report %v task %v\n", gId, index, reply)
	return reply	
}

// 发送一个RPC请求给master，等待返回结果
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock()
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

// 执行任务
func todoTask(task Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string) (bool, error) {
	if task.Phase == TaskPhaseMap {
		return todoMapTask(task, mapf)
	} else if task.Phase == TaskPhaseReduce {
		return todoReduceTask(task, reducef)
	}
	return false, fmt.Errorf("task.Phase err %s", task.Phase)
}

// map任务
func todoMapTask(task Task, mapf func(string, string) []KeyValue) (bool, error) {
	log.Printf("%d begin map task %v \n", gId, task)

	file, err := os.Open(task.FileName)
	if err != nil {
		return false, err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return false, err
	}
	kva := mapf(task.FileName, string(content))
	intermediateFiles := make([]*os.File, task.ReduceNum)
	defer func() {
		for _, file := range intermediateFiles {
			file.Close()
		}
	}()
	for i := 0; i < task.ReduceNum; i++ {
		intermediateFileName := "mr" + "-" + strconv.FormatUint(task.Index, 10) + "-" + strconv.Itoa(i)
		file, err := os.OpenFile(intermediateFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
		if err != nil {
			return false, err
		}
		intermediateFiles[i] = file
	}
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % task.ReduceNum
		enc := json.NewEncoder(intermediateFiles[reduceIndex])
		err := enc.Encode(&kv)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// reduce任务
func todoReduceTask(task Task, reducef func(string, []string) string) (bool, error) {
	log.Printf("%d begin reduce task %v \n", gId, task)

	// 不要创建空切片
	// intermediate := []KeyValue{}
	var intermediate []KeyValue
	for _, index := range task.MapIndex {
		intermediateFileName := "mr" + "-" + strconv.FormatUint(index, 10) + "-" + strconv.Itoa(task.OriginIndex)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			return false, err
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}	
	}
	sort.Sort(ByKey(intermediate))
	oname := "reduce-tmp-mr-out-" + strconv.FormatUint(task.Index, 10)
	ofile, _ := os.Create(oname)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	return true, nil
}
