package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

// Map函数返回KeyValue类型的切片
type KeyValue struct {
	Key string
	Value string
}

// Map函数处理完成后生成KeyValue
// 使用ihash(key) % NReduce来选择reduce task number
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go调用该函数
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 你的worker代码

	// 注释掉下面的行，可以发送一个RPC请求到Master
	// CallExample()
}

// 实例，如何发送RPC请求到master
func CallExample() {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}

	call("Master.Example", &args, &reply)
	fmt.Printf("reply.Y %v\n", reply.Y)
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