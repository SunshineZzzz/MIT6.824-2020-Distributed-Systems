package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// 你的定义

}

// 你的代码 -- 提供给worker的RPC相关函数

// 一个RPC函数的实例
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 开启一个协程监听RPC为worker提供服务
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go 周期性的调用该函数，该函数
// 返回所有worker job是否完成任务
func (m *Master) Done() bool {
	ret := false

	// 你的代码

	return ret
}

// 创建一个Master
// main/mrmaster.go 调用该函数
// nReduce代表reduce task数量
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// 你的代码

	m.server()
	return &m
}