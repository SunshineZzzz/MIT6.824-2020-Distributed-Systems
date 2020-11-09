package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// RPC相关定义
// 正常和错误的常量定义
const (
	OK = "OK"
	ErrNoKey = "ErrNoKey"
)
// 错误类型
type Err string
// 设置key-value请求类型
type PutArgs struct {
	Key string
	Value string
}
// 设置key-value请求的返回类型
type PutReply struct {
	Err Err
}
// 获取key-value请求类型
type GetArgs struct {
	Key string
}
// 获取key-value请求的返回类型
type GetReply struct {
	Err Err
	Value string
}

// Client
// 发起rpc连接
func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}
// 获取key-value
func get(key string) string {
	client := connect()
	args := GetArgs{"subject"}
	reply := GetReply{}
	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}
// 设置key-value
func put(key string, val string) {
	client := connect()
	args := PutArgs{"subject", "6.824"}
	reply := PutReply{}
	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
}

// Server
// k-v类型
type KV struct {
	mu sync.Mutex
	data map[string]string
}
// 开启rpc server
func server() {
	kv := new(KV)
	kv.data = map[string]string{}
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
	}()
}
// Get RPC 接口
func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.data[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}
// put RPC 接口
func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[args.Key] = args.Value
	reply.Err = OK
	return nil
}

// main
func main() {
	server()

	put("subject", "6.824")
	fmt.Printf("Put(subject, 6.824) done\n")
	fmt.Printf("get(subject) -> %s\n", get("subject"))
}