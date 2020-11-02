### 规则

1. `map`阶段应该将中间结果按照key划分到`nReduce`个文件中，等待`reduce task`处理，其中`nReduce`是`main/mrmaster.go`传递给`MakeMaster()`的参数

2. `worker`的实现应该将第`X`个`reduce`任务结果输出在文件`mr-out-X`中

3. `mr-out-X`文件的每个`Reduce`函数输出应该包含一行，该行应以`GO "%v%v"`格式生成，在`main/mrsequential.go`中查看

4. `main/mrmaster.go`需要`mr/master.go`实现`Done()`函数，该函数在`MapReduce`程序完成时返回`true`，这个时候`mrmaster.go`将退出

5. 任务完成，`worker`进程将退出，一个简单的实现方法是用`call()`的返回值：如果`worker`与`master`连接失败，可以假定主服务器由于任务完成而退出，因为	`worker`也可以退出。另一种实现方法，`master`可以发送退出任务给`worker`

### 提示

1. 一种开始的方法是修改`mr/worker.go`使用`RPC`发送给`master`请求任务。 然后`master`使用尚未开始的`map task`的文件名进行响应。 然后`worker`读取文件调用`Map function`，如`mrsequential.go`所示

2. 中间文件的合理命名约定是`mr-X-Y`，其中`X`是`Map`任务号，`Y`是`reduce`任务号。

3. 中间文件存储方式可以选择`encoding/json`，将`key/value`写入`JSON`文件：
```go
	enc := json.NewEncoder(file)
	for _, kv := ... {
		err := enc.Encode(&kv)

````
读取该文件
```go
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
```

4. `worker`的`map`阶段使用`ihash(key)`为给定的`key`选择对应的`reduce`任务

5. `worker`有时候需要等待，比如，`reduce`必须要等到最后一个map任务结束以后才可以开始。一种实现方式是`worker`定期向`master`请求任务，其他时间`time.Sleep()`

6. `master`无法区分崩溃的`worker`和因为某些原因执行缓慢而无法使用的`worker`。你能做的最好的事情就是让`master`等待一段时间，然后放弃并将任务重新发送给其他`worker`。在本次实验中`master`等待10秒，超出时间后`master`假设`worker`已经死亡

7. 为了确保`worker`崩溃时不会写入部分文件到中间文件中，`MapReduce`论文中提到了使用临时文件并在文件完全写入后自动重命名的技巧。你可以使用`ioutil.TempFile`创建一个临时文件，并使用`os.Rename`原子地对其进行重命名。