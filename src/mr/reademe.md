v1.0
单机版mapreduce：
步骤：
1、go build -race -buildmode=plugin ../mrapps/wc.go 编译wc.go文件，这个文件包含了map和reduce的自定义实现，是插件化的形式，所以是单独编译的。这部分是由mapreduce框架的使用方自行实现的。
2、go run -race mrcoordinator.go pg-*.txt 编译并运行mrcoordinator.go文件，这个文件里面实际调用的是coordinator，这个时候，它开始监听并等待map worker向他请求任务
3、go run -race mrworker.go wc.so map 在不同的终端运行四次，运行四个map worker，分别向coordinator请求任务，并拿到任务调用进行map处理，将处理完毕的信号发给coordinator
4、go run -race mrworker.go wc.so reduce 在不同的终端运行四次，运行四个reduce worker，reduce worker向coordinator请求任务，根据coordinator发送的任务编号从本地获取相应的文件进行reduce处理。这一点将在v2.0进行改进，即先运行reduce worker并等待，等coordinator接收到所有的map worker处理完毕的请求之后，再通知等待中的reduce worker执行任务。在实验材料中建议这样做：One possibility is for workers to periodically ask the coordinator for work, sleeping with time.Sleep() between each request. Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with time.Sleep() or sync.Cond. Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.

改进点：
1、协调员应该注意到工人是否没有在合理的时间内完成任务（对于本实验，使用十秒），并将相同的任务交给不同的工人。

V2.0
1、reduce worker的数量在启动coodinator的时候指配，可任意修改，根据ihash(key) % reducenum 方式将map worker处理的结果映射到不同的reduce index的文件中
2、一个worker既可以充当reduce又可以充当map，map工作做完了之后继续向coodinator请求工作，coodinator根据是否所有的map工作已经结束来判断是继续将map工作派给这个worker还是将reduce工作派给这个worker。
3、分配给map worker的工作被划分为最小单位，即每次只派一个文件处理任务给worker。根据lecture 2 中老师讲的一段话：
How does MR get good load balance?
  Wasteful and slow if N-1 servers have to wait for 1 slow server to finish.
  But some tasks likely take longer than others.
  Solution: many more tasks than workers.
    Coordinator hands out new tasks to workers who finish previous tasks.
    So no task is so big it dominates completion time (hopefully).
    So faster servers do more tasks than slower ones, finish abt the same time.
4、测试结果，每个worker每间隔5s向master请求任务，当只有一个worker的时候，需要花费1分7秒的时间；当有4个worker的时候，总共需要花费15秒的时间完成工作；当有8个worker的时候，总共需要花费9秒的时间完成工作；当有10个worker的时候，总共需要花费11秒的时间完成工作。
5、对比分布式和深度遍历（执行mrsequential.so）：深度遍历耗时3秒；分布式worker每隔500毫秒请求一次任务，需要花费8秒的时间；虽然对比结果是这样我想是没有什么意义的，因为两种方式都是在一台机器上运行的，他们的算力是相同的，反而分布式因为不同进程之间的相互切换和通信会浪费很多时间。所以必须要真实使用多台机器才能看出真正的效果。

v3.0
1、实现容错。如果任务进行的过程中，map worker或者reduce worker挂掉了，coodinator将worker未完成的任务分配给其他的worker，否则coodinator将会一直等待worker任务完成，而不中止进程。
2、如何实现容错？每次将任务派给一个worker就开启一个新的线程，检查10秒内worker有没有完成任务，检查10次，每次间隔1秒种，如果中间检查到已经完成任务，就提前中止线程。如果10秒钟之后还是检查没有完成任务，那就将没有完成的这个任务的index放入到一个队列，这里未完成的map task和未完成的reduce task分开放入两个失败队列中，每一次worker过来向coordinator请求任务，coordinator优先派出失败队列中的任务，然后再派发正常队列中未派完的任务。

设计说明：
master数据结构：
var maptaskstodeliever int[]; // 待分配的maptasks，每个元素代表文件的索引值，数组长度是文件的个数;其中包含处理失败的map tasks
var reducetaskstodeliever int[]; // 待分配的reducetasks,每个元素代表reduce worker的索引值，数组长度是reduce worker的总数;其中包含处理失败的reduce workers
var maptasksresults boolean[]; // 所有的maptasks的完成情况
var reducetasksresults boolean[]; // 所有的reducetasks的完成情况
var maptasksnum int; // 待分配的maptasks总数量
var reducetasksnum int; // 待分配的reducetasks总数量
var finishedmaptasknum int; // 已经完成的maptasks总数量
var fiinishedreducenum int; // 已经完成的reducetasks总数量

master设计思路：
在finishedmaptasknum < maptasksnum的时候，首先将maptaskstodeliever中的任务分配给worker，每分配一个任务就开启一个新的线程检查worker是否完成超时，如果超时则将该任务重新加入maptaskstodeliever中；异步接收worker完成map task的消息，每次接收到一次消息，就将在finishedmaptasknum增加1，同时将maptasksresults中对应的元素置为true；在finishedmaptasknum == maptasksnum之后，再分配reducetaskstodeliever的任务，同理按照上面的流程去处理。
