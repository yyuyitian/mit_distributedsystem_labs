v1.0
单机版mapreduce：
步骤：
1、go build -race -buildmode=plugin ../mrapps/wc.go 编译wc.go文件，这个文件包含了map和reduce的自定义实现，是插件化的形式，所以是单独编译的。这部分是由mapreduce框架的使用方自行实现的。
2、go run -race mrcoordinator.go pg-*.txt 编译并运行mrcoordinator.go文件，这个文件里面实际调用的是coordinator，这个时候，它开始监听并等待map worker向他请求任务
3、go run -race mrworker.go wc.so map 在不同的终端运行四次，运行四个map worker，分别向coordinator请求任务，并拿到任务调用进行map处理，将处理完毕的信号发给coordinator
4、go run -race mrworker.go wc.so reduce 在不同的终端运行四次，运行四个reduce worker，reduce worker向coordinator请求任务，根据coordinator发送的任务编号从本地获取相应的文件进行reduce处理。这一点将在v2.0进行改进，即先运行reduce worker并等待，等coordinator接收到所有的map worker处理完毕的请求之后，再通知等待中的reduce worker执行任务。在实验材料中建议这样做：One possibility is for workers to periodically ask the coordinator for work, sleeping with time.Sleep() between each request. Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with time.Sleep() or sync.Cond. Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.