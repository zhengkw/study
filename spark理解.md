# spark理解

## 前言

一直困扰我的是RDD的分区和节点有什么关系。HDFS上的文件是以文件块block来存储的。文件读取这些分片以后和task的数量关系是什么呢。

## Spark-core

这里简单叙述下，file分多个block，一个inputSplit是由多个block合并而成的。但是inputSplit**不能跨文件**！一个inputSplit与Task是一一对应的关系。这些Task每个都会被分配到集群上的某个节点的某个Executor去执行。

- 一个节点可以起一个或者多个Executor。
- 每个Executor是由若干个core组成，这里的core是指虚拟core（多线程技术），每个core一次只执行1个Task。
- 每个Task执行后的结果就是生成了目标RDD的一个partition。

## SparkStreaming

```scala
//间隔3秒
val ssc = new StreamingContext(conf, Seconds(3))
//将stream转rdd 每3秒一个rdd
DataStream.transform(rdd=>{

})
```



## 总结

Task的并行度是=num-executors(节点数)*executor-cores(每个节点核心数)

一个task对应一个分区对应一个core来处理（避免浪费）

多少个inputSplit决定多少个task 

可以通过修改默认分区来设置分区数

max（文件分片数，sc.defaultMinPartitions）

streaming中用transform处理所有的RDD