# 一	Spark入门

## 1.1 什么是Spark？

Spark是一个快速（基于内存），通用，可扩展的**集群计算引擎**。

## 1.2 Sprak的特点有哪些？

### 1.2.1 快速

与hadoop的MR相比，Spark基于内存的运算是MR的100倍，基于硬盘的运算也要快10倍以上。Spark实现了高效的DAG执行引擎，可以通过基于内存来高效处理数据流。 

![image-20200504183540672](D:\ProgramFiles\Typora\图片备份\image-20200504183540672.png)

### 1.2.2 易用

Spark 支持 Scala, Java, Python, R 和 SQL 脚本, 并提供了超过 80 种高性能的算法, 非常容易创建并行 App，而且 Spark 支持交互式的 Python 和 Scala 的 shell, 这意味着可以非常方便地在这些 shell 中使用 Spark 集群来验证解决问题的方法, 而不是像以前一样 需要打包, 上传集群, 验证等. 这对于原型开发非常重要。

![image-20200504183549026](D:\ProgramFiles\Typora\图片备份\image-20200504183549026.png)

### 1.2.3 通用

Spark 结合了SQL, Streaming和复杂分析.

Spark 提供了大量的类库, 包括 SQL 和 DataFrames, 机器学习(MLlib), 图计算(GraphicX), 实时流处理(Spark Streaming) .

可以把这些类库无缝的糅合在一个 App 中.

减少了开发和维护的人力成本以及部署平台的物力成本.

![image-20200504183630733](D:\ProgramFiles\Typora\图片备份\image-20200504183630733.png)

### 1.2.4 可融合性

Spark 可以非常方便的与其他开源产品进行融合.

比如, Spark 可以使用 Hadoop 的 YARN 和 Appache Mesos 作为它的资源管理和调度器, 并且可以处理所有 Hadoop 支持的数据, 包括 HDFS, HBase等.

![image-20200504183708963](D:\ProgramFiles\Typora\图片备份\image-20200504183708963.png)

## 1.3 Spark内置模块简介

![image-20200504183823835](D:\ProgramFiles\Typora\图片备份\image-20200504183823835.png)

### 1.3.1 集群管理器（Cluster Manager）

Spark设计为可以高效的在一个计算节点到数千个计算节点之间伸缩计算。

为了实现这样的要求，同时获得最大灵活性，Spark支持在各种集群管理器（Cluster Manager）上运行，目前Spark支持3种集群管理器：

- Hadoop YARN（在国内使用最广泛）
- Apache Mesos（国内使用较少，国外使用较多）
- Standalone（Spark自带的资源调度器，需要在集群中的每台节点上配置Spark）

### 1.3.2 SparkCore

实现了Spark的基本功能，包含任务调度、内存管理、错误恢复、与存储系统交互等模块。SparkCore中还包含了对弹性分布式数据集（Resilient Distributed DataSet，简称**RDD**)）的API定义。

### 1.3.3 Spark SQL

是Spark用来操作结构化数据的程序包。通过SparkSql，我们可以使用SQL或者ApacheHive版本的SQL方言（HQL）来查询数据。SparkSQL支持多种数据源，比如Hive表、Parquet以及JSON等。

### 1.3.4 Spark Streaming

是Spark提供的对实时数据进行流式计算的组件。提供了用来操作数据流的API，并且与SparkCore中的RDD API高度对应。

### 1.3.5 Spark MLlib

提供常见的机器学习功能的程序库。包括分类、回归、聚类、协同过滤等，还提供了模型评估、数据导入等额外的支持功能。

## 1.4 Spark核心组件概念介绍

### 1.4.1 Master

Spark特有资源调度系统的Leader。掌管整个集群的资源信息，类似于Yarn框架中的ResourceManager，主要功能如下：

1）监听Worker，看Worker是否正常工作。

2）Master对Worker、Application等的管理，如下：

- 接收Worker的注册并管理所有的Worker。
- 接收Client提交的Application。
- 调度等待的Application并向Wroker提交。

### 1.4.2 Worker

Spark持有资源调度系统的从节点，有多个。每个从节点掌管着所在节点的资源信息，类似于YARN框架中的NodeManager，主要功能如下：

- 通过RegisterWorker注册到Master。
- 定时发送心跳给Master。
- 根据Master发送的Application配置进程环境，并启动ExecutorBackend（执行Task所需的临时进程）

### 1.4.3 driver program（驱动程序）

每个Spark应用程序都包含一个驱动程序，驱动程序负责把并行操作发布到集群上，驱动程序包含Spark应用程序中的主函数，定义了分布式数据集以应用在集群中。

### 1.4.4 executor（执行器）

SparkContext对象一旦成功连接到集群管理器，就可以获取到集群中每个节点上的执行器***executor***

执行器是一个进程（进程名：ExecutorBackend，运行在Worker上）用来执行计算和为应用程序存储数据。

Spark会发送应用程序代码（比如：jar包）到每个执行器，最后SparkContext对象发送任务到执行器开始执行程序。

![image-20200504194300912](D:\ProgramFiles\Typora\图片备份\image-20200504194300912.png)

### 1.4.5 RDDs（弹性分布式数据集）

一旦拥有了SparkContext对象，就可以使用它来创建RDD了。

- 在wordcount例子中，我们调用sc.textFile(...)来创建了一个 RDD，表示文件中的每一行文本。我们可以对这些文本行运行各种各样的操作。

### 1.4.6 cluster manager（集群管理器）

为了在一个Spark集群上运行计算，SparkContext对象可以连接到几种集群管理器（Spark自己的独立集群管理器，Mesos或YARN）

集群管理器负责跨应用程序分配资源。

## 1.5 总结

![image-20200504185020209](D:\ProgramFiles\Typora\图片备份\image-20200504185020209.png)

由图可以看出，Spark像以火花般的速度，在集群管理器之上工作着，集群管理器管理者各自的分布式存储系统。

# 二	Spark运行模式

## 2.1 Spark Local模式

### 2.1.1 什么是Spark Local模式？

Local模式指的就是只在一台计算机上运行Spark，通常用于测试，实际生产环境中不会使用Local模式。

### 2.1.2 搭建步骤

1）解压Spark安装包

```bash
tar -zxvf spark-2.1.1-bin-hadoop2.7.tgz -C /opt/module
```

2）复制目录并将目录名称更改为`spark-local`以方便区分。

```bash
cp -r spark-2.1.1-bin-hadoop2.7 spark-local
```

### 2.1.3 spark-submit基本使用

#### 1）运行官方案例求PI

```
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master local[2] \
./examples/jars/spark-examples_2.11-2.1.1.jar 100
```

官方案例求PI的算法是利用蒙特卡罗算法实现的。

#### 2）关于spark-submit发布的参数说明

`bin/spark-submit`：使用spark-submit来发布应用程序。



`--class <main-class>`：程序的主方法入口。



`--master <master-url>`：master的URL参数。



`--deploy-mode <deploy-mode>`：是否发布你的驱动到 **worker**节点(**cluster** 模式) 或者作为一个本地客户端 (**client** 模式) (**default: client**)，对于local模式来说，该设置没有意义。



`--conf <key>=<value>`：任意的 Spark 配置属性， 格式**key=value**. 如果值包含空格，可以加引号**"key=value"**



`application-jar`：打包好的应用 jar,包含依赖. 这个 URL 在集群中全局可见。 比如**hdfs:// 共享存储系统**， 如果是 **file:// path**， 那么所有的节点的path都包含同样的jar。主程序如果需要传参，参数 跟在jar包后面。



`--executor-memory 1G`： 指定每个**executor**可用内存为1G



`--total-executor-cores 6`：指定所有executor使用的cpu核心数为6个。



`--executor-cores`：表示每个executor使用的CPU核数。



#### 3）关于Master URL的参数说明

| Master URL            | Meaning                                                      |
| --------------------- | ------------------------------------------------------------ |
| **local**             | 使用一个工作线程在本地运行Spark(即完全不存在并行性)。        |
| **local[K]**          | 使用K个工作线程在本地运行Spark(理想情况下，将其设置为计算机上的核心数量)。 |
| **local[\*]**         | 在本地运行Spark，使用与机器上的逻辑核心相同的工作线程。      |
| **spark://HOST:PORT** | 连接到给定的[Spark单机集群](http://spark.apache.org/docs/2.1.1/spark-standalone.html)主服务器。该端口必须是您的主服务器配置使用的端口，默认情况下是7077。 |
| **mesos://HOST:PORT** | 连接到给定的[Mesos](http://spark.apache.org/docs/2.1.1/run宁-on-mesos.html)集群。 |
| **yarn**              | 连接到一个[YARN](http://spark.apache.org/docs/2.1.1/run宁-on-yarn.html)集群，根据**部署模式**的值，连接到**client**或**cluster**模式下的一个[YARN](http://spark.apache.org/docs/2.1.1/run宁-on-yarn.html)。集群位置将基于**HADOOP_CONF_DIR**或**YARN_CONF_DIR**变量找到。 |

### 2.1.4 Spark-shell基本使用

#### 1）什么是Spark-shell？

Spark-shell 是 Spark 给我们提供的交互式命令窗口(类似于 Scala 的 REPL)

**什么是REPL？**

REPL(Read Eval Print Loop:交互式解释器) 表示一个电脑的环境，类似 Window 系统的终端或 Unix/Linux shell，我们可以在终端中输入命令，并接收系统的响应。

#### 2）wroldcount案例

使用Spark-shell来统计文件中各个单词的数量。

①在spark目录下打开Spark-shell

```bash
bin/spark-shell
```

②在界面运行程序

```
sc
.textFile("./input")
.flatMap(_.split(" "))
.map((_,1))
.reduceByKey(_+_)
.collect
```

#### 3）关于WorldCount参数说明

- sc：spark的上下文对象
- textFile()：读取文件输入，括号内传入文件路径，该方法会一行一行的读取文件路径下的所有文件，注意传入的路径下不能包含文件夹。
- flatMap()：先map改变数据结构，在对集合进行压平操作。
- map()：一进一出，改变数据的原有结构。
- reduceByKey()：按key分组，value聚合。等同于GroupBy+MapValues
- collect：收集，拉取。将数据收集到**Driver**端展示。

#### 4）对驱动程序的再说明

在wordcount案例中，spark-shell就是我们的驱动程序，所以我们可以在其中键入我们的操作，然后由他负责发布。

驱动程序通过sparkContext对象来访问spark，sparkContext对象相当于一个到Spark集群的连接，在Spark-Shell中会自动创建一个SparkContext对象，并将这个对象命名为sc。

![image-20200504194005272](D:\ProgramFiles\Typora\图片备份\image-20200504194005272.png)

## 2.2 Sparkalone模式

### 2.2.1 什么是Sparkalone模式？

Sparkalone模式就是构建一个由Mater+Worker组成的Spark集群，即Spark运行在自己独立的集群管理器上。相对于运行在YARN和Mesos来说，只用Spark来搭建一个集群，不需要借助其他两个框架。

### 1.3.1 模式搭建

1）复制spark文件夹，并命名为**spark-standalone**以方便区分。

2）进入Spark目录下的conf目录，复制conf目录下的**spark-env.sh.template**，并命名为**spark-evn.sh**

```bash
cp spark-env.sh.template spark-env.sh
```

3）在**spark-env.sh**文件中添加信息，指定master的地址

```bash
SPARK_MASTER_HOST=hadoop102
SPARK_MASTER_PORT=7077 # 默认端口就是7077, 可以省略不配
```

4）修改slaves文件，添加worker节点

```bash
mv slaves.template slaves
```

在slaves文件中配置节点信息：

```
hadoop102
hadoop103
hadoop104
```

5）分发spark-standalone文件到其他节点。

### 1.3.2 Standalone的基本使用

**使用Standalone模式运行任务**

1）启动Spark集群

```bash
sbin/start-all.sh
```

#### 方式一：使用spark-submit计算PI

```scala
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://hadoop102:7077 \
--deploy-mode client
--executor-memory 1G \
--total-executor-cores 6 \
--executor-cores 2 \
./examples/jars/spark-examples_2.11-2.1.1.jar 100
```

注：

- client表示driver在客户端（提交应用的位置）。

- cluster表示driver在集群的某个worker上。

  该设置在yarn模式下照样成立。

#### 方式二：使用Spark-shell执行wordcount

- 启动Spark-shell

  ```
  bin/spark-shell --master spark://hadoop102:7077
  ```

  参数说明：**--master spark://hadoop102:7077**指定要连接的集群的**master**

- 执行wordcount程序

  ```scala
  sc.textFile("./input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).collect
  ```

### 1.3.3 HA配置

1）在spark-env.sh 添加如下配置：

```xml
# 注释掉如下内容：
#SPARK_MASTER_HOST=hadoop102
#SPARK_MASTER_PORT=7077
# 添加上如下内容：
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=hadoop102:2181,hadoop103:2181,hadoop104:2181 -Dspark.deploy.zookeeper.dir=/spark"
```

2）分发配置文件

3）启动Zookeeper

4）在hadoop102启动全部节点

```
sbin/start-all.sh
```

注：在102节点启动就会在102当前节点上起一个master，状态标记为avtive

5）在hadoop103启动一个master

```
sbin/start-master.sh
```

6）查看master状态

7）测试HA可以杀死102节点上的master，去103的8080端口看master是否会自动切换成Active

### 1.3.4 Standalone工作模式图解

![image-20200504212752312](D:\ProgramFiles\Typora\图片备份\image-20200504212752312.png)

### 1.3.5 注意事项

1） 如果启动的时候报：**JAVA_HOME is not set**, 则在**sbin/spark-config.sh**中添加入**JAVA_HOME**变量即可，不要忘记分发修改的文件

![image-20200504200020168](D:\ProgramFiles\Typora\图片备份\image-20200504200020168.png)

## 2.3 SparkYarn模式

### 2.3.1 什么是SparkYarn模式？

Spark客户端可以直接连接YARN，不需要额外构建Spark集群。

有 **client** 和 **cluster** 两种模式，主要区别在于：Driver 程序的运行节点不同。

- **client模式：**Driver程序运行在客户端，适用于交互、调试，希望立即看到app的输出
- **cluster模式：**Driver程序运行在由 RM（ResourceManager）启动的 AM（AplicationMaster）上, 适用于生产环境。

### 2.3.2 SparkYarn模式图解

![image-20200504213300679](D:\ProgramFiles\Typora\图片备份\image-20200504213300679.png)

### 2.3.3 模式搭建

1）由于测试环境的虚拟机内存太少，防止将来任务被以外杀死，所以需要在yarn下增加如下配置并分发：

```xml
<!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
</property>
<!--是否启动一个线程检查每个任务正使用的虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
</property>
```

2）复制spark文件，命名为spark-yarn

3）修改spark-env.sh文件，添加如下配置：

```xml
SPARK_MASTER_HOST=hadoop102
SPARK_MASTER_PORT=7077 # 默认端口就是7077, 可以省略不配
HADOOP_CONF_DIR=/opt/module/hadoop-2.7.2/etc/hadoop  # 配置spark运行在yarn上
```

### 2.3.4 SparkYarn基本使用

#### 方式一：使用spark-submit提交执行任务

```bash
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode client \
./examples/jars/spark-examples_2.11-2.1.1.jar 100
```

注：--deploy-mode参数说明

- client表示driver在客户端（提交应用的位置）。
- cluster表示driver在集群的某个NodeManager上。

#### 方式二：使用spark-shell提交执行任务

```scala
sc.textFile("/input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).collect
```

#### 注意事项

在使用spark-shell出现`Caused by: java.lang.ClassNotFoundException: Class com.hadoop.compression.lzo.LzoCodec not found`异常。

解决方案：

- 在hadoop的core.site.xml中去掉相关的配置

- 在spark-defaults.conf中指定一下lzo jar包的位置，添加如下配置：

  ```
  spark.jars=/opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar
  ```

  

### 2.3.5 日志服务

在yarn的页面中点击 history 无法直接连接到 spark 的日志.

可以在**spark-default.conf**中添加如下配置达到上述目的

```
spark.yarn.historyServer.address=hadoop102:18080
spark.history.ui.port=18080
```

##### 注意事项

1）如果在 yarn 日志端无法查看到具体的日志, 则在**yarn-site.xml**中添加如下配置

![image-20200504214858594](D:\ProgramFiles\Typora\图片备份\image-20200504214858594.png)

```xml
<property>
    <name>yarn.log.server.url</name>
    <value>http://hadoop102:19888/jobhistory/logs</value>
</property>
```

注意事项：

1） spark-shell运行在yarn的实时, `deploy-mode`只能是`client`

## 2.4 配置Spark任务历史服务器

### 2.4.1 为什么要配置历史服务器？

在Spark-shell没有退出之前，我们是可以从4040端口看到正在运行的任务的日志情况，但是退出Spark-shell之后，执行的所有任务记录全部都会丢失。所以需要配置任务的历史服务器，方便在任何时候都可以去查看日志。

### 2.4.2 配置历史服务器

1）配置spark-default.conf文件，开启Log

- 先将spark-defaults.conf.template文件修改为spark-defaults.conf
- 在spark-defaults.conf文件中添加如下内容：

```xml
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://hadoop102:9000/spark-log-dir
```

2）修改spark-env.sh文件，添加如下内容：

```xml
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=30 -Dspark.history.fs.logDirectory=hdfs://hadoop102:9000/spark-log-dir"
```

> 注意：目录要提前手动创建好

3）分发配置文件

### 2.4.3 启动历史服务器

因为我们配置的历史日志存储在HDFS上，所以需要先启动HDFS然后在启动历史服务器。

```bash
sbin/start-history-server.sh
```

### 2.4.4 注意事项

1）在配置历史日志目录时，目录必须提前存在，名字没有要求。

2）如果历史日志存储在HDFS上，启动历史服务器前需要先启动HDFS

## 2.5 在IDEA中运行Spark

### 2.5.1 为什么需要在IDEA中运行Spark？

Spark Shell 仅在测试和验证我们的程序时使用的较多，在生产环境中，通常会在 IDE 中编制程序，然后打成 jar 包，然后提交到集群，最常用的是创建一个 Maven 项目，利用 Maven 来管理 jar 包的依赖。

### 2.5.2 导入依赖

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
        <version>2.1.1</version>
    </dependency>
</dependencies>
<build>
    <plugins>
        <!-- 打包插件, 否则 scala 类不会编译并打包进去 -->
        <plugin>
            <groupId>net.alchim31.maven</groupId>
            <artifactId>scala-maven-plugin</artifactId>
            <version>3.4.6</version>
            <executions>
                <execution>
                    <goals>
                        <goal>compile</goal>
                        <goal>testCompile</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

### 2.5.3 基本使用

#### wordcount案例

1）编译WordCount.scala

```scala
object Hello {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个SparkContext(spark-shell中, 自动创建)  sc
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("Hello")
    val sc = new SparkContext(conf)
    // 2. 通过sc从数据源（比如文件）得到数据, 第一个RDD  (文件地址从main函数传递)
    val lineRDD: RDD[String] = sc.textFile(args(0))
    // 3. 对RDD做各种转换
    val wordCountRDD = lineRDD
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    // 4. 执行一个行动算子(collect: 把每个executor中执行的结果, 收集到driver端)
    val arr: Array[(String, Int)] = wordCountRDD.collect
    arr.foreach(println)
    // 5. 关闭SparkContext
    sc.stop()
  }

}
```

**注：**

- RDD做的一系列转换都是lazy
- 执行行动算子，RDD前面做的一系列转换才会开始执行转换。
- collect行动算子：把执行后的数据从executor收集到driver。

2）将源码打包在linux下运行

```
 bin/spark-submit --class com.atguigu.spark.core.Hello --master yarn SparkCore-1.0-SNAPSHOT.jar /input
```

**注：**

- --class后面要跟主程序的全类名

- jar包后面跟的参数就是main方法输入的参数args[0]

### 2.5.4 注意事项

1）如果在idea中直接运行, 则必须有`setMaster("local[*]")`，或者设置VM options项为-Dspark.master=local

2）如果要打包到`linux`执行, 则必须把``setMaster("local[*]")``去掉., 在使用`spark-submit`指定`master`

3）如果是yarn模式，指定的路径为HDFS上的路径。

4）如果是其他模式，指定的路径为本地路径。

## 2.6 端口总结

Master UI端口：8080

Master 通讯端口：7070

Worker UI端口：8081

历史服务器 UI端口：18080

查看正在运行的`app`的情况：4040（一旦这个应用结束, 则无法查看）

## 2.7 将代码打包在LInux上运行

```scala
bin/spark-submit --master local[*] --class com.atguigu.spark.core.day05.readwrite.TextWrite SparkCore-1.0-SNAPSHOT.jar hdfs://hadoop102:9000/text_input
```



# 三 Spark Core

SparkCore就是RDD编程。

## 3.1 什么是RDD？

1）RDD又叫弹性分布式数据集，是Spark中最基本的数据抽象。在代码中是一个抽象类。

```scala
abstract class RDD[T: ClassTag]
```

特点如下：

- **弹性**

  > 存储的弹性：内存与磁盘的自动切换。spark原则上来说所有的数据都是存储在内存，当内存存不下时会自动将数据溢写到磁盘，这样会影响执行效率但是可以保证代码正常运行。
  >
  > 
  >
  > 容错和计算的弹性：如果计算过程中分区数据丢失，会尝试去源文件中重新读取分区数据，重建分区进行计算。前提是数据源的数据还在。本质是血缘关系还在，可以追溯。
  >
  > 

  > 分区的弹性：在分区的计算过程中可以根据数据量适当的调整分区，一个分区由一个核心计算。

- **只读、不可变（如发生变化，将会得到一个新的RDD）**

  > RDD表示只读的分区的数据集，对RDD进行改动，只能通过RDD的转换操作，然后的到新的RDD，不会对运来的RDD有任何影响。
  >
  > RDD的转换操作算子包括两大类：
  >
  > **transformation**：转换算子，用来构建血缘关系。
  >
  > **action**：行动算子，触发RDD进行计算，得到相关结果或者保存RDD数据到文件系统中。

- **可分区**

  > RDD逻辑上是分区的，每个分区的数据是抽象存在的，计算的时候会用过一个compute函数得到每个分区的数据。
  >
  > 如果RDD是通过已有文件系统构建，则compute函数读取指定文件系统中的数据。
  >
  > 如果RDD是通过其他RDD转换而来，则compute函数执行转换逻辑将其他RDD的数据进行转换得到数据。

- **每个分区可并行计算**

  > 一个RDD可以简单的理解为一个**分布式**（一个分区由一个核心处理）的元素集合。

- **依赖**

  > RDDs通过操作算子进行转换，转换得到的新RDD包含了从其他RDDs衍生所必需的信息，RDDs之间维护着这种血缘关系，称为依赖。
  >
  > 依赖包括两种：
  >
  > ①窄依赖：RDDs之间分区是一一对应或多对一的。
  >
  > ②宽依赖：父RDD的分区与子RDD的分区形成了一对多的关系。
  >
  > 依赖：
  >
  > 窄依赖的好处，并行计算不需要等待别的分区的数据计算结果。
  >
  > 宽依赖，一个分区计算完了还需要等待剩下的分区计算结果完成才可以进行下一步操作。

- **缓存**

  > 如果一个应用程序中多次使用同一个RDD，可以将该RDD缓存起来，该RDD只有在第一次计算的时候会根据血缘关系得到分区的数据，后续在使用到该RDD的时候，会直接从缓存处取而不用再根据血缘关系重新计算，加速后期重用，提高计算效率。

- **检查点（checkpoint）**

  > 虽然RDD的血缘关系可以实现容错，当RDD的某个分区数据计算失败或丢失，可以通过血缘进行重建。但是当迭代层数较多时，血缘关系越来越长，一旦后续迭代过程中出错，则需要通过非常长的血缘关系去重建，势必影响性能。
  >
  > 为此，RDD支持**checkpoint** 将数据保存到持久化的存储中，这样就可以切断之前的血缘关系，因为checkpoint后的RDD不需要知道它的父RDDs了，可以直接从checkpoint拿取数据，减少了重建的次数。

2）在Spark中对RDD的操作分为两种：

- 创建RDD或转换已存在的RDD成为新的RDD
- 执行行动操作来得到计算结果

3）每个RDD被切分成多个分区，每个分区可能会在集群中不同的节点上进行计算，达到并行计算，提高执行效率。

## 3.2 RDD的5个主要属性及分区如何分配？

### **1）A list of partitions（分区列表）**

- 一个RDD有多个分区，分区可以看成是组成数据的基本单位。

- 对于RDD来说，每个分区都会被一个计算任务处理（Task）处理，分区数决定了并行计算的粒度。
- 用户可以在创建RDD时指定分区数，如果没有指定，默认使用程序所分配的COU Core的数量。
- 每个分区的存储是由BlockManager实现的，每个分区都会被逻辑映射成BlockManager的一个Block，而这个Block会被一个Task负责计算。

#### **分区数如何确定？**

- 默认分区数 = 总的核心数
- 指定分区数

### **2）A function for computing each split（计算切片函数）**

Spark中RDD的计算是以分片为单位的，每个RDD都会实现compute函数以达到这个目的。

#### **创建RDD时分区如何切片？**

```scala
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
```

注：这个分区切片和RDD的KV分区不是一个概念，这个是默认的没有使用分区器。如果使用分区器，默认的是hashpartitioner。

### 3）A list of dependencies on other RDDs（依赖列表）

RDD的每次转换都会生成一个新的RDD，所以RDD之间会形成类似于流水线一样的前后依赖关系，在部分分区数据丢失时，Spark可以通过这个依赖列表重新计算丢失的分区数据，而不是对RDD的所有分区进行计算。

### **4）Optionally, a Partitioner for key-value RDDs（可选的键值分区器）**

对存储键值对的RDD还有一个可选的分区器。

只有对于key-vue的RDD，才会有partitioner，非key-value的RDD的partitioner的值是None，partitioner不但决定了RDD的分区数量，也决定了parentRDD Shuffle输出时的分区数量。

### **5）Optionally, a list of preferred locations to compute each split on（RDDs的首选位置列表）**

存储每个切片优先位置的列表，比如对于一个HDFS文件来说，这个列表保存的就是每个partition所在文件块的位置，按照移动数据不如移动计算的理念，Spark在进行任务调度时，会尽可能的将计算任务分配到其所要处理数据块的存储位置，类比hadoop中的机架感知原理。

## 3.3 RDD编程基本思路？

1）在Spark中，RDD被表示为对象，通过对象中的方法调用来对RDD进行各种转换。经过一系列的**transformations**定义RDD之后，就可以调用actions触发RDD的计算，action可以是向程序返回结果（**count**, **collect**等），或者向存储系统保存数据（**saveAsTextFile**等）。

2）在Spark中，只有遇到action，才会执行RDD的计算（lazy），这样在运行时可以通过管道的方式传输多个转换。

3）要使用Spark，需要编写一个Driver程序，Driver程序被提交到集群运行在Worker上，Driver中定义了一个或多个RDD，并调用RDD上的action，Worker则执行RDD分区的计算任务。

## 3.4 创建RDD的三种方式？

### 3.4.1 通过Scala集合创建RDD

##### 基本使用

```scala
object CreateRDDdemo {
  def main(args: Array[String]): Unit = {
    //创建配置文件信息
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreateRDDdemo")
    //通过配置文件创建一个SparkContext对象
    val sc = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 5, 6)
    //从集合中创建RDD的第一种方式parallelize
    //    val RDD = sc.parallelize(list)
    //从集合中创建RDD的第二种方式makeRDD
    val RDD = sc.makeRDD(list)
    //两种方式其本质一样，makeRDD底层也是调用了parallelize来创建一个RDD抽象集合
    //当RDD创建成功，就可以通过并行的方式去操作这个分布式的数据集。
    //如何理解并行？
    //首先要知道RDD是由多个分区，组成的数据集。这里的并行指的就是在多个分区中每个核心各自处理每个分区的数据。同时进行，提高计算效率

    //通过行动算子collect得到一个数组
    val arr = RDD.collect()
    //使用scala的算子打印数组
    arr.foreach(println)
  }

}
```

##### 注意事项

1）在创建RDD时如果没有手动设置分区数量，则会默认设置分区数量=CPU Cores

2）使用makeRDD和parallelize两种方式创建RDD本质都是一样的。

3）在使用RDD的算子时，要清楚的知道当前算子是属于scala的还是spark的。

### 3.4.2 从外部数据源创建RDD

通过外部数据读取数据(`文件, hive, jdbc,...`), 然后得到`RDD`

### 3.4.3 从其他RDD转换得到新的RDD

#### 单Value

- map：传入RDD集合中的每个元素，调用匿名函数进行处理。原则，一进一出。
- mapPartitions：传入n个分区的数据返回n个集合`（it => it）。`
- mapPartitionsWithIndex：传入一个分区索引和分区数据返回一个集合 `( (index,it)=>it )`。
- glom：返回一个`RDD[Array[Int]]`, 存储的是数组, 每个数组的数据是每个分区的数据。
- filter：经常配合`coalesce`使用，在产生数据倾斜时，减少分区达到合并的效果。在合并时会自动去找数据少的分区进行合并。
- groupBy：会产生宽依赖（`shuffle`），遍历集合，按匿名函数的返回值作为key，传入的元素作为value，对遍历后的结果进行分组，key相同的value分到一组。(key,(v1,v2))
- coalesce和repartition：减少分区使用`coalesce`增加分区使用`repartition`
- sample：放回抽样和不放回抽样。
- distinct：去重，如果去重的是自定义类型，需要实现`hashCode`和`equals`，判断两个元素是否相等。
- SortBy：传入排序指标，如果有升有降需要使用到Ordring，传入classTag
- pipe：可以让linux或脚本去处理RDD中的数据。

#### 双Value

- 并集：union

- 交集：intersection

- 差集：subtract

- 笛卡尔积：cartesian

- 拉链：zip站在元素的角度、zipPartitions站在分区的角度、zipWithIndexs元素和索引拉

## 3.5 RDD单Value算子

### 3.5.1 mapPartitions算子

#### 1、基本介绍

```scala
  def mapPartitions[U: ClassTag](
                                      f: Iterator[T] => Iterator[U],
                                      preservesPartitioning: Boolean = false): RDD[U] 
```

mapPartitions算子是对每个分区执行一次map操作，满足一进一出原则。

要求返回一个Iterator集合！！！

#### 2、基本使用

```scala
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list = List(1, 4, 5, 6, 2, 8)
    val RDD = sc.parallelize(list)
    //RDD的算子mapPartitions的使用：和map不同的是，它是一个分区一个分区的处理。而map是一个元素一个元素的处理
    //返回的每一个分区，分区的数组使用Iterator存储，调用scala的map方法对分区里每一个元素进行处理
    val RDD1 = RDD.mapPartitions(_.map(_ * 10))

    val arr = RDD1.collect()
    arr.foreach(println)
    sc.stop()
  }

}
```

#### 3、注意事项

1）mapPartitions和map算子的异同

- 同：都是在做map操作
- 异：map会对每个元素执行一次map中的匿名函数，而mapPartitions是对每个分区执行一次。

2）相对于map算子来说，mapPartitions效率会高一些，因为分区计算是并行计算。

3）使用mapPartitions会有内存溢出的风险

> 如果想使用迭代器，前提是得有一个容器，那么再使用mapPartitions将分区转换成迭代器的过程中，势必需要先将分区的数据全部装到一个集合中，然后使用迭代器方法来进行操作集合。

> 把迭代器转成容器式集合(List, Array)的过程中, 如果这个分区的数据特别大, 会有内存溢出的风险。
>
> 如果没有内存溢出, 则效率要比map高。

### 3.5.2 mapPartitionsWithIndex算子

#### 1、基本介绍

该方法传入一个匿名函数，匿名函数的参数包括该分区的索引index和该分区的数据集合。

返回值为一个Iterator

```scala
def mapPartitionsWithIndex[U: ClassTag](
 f: (Int, Iterator[T]) => Iterator[U],
 preservesPartitioning: Boolean = false): RDD[U]
```

#### 2、基本使用

```scala
object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("MapPartitionsWithIndex")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(1, 4, 5, 6, 2, 8)
    val RDD: RDD[Int] = sc.parallelize(list, 3)
    
    //mapPartitionsWithIndex方法：传入每个分区index和每个分区的数据集合返回一个Iterator集合
    val RDD1: RDD[(Int, Int)] = RDD.mapPartitionsWithIndex((index, it) => it.map((_, index)))
    val arr = RDD1.collect()
    arr.foreach(println)
  }

}
```

#### 3、注意事项

1）该算子匿名函数传入的第一个参数为Int类型的值，表示分区的索引。第二个参数为一个Iterator表示分区的数据，返回值为一个Iterator。

### 3.5.2 glom算子

#### 1、基本介绍

```scala
def glom(): RDD[Array[T]]
```

glom算子是把每个分区的数据放入到一个数组中，如果有n个分区，得到的新的RDD就有n个数组。

#### 2、基本使用

```scala
object Glom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Glom")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(1, 4, 5, 6, 2, 8)
    val RDD: RDD[Int] = sc.parallelize(list,3)

    //使用glom算子，将RDD中三个分区的数据放入到三个数组中
    val RDD1: RDD[Array[Int]] = RDD.glom()
    val arr: Array[Array[Int]] = RDD1.collect()

    println(arr.length)
  }

}
```

### 3.5.3 groupBy算子

#### 1、基本介绍

```scala
def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] 
```

- groupBy先调用代码体中的逻辑对进来集合中的每一个元素进行处理。
- 对每个元素都会返回一个结果。
- **当集合中的所有元素都处理完毕以后，对代码体生成的结果进行聚合。**
- **返回值相同的为一组，返回值作为key，传进来的每一个元素作为value，聚合结果为List(value,...)。**

#### 2、基本使用

```scala
object GroupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("GroupBy")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(1, 4, 5, 6, 2, 8)
    val RDD: RDD[Int] = sc.parallelize(list, 3)
    //需求，计算list集合中偶数和奇数的个数
    
    //使用RDD的groupBy算子，对进来的每一个元素进行处理，结果相同的元素分到一组，结果为key，每个元素都存到一个Iterable中作为value。
    val RDD1: RDD[(String, Iterable[Int])] = RDD.groupBy(x => if (x % 2 == 0) "偶数" else "奇数")
    val RDD2: RDD[(String, Int)] = RDD1.map {
      case (k, v) => (k, v.sum)
    }
    val arr: Array[(String, Int)] = RDD2.collect()
    arr.foreach(println)
  }
}
```

#### 3、注意事项

1）因为这个算子会产生宽依赖，使用groupBy就一定有shuffle，shuffle需要借助磁盘，所以效率较低以后慎用。

2）可以使用groupBy去重，但是没有预聚合，效率较低。

3）在分完组以后，RDD是以KV形式存在的，所以需要重新分区会产生shuffle。

4）默认的分区器是哈希分区器。

5）由源码可知，groupBy返回的是一个Iterable集合，Iterable集合是不能进行排序的。

- 当有需要需要对分组后的集合进行在排序时，需要将Iterable集合转成List或Array才可以二次排序！！！

### 3.5.4 sample抽样算子

#### 1、基本介绍

数据的抽样

参数1: 表示是否放回. 如果是true表示放回, 元素可以被重复抽到. 所以, 后面的抽样比例 [0, ∞). 如果是否`false`, 表示不放回, 元素不会被重复抽到. 所以抽样比例: `[0, 1]`

参数2: 抽样比例

参数3: 随机种子. 一般使用系统的时间. 如果每次种子都一样, 则抽到的值也是一样的!!!

作用: 对比数据做评估.

#### 2、基本使用

```scala
object Sample {
  def main(args: Array[String]): Unit = {
    //创建配置文件信息
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreateRDDdemo")
    //通过配置文件创建一个SparkContext对象
    val sc = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 5, 6)
    val RDD = sc.parallelize(list, 3)
    //使用sample抽样算子
    //1 抽出不放回 抽样范围在[0,1]
    val RDD1 = RDD.sample(false, 0.1)
    //2 抽出放回 抽样范围在[0,正无穷)
    val RDD2 = RDD.sample(true,1.5)

    //3 随机种子设置为1
    val RDD3 = RDD.sample(false,0.5,1)
    val RDD4 = RDD.sample(false,0.5,1)
    val arr = RDD1.collect()
    val arr1 = RDD2.collect()
    val arr2 = RDD3.collect()
    val arr3 = RDD4.collect()
    arr.foreach(println)
    println("-----------------")
    arr1.foreach(println)

    arr2.foreach(print)
    println("------------测试随机种子")
    arr3.foreach(print)
  }

}
```



#### 3、注意事项

1）如果设置了抽取不放回，需要注意抽样范围。

2）随机种子的值一般使用系统时间。

### 3.5.5 distinct去重算子

#### 1、基本介绍

对 RDD 中元素执行去重操作. 参数表示任务的数量.默认值和分区数保持一致.

#### 2、基本使用

```scala
object Distinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Distinct")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(1, 4, 4, 6, 2, 8, 2, 1)
    val RDD: RDD[Int] = sc.parallelize(list, 3)

    //使用distinct去重
    val RDD1 = RDD.distinct()
    
    //使用groupBy去重
    val RDD2 = RDD.groupBy(x=>x).map(_._1)

    val arr = RDD1.collect()
    val arr1 = RDD2.collect()

    arr.foreach(println)
    arr1.foreach(println)
    
  }

}
```

#### 3、注意事项

1）如果去重的是自定义类型，需要实现`hashCode`和`equals`，判断两个元素是否相等。

2）`hashCode`和`equals`要兼容：

- 如果两个对象的`hashCode`返回值相等，则`equals`应该返回true。

  ```scala
  def hashCode = name.hashCode
  ```


3）distinct底层本质还是使用reduceByKey来去重的。

```scala
def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
}
```

### 3.5.6 改变分区算子

#### coalesce减少

##### 1、基本介绍

```scala
def coalesce(numPartitions: Int, shuffle: Boolean = false,
             partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
            (implicit ord: Ordering[T] = null)
: RDD[T]
```

coalesce方法有个默认参数，默认shuffle为false即默认情况下减少分区，没有shuffle。

##### 2、基本使用

```scala
object Coalesce {
  def main(args: Array[String]): Unit = {
    //创建配置文件信息
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreateRDDdemo")
    //通过配置文件创建一个SparkContext对象
    val sc = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 5, 6)
    val RDD = sc.parallelize(list, 3)
    println(RDD.getNumPartitions)
    //减少分区不需要开启shuffle
    val RDD1 = RDD.coalesce(2)
    //增加分区需要开启shuffle
    val RDD2 = RDD.coalesce(4,true)
    RDD1.collect()
    println(RDD1.getNumPartitions)
    println(RDD2.getNumPartitions)
    sc.stop()

  }

}

```

##### 3、注意事项

1）默认情况下coalesce只能减少分区，不能增加分区。因为coalesce默认是不shuffle的，如果要增加分区势必要经过shuffle。

2）如果想使用coalesce方法增加分区，需要启用shuffle，会产生宽依赖，启用shuffle会打乱数据，数据会落盘影响效率。

3）当启用shuffle时，可以减少分区也可以增加shuffle，但是减少分区时，不建议启用shuffle。

减少分区的可以解决部分数据倾斜问题。

4）在减少分区即合并分区解决数据倾斜问题时，我们没有办法控制如何去合并在spark内部已经实现了如何去合并，在合并时会自动去找数据少的分区进行合并。

#### repartition增加

##### 1、基本介绍

本质是coalesce的shuffle版本。

```scala
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
}
```

##### 2、基本使用

```scala
object Repartition {
  def main(args: Array[String]): Unit = {
    //创建配置文件信息
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreateRDDdemo")
    //通过配置文件创建一个SparkContext对象
    val sc = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 5, 6)
    val RDD = sc.parallelize(list)
    println(RDD.getNumPartitions)
    val RDD1 = RDD.repartition(4)
    println(RDD1.getNumPartitions)

  }

}
```

##### 3、注意事项

1）增加分区可以提高数据并行计算效率，一个分区由一个核心负责。

2）分区数量没有上限，比如核心有100个，但是分区可以有1000个。只是同时只能运行100个分区的数据。

3）分区时根据key来选择分区。

#### 两种算子总结

1）如果是减少分区，尽量不要使用shuffle，只有增加分区才shuffle。

2）实际应用中，如果减少分区就使用coalesce，增加分区就用repartition。其实本质没有区别

### 3.5.7 sortBy排序算子

#### 1、基本介绍

```scala
def sortBy[K](
                 f: (T) => K,
                 ascending: Boolean = true,
                 numPartitions: Int = this.partitions.length)
             (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
```

sortBy算子的排序原理和scala类似，传入一个排序指标，默认是按升序方式进行排序。

#### 2、基本使用

```scala
object SortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SortBy")
    val sc = new SparkContext(conf)
    val list = List("aaa", "a", "aa", "hello", "cc", "bb")
    val RDD = sc.parallelize(list)
    //sortBy算子，传入一个排序指标
    //需求一：按照list集合中的每个字符串的长度进行升序排序
    //    val RDD1 = RDD.sortBy(_.length)
    //需求二：按照list集合中的每个字符串的长度进行倒序排序
    //    val RDD1 = RDD.sortBy(_.length, false)
    //需求三：按照集合中的字符串进行字典排序，如果字母相同按照长度排序
    //    val RDD1 = RDD.sortBy(x => (x, x.length))
    //需求四：按照集合中的长度进行排序，如果长度相同按照字典进行倒序排序
    val RDD1 = RDD.sortBy(x => (x.length, x))(
      Ordering.Tuple2(Ordering.Int, Ordering.String.reverse), ClassTag(classOf[(Int, String)]))
    //def collect(): Array[T] collect算子，返回一个Array数组
    val arr = RDD1.collect()
    println(RDD1.getNumPartitions)
    arr.foreach(println)
    sc.stop()
  }

}
```



#### 3、注意事项

1）排序一定有shuffle会产生宽依赖，所以不管多大的数据量都不会出现内存溢出的情况。

2）classTag的作用：因为代码在编译后有泛型擦除，classTag的作用就是为了让代码在运行时记住它的泛型。

- 泛型擦除，Scala中的泛型基本都是在编译器这个层次上实现的，在生成的字节码中是不包含泛型中的类型信息的。所以在编译后，所有的泛型信息都会被擦掉

3）如果想倒序将第二个参数设置为false

4）**Iterable是没有sortBy排序的，需要转成List或array**

5）底层本质也是调用了sortByKey来实现

6）SortBy底层使用了rangpartition分区器，会使用抽样。多一个job

```scala
   def sortBy[K](
                     f: (T) => K,
                     ascending: Boolean = true,
                     numPartitions: Int = this.partitions.length)
                 (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
        this.keyBy[K](f)
            .sortByKey(ascending, numPartitions)
            .values
    }
```



### 3.5.8 pipe算子

#### 1、基本介绍

针对每一个分区，把RDD中的每个数据通过管道传递给shell命令或脚本，返回输出的RDD。每个分区都会执行一次这个命令。

脚本需要放置在Worker节点可以访问到的位置。

#### 2、基本使用

1）创建一个脚本文件pipe.sh

```bash
echo "hello"
while read line;do
    echo ">>>"$line
done
```

2）启动spark-shell 创建只有1个分区的RDD

```scala
scala> val rdd1 = sc.parallelize(Array(1,2,3,4), 1)
```

3）执行pipe

```scala
scala> rdd1.pipe("./pipe.sh").collect
```

结果：**res0: Array[String] = Array(hello, >>>1, >>>2, >>>3, >>>4)**

4）创建2个分区的RDD

```scala
val rdd2 = sc.parallelize(Array(1,2,3,4),2)

rdd2.pipe("./pipe.sh").collect
```

5）结果：**res1: Array[String] = Array(hello, >>>1, >>>2, hello, >>>3, >>>4)**

#### 3、注意事项

1）脚本中echo输出的数据都会进入到一个新的RDD中

2）脚本的执行过程：每一个分区都执行一遍脚本。

3）脚本路径需要注意当前Spark模式是集群模式还是本地模式。



## 3.6 RDD双Value交互算子

### 3.6.1 什么是双value交互？

交互指定的是两个RDD进行并集，交集，差集，笛卡尔积，拉链（zip）

### 3.6.2 并集

并集是窄依赖，分区数相加

```scala
    //并集(分区数相加)
    val RDD3 = RDD1 ++ RDD2
    val RDD3 = RDD1.union(RDD2)
```

**并集没有去重的效果**。

### 3.6.3 交集

```scala
    //交集(默认情况：分区数和两个RDD中最大的那个分区数相等)有去重效果
    val RDD3 = RDD1.intersection(RDD2)
    println(RDD3.getNumPartitions)
```

默认情况下分区数就等于RDD中最大的那个分区数。

**交集有去重的效果**。

### 3.6.4 差集

```scala
    //差集
     val RDD3 = RDD1.subtract(RDD2)
```



**差集没有去重的效果**。

### 3.6.5 笛卡尔积

```scala
    //笛卡尔积：生成x*y个元组 一般很少使用
    val RDD3 = RDD1.cartesian(RDD2)
```



### 3.6.6 拉链

#### 1、zip 站在元素的角度

分区数必须相等且对应分区中的元素也必须相同（总的元素数相同）。

```scala
    //zip
    val RDD3 = RDD1.zip(RDD2)
```



#### 2、zipPartitions站在分区的角度

分区数必须相同，分区数的元素可以调用scala的方法处理。

```scala
object zip {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("union").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    /**
     * zipPartitions方法分析：
     * ①柯里化函数第一个括号内传入RDD
     * ②第二个括号里传入一个匿名函数，该匿名函数表示传入两个RDD的分区迭代器返回一个迭代器
     * ③在匿名函数里我们可以调用scala的zipAll方法，解决spark中的zip方法中当两个分区中的元素总数不同时无法执行的问题。
     * def zipPartitions(rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V]
     */
    val list1 = List(30, 50, 70, 70, 60, 99, 22)
    val list2 = List(3, 50, 70, 6, 2)
    val RDD1 = sc.makeRDD(list1, 2)
    val RDD2 = sc.makeRDD(list2, 2)
    
    val RDD3 = RDD1.zipPartitions(RDD2)(
      (it1, it2) => it1.zipAll(it2, 1, -1)
      //(it1, it2) => it1.zip(it2)
    )
    
    val arr = RDD3.collect()
    arr.foreach(println)
    sc.stop()
  }
```

#### 3、zipWithIndex 自己和索引拉

```scala
val RDD3 = RDD1.zipWithIndex()
```

#### 4、拉链案例

需求：

val list1 = List(30, 50, 70, 60, 10, 20)转换成rdd:  "30->50" , "50->70",  "70->60", "60->10", "10->20"

```scala
object ZipDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ZipDemo1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    // rdd:  "30->50" , "50->70",  "70->60", "60->10", "10->20"

    //使用scala的衍生集合方法，将list改造成我们需要的结构
    //    val list2 = list1.init
    val list2 = list1.take(list1.length - 1)
    //    val list3 = list1.tail
    val list3 = list1.takeRight(list1.length - 1)
    val RDD1 = sc.makeRDD(list2)
    val RDD2 = sc.makeRDD(list3)
    //拉完以后的int元组使用偏函数改将每一个元组都变成字符串形式，
    val arr = RDD1.zip(RDD2).map {
      case (k, v) => s"$k -> $v"
    }.collect()
    arr.foreach(println)
  }

}
```



#### 5、注意事项

1）不管是zip还是zipPartitions，想使用拉链都必须满足一个条件，那就是分区数必须相同。

2）相对于zip而言，zipPartitions可以在柯里化函数中调用Scala的zipAll方法对缺少的元素使用默认值补位，在数据量不一致时也可以完成拉链。

## 3.7 RDD键值对算子

### 3.7.1 分区算子

#### 1、partitionBy

##### 基本介绍

KV键值对的分区算子，使用传入的分区器在分区的时候按照key来对RDD重新分区，和value没有任何关系。

##### 基本使用

```scala
object Partitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Partitions$")
    val sc = new SparkContext(conf)
    val list = List(("a", 1), ("ad", 5), ("bb", 6), ("cc", 8), ("c", 10))
    val RDD = sc.makeRDD(list, 2)
    //partitionBy方法，可以对kv形式的RDD进行再分区，需要传一个分区比较器。
    //创建一个hash分区比较器，分区为3
    RDD
      .partitionBy(new HashPartitioner(3))
      .collect()
      .foreach(println)

    //需求：目前partitionBy默认只能按照Key进行分区，如果要求按照V进行分区呢？
    //思路：将(k,v)进行倒转变成(v,k)分完区以后再转换即可
    RDD
      .map {
        case (k,v) => (v,k)
      }
      .partitionBy(new HashPartitioner(3))
      .map{
        case (k,v) =>(v,k)
      }
      .collect()
      .foreach(println)

  }

}
```



##### 注意事项

1）只有kv形式的RDD才可以使用分区器进行分区。

2）partitionBy算子默认只对key进行分区。

3）使用分区器进行分区时，一般会产生宽依赖。

4）如果`RDD1[(k, v)] ` 分区器是 `P1`, 对`RDD1`进行重新分区, 使用的分区器对象和`p1`相等, 那么这个时候, 不会真正的分区 。

5）案例中使用的分区器hash分区器，是取key的hash值取模分区数进行分区。

**即分区时，如果分区数和上次分区数一致，且分区器对象也是一样的，不会产生宽依赖，注这句话成立的前提是分区器对象必须是同一个对象！！！。**

#### 2、repartitionAndSortWithinPartitions

##### 基本介绍

```scala
def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
  new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
}
```

由源码可知，该算子需要传入一个分区比较器。

- 根据比较器进行分区，调用Ordering。
- 按照Key对区内的元素进行排序。

##### 基本使用

```scala
object repartitionAndSortWithinPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("repartitionAndSortWithinPartitions$")
    val sc = new SparkContext(conf)
    val list = List(("a", 1), ("ad", 5), ("bb", 6), ("cc", 8), ("c", 10))
    val RDD = sc.makeRDD(list, 2)

    // 使用repartitionAndSortWithinPartitions按照分区器分区, 每个分区内再按照key升序排列
    RDD
      .repartitionAndSortWithinPartitions(new HashPartitioner(2))
      //将每个分区内的数据转换成数组
      .glom()
      //为了方便打印，将数组转换成List
      .map {
        arr => arr.toList
      }
      .collect()
      .foreach(println)

    sc.stop()

  }

}
```

##### 注意事项

1）在有分区，且区内的元素有序的需求时可以使用该算子，效率较高。

### 3.7.2 聚合算子

#### 聚合算子基本介绍

1）什么是聚合算子？

- 聚合算子，只能用来kv形式的聚合。

- 按照key进行聚合，对相同的key的value进行聚合。

- 所有的聚合类算子都有预聚合。

2）什么是预聚合？

预聚合也叫分区内聚合，因为指定了是reduce，所以在map端就提前进行了聚合，减少了网络IO，类似于hadoop中combine的效果。

#### 1、reduceByKey

##### 基本介绍

按照key进行分组，对key相同的value进行聚合操作。

##### 基本使用

```scala
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKey$")
    val sc = new SparkContext(conf)
    val list = List(("a", 1), ("a", 5), ("bb", 6), ("bb", 8), ("c", 10))
    val RDD = sc.makeRDD(list, 2)
    
    //需求：将key相同的分到一组，计算value的和
    //使用reducebykey，由def reduceByKey(func: (V, V) => V): RDD[(K, V)]方法可知
    //该算子返回一个RDD集合，里面存放一个元组(k,v)，表示由key相同的value进行聚合得到的结果
    RDD
      .reduceByKey((x, y) => x + y)
      .collect()
      .foreach(println)
  }

}
```



##### 注意事项

1）该算子大部分情况下为多进一出，使用场景为key相同时，需要对value进行聚合操作时使用。

#### 2、foldByKey

##### 基本介绍

```
def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
```

1）相比于reduceByKey多了一个zero，由源码可知，zero的类型必须和value的类型保持一致。

2）和reduceByKey一样也是属于区内聚合。

##### 基本使用

```scala
object FlodByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("FlodByKey$")
    val sc = new SparkContext(conf)
    val list = List(("a", 1), ("b", 5), ("bb", 1), ("a", 6), ("b", 8), ("bb", 10))
    val RDD = sc.makeRDD(list, 2)

    //需求：对key相同的value进行聚合
    RDD
      .foldByKey(0)(_ + _)
      .collect()
      .foreach(println)

    //对zero的参与聚合次数进行测试
    //当zero为1时，测试zero参与了几次运算  结果显示：只在分区内计算了一次。分区后在聚合时，不会参与运算
    RDD
      .foldByKey(1)(_ + _)
      .collect()
      .foreach(println)
    sc.stop()
  }

}
```



##### 注意事项

1）zero的类型必须和v的类型一致。

2）zero只在分内聚合（预聚合 map端）的时候参与运算，分区间聚合（最终聚合 reduce端）不参与运算。

3）对一个key的聚合，zero最多参与n次聚合，（n=分区数）。



#### 3、aggregateByKey（区内区间聚合）

##### 基本介绍

```scala
def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
```

分区内+分区间的聚合算子，由源码可知，该算子有两个参数列表是一个柯里化函数。

- 第一个参数列表传zero
- 第二个参数里面里，需要传两个匿名函数，第一个匿名函数表示区内聚合，第二个匿名函数表示区间聚合。
- 返回值为key和聚合结果。

##### 基本使用

案例1：使用aggregateByKey对分区内求最大值，分区间最大值进行相加

```scala
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AggregateByKey$")
    val sc = new SparkContext(conf)
    val list = List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8))
    val RDD = sc.makeRDD(list, 2)

    //需求：取出每个分区相同key对应的最大值，然后相加
    RDD
      .aggregateByKey(Int.MinValue)(
        //分区内两两比较，每次都聚合出一个最大值
        (u, v) => u.max(v),
        //分区间两两相加，每次聚合出一个sum
        (max1, max2) => max1 + max2
      )
    .collect()
    .foreach(println)

    sc.stop()

  }

}
```

案例2：使用aggregateByKey对分区内同时求最大值和最小值，分区间最大值和最小值分别进行相加

```scala
object AggregateByKey2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AggregateByKey2$")
    val sc = new SparkContext(conf)
    val list = List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8))
    val RDD = sc.makeRDD(list, 2)

    //需求：取出每个分区相同key对应的最大值，然后相加
    RDD
      .aggregateByKey((Int.MinValue, Int.MaxValue))(
        //分区内的key相同的每一个value都和聚合后的元组中的(max,min)进行一次聚合比较，返回一个新的(max,min)元组
        (u, v) => (u._1.max(v), u._2.min(v)),
        //分区间的对每个分区内聚合的最终元组进行相加
        (maxMin1, maxMin2) => (maxMin1._1 + maxMin2._1, maxMin1._2 + maxMin2._2)
      )
    .collect()
    .foreach(println)

    sc.stop()

  }
```

案例3：每个key对应的value相加，求每个key对应的value的平均值

```scala
object AggregateByKey3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AggregateByKey2$")
    val sc = new SparkContext(conf)
    val list = List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8))
    val RDD = sc.makeRDD(list, 2)

    //需求：每个key对应的value相加，求每个key对应的value的平均值
    //(zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
    RDD
      //zero含义，代表value的和key出现的次数
      .aggregateByKey((0, 0))(
        //区内匹配到相同的key时，value和sum累加聚合，count +1
        {
          case ((sum, count), value) => (sum + value, count + 1)
        },
        //区间，相同key的区内的聚合结果sum进行聚合，次数进行聚合
        {
          case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
        }
      )
      //使用mapvalues对value结果二次处理得到平均数
      .mapValues(sumcount => sumcount._1.toDouble / sumcount._2)
      .collect()
      .foreach(println)

    sc.stop()
  }

}

```





##### 注意事项

1）aggregateByKey和flodByKey的区别在于，flodByKey在区内进行聚合，而aggregateByKey会在区内和分区间都进行聚合，适用于区内逻辑和区间逻辑不同的聚合场景。

2）在对aggregateByKey进行传参时，第一个参数列表中如果有多个参数一定要使用圆括号括起来，否则会被认为传的是另外一个隐式参数partitioner。

![image-20200506223848389](D:\ProgramFiles\Typora\图片备份\image-20200506223848389.png)

3）**aggregateByKey的zero值在分区间聚合时不参与运算。**

#### 4、combineByKey（区内区间聚合）

##### 基本介绍

```
def combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): RDD[(K, C)]
```

combineByKey也可以实现区内聚合和区间聚合。

参数说明：

- createCombiner：在每个分区内，对于不同的key来说，都会执行一次这个方法，返回一个值，作用等同于之前的zero。
- mergeValue：区内聚合。
- mergeCombiners：区间聚合。

##### 基本使用

```scala
object CombinerByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val RDD = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    // 计算每个key对应value的和 和他们的个数
    /**
     * def combineByKey[C](
     * createCombiner: V => C,
     * mergeValue: (C, V) => C,
     * mergeCombiners: (C, C) => C)
     */
    RDD.combineByKey(
      //遇到第一个分区内第一个key的zero值
      v => (v, 1),
      //区内聚合从zero开始，轮询聚合
      (sumCount: (Int, Int), v: Int) => (sumCount._1 + v, sumCount._2 + 1),
      //区间聚合，区内聚合结果在区和区之间再次聚合
      (sumCount1: (Int, Int), sumCount2: (Int, Int)) => (sumCount1._1 + sumCount2._1, sumCount1._2 + sumCount2._2)
    )
    .collect()
    .foreach(println)

    sc.stop()
  }

}
```



##### 注意事项

1）使用combineByKey必须补充匿名函数的参数类型。

#### 聚合算子之间的联系

```scala
combineByKey
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)

aggregateByKey
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
          cleanedSeqOp, combOp, partitioner)

foldByKey
    combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
          cleanedFunc, cleanedFunc, partitioner)

reduceByKey
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)

```

由源码可知，四种聚合算子底层本质都会去调用combineByKeyWithClassTag这个方法。

**使用指导:**

- 分区内聚合和分区间聚合的逻辑一样使用： `reduceByKey`、`foldByKey`

- 如果分区内聚合和分区间逻辑不一样使用：` aggregateByKey `、`combineByKey`

### 3.7.3 分组算子

#### 1、groupByKey

##### 基本介绍

```
def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] 
```

按key进行分组，key相同的value进行聚合，其本质就是groupBy的定制版，groupBy则相对更灵活一些。

##### 基本使用

```scala
object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("GroupByKey$")
    val sc = new SparkContext(conf)
    val list = List("hello", "world", "atguigu", "hello", "are", "go")
    val RDD = sc.makeRDD(list, 2)
    //需求：对list做wordcount
    RDD
      .map((_, 1))
      .groupByKey()
      .mapValues(_.sum)
      .collect()
      .foreach(println)
    
    sc.stop()
  }
}
```

##### 注意事项

1）groupByKey分组没有预聚合，在有聚合需求时能不使用就不要使用。

2）groupBy底层其实是先将元素变成k，v形式，然后调用groupByKey进行分组。

3）groupByKey本质底层还是属于聚合算子，只是关闭了预聚合。

4）和reduceByKey相比：

- 如果分组的目的是为了聚合使用`reduceByKey`因为它有预聚合，可以提高性能。
- 如果分组的目的不是为了聚合，可以使用`groupByKey`

5）groupByKey返回的是一个Iterable集合，注意二次排序问题。

### 3.7.4 排序算子

#### 1、sortByKey

##### 基本介绍

```
def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length): RDD[(K, V)]
```

1）由源码可知，sortByKey的参数列表由两个默认参数组成，第一个参数代表升序排序默认为true。

2）其实sortBy的底层本质就是使用sortByKey来实现的。

##### 基本使用

```scala
object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SortByKey$")
    val sc = new SparkContext(conf)
    val RDD = sc.parallelize(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))

    //需求：按key进行倒序排序
    RDD
      .sortByKey(false)
      .collect()
      .foreach(println)

    sc.stop()
  }

}

```



##### 注意事项

1）相较于sortByKey，sortBy更加灵活，只需要传入排序指标即可排序。而sortByKey只能按照Key进行排序，不过sortBy的底层实现还是SortByKey。

### 3.7.5 连接算子

#### 1、Join

##### 基本介绍

本质就是SQL中的内连接 = 共有

##### 基本使用

```scala
object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Join$")
    val sc = new SparkContext(conf)
    val list1 = List(("a", 1), ("ad", 5), ("bb", 6), ("cc", 8), ("c", 10))
    val RDD1 = sc.makeRDD(list1, 2)
    val list2 = List(("a", 10), ("d", 5), ("bb", 60), ("cc", 80), ("cc", 10))
    val RDD2 = sc.makeRDD(list2, 2)

    //轮询匹配，从RDD2中取出一个元素，依次和RDD1中的每一个元素进行匹配
    // 如果key匹配成功，则value组成一个嵌套元组(key,(v1,v2))
    RDD1
      .join(RDD2)
      .collect()
      .foreach(println)

    sc.stop()
  }

}
```

##### 注意事项

1）join只取两个集合共有部分。

#### 2、leftOuterJoin

##### 基本介绍

本质就是SQL中的左外连接 = 左边独有 + 共有

##### 基本使用

```scala
object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Join$")
    val sc = new SparkContext(conf)
    val list1 = List(("a", 1), ("ad", 5), ("bb", 6), ("cc", 8), ("c", 10))
    val RDD1 = sc.makeRDD(list1, 2)
    val list2 = List(("a", 10), ("d", 5), ("bb", 60), ("cc", 80), ("cc", 10))
    val RDD2 = sc.makeRDD(list2, 2)

    RDD1
      .leftOuterJoin(RDD2)
      .collect()
      .foreach(println)

    sc.stop()
  }

}
```

##### 注意事项

1）leftOuterJoin返回的是一个`RDD[(K, (V, Option[W]))]`形式的RDD集合。

- 当有共有的值时会使用(v1,some(v2))作为value。
- 当没有值时会使用(v1,none)作为value。

#### 3、fullOuterJoin

##### 基本介绍

全连接 = 左独有 + 共有 + 右独有

##### 基本使用

```scala
package com.atguigu.spark.core.day03.selfstudy

import org.apache.spark._

/**
 * @Classname Join
 * @Description TODO
 *              Date ${Date} 23:04
 * @Create by childwen
 */
object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Join$")
    val sc = new SparkContext(conf)
    val list1 = List(("a", 1), ("ad", 5), ("bb", 6), ("cc", 8), ("c", 10))
    val RDD1 = sc.makeRDD(list1, 2)
    val list2 = List(("a", 10), ("d", 5), ("bb", 60), ("cc", 80), ("cc", 10))
    val RDD2 = sc.makeRDD(list2, 2)

    RDD1
      .fullOuterJoin(RDD2)
      .collect()
      .foreach(println)

    sc.stop()
  }

}

```

##### 注意事项

1）和leftOuterJoin返回的是一个`RDD[(K, (V, Option[W]))]`形式的RDD集合。

#### 4、cogroup

##### 基本介绍

```scala
def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] 
```

该算子返回一个key，和一个元组，元组里存了两个it，分别放了第一个RDD1中对应key的value组成的集合和第二个RDD2中对应key的value组成的集合，如果当前RDD中没有该key对应的value没有则返回的是一个空集合。

##### 基本使用

```scala
object Join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Join$")
    val sc = new SparkContext(conf)
    val list1 = List(("a", 1), ("ad", 5), ("bb", 6), ("cc", 8), ("c", 10))
    val RDD1 = sc.makeRDD(list1, 2)
    val list2 = List(("a", 10), ("d", 5), ("bb", 60), ("cc", 80), ("cc", 10))
    val RDD2 = sc.makeRDD(list2, 2)
    
    RDD1
      .cogroup(RDD2)
      .collect()
      .foreach(println)

    sc.stop()
  }

}
```



##### 注意事项

1）cogroup的使用效果是在key相同时，将两个RDD中的value聚合到了一个元组里分别**Iterable**使用存储起来。



### 3.7.6 RDD实战案例

```scala
/**
 * 数据结构：时间戳，省份，城市，用户，广告 字段使用空格分割。
 * 需求：
 * 1516609143867 6 7 64 16
 * 1516609143869 9 4 75 18
 * 1516609143869 1 7 87 12
 * 统计出每一个省份广告被点击次数的 TOP3
 * 省份->((广告1,次数),(广告2,次数)，(广告3,次数))
 */
```
```scala
object RDDPractice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDPractice").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val RDD = sc.textFile("D:\\源笔记资料\\hadoop生态圈\\502_大数据之 spark\\02_资料\\agent")

    RDD
      .map(
        line => {
          val words = line.split(" ")
          ((words(1), words(4)), 1)
        }
      )
      //((城市,广告),点击总次数)
      .reduceByKey(_ + _)
      .map {
        case ((city, adv), count) => (city, (adv, count))
      }
      .groupByKey()
      .mapValues(
        _
          .toList
          .sortBy(-_._2)
          .take(3)
      )
      .sortByKey()
      .collect()
      .foreach(println)

  }
}
```



## 3.8 RDD行动算子

### 3.8.1 什么是行动算子？

1）在RDD中所有的转换算子，都是lazy的，只有碰到了行动算子才会执行转换。

2）在执行行动算子以后，同一个`stage`（阶段）的算子并行运行，不同`stage`之间串行运行。

3）即在运行shuffle阶段时，如果某一task提前计算完了需要等待其他task全部计算完成，才可以进入到下一阶段。

- 什么是阶段？

​       判断是否当前是否是一个阶段可以使用shuffle来划分。

4）当执行行动算子时，每个匿名函数的内部逻辑可能被分配到不同的executor上执行。

### 3.8.2 收集行动算子

#### 1、collect

##### 基本介绍

以数组的形式拉取返回RDD中的所有元素，返回到Driver端。

##### 注意事项

1）在生产环境中慎用此算子，数据规模很大时如果将数据一次性全部拉取到驱动端有可能发生OOM内存溢出。

2）collect的收集顺序是按照分区的索引顺序来的，所以拉取到驱动端的分区是有序的。

#### 2、take(n)

##### 基本介绍

返回一个数组到驱动端，数组中存放RDD前n个元素。

##### 注意事项

n的值不建议太大，有可能造成OOM。

#### 3、takeOrdered(n)

##### 基本介绍

返回一个数组到驱动端，数组中存放RDD前n个元素，默认是升序排序。

##### 注意事项

1）先按升序排，排完以后再取前n个元素。

2）如果需要降序，传入一个Ordering比较器即可，RDD.takeOrdered(n)(Ordering.Int.reverse)



#### 4、first

##### 基本介绍

类似于take(n)，返回RDD中的第一个元素。

#### 5、takeSample

##### 基本介绍

返回一个数组到驱动端，数组中存放从当前RDD中进行随机抽样的数据。

参数列表解读：

- 是否放回样本
- 抽样个数
- 随机种子

##### 基本使用

```scala
object ActiveDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ActiveDemo$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)

    //   def takeSample(
    //                      withReplacement: Boolean, 是否放回
    //                      num: Int, 取多少个
    //                      seed: Long  随机种子= Utils.random.nextLong): Array[T]
    val RDD1 = RDD.takeSample(false, 3)

    RDD1.foreach(println)
    
    sc.stop()
  }

}
```



##### 注意事项

1）只有结果RDD数据规模不大时，才使用此算子。因为在拉取时，需要提前将所有的数据存入到驱动端的内存中。

### 3.8.3 计数行动算子

#### 1、count

##### 基本介绍

返回RDD中元素的个数，生产中不建议使用本质还是需起一个job才可以计算个数，很消耗资源。

#### 2、countByKey

##### 基本介绍

```scala
def countByKey(): Map[K, Long] = self.withScope {
  self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
}
```

计算KV形式的RDD中每个Key的个数，返回一个Map。适用于KV形式的RDD。

##### 注意事项

1）由源码可知，本质是先map转换value为1，在reducebyKey统计每个key的数量，然后拉取到驱动端使用toMap将数组转换为map。

2）因为在使用时也会把所有的数据都拉取到驱动端，所以当数据量很大时，不建议使用。



### 3.8.4 遍历行动算子

#### 1、foreach

##### 基本介绍

运行一个函数，对分区内数据集的每一个元素上都执行一次这个函数，可以用来与外部的存储系统进行交互。

按分区**并行遍历**RDD中的每个元素。

##### 基本使用

```scala
object ActiveDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ActiveDemo$")
    val sc = new SparkContext(conf)
    val list = List(1,3,5,2,4,6)
    val RDD = sc.makeRDD(list, 2)

    val RDD1 = RDD.foreach(println)
      //运行三次数据：
    // 214365
    // 123456
    // 124635
    sc.stop()
  }

}
```



##### 注意事项

1）和scala中foreach的异同

- **异**：在Spark中有分区的概念，在遍历时多个分区并行运行，每个分区都有可能被优先遍历，这就导致了遍历的顺序和数据集的插入顺序不一致。但是scala没有分区的概念，遍历出来是有序的。

  >  Spark的foreach在遍历时分区的先后顺序不一定，且是并行遍历，所以就会导致遍历出来的数据是无序的，但是区内的数据是有序的。

- **同**：两个算子，本质都是遍历。

2）通过上述案例可以看出，三次的数据都不相同。但是可以发现，当遍历2号分区时，2总是在最前面，遍历1号分区时，1总是在最前面。这就说明，在遍历分区内部时，是有序的。但是分区是在并行运行，有可能上一个结果显示的是1分区的数据，下一个结果是2分区的数据。即表现出来无序

#### 2、foreachPartition

##### 基本介绍

作用和foreach类似，只不过foreachPartition是针对整个分区来运行一次函数。

##### 基本使用

```scala
object ActiveDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ActiveDemo$")
    val sc = new SparkContext(conf)
    val list = List(1,3,5,2,4,6)
    val RDD = sc.makeRDD(list, 2)

    val RDD1 = RDD.foreachPartition(
      res => println(res.toList)
    )
//打印结果		                                                                     //List(1, 3, 5)
		//List(2, 4, 6)
      
    sc.stop()
  }

}
```

##### 注意事项

1）foreachPartition和foreach的区别在于，一个是站在分区的角度一个是站在元素的角度。

### 3.8.5 聚合行动算子

#### 1、reduce

##### 基本介绍

```scala
def reduce(f: (T, T) => T): T 
```

有分区内的聚合也有分区间的聚合，区内和区间的聚合逻辑相同。

##### 基本使用

```scala
object reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("reduce$")
    val sc = new SparkContext(conf)
    val list = List("a", "b", "c", "v", "x", "f")
    val RDD = sc.makeRDD(list, 2)
    //使用reduce行动算子对RDD进行聚合。
    //无zero值。
    //区内区间聚合逻辑一致。
    //返回值类型和RDD中元素类型一致。
    println(RDD
      .reduce(_ + _))
    
    sc.stop()
  }

}
```

##### 注意事项

1）reduce没有零值，区内区间的聚合逻辑一致且返回值类型必须和RDD中元素类型一致。

#### 2、fold

##### 基本介绍

使用方式和reduce类似，但是多了零值，在区间聚合时零值也会参与计算。

```scala
def fold(zeroValue: T)(op: (T, T) => T): T
```

##### 基本使用

```scala
object fold {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("fold")
    val sc = new SparkContext(conf)
    val list = List("a", "b", "c", "v", "x", "f")
    val RDD = sc.makeRDD(list, 2)
    //使用fold行动算子对RDD进行聚合。
    //有zero值，每个分区内聚合时参与聚合，分区间聚合参与一次。
    //区内区间聚合逻辑一致。
    //返回值类型和RDD中元素类型一致。

    println(RDD
      .fold("@")(_ + _))

    sc.stop()

  }

}
```



##### 注意事项

1）zero的值类型：和RDD中元素类型保持一致。

2）zero的使用次数：在**每个**分区内聚合时使用一次，**分区间聚合时使用一次**。

> 综上：zero参与的总次数 = 分区数 + 1

3）返回值类型：和RDD中元素类型保持一致。

4）和reduce的异同

**同**：区内和区间的聚合逻辑相同。

**异**：聚合次数不同，zero会参与区间聚合。

#### 3、aggregate

##### 基本介绍

```scala
def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
```

由源码可知：aggregate算子是一个柯里化函数。

需要传两个参数，第一个参数为零值，第二个参数中包含了2个函数，一个区内聚合函数和一个区间聚合函数。

##### 基本使用

```scala
object aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("aggregate")
    val sc = new SparkContext(conf)
    val list = List(11, 2, 3, 5, 9, 20)
    val RDD = sc.makeRDD(list, 2)
    //使用aggregate动算子对RDD进行聚合。
    //有zero值，每个分区内聚合时参与聚合，分区间聚合参与一次。
    //区内区间聚合逻辑可以不一致。
    //返回值类型和zero中元素类型一致。

    //需求，使用aggregate同时求最大值和最小值的和
    //def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U

    RDD
      .aggregate((Int.MinValue, Int.MaxValue))(
        //分区内聚合逻辑
        (maxMin, value) => (maxMin._1.max(value), maxMin._2.min(value)),
        //分区间聚合逻辑
        {
          case (maxMin1, maxMin2) => (maxMin1._1 + maxMin2._1 - Int.MinValue, maxMin1._2 + maxMin2._2 - Int.MaxValue)
        }
      )
        //注意：最后的结果需要减掉零值，因为在区间聚合时，零值也会参与运算！！！
    match {
      case (max, min) => println((max - Int.MinValue, min - Int.MaxValue))
    }
    
    sc.stop()
    
  }

}

```



##### 注意事项

1）zero的值类型：不需要和RDD的元素类型保持一致。

2）zero的使用次数：在**每个**分区内聚合时使用一次，**分区间聚合时使用一次**。

> 和fold的zero运算一致。

3）返回值类型和zero的值类型一致。

4）和reduce的区别

- 区内和区间的聚合逻辑可以不同。

- 聚合次数不同，zero会参与区间聚合！！！

## 3.9 RDD中函数传递

### 3.9.1 序列化问题

当我们给RDD传递函数时，需要关注在Driver中传递的函数和参数在executor上有没有实现序列化。

- 函数
- 属性

解决方案：

1）传递类的方法时让类实现序列化

2）使用匿名函数传递属性时，使用局部变量（局部变量类型也需要序列化）

### 3.9.2 案例演示

```scala
package com.atguigu.spark.core.day04.selfstudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Classname SerDemo
 * @Description TODO
 *              Date ${Date} 19:37
 * @Create by childwen
 */
object SerDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val RDD: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
    //查找包含hello子字符串的元素
    val searcher = new Searcher("hello")
    val RDD1 = searcher.getMatch1(RDD)
    RDD1
      .collect()
      .foreach(println)
  }

}

class Searcher(val query: String) extends Serializable {

  //判断字符串是否包含子字符串
  def isContains(str: String) = {
    str.contains(query)
  }

  //1 传入一个RDD集合，返回一个RDD，RDD调用给类的方法，所需类必须要实现序列化
  def getMatch(RDD: RDD[String]) = {
    RDD.filter(isContains)
  }

  //2 使用变量将string封装起来，在方法里调用的s是string类型，string类型已经实现了序列化所以这里编译可以通过
  def getMatch1(RDD: RDD[String]) = {
    val s = query
    RDD.filter(_.contains(s))
  }

  //3 这种方式在调用方法运行到contains时，会去找query，发现query是自定义类的属性。而自定义类如果没有实现序列化方法，就会报错
  def getMatch2(RDD: RDD[String]) = {
    RDD.filter(_.contains(query))
  }

}

```



### 3.9.3 使用kryo作为序列化器

#### 1、为什么要使用kryo作为序列化器？

因为Spark默认使用的是Java的Serializable序列化器比较重量级，会影响计算的效率。在Spark2.0以后支持使用kryo作为序列化器。

#### 2、kryo序列化器的好处有哪些？

`kryo`, 是一个第三方的序列化机制，比`java`的序列化机制要快，和轻量级。

`spark2.0`才开始支持，在内部值类型和值类型的数组已经默认采用这种机制。

#### 3、如何更换kryo序列化器？

```scala
// 更换序列器
.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
// 注册需要序列化的类
.registerKryoClasses(Array(classOf[Searcher2]))
```

#### 4、基本使用

```scala
package com.atguigu.spark.core.day04.selfstudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Classname SerDemo
 * @Description TODO
 *              Date ${Date} 19:37
 * @Create by childwen
 */
object SerDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SerDemo")
      .setMaster("local[*]")
      //更换序列化器
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Searcher]))

    val sc = new SparkContext(conf)
    val RDD: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
    //查找包含hello子字符串的元素
    val searcher = new Searcher("hello")
    val RDD1 = searcher.getMatch(RDD)
    RDD1
      .collect()
      .foreach(println)
  }

}

case class Searcher(query: String) {

  //判断字符串是否包含子字符串
  def isContains(str: String) = {
    str.contains(query)
  }

  //1 传入一个RDD集合，返回一个RDD，RDD调用给类的方法，所需类必须要实现序列化
  def getMatch(RDD: RDD[String]) = {
    RDD.filter(isContains)
  }

  //2 使用变量将string封装起来，在方法里调用的s是string类型，string类型已经实现了序列化所以这里编译可以通过
  def getMatch1(RDD: RDD[String]) = {
    val s = query
    RDD.filter(_.contains(s))
  }

  //3 这种方式在调用方法运行到contains时，会去找query，发现query是自定义类的属性。而自定义类没有实现序列化方法，所以编译报错
  def getMatch2(RDD: RDD[String]) = {
    RDD.filter(_.contains(query))
  }

}

```



#### 5、注意事项

1）更换序列化器以后还需要手动继承Serializable。

2）大部分情况下我们自定义类时都会使用样例类来创建，可以省略serializer，因为样例类中已经帮我们实现了。

3）在配置conf文件时，如下语句可以省略不写。因为Spark底层，在进行注册时，会自动去设置一次。

> ```scala
> .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
> ```

## 3.10 RDD的依赖关系

### 1、查看血缘关系RDD.toDebugString

1）在一个阶段内每个分区的并行度是一致的。

2）所谓的血缘关系指的是，当前RDD依赖于上一个RDD的结果，当前阶段依赖于上一阶段的结果。

### 2、查看依赖RDD.dependencies

1）如何判断窄依赖

只要不带by，和不产生重新分区的都是窄依赖，如map和flatMap，filter。

2）如何判断宽依赖

只要父类的RDD出现了一对多的情况，就可以认为出现了宽依赖，产生了shuffle。

### 3、宽依赖、窄依赖和shuffle的关系

从算子上来看带By的都是宽依赖。

宽依赖一定有shuffle，需要借助网络和磁盘IO。

窄依赖一定没有shuffle。

**问题：宽依赖和窄依赖的数据传输问题？**

答：

1）如果产生了宽依赖那么就会有shuffle。有shuffle一定会有网络间的传输，那么数据就需要落盘实现序列化。

2）窄依赖是通过管道式传输，直接在内存中传输。不借助于磁盘，也不会产生网络IO。窄依赖一定是在一个阶段里面的。

## 3.11 RDD工作的划分

### 3.11.1 Application

#### 什么是Application？

应用：创建一个SparkContext就可以认为创建了一个Application。



### 3.11.2 Job

#### 什么是Job？

1）在一个application中，每执行一次行动算子，就会创建一个job。

2）在4040端口可以查看Job编号。

3）Job和Job之间是串行的关系，上一个Job计算完成才会计算下一个。

4）在一个APP中可以有多个Job。



### 3.11.3 stages

#### 什么是stages？

1）一个Job中至少有一个stage，以后每碰到一个shuffle算子就会产生一个stage。

2）stage一定是先执行前面的阶段，前面的阶段执行完了才会执行后面的阶段。



### 3.11.4 Tasks

#### 什么是Task？

1）task=任务，表示执行阶段的分区数。

2）在执行层中，task是最小执行单位。

3）**每一个task表现为一个本地计算**

4）**一个stage中的所有taks（分区）会对不同的数据执行相同的匿名函数代码。**

5）每个stage的task数量对应着分区的数量，即分数区=taks数

6）分区是站在数据的存储角度，task是站在计算的角度。



### 3.11.5 节点

一个节点（设备）可以运行多个executor（进程），进程间内存隔离不能互相访问。



### 3.11.6 executor

一个executor可以运行多个task，每个task是一个线程。



### 3.11.7 核心数

核心数表示能够同时运行的task数量。

比如有100个task，但是应用只申请了10个CPU核心，表示只能同时运行10个task，剩下的90个等待。

RDD并行度由核心数决定，核心数决定了能同时运行多少个任务。

可以理解为`Min(核心数，分区数)`



### 3.11.8 DAG

#### 什么是DAG？

在Driver中创建，简单来说就是建立了一个算子之间的执行顺序关系。

#### DAG干了什么？

DAG调度器为每个Job生成一个由stages构成的有向无环图，负责调度每个任务的位置告诉executor进程来执行Task线程任务。



## 3.12 RDD的持久化

### 3.12.1 持久化

#### 1、什么是持久化？

1）Spark 一个重要能力就是可以持久化数据集在内存中. 当我们持久化一个 RDD 时, 每个节点都会存储他在内存中计算的那些分区, 然后在其他的 action 中可以重用这些数据. 这个特性会让将来的 action 计算起来更快(通常快 10 倍). 对于迭代算法和快速交互式查询来说, 缓存(Caching)是一个关键工具。

2）可以使用方法**persist()**或者**cache()**来持久化一个 RDD. 在第一个 action 会计算这个 RDD, 然后把结果存储到他的节点的内存中. Spark 的 Cache 也有容错机制: 如果 RDD 的任何一个分区的数据丢失了, Spark 会自动的重新计算。

3）RDD 的各个 Partition 是相对独立的, 因此只需要计算丢失的部分即可, 并不需要重算全部 Partition

4）另外, Spark允许我们对持久化的 RDD 使用不同的存储级别.

#### 2、基本使用

```scala
object CaCheDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CacheDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List("hello world", "hello")
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = rdd1.flatMap(x => {
      println("flatMap..." + x)
      x.split(" ")
    })
      .map(x => {
        println("map..." + x)
        (x, 1)
      })

    //对那些需要重复使用的RDD做持久化缓存
    //def persist(): this.type = persist(StorageLevel.MEMORY_ONLY) 该方法默认缓存到内存
    //rdd2.persist()
      rdd2.cache()

    //rdd2经过第一个行动算子之后，会将刚刚计算的结果默认缓存到内存。
    //一般情况下都是持久化到内存
    rdd2.collect()

    println("-----第二次执行collect-------")
    rdd2.collect()

    println("------第三次执行collect----------")
    rdd2.collect()



  }

}
```



#### 3、注意事项

1）默认是持久化到内存，通常只持久化到内存。

2）在哪个分区执行的RDD，就缓存到哪个分区所对应的节点内存中。

3）如果持久化到磁盘，位置我们无法控制，Spark会放入到一个临时目录中。

4）对于shuffle算子来说，Spark会自动给shuffle后的RDD添加缓存。

>  因为对于shuffle算子来说，一个分区的数据可能来自于多个分区。一旦有一个分区数据出了问题，前面所有分区的数据都需要重新计算，代价非常高。

>  shuffle触发的缓存默认是memory_only。
>
> ![image-20200508211220371](D:\ProgramFiles\Typora\图片备份\image-20200508211220371.png)

5）cache方法的本质也是调用了persist

```scala
def cache(): this.type = persist()
```

6）实际生产中，当RDD不在使用时需要手动清除缓存，释放内存。

```scala
rdd.unpersist
```



### 3.12.2 检查点

#### 1、什么是检查点？

RDD支持**checkpoint** 将数据保存到持久化的存储中，这样就可以切断之前的血缘关系，因为checkpoint后的RDD不需要知道它的父RDDs了，可以直接从checkpoint拿取数据，减少了重建的次数。

#### 2、基本使用

```scala
package com.atguigu.spark.core.day04.selfstudy

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Classname CaCheDemo
 * @Description TODO
 *              Date ${Date} 20:47
 * @Create by childwen
 */
object CaCheDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("CacheDemo2")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)
    //设置检查点目录
    sc.setCheckpointDir("./ck1")
    val list1 = List("hello world", "hello")
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = rdd1
      .flatMap(x => {
        println("flatMap..." + x)
        x.split(" ")
      })
      .map(x => {
        println("map..." + x)
        (x, 1)
      })
      .reduceByKey(_ + _)

    //设置检查点，仅仅是一个计划。如果没有行动算子，不会加载。
    rdd2.checkpoint()
    //当我们设置checkPoint之后，发现当rdd2job运行完以后，checkPoint并没有直接将结果存起来。而是再执行了一次job
    //这样会导致我们的执行效率变低，一般我们会选择cache和checkpoint配合使用，这样可以提高效率。只执行一次job即可。
    rdd2.cache()

    rdd2.collect()

    println("-----第二次执行collect-------")
    rdd2.collect()

    println("------第三次执行collect----------")
    rdd2.collect()
    Thread.sleep(100000000)


  }

}

```

#### 3、注意事项

1）如果对RDD做checkpoint，检查点不会使用上一个Job的结果，Spark会自动启用一个新的Job专门的去做checkPoint。

2）checkPoint仅仅是一个计划，需要行动算子来触发。

3）checkPoint会切断RDD的血缘关系。

![image-20200508211006348](D:\ProgramFiles\Typora\图片备份\image-20200508211006348.png)

4）不管是持久化还是checkPoint都是针对在同一个APP中使用，只有存在RDD被重复使用的时候才有意义。

5）实际使用的时候，一般会把缓存和checkPoint配合使用。

> 即先缓存RDD，然后在checkPoint，当执行checkPoint发现已经有缓存了就不会在另起一个Job在计算一次，可以提高计算效率。
>
> 虽然对RDD做了缓存，但是使用checkPoint还是会启动一个新的job，只不过这个Job会直接取缓存的结果，减少了执行Job计算的过程。

6）checkPoint必须在Job被执行之前调用。

7）checkPoint产生的目录文件需要手动清理。

### 3.12.3 持久化和CheckPoint的区别

1）持久化只是将数据保存在 BlockManager 中，而 RDD 的 Lineage（血缘关系） 是不变的。但是**checkpoint** 执行完后，RDD 已经没有之前所谓的依赖 RDD 了，而只有一个强行为其设置的**checkpointRDD**，RDD 的 Lineage 改变了。

2）持久化的数据丢失可能性更大，磁盘、内存都可能会存在数据丢失的情况。但是 **checkpoint** 的数据通常是存储在如 HDFS 等容错、高可用的文件系统，数据丢失可能性较小。

3）注意: 默认情况下，如果某个 RDD 没有持久化，但是设置了**checkpoint**，会存在问题. 本来这个 job 都执行结束了，但是由于中间 RDD 没有持久化，checkpoint job 想要将 RDD 的数据写入外部文件系统的话，需要全部重新计算一次，再将计算出来的 RDD 数据 checkpoint到外部文件系统。 所以，建议对 **checkpoint()**的 RDD 使用持久化, 这样 RDD 只需要计算一次就可以了.

## 3.13 RDD分区器

### 3.13.1 默认分区器

1）默认的分区器一般都是HashPartitioner。

2）HashPartitioner在极端情况下，会造成严重的数据倾斜。

- 如一个RDD中的key大部分都是偶数的情况下。

#### HashPartitioner分区的原理

```scala
//返回分区的方法
def getPartition(key: Any): Int = key match {
  case null => 0
  case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
}

//实际计算的方法
 def nonNegativeMod(x: Int, mod: Int): Int = {
   val rawMod = x % mod
   rawMod + (if (rawMod < 0) mod else 0)
 }
```

**方法说明：**

> 对于给定的**key**，计算其**hashCode**，并除以分区的个数取余，如果余数小于 0，则用**余数+分区的个数**（否则加0），最后返回的值就是这个**key**所属的分区**ID**。

### 3.13.2 自定义分区器

#### 基本使用

继承Partitioner实现方法。

```scala
package com.atguigu.spark.core.day04.selfstudy

import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
 * @Classname PartitionerDemo
 * @Description TODO
 *              Date ${Date} 21:15
 * @Create by childwen
 */
object PartitionerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("PartitionerDemo$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8)
    val RDD = sc.makeRDD(list, 2)

    //使用自定义分区器对RDD进行重新分区
    RDD
      //使用map对list进行转换，变成KV
      .map((_, 1))
      //使用partitionBy对RDD重新分组
      .partitionBy(new myPartitioner(3))
      //测试 对RDD在使用一次分组看是否会产生宽依赖
      .partitionBy(new myPartitioner(3))
      //使用glom将每一个分区的数据都转换成数组
      .glom()
      //将分区数组转换成List方便查看
      .map(_.toList)
      .collect()
      .foreach(println)

    Thread.sleep(100000)

    sc.stop()

  }

}

//自定义一个分区器
class myPartitioner(val num: Int) extends Partitioner {
  //实现方法
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.hashCode().abs % num
  }
}

```



#### 注意事项

1）分区时，如果分区数和上次分区数一致，且分区器也是一样的，不会产生宽依赖，

> **注：这句话的前提是用的分区器对象一样！！！**
>
> ![image-20200508214214032](D:\ProgramFiles\Typora\图片备份\image-20200508214214032.png)

2）如何让对象相等？

> 重写对象的hashCode和equals方法。



### 3.13.3 RangerPartitioner

#### 基本介绍

范围分区器，是Spark提供的一个分区器，可以最大程度的避免数据倾斜。

#### 为什么要使用RangerPartitioner？

**HashPartitioner** 分区弊端： 可能导致每个分区中数据量的不均匀，极端情况下会导致某些分区拥有 RDD 的全部数据。比如我们前面的例子就是一个极端, 他们都进入了 0 分区.

**RangePartitioner** 作用：将一定范围内的数映射到某一个分区内，尽量保证每个分区中数据量的均匀，而且分区与分区之间是有序的，一个分区中的元素肯定都是比另一个分区内的元素小或者大，但是分区内的元素是不能保证顺序的。简单的说就是将一定范围内的数映射到某一个分区内。实现过程为：

第一步：先从整个 RDD 中抽取出样本数据，将样本数据排序，计算出每个分区的最大 **key** 值，形成一个**Array[KEY]**类型的数组变量 **rangeBounds**；(边界数组).

第二步：判断**key**在**rangeBounds**中所处的范围，给出该**key**值在下一个**RDD**中的分区**id**下标；该分区器要求 RDD 中的 **KEY** 类型必须是可以排序的.

比如[1,100,200,300,400]，然后对比传进来的**key**，返回对应的分区**id**。

> RangerPartitioner最核心的地方就在于确定范围数组的边界值。

>  RangerPartitioner范围抽样采用的算法是水塘抽样。

#### 基本使用

```scala
RDD.partitionBy(new RangePartitioner(3, RDD))
```



### 3.13.4 查看RDD分区器信息

```scala
RDD.partitioner
```



## 3.14 外部数据读取和存储

### 3.14.1 如何读取和存储？

1）在Spark中从文件中读取数据是通过创建RDD的方式，将数据保存到文件是通过行动算子来进行操作。

2）Spark的数据读取及数据保存可以从两个维度来作为区分：

- 文件格式：Text文件、Json文件、Csv文件、Sequence文件以及Object文件
- 文件系统：本地文件系统、HDFS、Hbase 以及 数据库。

3）在HDFS上读写文件，本质也是通过Fileinputformat和Fileoutputformat来操作的。

### 3.14.2 读

#### 读Text文件

##### 基本使用

```scala
object TextWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TextWrite")
    val sc = new SparkContext(conf)

    //通过textFile创建一个RDD对象读取文件内容。
    val RDD = sc.textFile("./text")
    RDD
      .collect()
      .foreach(println)

    sc.stop()
  }

```



#### 读Jason文件

##### 基本使用

```scala
   import scala.util.parsing.json.JSON
   val rdd2 = rdd1.map(x => JSON.parseFull(x))
```

> 仅做了解，后期使用spark-sql

##### 注意事项

1）json文件，必须保证每一行是一个完整的json数据。

> 这种为错误格式

> ```scala
> {
>  "name": "zs",
>  "age": 20
> }
> ```
>
> 正确格式：
>
> ```scala
> {"name":"Michael"}
> {"name":"Andy", "age":30}
> {"name":"Justin", "age":19}
> ```
>
> 

2）本质还是读文本文件，然后使用json工具解析。



解析json，

```scala
map(x => JSON.parseFull(x))
```



#### 读SequenceFile文件

```scala
object SequenceFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SequenceFile")
    val sc = new SparkContext(conf)
    sc
      .sequenceFile[String, Int]("./SequenceFile")
      .collect()
      .foreach(println)
  }
}
```



##### 注意事项

1）在读时编译器不会自动推导，必须指定读取的KV键值对的泛型类型。



#### 读对象文件

##### 基本使用

```scala
object ObjectRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ObjectRead$")
    val sc = new SparkContext(conf)
    //使用Object读取文件内容，必须指定泛型类型
    val RDD = sc.objectFile[Int]("./ObjectFile")
    RDD
      .collect()
      .foreach(println)

    sc.stop()
  }

}
```



##### 注意事项

1 ）在读的时候也要加泛型指定Obj的类型。



#### 从JDBC读数据

##### 依赖相关

```xml
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.17</version>
        </dependency>
```



##### 基本思路

1）创建SparkContext对象。

2）创建一个JdbcRDD对象，传参：

- SparkContext对象

- 获取数据库对象的函数

  > 创建数据库连接的步骤：
  >
  > 数据库驱动：com.mysql.jdbc.Driver
  >
  > 数据库URL：jdbc:mysql://hadoop102:3306/test
  >
  > 用户名user：root
  >
  > 密码pw：123123
  >
  > ①通过反射注册数据库驱动 Class.forName(driver)
  >
  > ②通过驱动管理器获取连接 DriverManager.getConnection(URL, user, pw)

- 编写SQL语句，必须带上下界条件。

- 上界条件

- 下界条件

- 分区数

- 传进来每行的数据，通过对应的列名可以调用的函数。

##### 基本使用

```scala
object JdbcRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("JdbcRead$")
    val sc = new SparkContext(conf)

    /**
     * 构造器所需参数
     * class JdbcRDD[T: ClassTag](
     * sc: SparkContext, Spark配置文件
     * getConnection: () => Connection, 数据库连接对象
     * sql: String, SQL语句
     * lowerBound: Long, 查询条件下界
     * upperBound: Long, 查询条件上界
     * numPartitions: Int, 分几个分区存放
     * mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _) 返回每一行的数据
     */
      
    val driver = "com.mysql.jdbc.Driver"
    val usr = "root"
    val psd = "123123"
    val url = "jdbc:mysql://hadoop102:3306/test"
    val sql =
      """select id,name
        |from A
        | where id >= ? and id <= ?
        |""".stripMargin
    val lowerBound = 1
    val upperBound = 3
    val numPartitions = 2

    def con(): Connection = {
      Class.forName(driver)
      DriverManager.getConnection(url, usr, psd)
    }


    val RDD = new JdbcRDD(
      sc,
      con,
      sql,
      lowerBound,
      upperBound,
      numPartitions,
      //将查询到的结果一行一行的返回
      row => (row.getInt("id"),row.getString("name"))
    )
    .collect()
    .foreach(println)

    sc.stop()

  }

}
```





#### 从HBase读数据

##### 依赖相关

```xml
    <dependencies>
    <dependency>
        <groupId>org.apache.hbase</groupId>
        <artifactId>hbase-server</artifactId>
        <version>1.3.1</version>
        <exclusions>
            <exclusion>
                <groupId>org.mortbay.jetty</groupId>
                <artifactId>servlet-api-2.5</artifactId>
            </exclusion>
            <exclusion>
                <groupId>javax.servlet</groupId>
                <artifactId>servlet-api</artifactId>
            </exclusion>
        </exclusions>
    </dependency>
    </dependencies>
```



##### 基本使用

```scala
  object HbaseRead {
    def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("HbaseRead").setMaster("local[2]")
      val sc: SparkContext = new SparkContext(conf)
      //通过工厂类去拿到hadoop和Hbase的配置
      val hbaseConf: Configuration = HBaseConfiguration.create()
      // zookeeper配置
      hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
      hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")

      // 通用的读法 noSql key-value cf
      val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
        hbaseConf,
        classOf[TableInputFormat], // InputFormat
        classOf[ImmutableBytesWritable], //hbase + mapreduce
        classOf[Result]
      )

      val rdd2 = hbaseRDD.map {
        case (ibw, result) =>
          //                Bytes.toString(ibw.get())
          // 把每一行所有的列都读出来, 然后放在一个map中, 组成一个json字符串
          var map = Map[String, String]()
          // 先把row放进去
          map += "rowKey" -> Bytes.toString(ibw.get())
          // 拿出来所有的列
          val cells: util.List[Cell] = result.listCells()
          // 导入里面的一些隐式转换函数, 可以自动把java的集合转成scala的集合
          import scala.collection.JavaConversions._
          for(cell <- cells){ // for循环, 只支持scala的集合
            val key = Bytes.toString(CellUtil.cloneQualifier(cell))
            val value = Bytes.toString(CellUtil.cloneValue(cell))
            map += key -> value
          }
          // 把map序列化成json字符串
          // json4s 专门为scala准备的json工具
          implicit val d: DefaultFormats =  org.json4s.DefaultFormats
          Serialization.write(map)
      }
      //        rdd2.collect.foreach(println)
      rdd2.saveAsTextFile("./hbase")
      sc.stop()

    }
  }
```



### 3.14.2 写

#### 写Text

##### 基本使用

**在IDEA中编译**

```scala
object TextWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TextWrite$")
        val sc = new SparkContext(conf)
        val list = List(("a",1),("ad",5),("bb",6),("cc",8),("c",10))
        val RDD = sc.makeRDD(list, 2)

    //使用行动算子向HDFS上存储数据。
        RDD
            .saveAsTextFile(args(0))

         sc.stop()
  }
}
```

**打包在Linux中运行**

```scala
bin/spark-submit --master local[*] --class com.atguigu.spark.core.day05.readwrite.TextWrite SparkCore-1.0-SNAPSHOT.jar hdfs://hadoop102:9000/text_input
```

##### 注意事项

1）向HDFS存储时，必须保证当前目录不存在。

2）在写文件时，每个分区写一个文件。

#### 写JSON文件



#### 写SequenceFile文件

```scala
object SequenceFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SequenceFile")
    val sc = new SparkContext(conf)
    val list = Array(("a", 1),("b", 2),("c", 3))
    val RDD = sc.makeRDD(list, 2)
    RDD.saveAsSequenceFile("./SequenceFile")
  }
}
```

##### 注意事项

1）写的时候必须保证RDD是KV格式的。



#### 写对象文件

##### 基本使用

```scala
object ObjectRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ObjectRead$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)
    RDD
        .saveAsObjectFile("./ObjectFile")

    sc.stop()
  }

}
```



##### 注意事项

1）任何RDD都可以存入到对象文件。





#### 写JDBC

##### 基本思路

1）将RDD中的写入到JDBC，没有相应的行动算子。只能自己手动去写。

2）向JDBC写有两种思路：

第一种思路是拉取到驱动端：这种情况适用于数据量小的情况，如果数据量过大会造成OOM。

第二种思路是让每个executor自己写，这种情况适合大数据场景。分而治之的思想。

**下面着重介绍第二种思路：**

> 1）加载驱动 Class.forName(driver)
>
> 2）获取连接 DriverManager.getConnection(url, user, pw)
>
> 3）预编译SQL语句，注意使用占位符 con.prepareStatement(sql)
>
> 4）设置占位符 ps.setInt(2, count)
>
> 5）执行预编译 ps.execute()
>
> 6）关闭连接

##### 使用foreach写

```scala
object JdbcWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("JdbcWrite$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)

    val URL = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val psd = "root"
    val sql = "insert into a values(?,?)"
    //通过foreac行动算子，读取RDD中每一个数据，然后存入到数据库中。
    RDD
      .map((_, "a"))
      .foreach(
        {
          case (id, name) => {
            //加载驱动
            Class.forName("com.mysql.jdbc.Driver")
            //创建数据库连接
            val con = DriverManager.getConnection(URL, user, psd)
            //预编译SQL
            val ps = con.prepareStatement(sql)
            //设置sql占位符
            ps.setInt(1,id)
            ps.setString(2,name)
            //执行预编译
            ps.execute()
            //关闭资源
            ps.close()
            con.close()
          }
        }
      )
    sc.stop()
  }

}
```



##### 使用foreachPartition写

思路：

遍历每一个分区得到每一个分区的It集合

在it集合的foreach方法内，连接JDBC操作数据库。

```scala
package com.atguigu.spark.core.day05.selfstudy


import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Classname JdbcRead
 * @Description TODO
 *              Date ${Date} 20:21
 * @Create by childwen
 */
object JdbcWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("JdbcWrite$")
    //设置kryo作为序列化器，将方法序列化传递到executor
    conf.registerKryoClasses(Array(classOf[Util]))

    val sc = new SparkContext(conf)
    val list = List(10, 30, 40, 60, 70, 80, 90, 20)
    val RDD = sc.makeRDD(list, 2)
    val driver = "com.mysql.jdbc.Driver"
    val URL = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val psd = "root"
    val sql = "insert into a values(?,?)"

    val writeUtil = new Util(driver, URL, user, psd)
    //通过foreacPartition行动算子，一次读取RDD一个分区的数据，然后存入到数据库中。
    RDD
      .map((_, "a"))
      .foreachPartition(it => {

        val con = writeUtil.getCon()
        //预编译sql
        val ps: PreparedStatement = con.prepareStatement(sql)
        //批处理深度
        var depth = 0
        //foreachPartition取出每一个分区的数据，返回一个迭代器。
        //使用foreach遍历迭代器中的数据
        it.foreach {
          case (id, name) => {
            //设置占位符
            ps.setInt(1, id)
            ps.setString(2, name)
            //执行预编译,这样每次来一条数据就执行一次效率太低。设置为批处理。
            ps.addBatch()
            //批处理深度设置
            depth += 1
            if (depth >= 10) {
              ps.executeBatch()
              //批处理深度清0
              depth = 0
            }

          }
        }
        //当分区元素遍历完毕在提交一次，因为有可能最后一次没达到批处理深度还在ps中，不提交的话数据就会丢失
        ps.executeBatch()
        //关闭连接
        writeUtil.close(ps,con)
      })

    sc.stop()
  }


}

//创建JDBC工具类
case class Util(driver: String, URL: String, user: String, psd: String) {
  def getCon(): Connection = {
    //注册驱动
    Class.forName(driver)
    //通过驱动管理器获取连接
    DriverManager.getConnection(URL, user, psd)
  }

    def close(ps: PreparedStatement, con: Connection): Unit = {
      ps.close()
      con.close()
    }
}
```



##### foreach和foreachPartition的区别？

两者本质都是遍历集合中的元素，

- foreach是将集合中的元素每一个都遍历一次。

  > 因为连接对象不能被序列化，所以使用foreach行动算子来进行JDBC操作时，必须在foreach内部创建连接，这样没进来一个数据就要创建一次连接很浪费资源。

- foreachPartition是按分区遍历。将每个分区的数据存到一个迭代器中。这样在我们使用JDBC操作数据库时，就可以减少连接次数，从而提高写的速度。

##### 注意事项

1）在Spark中连接对象不能序列化，所以在Driver建立的驱动到executor不能使用。

> 连接对象的底层已经和本地的网络端口IP地址等绑定了，如果Driver端的连接对象序列化到executor，地址无法对应是无法使用的。

2）在大数据场景下，不建议使用foreach行动算子来进行操作JDBC，容易造成OOM。

3）在批处理提交数据时，记得设置一下批处理深度，防止分区中数据过大造成OOM。

#### 写Hase

##### 基本使用

```scala
object HbaseWrite {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseWrite").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val list = List(
            ("2100", "zs", "male", "10"),
            ("2101", "li", "female", "11"),
            ("2102", "ww", "male", "12"))
        val rdd1 = sc.parallelize(list)
        
        // 把数据写入到Hbase
        // rdd1做成kv形式
        val resultRDD = rdd1.map {
            case (rowKey, name, gender, age) =>
                val rk = new ImmutableBytesWritable()
                rk.set(Bytes.toBytes(rowKey))
                val put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("gender"), Bytes.toBytes(gender))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(age))
                (rk, put)
        }
        
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104") // zookeeper配置
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")  // 输出表
    
        val job: Job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        
        resultRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
        
        sc.stop()
        
    }
}
```



## 3.15  累加器

#### 3.15.1 什么是累加器？

Spark提供的Accumulator，主要用于多个节点对一个变量进行共享性的操作。Accumulator只提供了累加的功能，只能累加，不能减少。累加器只能在Driver端构建，并只能从Driver端读取结果，在Task端只能进行累加。

累加器是一种变量，仅仅支持`add`，支持并发，累加器用于实现去实现计数器或者求和。Spark内部已经支持数字类型的累加器。

#### 3.15.2 系统累加器

##### 基本使用

```scala
object ACC1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ACC1$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)
    //使用系统累加器
    val countAcc: LongAccumulator = sc.longAccumulator("countAcc")

    //使用累加器计算list集合中元素的个数，累加器的触发也是lazy的需要通过行动算子来触发
    RDD
      .foreach(x => countAcc.add(1))
    //取出累加器中的值
    println(countAcc.value)

    val sumAcc = sc.longAccumulator("sumAcc")
    //使用累加器计算集合中元素的总和
    RDD
        .foreach(x => sumAcc.add(x))
    //取出累加器中的值
    println(sumAcc.value)


    sc.stop()
  }

}
```



#### 3.15.3 自定义累加器

##### 基本思路

1）自定义类继承抽象类累加器

2）实现累加器的6个方法

3）向SparkContext注册累加器

4）使用累加器对元素进行操作

##### 基本使用

案例1：自定义累加器。求和，求平均值，求count

```scala
object ACC2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ACC2$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)
    val myAcc = new MyAcc
    sc.register(myAcc, "myAcc")
    RDD
      .foreach(x => myAcc.add(x))
    println(myAcc.count())
    println(myAcc.sum())
    println(myAcc.avg())
    println(myAcc.value)

    sc.stop()
  }

}

/**
 * 自定义一个累加器，实现求和、求count的需求
 */
//AccumulatorV2：抽象类泛型类
//abstract class AccumulatorV2[IN, OUT] extends Serializable
class MyAcc extends AccumulatorV2[Int, Int] {
  //_sum 表示仅仅内部使用
  private var _sum = 0
  private var _count = 0

  //判零，对缓冲区进行初始化判断
  override def isZero: Boolean = _sum == 0.0 && _count == 0

  //复制累加器，返回一个新的累加器，让当前累加器中缓冲区的值，复制到新的累加器。
  override def copy(): AccumulatorV2[Int, Int] = {
    val newAcc = new MyAcc
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  //重置累加器，将类加器中缓冲区的值重置为zero值
  override def reset(): Unit = {
    _sum = 0
    _count = 0
  }

  //分区内累加逻辑
  override def add(v: Int): Unit = {
    _sum += v
    _count += 1
  }

  //在驱动端的行动算子之后调用该方法。返回的属性是累加后的总结果,其本质等价于Value方法
  def sum() = _sum

  def count() = _count

  def avg() = _sum.toDouble / _count

  //分区间的合并，其实就是把other中的值合并到this的值中。
  override def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
    case o: MyAcc =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Int = _sum

}
```

案例2：自定义累加器，计算元素的和，个数，平均数，最大值，最小值。

```scala
object ACC2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ACC2$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)
    val myAcc = new MyAcc
    //注册自定义累加器
    sc.register(myAcc, "myAcc")
    RDD
      .foreach(x => myAcc.add(x))
    
    val res = myAcc.value
    println(res)
    
    sc.stop()
  }

}

/**
 * 自定义一个累加器，实现求和、求count的需求
 */
//AccumulatorV2：抽象类泛型类
//abstract class AccumulatorV2[IN, OUT] extends Serializable
class MyAcc extends AccumulatorV2[Int, Map[String, Any]] {
  //_sum 表示仅仅内部使用
  private var _sum = 0
  private var _count = 0
  private var _minNum = Int.MaxValue
  private var _maxNum = Int.MinValue

  //判零，对缓冲区进行初始化判断
  override def isZero: Boolean = _sum == 0 && _count == 0 && _minNum == Int.MaxValue && _maxNum == Int.MinValue

  //复制累加器，返回一个新的累加器，让当前累加器中缓冲区的值，复制到新的累加器。
  override def copy(): AccumulatorV2[Int, Map[String, Any]] = {
    val newAcc = new MyAcc
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc._minNum = this._minNum
    newAcc._maxNum = this._maxNum
    newAcc
  }

  //重置累加器，将类加器中缓冲区的值重置为zero值
  override def reset(): Unit = {
    _sum = 0
    _count = 0
    _minNum = Int.MaxValue
    _maxNum = Int.MinValue
  }

  //分区内累加逻辑
  override def add(v: Int): Unit = {
    _sum += v
    _count += 1
    setMin(v)
    setMax(v)
  }

  //在驱动端的行动算子之后调用该方法。返回的属性是累加后的总结果,其本质等价于Value方法
  def sum() = _sum

  def count() = _count

  def minNum() = _minNum

  def maxNum() = _maxNum

  def avg() = _sum.toDouble / _count
  
  private def setMin(value: Int) = {
    _minNum = _minNum.min(value)
  }

  private def setMax(value: Int) = {
    _maxNum = _maxNum.max(value)
  }

  //分区间的合并，其实就是把other中的值合并到this的值中。
  override def merge(other: AccumulatorV2[Int, Map[String, Any]]): Unit = other match {
    case o: MyAcc =>
      _sum += o.sum
      _count += o.count
      setMin(o.minNum)
      setMax(o.maxNum)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  //返回值类型必须和泛型类中定义的OUT的类型保持一致
  override def value: Map[String, Any] = Map("sum" -> sum, "count" -> count, "avg" -> avg, "maxNum" -> maxNum, "minNum" -> minNum)

}
```

方式2：

```scala
object ACC3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ACC2$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)

    val myAcc1 = new MyAcc1
    sc.register(myAcc1, "mapAcc")
    RDD.foreach(x=>myAcc1.add(x))
    println(myAcc1.value)
    sc.stop()
  }

}

/**
 * 自定义一个累加器，实现求和、求count的需求
 */
//AccumulatorV2：抽象类泛型类
//abstract class AccumulatorV2[IN, OUT] extends Serializable
class MyAcc1 extends AccumulatorV2[Int, Map[String, Any]] {

  private var map = Map[String, Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Int, Map[String, Any]] = {
    val newAcc1 = new MyAcc1
    newAcc1.map = this.map
    newAcc1
  }

  override def reset(): Unit = map = map.empty

  //区内聚合
  override def add(v: Int): Unit = {
    map += ("sum" -> (map.getOrElse("sum", 0) + v))
    map += ("count" -> (map.getOrElse("count", 0) + 1))
    map += "max" -> map.getOrElse("max", Int.MinValue).max(v)
    map += "min" -> map.getOrElse("min", Int.MaxValue).min(v)
  }

  //区间聚合
  override def merge(other: AccumulatorV2[Int, Map[String, Any]]): Unit = other match {
    case o: MyAcc1 => {
      map += "sum" -> (o.map.getOrElse("sum", 0) + map.getOrElse("sum", 0))
      map += "count" -> (o.map.getOrElse("count", 0) + map.getOrElse("coun", 0))
      map += "max" -> o.map.getOrElse("max", Int.MinValue).max(map.getOrElse("max", Int.MinValue))
      map += "min" -> o.map.getOrElse("min", Int.MaxValue).max(map.getOrElse("min", Int.MaxValue))
    }
  }

  override def value: Map[String, Any] = {
    map += "avg" -> map.getOrElse("sum", 0) / map.getOrElse("count", 1)
    map
  }
}
```



#### 3.15.4 注意事项

1）累加器只在行动算子中使用，不能在转换算子中使用。

>  比如对RDD做了一次checkPoint，会重新起一个Job在进行一次运算，算出来的结果会累加两次。

2）使用累加器时需要注意只有Driver端能够取到累加器的值，Task端进行的是累加操作。

> ```
> 在Task节点，准确的就是说在executor上；
> 每个Task都会有一个累加器的变量，被序列化传输到executor端运行之后再返回过来都是独立运行的；
> 如果在Task端去获取值的话，只能获取到当前Task的，Task与Task之间不会有影响
> ```

3）在自定义累加器内部，可以灵活的定义各种我们想要的方法来获取结果。

>  在Driver端调用内部的属性值都是最终的累加结果。本质等价于value方法。
>
> ```scala
> 
>   //在驱动端的行动算子之后调用该方法。返回的属性是累加后的总结果,其本质等价于Value方法
>   def sum() = _sum
> 
>   def count() = _count
> 
>   def avg() = _sum.toDouble / _count
> ```
>
> 

## 3.16 共享变量的问题

### 3.16.1 共享变量写的问题

使用累加器解决的共享变量写的问题。



### 3.16.2 共享变量读的问题

1）当变量中存在上G数据的情况下，线程之间的内存不能共享，会导致executor内存压力太大。

2）使用广播变量解决共享变量读的问题

#### 1、广播变量

##### 什么是广播变量？

通过SparkContext对象在每个节点上保存一个只读的变量的缓存，不需要给每个Task都copy一个变量，在有较大的输入数据集时，是一个节省内存比较高效的方法，Spark也会用该对象的广播逻辑去分发广播变量来降低通讯的成本。

#### 2、基本使用

```scala
object BdDEMO {
  val arr = Array(3, 6, 10, 100, 300)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("BdDEMO$")
    val sc = new SparkContext(conf)
    val list = List(1, 3, 4, 6, 7, 8, 9, 2)
    val RDD = sc.makeRDD(list, 2)
    //使用广播变量将数组缓存起来，使用时广播到每一个节点。
    val bdArr: Broadcast[Array[Int]] = sc.broadcast(arr)
    RDD
      //广播变量取值使用.value
      .filter(bdArr.value.contains(_))
      .foreach(println)

    sc.stop()
  }

}
```

#### 3、注意事项

1）对广播变量，不要轻易去更改，一旦更改所有的task线程都会受影响。

2）一个Job中可以有多个广播变量，当存在多个广播变量时，会按照先后顺序进行编号。

```scala
   val bdList = sc.broadcast(list)
   val bdList1 = sc.broadcast(list)
   println(bdList.id)
   println(bdList1.id)
```

> 从0开始编号，依次递增。



## 3.17 RDD如何分区？

### 3.17.1 使用集合创建RDD时如何分区？



### 3.17.2 读取文件时如何分区？

读取文件传入的路径下不能有目录



# 四 Spark Core项目实战

## 4.1 map合并的两种方式

### 4.1.1 使用foreach遍历

#### 基本思路

使用foreach遍历map1中的每一个元素，然后map2使用getorelse进行判断，如果有该元素则取出value值进行相加，如果没有则使用默认值。

精髓在key

```scala
 other match {
      case o: Acc =>
        o.map.foreach {
          case (cid, (click, order, pay)) =>
            val (thisClick, thisOrder, thisPay) = map.getOrElse(cid, (0L, 0L, 0L))
            map += cid -> (click + thisClick, order + thisOrder, pay + thisPay)
            map
        }
     
           case _ =>
    }
  }
```

### 4.1.2 使用foldLeft聚合

#### 基本思路

使用其中一个map作为zero值，进行聚合

```scala
  override def merge(other: AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]]): Unit = {
    other match {
      case o: Acc =>
        //        o.map.foreach {
        //          case (cid, (click, order, pay)) =>
        //            val (thisClick, thisOrder, thisPay) = map.getOrElse(cid, (0L, 0L, 0L))
        //            map += cid -> (click + thisClick, order + thisOrder, pay + thisPay)
        //            map
        //        }
        //两个map的合并
        o.map.foldLeft(this.map) {
            //变量匹配 + 内容匹配
          case (map, (cid, (click, order, pay))) =>
            val (thisClick, thisOrder, thisPay) = map.getOrElse(cid, (0L, 0L, 0L))
            this.map += cid -> (click + thisClick, order + thisOrder, pay + thisPay)
            this.map
        }


      case _ =>
    }
  }
```

## 4.2 当it集合中的数量很大的处理方式

### 4.2.1 使用filter过滤一部分数据，分步进行处理

```scala
 //    Top10热门品类中每个品类的 Top10 活跃 Session 统计
    val CateList = RDD1
      .map(_.categoryId.toLong)

    val cidsRDD = RDD
      .filter(x => CateList.contains(x.click_category_id))
      .map(x => ((x.click_category_id, x.session_id), 1))
      .reduceByKey(_ + _)
      .map {
        case ((cid, sid), count) => (cid, (sid, count))
      }

    //将每一个品类过滤出来，转换成RDD处理，防止在方法一种直接转List时出现OOM
    val buffer = ListBuffer[(Long, List[(String, Int)])]()
    for (cid <- CateList) {
      val cidRDD = cidsRDD
        .filter(_._1 == cid)

      val resRDD = cidRDD
        .sortBy(x => -x._2._2)
        .take(10)
        .map(_._2)
      buffer += ((cid, resRDD.toList))
    }
    buffer.foreach(println)


```

filter的作用：将每个特定品类的数据全部过滤出来，对每个品类单独起一个RDDJob进行排序处理。

优势：省略了groupBy，不会出现内存溢出的情况

缺点：Job过多，影响效率

### 4.2.2 利用Set的自然排序

当it集合中的数量很大，但是又要对it进行前n个排序时，可以使用TreeSet，遍历it，每当treeset满n个时就只取前n个数。

```scala
  //    Top10热门品类中每个品类的 Top10 活跃 Session 统计
    val CateList = RDD1
      .map(_.categoryId.toLong)

    implicit def ord: Ordering[SessionCount] = new Ordering[SessionCount] {
      override def compare(x: SessionCount, y: SessionCount): Int =
        if (x.count > y.count) -1
        else 1
    }

    RDD
      .filter(x => CateList.contains(x.click_category_id))
      .map(x => ((x.click_category_id, x.session_id), 1))
      .reduceByKey(_ + _)
      .map {
        case ((cid, sid), count) => (cid, (sid, count))
      }
      .groupByKey()
      //使用map对每一个品类的value集合进行Set处理。
      //利用了TreeSet的自然排序
      .map {
        case (cid, it) => {
          var set = new TreeSet[SessionCount]()
          it.foreach{
            case (sid,count) => set += SessionCount(sid,count)
              if(set.size > 10) set = set.take(10)
          }
          (cid,set.toList)
        }
      }
    .collect()
    .foreach(println)

  }

```

### 4.2.3 自定义分区器结合TreeSet，将不同的key分到不同的分区

等于少了一个shuffle阶段，把groupBy的事做了

#### 自定义分区器

分区器示例：

```scala
case class SeesionPartitioner(CateList: List[Long]) extends Partitioner {
  //思路：如果通过品类获取唯一的分区？
  //利用拉链表的特性
  //合成一个Map，使用传入的每个品类来获取对应的分区索引
  private val indexMap: Map[Long, Int] = CateList.zipWithIndex.toMap

  override def numPartitions: Int = CateList.size

  override def getPartition(key: Any): Int = key match {
    case (cid: Long, _) => indexMap(cid)
    case _ => throw NullPointerException
  }
}
```

代码示例：

```scala
 //    Top10热门品类中每个品类的 Top10 活跃 Session 统计
    val CateList = RDD1
      .map(_.categoryId.toLong)

    implicit def ord: Ordering[SessionCount] = new Ordering[SessionCount] {
      override def compare(x: SessionCount, y: SessionCount): Int =
        if (x.count > y.count) -1
        else 1
    }

    var set = new TreeSet[SessionCount]()
    RDD
      .filter(x => CateList.contains(x.click_category_id))
      .map(x => ((x.click_category_id, x.session_id), 1))
      .reduceByKey(SeesionPartitioner(CateList), _ + _)
      .foreachPartition(it => {
        //标记变量
        var id = 0L
        it.map {
          case ((cid, sid), count) => (cid, (sid, count))
        }
          .foreach {
            case (cid, (sid, count)) => set += SessionCount(sid, count)
              id = cid
              if (set.size > 10) set = set.take(10)
          }
        List((id, set.toList))
        .foreach(println)

      })

  }
```



#### 使用mapPartition算子结合TreeSet，计算每个分区的数据

```scala
def statCategoryTop10Session4(sc: SparkContext, rdd1: RDD[UserVisitAction], rdd2: List[CategoryCountInfo]) = {
    //思路四 使用分区，将十个品类全部分到十个分区来处理
    //需要一个分区器，这个分区器的作用是将每一个KV分到对应K的分区
    val cateList = rdd2.map(_.categoryId.toLong)
    val RDD1 = rdd1.filter(x => cateList.contains(x.click_category_id))
    //格式转换，（品类，（会话，次数））
    implicit val ord: Ordering[SessionInfo] = new Ordering[SessionInfo] {
      override def compare(x: SessionInfo, y: SessionInfo): Int =
        if (x.count > y.count) -1
        else 1
    }
    val RDD2 = RDD1.map {
      x => ((x.click_category_id, x.session_id), 1)
    }
      .reduceByKey(SessionPartition(cateList), _ + _)
      .map {
        case ((cid, sid), count) => (cid, (sid, count))
      }

    val resRDD = RDD2.
      mapPartitions(it => {
        var id = 0L
        var set = new TreeSet[SessionInfo]()
        it.foreach {
          case (cid, (sid, count)) => set += SessionInfo(sid, count)
            if (set.size > 10) set = set.take(10)
            id = cid
        }
        Iterator((id, set.toList))
      }
      )
    resRDD.foreach(println)


  }
```

## 4.3 连续问题，使用拉链解决

```scala
package com.atguigu.spark.project.app

import java.text.DecimalFormat

import com.atguigu.spark.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @Classname Need3
 * @Description TODO
 *              Date  23:18
 * @Create by childwen
 */
object Need3 {
  def PagesTransFrom(sc: SparkContext, RDD: RDD[UserVisitAction], pagesStr: String) = {
    //页面转化率 = 跳转次数 / 页面点击
    //求页面点击数
    val page = pagesStr.split(",")

    val pageMap = RDD
      .filter(x => page.contains(x.page_id.toString))
      .map(x => (x.page_id, 1))
      .countByKey()

    // 1->2,2->3
    val pageFlow = page.init.zip(page.tail)

    //求页面跳转次数 1->2 2->3
    val resMap = RDD
      .groupBy(_.session_id)
      .map(_._2.toList.sortBy(_.action_time))
      .map(x => x.map(_.page_id))
      .flatMap {
        op =>
          val init = op.init
          val tail = op.tail
          init.zip(tail)
      }
      .map {
        case (a, b) => (s"$a->$b", 1)
      }
      .reduceByKey(_ + _)
      .collect()
      .toMap

    val foo = new DecimalFormat(".00%")
    pageFlow
      .map {
        case (a, b) => (s"$a->$b",foo.format(resMap(a + "->" + b).toDouble / pageMap(a.toLong)))
      }
      .foreach(println)


  }

}

```



## 4.4 项目总结

1）在进行算子运算时，要分清楚函数体中的作用域问题。

2）每个算子的返回值，参数列表分别代表什么含义。

3）在函数体中灵活使用变量累加

![image-20200512105241479](D:\ProgramFiles\Typora\图片备份\image-20200512105241479.png)

从0切到length-1

4）如果算子需要返回值，可以使用map，不需要返回值使用foreach。

5）对连续问题求解，巧用拉链。

6）时间格式化函数

```scala
val foo = new DecimalFormat(".00%")
```

# 五 Spark SQL

## 5.1 什么是SparkSQL？

1）Spark SQL是Spark用于结构化数据（structured data）处理的spark模块。

2）**与Spark RDD API区别**

- 内部对计算进行优化

> SparkSQL的抽象数据类型为Spark提供了关于数据结构和正在执行的计算的更多信息，在SparkSQL内部会使用这些额外的信息去做一些额外的优化。

- 可以在不同的API之间切换

> 可以有多种方式与SparkSQL进行交互，比如：SQL和Dataset API，当计算结果的时候，使用的是相同的执行引擎，不依赖你正在使用哪种API或者语言。
>
> 这种统一也就意味着开发者可以很容易在不同的API之间进行切换，这些API提供了最自然的方式来表达给定的转换。

3）**与Hive的异同**

- 异

> Hive是将Hive SQL转换成MR然后提交到集群上执行，简化了编写MapReduce程序的复杂性，缺点就是MR计算模型执行效率较慢。
>
> 所以Spark SQL应运而生，它是将Spark SQL转换成了RDD，然后提交到集群执行，因为RDD是基于内存进行计算的模型，所以执行效率非常快。

- 同

> 同Hive简化了MR程序编程困难的问题一样，
>
> SparkSQL同样也解决了使用SparkCore编程困难的问题。

4）Spark提供了两个基本的编程抽象，类似于Spark Core中的RDD

> `DataFrame`
>
> `DataSet`

## 5.2 Spark SQL的特点

### 5.2.1 Integrated（易整合）

SparkSQL无缝的整合了SQL查询和Spark编程。

![image-20200513100007645](D:\ProgramFiles\Typora\图片备份\image-20200513100007645.png)

### 5.2.2  Uniform Data Access(统一的数据访问方式)

使用相同的方式连接不同的数据源。

![image-20200513100206190](D:\ProgramFiles\Typora\图片备份\image-20200513100206190.png)

### 5.2.3  Hive Integration(集成 Hive)

在已有的数据仓库上运行SQL或者HiveSQL

![image-20200513100305806](D:\ProgramFiles\Typora\图片备份\image-20200513100305806.png)

### 5.2.4  Standard Connectivity(标准的连接方式)

通过JDBC或ODBC来连接

![image-20200513100415225](D:\ProgramFiles\Typora\图片备份\image-20200513100415225.png)





## 5.3 Spark SQL编程基本介绍

### 5.3.1 SparkSession

#### 基本介绍

##### 1）新版本和老版本的区别

在老的版本中，SparkSQL 提供两种 SQL 查询起始点：一个叫**SQLContext**，用于Spark 自己提供的 SQL 查询；一个叫 **HiveContext**，用于连接 Hive 的查询。

从2.0开始, **SparkSession**是 Spark 最新的 SQL 查询起始点，实质上是**SQLContext**和**HiveContext**的组合，所以在**SQLContext**和**HiveContext**上可用的 API 在**SparkSession**上同样是可以使用的。

##### 2）SparkSession的本质

**SparkSession**内部封装了**SparkContext**，所以计算实际上是由**SparkContext**完成的。

当我们使用 spark-shell 的时候, spark 会自动的创建一个叫做**spark**的**SparkSession**, 就像我们以前可以自动获取到一个**sc**来表示**SparkContext**一样。

![image-20200513112305674](D:\ProgramFiles\Typora\图片备份\image-20200513112305674.png)



### 5.3.2 DataFrame编程

#### 什么是DataFrame？

##### 1）分布式

与RDD类似，DataFrame也是一个**分布式**数据容器。

##### 2）schema

除了分布式的特点以外，DataFrame更像传统数据库的二维表格，除了保存数据以外，还记录了数据的结构信息（元数据），即`schema`



##### 3）支持嵌套数据类型

与Hive类似，DataFrame也支持嵌套数据类型`struct`、`array`、`map`。



##### 4）易用

和函数式编程的RDD API相比，DataFrame API提供的是一套高层的关系操作，从易用性的角度来看要DF要更加友好，门槛更低。

##### 5）弱类型

DataFrame是弱类型的数据集合，里面存放的类型只有在运行时才知道。



#### 和RDD编程的异同？

<img src="D:\ProgramFiles\Typora\图片备份\image-20200513101528357.png" alt="image-20200513101528357" style="zoom: 67%;" />

上图直观的体现了`DataFrame`和`RDD`的异同：

##### 1）异

1）数据集中包含的结构信息

> 左侧的`RDD[Person]`虽然以`person`为类型参数，但Spark框架本身不了解`Person`类的内部结构是什么，只有程序员自己才清楚。
>
> 而右侧的`DataFrame`却提供了详细的结构信息，这样SparkSQL可以清楚的知道该数据集中包含了哪些列以及每列的名称和类型各是什么。

2）`DataFrame`为数据集提供了`Schema`视图，可以将该视图当做数据库中的一张表来对待。

3）SparkSQL性能上比RDD高，原因如下：

> 优化的执行计划：查询计划通过Spark catalyst optimiser（优化器）进行优化。
>
> 如何优化？
>
> 比如说：
>
> 在一条查询语句中，有join和filter。如果原封不动的执行这个计划，最终的执行效率是不高的，因为join是一个代价比较大的操作，也可能会产生一个较大的数据集，如果能将filter先执行，先对数据集进行过滤，在join过滤后较小的数据集，便可以有效的缩短查询时间，提高效率。
>
> 结论如下：
>
> SparkSQL的查询优化器，正是这样做的。SparkSQL查询优化就是一个利用基于关系代数的等价变化，将查询语句中高成本的操作替换为低成本操作的过程。
>
> <img src="D:\ProgramFiles\Typora\图片备份\image-20200513102503289.png" alt="image-20200513102503289" style="zoom:67%;" />

从上图我们可以看出，对一条语句进行聚合操作时，使用RDD编程和使用DataFrame编程的效率。

- 当使用RDD的GroupByKey分组算子时，为红色柱子。时间最久，原因是GroupBy底层是没有开启预聚合的。

- 当使用RDD的ReduceByKey聚合算子时，为蓝色柱子。执行时间上明显减少了很多，原因是在ReduceByKey底层开启了预聚合。
- 当使用DataFrame时，为橙色柱子，执行时间上最少的，原因在于SparkSQL采用了查询优化器，会在内部对计算进行优化，将查询语句中的高成本操作替换为低成本操作，以达到优化效果。

##### 2）同

和RDD一样，DataFrame也是懒加载的，只有被触发才会执行。



### 5.3.3 DataSet编程

#### 什么是DataSet？

1）是`DataFrame API`的一个扩展，是SparkSQL最新的数据抽象（1.6新增）。

2）友好的API风格，即具有类型安全检查也具有`DataFrame`的查询优化特性。

3）`DataFrame`支持编解码器，当需要访问非堆上的数据时，可以避免序列化整个对象，提高效率。

> 什么是非堆上的数据？
>
> 堆外内存就是把内存对象分配在Java虚拟机的堆以外的内存，这些内存直接受操作系统管理（而不是虚拟机），这样做的结果就是能够在一定程度上减少垃圾回收对应用程序造成的影响。

4）样例类被用来在`DateSet`中定义数据的结构信息，样例类中每个属性的名称直接映射到`DataSet`中的字段名称。

5）`DataFrame`是`DataSet`的特列，`DataFrame = DataSet[Row]`，可以通过`as`方法将DataFrame转换为DataSet。`Row`是一个类型，跟`Car、Person`这些的类型一样，所有的表结构信息都用`Row`来表示。

6）DataSet是强类型的，比如可以有`DataSet[Car]`，`DataSet[Person]`

7）`DataFrame`只是知道字段，但是不知道字段的类型，所以在执行这些操作的时候是没有办法在编译时检查是否会类型失败。

> 比如对一个String进行减法操作，只有在执行这个操作的时候才会报错。

而DataSet不仅知道字段，而且知道字段类型，所有会有更严格的错误检查。

#### 什么是强类型？什么是弱类型？什么区别？

强类型和弱类型主要是站在变量类型处理的角度进行分类的。

强类型是指不允许隐式变量类型转换，弱类型则允许隐式类型转换。

所以，关键在于变量数据类型的转换。

##### 1）强类型

比如说，Java是强类型语言，不允许隐式转换，也就是说，如果你需要拿一个字符串变量来当做整形用，则必须要显示的将变量类型转换好。

简言之，强类型语言就是当你定义一个变量是某个类型以后，如果不经过代码显式转换或强制转换过，它就永远都是这个类型，如果把它当做其他类型来用，就会报错。

##### 2）弱类型

弱类型语言就是你想把这个变量当做什么类型来用就当做什么类型来用，语言的解析器会自动（隐式）转换。



##### 3）区别

- 开发效率角度

弱类型显然让开发者更省力一些，一些数据类型不是很复杂的场景中基本可以不用关注数据类型的问题，这可以提高开发者的业务处理专注力，提升逻辑开发效率。

- 数据安全性角度

但同样，弱类型也因为它的特性，使开发者在开发过程中对变量类型的检测力度不够大，由此提高数据类型方面出现问题的风险。

- 内存利用角度

弱类型语言的运行效率，内存利用率显然比不上强类型语言。因为弱类型语言在运行过程中，存在变量类型的隐式转换，多了一些需要执行的操作，并且，分配内存时，会考虑通用而多分配一些，而强类型则专门为各种类型的变量量身定做地分配内存，内存利用率显然比弱类型会高。

- 代码角度

强类型语言在编译时类型和运行时类型必须保持一致。

弱类型语言编译时类型和运行时类型可以进行各种转换，即弱类型语言的类型看最终的运行类型。



## 5.4 使用DataFrane编程

### 5.4.1 基本介绍

1）SparkSQL的`DataFrameAPI`允许我们使用`DataFrame`进行操作而不是必须去注册临时表或者生成SQL表达式。

2）`DataFrame API`即有转换操作也有行动操作，`DataFrame`的转换从本质上来说更具有关系，而`DataSet API`提供了更加函数式的API。

### 5.4.2 创建DataFrame的几种方式

#### 基本介绍

有了 **SparkSession** 之后, 通过 **SparkSession**来创建**DataFrame**：

#### 1）通过Spark数据源创建

##### 基本使用

在Spark-Shell界面操作：

```scala
// 读取 json 文件
scala> val df = spark.read.json("/opt/module/spark-local/examples/src/main/resources/employees.json")

df: org.apache.spark.sql.DataFrame = [name: string, salary: bigint]

// 展示结果
scala> df.show
```

##### 注意事项

① Spark支持的数据源包括如下：

> csv、jdbc、load、options、parquet、table、textFile、format、json、option、orc、schema、text

#### 2）通过已有RDD创建

##### 导入Spark SQL依赖

```xml
      <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>2.1.1</version>
        </dependency>
```



##### 基本使用

```scala
object RDD2DF {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()

    //通过Spark数据源得到DF
    /*
    val df = spark.read.json("d:/input/people.json")
    df.createOrReplaceTempView("people")
    spark.sql(
      """
        |select *
        |from people
        |""".stripMargin).show
        */

    // 2.将RDD转换成df
    val rdd = spark.sparkContext.parallelize(List((10, "a"), (20, "b"), (30, "c"), (40, "d")))
    //注：因为RDD要早于DF，所以在RDD中没有提供直接转换成DF的方法，需要手动导入SparkSession对象中的implicits对象
    import spark.implicits._
    val df: DataFrame = rdd.toDF("age", "name")
    df.printSchema()
    df.show()

    // 3.关闭
    spark.close()

  }
}
```

##### 注意事项

① 因为RDD要早于DF，所以在RDD中没有提供直接转换成DF的方法，需要手动导入SparkSession对象中的implicits对象中的方法。

```scala
object implicits extends SQLImplicits with Serializable {
  protected override def _sqlContext: SQLContext = SparkSession.this.sqlContext
}
```

#### 3）通过查询Hive表创建

##### 基本使用

```scala

```

#### 4）通过List集合创建

```scala
object RDD2DF {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()


    // 2.将集合转换成df
    val map = Map("a" -> 1, "b" -> 2)
    import spark.implicits._
    val df = map.toList.toDF()
    df.printSchema()
    

    // 3.关闭
    spark.close()

  }
}
```

#### 5）手动创建DF

```scala
object RDD2DFDemo5 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RDD2DFDemo5")
      .getOrCreate()

    val list = List(user(18, "张三"), user(22, "王五"), user(33, "李四"))
    //2.通过SparkSession中的SparkContext创建RDD
    val RDD = spark.sparkContext.makeRDD(list)

    val RDD1 = RDD
      .map {
        user => Row(user.age, user.name)
      }
    //fields: Array[StructField]
    val schema = StructType(List(StructField("age", IntegerType), StructField("name", StringType)))
    //手动创建df
    val df = spark.createDataFrame(RDD1, schema)
    df.show()


    //3.关闭资源
    spark.close()
  }

}
```



### 5.4.3 DataFrame语法风格

#### 1）SQL语法风格（主要使用）

##### 基本介绍

SQL语法风格是在查询数据时使用SQL语句来进行查询。

##### 基本使用

```scala
//创建DF对象
val df = spark.read.json("examples/src/main/resources/people.json")

//创建临时视图
df.createOrReplaceTempView("user")

//使用SQL语句进行查询
spark.sql("select * from user where salary >= 4500").show
```

##### 注意事项

① 使用SQL语法必须要有临时视图，或全局视图来辅助。

② 临时视图只在当前Session有效，在新的Session中无效。

> createOrReplaceTempView：如果当前要创建的视图已经存在，会覆盖原来的视图。
>
> createTempView：如果当前要创建的视图已经存在，不会覆盖，抛出异常。

③ 创建全局视图时，访问需要全路径

```scala
 //创建全局视图
 df.createGlobalTempView("people")
//访问全局视图
spark.sql("select * from global_temp.people").show
```

④ Spark SQL中的临时视图是会话作用域的，如果创建它的会话终止，它将消失。如果要在所有会话之间共享一个临时视图并保持活动状态，直到Spark应用程序终止，则可以创建全局临时视图。全局临时视图与系统保留的数据库相关联global_temp，我们必须使用限定名称来引用它，例如

```sql
SELECT * FROM global_temp.view1。
```

#### 2）DSL语法风格（了解）

##### 基本介绍

`DataFrame`提供一个特定领域语言(domain-specific language, DSL)去管理结构化的数据，可以在Scala、Java、Python和R中使用DSL。

使用DSL语法不需要创建临时视图。

##### 基本使用

```scala
//创建DF
val df = spark.read.json("examples/src/main/resources/people.json")

//查看schema信息
df.printSchema

//只查询name列的数据
df.select("name").show

//只查询name和age的数据
df.select("name","age").show

//查询name列和age列，且每个人的年龄+1
df.select($"name",$"age"+1).show

//查询age大于20的数据
df.filter($"age" > 20).show

//按照age分组，查看相同年龄的数据条数
df.groupBy("age").count.show
```

##### 注意事项

① 设计到运算的时候，每一列都必须要使用$

> ```sql
> df.select($"name",$"age"+1).show
> ```

② 通常使用SQL来进行DF编程，DSL语法使用的较少。

### 5.4.4 RDD和DF的相互转换

#### 1）RDD To DataFrame

##### 基本使用

案例一：RDD简单类型转DF

```scala
object RDD2DF {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()

    //通过Spark数据源得到DF
    /*
    val df = spark.read.json("d:/input/people.json")
    df.createOrReplaceTempView("people")
    spark.sql(
      """
        |select *
        |from people
        |""".stripMargin).show
        */

    // 2.将RDD转换成df
    val rdd = spark.sparkContext.parallelize(List((10, "a"), (20, "b"), (30, "c"), (40, "d")))
    //注：因为RDD要早于DF，所以在RDD中没有提供直接转换成DF的方法，需要手动导入SparkSession对象中的implicits对象
    import spark.implicits._
    val df: DataFrame = rdd.toDF("age", "name")
    df.printSchema()
    //创建视图
    df.createOrReplaceTempView("user")
    //使用SQL编程
    spark.sql(
      """
        |select *
        |from user
        |where age >= 20
        |""".stripMargin).show

    // 3.关闭
    spark.close()

  }
}
```



案例二：RDD自定义类型转DF

```scala
object RDD2DF2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    //注：SparkSession = SparkContext + HiveContext
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF2")
      .getOrCreate()


    // 2.将RDD转换成df
    val list = List(User("张三", 18), User("李四", 28), User("王五", 33))
    //创建RDD
    val RDD = spark
      .sparkContext
      .makeRDD(list, 2)
    //转换
    import spark.implicits._
    val df = RDD.toDF
    //打印schema
    df.printSchema()
    //打印表格
    df.show()

    // 3.关闭
    spark.close()

  }
}

case class User(name: String, age: Int)
```

##### 注意事项

① 样例类，转换成DF以后会自动获取对应属性的变量名为列名。

② 如果不是样例类, 需要手动指定列名`rdd.toDF(c1, c2, ...)`

#### 2）DataFrame To RDD

##### 基本使用

```scala
object DF2RDD {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()

    //通过Spark数据源得到DF
    val df = spark.read.json("d:/input/people.json")
    df.printSchema()
    // 2.将df转换成RDD
    //注：从df转成RDD之后，rdd内存储的是DF中的每一行的数据，类型为ROW
    //Row当成一个集合，表示一行数据，弱类型：只有在运行时才能确定类型。
    val RDD: RDD[Row] = df.rdd
    val RDD1 = RDD.map {
      row => {
        //从RDD中取数据的第一种方式

        //取出RDD中每一行的数据，索引0对应着第一列的字段，以此类推
        //row.isNullAt(index)：如果该列对应的数据为null则使用-1代替
        //        val age = if (row.isNullAt(0)) -1 else row.getLong(0)
        //        val name = row.getString(1)

        //从RDD中取数据的第二种方式
        //row.getAs[Int](fieldName)：该列的数据为Long类型(类名)
        //        val age = row.getAs[Double]("age")
        //        val name = row.getAs[String]("name")

        //从RDD中取数据的第三种方式
        val age = row.get(0)
        val name = row.get(1)
        (age, name)
      }
    }

    RDD1
      .collect()
      .foreach(println)
    //    RDD.foreach(println)

    // 3.关闭
    spark.close()

  }
}
```



##### 注意事项

① 在使用第一种方式取值时，如果某一列有null值，在不过滤的情况下，会抛出空指针异常。

> java.lang.NullPointerException: Value at index 0 is null

② 在使用第二种方式进行转换时，必须和schema中的数据类型保持一致。否则会抛出异常。

> java.lang.ClassCastException: java.lang.Long cannot be cast to java.lang.Double

③ 在使用第三种方式进行转换时，即使数值为null也不会抛出异常。



## 5.5 使用DataSet编程

### 5.5.1 基本介绍

1）DataSet和RDD类似，但是DataSet没有使用Java序列化或者Kryo序列化，而是使用一种专门的编码器去序列化对象，然后在网络上处理或者传输。

2）虽然编码器和标准序列化都负责将对象转换成字节，但编码器是动态生成的代码，使用的格式允许Spark执行许多操作，如过滤、排序和哈希，而无需将字节反序列化回对象。

3）DataSet是强类型的数据集合。

4）当DataSet中存放的是Row的时候，就是特殊的DataFrame，DF有的DS一定有

5）DS中一般存放的是样例类

### 5.5.2 创建DataSet的几种方式

#### 1）通过集合创建

```scala
object DSDemo1 {
  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DSDemo1")
      .getOrCreate()

    //2.方式一：通过List集合创建DS
    val list = Map("a" -> 1, "b" -> 2).toList
    import spark.implicits._
    val ds = list.toDS()
    ds.printSchema()
    ds.show

    //3.关闭资源
  }

}
```

#### 2）通过RDD创建

```scala
object DSDemo1 {
  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DSDemo1")
      .getOrCreate()

    //2.方式二：通过RDD创建DS
    val list = Map("a" -> 1, "b" -> 2).toList
    val RDD = spark.sparkContext.parallelize(list, 2)
    import spark.implicits._
    val ds = RDD.toDS
    ds.printSchema()
    ds.show

    

    //3.关闭资源
    spark.close
  }

}
```

#### 3）通过DF创建

```scala
object DSDemo1 {
  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DSDemo1")
      .getOrCreate()

    //2.方式三：通过DF创建DS
    //注：在读取json文件时，Spark通常会将数字默认读取为Long类型
    val df = spark.read.json("d:/input/people.json")
    import spark.implicits._
    //df转成ds也需要使用到SparkSession对象中的隐式转换方法
    val ds = df.as[user]
    ds.printSchema()
    ds.show



    //3.关闭资源
    spark.close
  }

}
```

### 5.5.3 注意事项

1）DataSet是强类型数据集合，即集合中的数据类型在编译时就必须确定，除非经过显式转换。

2）从DataFrame到DataSet需要有样例类。

3）DS到DF直接转即可。

4）Row提供了apply方法，可以通过Row()直接创建对象

5）Row中的所有数据都只能读不能写。

6）在实际使用时，更多是通过RDD来得到DataSet。

## 5.6 RDD、DataFrame和DataSet的联系

### 5.6.1 版本顺序

在SparkSQL中Spark为我们提供了两个新的抽象，分别是DataFrame和DataSet。他们和RDD的区别从版本上来看如下：

RDD(Spark1.0) -> DataFrame(Spark1.3) -> DataSet(Spark1.6)

> 同样的数据都给到三个数据结构，他们分别计算之后，都会给出相同的结果。不同的是执行效率，和执行的方式。
>
> 在后期Spark版本中，DataSet会逐步取代RDD和DataFrame成为唯一的API接口。
>
> DataSet是强类型数据集合，DataFrame是弱类型数据集合。

### 5.6.2 三者共性

#### 1）弹性数据集

> RDD、DataFrame、DataSet全都是Spark下的分布式弹性数据集，为处理超大型数据提供便利。

#### 2）惰性机制

> 三者都有惰性机制，在进行创建、转换，如map方法时，不会立即执行，只有在遇到Action如foreach时，三者才会开始加载执行操作。

#### 3）自动缓存运行

> 三者都会根据Spark的内存情况自动缓存运行，这样即使数据量很大，也不用担心会内存溢出。

#### 4）分区

> 三者都有分区（partition）的概念

#### 5）部分函数一致

> 三者有许多共同的函数，如map、filter等

#### 6）隐式方法转换

> 在DataFrame和DataSet进行操作时，许多的操作都需要`import spark.implicits._`这个对象的隐式方法来进行支持。

#### 7）模式匹配

> DataFrame和DataSet均可配合map等函数使用模式匹配来获取集合中的每一行数据匹配每个字段的值和类型。

```scala
    val df = spark.read.json("d:/input/people.json")
    df.printSchema()
    import spark.implicits._
    val df1 = df.map {
      case Row(age: Long, name: String) =>
        println(s"age = $age , name = $name")
        age
    }.show
```

### 5.6.3 三者区别

#### RDD

1）RDD一般和Spark机器学习同时使用

2）RDD不支持SparkSQL操作

#### DataFrame

1）与RDD、DataSet不同，DataFrame每一行的类型固定为Row类型，每一列的值没办法直接访问，只有在运行时才能解析获取到每个字段的值。

2）DataFrame与DataSet一般不与Spark机器学习同时使用。

3）DataFrame与DataSet均支持SparkSQL操作，比如`select,groupBy`之类，还能注册临时表/视图，进行SQL语句操作。

4）DataFrame与DataSet支持一些特别方便的保存方式，比如保存为CSV格式，可以带上表头，这样每一列的字段名一目了然。

#### DataSet

1）DataSet和DataFrame拥有完全相同的成员函数，区别只是每一行的数据类型不同。DataFrame其实就是DataSet的一个特例。

2）DataFrame也可以叫DataSet[Row]，每一行的类型是Row（弱类型），如果不进行解析那么每一行有多少字段以及各个字段的类型都无从得知，只能用到getAS方法或者使用模式匹配拿出特定字段。

### 5.6.4 三者之间的互相转换

#### RDD

> ```scala
> RDD.toDF
> RDD.toDS
> ```
>
> 通常会选择在RDD中封装样例类。

#### DataFrame

> ```scala
> DataFrame.rdd
> DataFrame.as[case class]
> ```
>
> 在DataFrame转换为DataSet时，通常会创建样例类，将DF中的数据封装为样例类转换为DS

#### DataSet

> ```scala
> DataSet.rdd
> DataSet.toDF
> ```





## 5.7 自定义函数

### 5.7.1 基本介绍

在Spark中可以通过`Spark.udf`功能自定义函数。

```
def register(name: String, func: Function1[A1, RT]): UserDefinedFunction
```



### 5.7.2 自定义UDF函数

```scala
package com.atguigu.spark.sql.day02.udf

import org.apache.spark.sql.SparkSession

/**
 * @Classname UDFDemo1
 * @Description TODO
 *              Date ${Date} 11:00
 * @Create by childwen
 */
object UDFDemo1 {
  
  //自定义UDF函数，需求转大写
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("UDFDemo1")
      .getOrCreate()
    
    //  注册UDF函数
    spark.udf.register("myToUpper", (str: String) => str.toLowerCase)
    
    //创建DF
    val df = spark.read.json("D:\\input\\people.json")
    df.createOrReplaceTempView("p")
    spark.sql(
      """
        |select myToUpper(name) name
        |from p
        |""".stripMargin).show()
    
    //关资源
    spark.close()

  }
}

```



### 5.7.3 自定义UDAF函数

**需求一：自定义UDAF函数求和**

```scala
package com.atguigu.spark.sql.day02.udaf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @Classname UDAFDemo1
 * @Description TODO
 *              Date ${Date} 11:16
 * @Create by childwen
 */
object UDAFDemo1 {
  def main(args: Array[String]): Unit = {
    //1 创建sparkSession
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("UDAFDemo1")
      .getOrCreate()
    //注册函数
    spark.udf.register("my_sum", new mySum)
    //2 创建DF
    //df在读Json时，一般都是Long类型
    val df = spark.read.json("D:\\input\\people.json")
    df.createOrReplaceTempView("p")
    spark.sql(
      """
        |select my_sum(age)
        |from p
        |""".stripMargin).show


    //3 关闭资源
    spark.close()
  }

}

class mySum extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("ele", LongType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("ele", LongType) :: Nil)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0L

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
    }

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Any = buffer(0)
}

```

**需求二：自定义UDAF函数求平均值**

```scala
package com.atguigu.spark.sql.day02.udaf

import java.text.DecimalFormat

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @Classname UDAFDemo1
 * @Description TODO
 *              Date ${Date} 11:16
 * @Create by childwen
 */
object UDAFDemo2 {
  def main(args: Array[String]): Unit = {
    //1 创建sparkSession
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("UDAFDemo2")
      .getOrCreate()
    //注册函数
    spark.udf.register("my_avg", new myAvg)
    //2 创建DF
    //df在读Json时，一般都是Long类型
    val df = spark.read.json("D:\\input\\people.json")
    df.createOrReplaceTempView("p")
    spark.sql(
      """
        |select my_avg(age)
        |from p
        |""".stripMargin).show


    //3 关闭资源
    spark.close()
  }

}

class myAvg extends UserDefinedAggregateFunction {
  //输入的元数据类型
  override def inputSchema: StructType = StructType(StructField("ele", LongType) :: StructField("count", LongType) :: Nil)

  //聚合缓冲区中的值类型
  override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

  //最终返回值类型
  override def dataType: DataType = StringType

  //确定性
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
    }
    buffer(1) = buffer.getLong(1) + 1L
  }

  //区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终的结果，因为是聚合函数，所以最后只有一行
  override def evaluate(buffer: Row): String = {

    val sum = buffer.getLong(0)
    val count = buffer.getLong(1)
    val avg = sum.toDouble / count
    val foo = new DecimalFormat(".00")
    val avgStr = foo.format(avg)
    avgStr

  }
}

```



### 5.7.4 注意事项

1）UDAF：一个分区只有一个buffer

2）UDF和UDAF只能用在弱类型语言中，即只能用在df上，在SQL语句中使用。

## 5.8 SparkSQL读写数据源

### 5.8.1 基本介绍

1）SparkSQL的DataFrame接口支持操作多种数据源，一个DataFrame类型的对象可以像RDD那样操作，也可以用来创建临时表，在临时表上执行SQL查询。

2）默认的数据源格式`parquet`，可以通过`spark.sql.sources.default`来设置默认的数据源。

3）Parquet 是一种流行的列式存储格式，可以高效地存储具有嵌套字段的记录。Parquet 格式经常在 Hadoop 生态圈中被使用，它也支持 Spark SQL 的全部数据类型。Spark SQL 提供了直接读取和存储 Parquet 格式文件的方法。

4）自定义UDF也可以支持多进一出，有重载方法。最多支持22个参数。

### 5.8.2 读数据

#### 基本介绍

通用读取数据的方式为

>  以Json格式为例
>
> `spark.read.format("json").load("FilePath")`

专用读取数据的方式为

> `spark.read.json("FilePath")`

读取文件内容直接运行SQL

> ```
> spark.sql("select * from json".`examples/src/main/resources/people.json` )
> ```
>
> 注：json表示文件的格式，后面的文件具体路径需要用反引号括起来。

#### 基本使用

##### 1）从数据源读数据

```scala
package com.atguigu.spark.sql.day02.datasource

import org.apache.spark.sql.SparkSession

/**
 * @Classname read
 * @Description TODO
 *              Date ${Date} 15:10
 * @Create by childwen
 */
object read {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("read")
      .getOrCreate()

    //读取数据源通用写法 spark.read.format(数据源格式).数据源(path)
//    val df = spark.read.format("csv").load("D:\\input\\csv.csv")

    //专用写法
    val df = spark.read.csv("D:\\output")
    df.show


    spark.close()
  }

}

```

##### 2）从JDBC读数据

```scala
package com.atguigu.spark.sql.day02.datasource

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * @Classname JDBCRead
 * @Description TODO
 *              Date ${Date} 15:38
 * @Create by childwen
 */
object JDBCRead {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("read")
      .getOrCreate()
    val url = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val psd = "root"
    val table = "emp"

    //通用方式：读取jdbc数据
    spark
      .read
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", psd)
      .option("dbtable", "a")
      .load()
      .createOrReplaceTempView("a")


    //专用方式：读取jdbc数据
    //本质底层也是调用了format("jdbc").load()
    val pro = new Properties
    pro.setProperty("user", user)
    pro.setProperty("password", psd)
    //读取JDBC数据，创建临时视图
    spark
      .read
      .jdbc(url, table, pro)
      .createOrReplaceTempView("emp")


    //执行SparkSQL查询视图中的数据
    spark
      .sql(
        """
          |select emp.*
          |from emp join a
          |on emp.id = a.id
          |""".stripMargin)
      .show
  }

}

```



#### 注意事项

1）正常情况下读取数据要执行SQL，需要先创建临时表。如果想在Sparksql中读取文件直接执行SQL，需要如下操作：

```scala
spark.sql("select * from json.`examples/src/main/resources/people.json`").show
```

2）推荐使用专用写法，本质底层还是调用了通用的格式去读取数据源。

3）SparkSQL支持的数据源有

> csv、jdbc、load、options、parquet、table、textFile、format、json、option、orc、schema、text

4）CSV：用逗号隔开的就是CSV格式。

5）读取文件时，文件的顺序不可控。

5）**Parquet**格式的文件是 Spark 默认格式的数据源.所以, 当使用通用的方式时可以直接保存和读取.而不需要使用**format**

### 5.8.3 写数据

#### 基本介绍

写数据也分两种，通用和专用，

通用写数据的方式为

> `df.write.firmat("json").save("FilePath")`

专用写数据的方式为

> `df.write.json("FilePath")`

在写的时候，如果路径已经存在，默认会抛出异常。

可以使用模式来控制：

| Scala/Java                          | Any Language         | Meaning                    |
| ----------------------------------- | -------------------- | -------------------------- |
| **SaveMode.ErrorIfExists**(default) | **"error"**(default) | 如果文件已经存在则抛出异常 |
| **SaveMode.Append**                 | **"append"**         | 如果文件已经存在则追加     |
| **SaveMode.Overwrite**              | **"overwrite"**      | 如果文件已经存在则覆盖     |
| **SaveMode.Ignore**                 | **"ignore"**         | 如果文件已经存在则忽略     |

#### 基本使用

##### 1）向文件中写数据

```scala
package com.atguigu.spark.sql.day02.datasource

import org.apache.spark.sql.SparkSession

/**
 * @Classname read
 * @Description TODO
 *              Date ${Date} 15:10
 * @Create by childwen
 */
object Write {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Write")
      .getOrCreate()

    //写
    val df = spark.read.json("D:\\input\\people.json")
    //通用写法
//    df.write.format("csv").save("D:\\output")
//专用写法
    /**
     * case "overwrite" => SaveMode.Overwrite 复写
     * case "append" => SaveMode.Append 追加
     * case "ignore" => SaveMode.Ignore 如果文件存在则忽略该文件
     * case "error" | "default" => SaveMode.ErrorIfExists 默认，为error 如果文件路径存在抛出异常
     */
    df.write.mode("append").json("D:\\output")

    spark.close()
  }

}

```



##### 2）向JDBC中写数据

```scala
package com.atguigu.spark.sql.day02.datasource

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @Classname JDBCWrite
 * @Description TODO
 *              Date ${Date} 16:11
 * @Create by childwen
 */
object JDBCWrite {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("JDBCWrite")
      .getOrCreate()

    val url = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val psd = "root"
    val table = "Spark1128"

    //JDBC写 通用形式
    //    spark
    //      .read
    //      .json("D:\\input\\people.json")
    //      .write
    //      .format("jdbc")
    //      .option("url",url)
    //      .option("user",user)
    //      .option("password",psd)
    //      .option("dbtable",table)
    //      .mode(SaveMode.Append)
    //      .save()

    //JDBC写 专用形式
    val pro = new Properties
    pro.setProperty("user", user)
    pro.setProperty("password", psd)
    spark
      .read
      .json("D:\\input\\people.json")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, table, pro)

  }

}

```



#### 注意事项

1）有一点很重要: 这些 SaveMode 都是没有加锁的, 也不是原子操作. 还有, 如果你执行的是 Overwrite 操作, **在写入新的数据之前会先删除旧的数据**

2）在设置mode时，除了直接填入字符串以外还可以使用saveMode枚举类对象来设置。

```
.mode(SaveMode.Overwrite)
```



## 5.9 Spark整合Hive

### 5.9.1 基本介绍



有两种整合方式:

1. `hive on spark`

   `hive on mr, hive on tez(hive 3.0开始, 默认)`

   是几家比较大的公司, 联合搞起来. 但是到目前, 用的人极少, 还是实验方案.

   `hive的版本和spark`的兼容性问题比较大

2. `spark on hive`(`spark`官方整出来)

   其实就是咱们讲的`spark-sql`

   我们使用的官方的版本, 内部已经编译进去了`hive`

### 5.9.2 配置Spark On Hive

#### 内置`hive`

- 默认是使用的内置`hive`

- 企业中一般不会使用这种内置`hive`

- 如果使用 Spark 内嵌的 Hive, 则什么都不用做, 直接使用即可.

  Hive 的元数据存储在 derby 中, 仓库地址:**$SPARK_HOME/spark-warehouse**

  在实际使用中, 几乎没有任何人会使用内置的 Hive

#### 接管外置的`hive`(重点)

数据都在`hive`上, `spark`还要找到`hive`

1）把`hive-site.xml`拷贝到`spark`配置目录下(目的: 找到`hive`元数据)

`tez`的配置去掉

2）因为我的hive默认元数据是存放在mysql中的，所以还需要拷贝`mysql`驱动到 **jars/**目录下.

> 如果还有报错: 有些情况下, 需要拷贝`core-site.xml, hdfs-site.xml`

3）如果hive是2.x以上的版本，需要启动元数据服务metastore

4）启动hadoop，启动spark-shell   

#### 注意事项

1）在使用spark-shell出现`Caused by: java.lang.ClassNotFoundException: Class com.hadoop.compression.lzo.LzoCodec not found`异常。

解决方案：

- 在hadoop的core.site.xml中去掉相关的配置

- 在spark-defaults.conf中指定一下lzo jar包的位置，添加如下配置：

  ```
  spark.jars=/opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar
  ```


2）如果没有部署外置Hive，SparkSQL会在当前工作目录中创建出自己的Hive元数据仓库，叫作`metastore_db`。

3）如果外置Hive修改了默认元数据存储为其他数据库，那我们还需要提供相应的连接驱动到`$SPARK_HOME/jars`目录下。

4）如果Hive没有配置文件系统，默认在SparkSQL中创建表时，这些表会放在默认的文件系统`/user/hive/warehourse`中，如果配置了文件系统为HDFS，则会存储在HDFS上。

### 5.9.3 Spark-sql工具

#### 基本介绍

在Spark-shell中执行hive查询每次都要使用`spark.sql("sql语句").show`比较麻烦，SparkSQL还提供了一种内置工具spark-sql，可以在界面中直接执行SQL语句。

#### 基本使用

##### 1）在外部执行SQL脚本

```
spark-sql -e "sql语句"
```

##### 2）启动脚本在内部执行SQL语句

```
bin/spark-sql
```

> 在yarn模式，需要指定master的为yarn模式

#### 注意事项

1）在执行时打印日志太多，可以修改log4j日志级别：category

2）使用spark-sql对hive脚本进行优化时，只需要将执行SQL语句的路径修改为spark-sql的路径即可。

> 例如`hive -e $sql`将hive修改为使用`spark-sql`脚本执行 

3）使用spark-yarn模式运行`spark-sql`一定要指定master为yarn模式

```
 bin/spark-sql --master yarn
```

4）在yarn模式，spark-sql，spark-shell中只支持depoly-mode 为client，默认情况下该模式为Client。



### 5.9.4 使用beeline连接hiveserver2

#### 基本介绍

hiveserver2本质就是用thriftserver写的，客户端通过thriftserver服务连接到集群。

#### 基本使用

启动thriftserver服务

```bash
sbin/start-thriftserver.sh \
--master yarn \
--hiveconf hive.server2.thrift.bind.host=hadoop102 \
-–hiveconf hive.server2.thrift.port=10000 \
```

启动beeline服务

```bash
bin/beeline
```

输入URL，用户，密码

```
!connect jdbc:hive2://hadoop102:10000
//输入用户名，用户名必须要正确
Enter username for jdbc:hive2://hadoop102:10000: atguigu
Enter password for jdbc:hive2://hadoop102:10000: ******
```

#### 注意事项

1）spark的thriftserver服务和hive中的hiveserver2服务共用同一个端口号。

2）如果想修改thriftserver服务的端口，只需要在启动thriftserver服务时指定端口号即可。

```
-–hiveconf hive.server2.thrift.port=10000
```



### 5.9.5 在IDEA中访问Hive

#### 基本配置

1）需要在SparkSession中追加`.enableHiveSupport()`

2）添加sparkhive依赖

```xml
<!--        Spark-hive驱动-->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.11</artifactId>
            <version>2.1.1</version>
        </dependency>
```

> 在linux下可以用的原因是因为我们下载的Jar包中已经包含了该依赖。

3）添加mysql驱动依赖

```xml
<!--mysql驱动-->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.27</version>
        </dependency>
```

4）将hive-site.xml文件放到resource路径下。

#### 注意事项

1）在IDEA中读取hadoop中的数据不存在权限问题，但是在修改时会出现用户权限问题，可以在IDEA代码中设置为有更改权限的用户：

```scala
System.setProperty("HADOOP_USER_NAME", "atguigu")
```



2）在开发工具中创建数据库默认是在本地仓库，

通过SparkSession对象中的配置参数修改数据库仓库的地址: 

```scala
.config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse")
```

3）在windows下读取LZO出错，原因是因为spark下的没有编译lzo，将core-site.xml中的lzo去掉，打包到linux下运行就好了。



### 5.9.6 在IDEA中操作Hive

#### 读的基本使用

通常读使用`spark-sqk`编写hivesql语句即可。

#### 写的基本使用

有三种方式可以向Hive中写数据，`saveAsTable`，`insertInto`，hivesql的`insert`语句

```scala
package com.atguigu.spark.sql.day03

import org.apache.spark.sql.SparkSession

/**
 * @Classname HiveDemo1
 * @Description TODO
 *              Date ${Date} 10:50
 * @Create by childwen
 */
object HiveDemo3 {
  def main(args: Array[String]): Unit = {
    //在连接hadoop时，修改一下系统用户名，防止因为修改数据而发生的权限问题
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    //在IDEA中连接hive
    //1.在sparkSession对象中添加支持hive方法 .enableHiveSupport()
    //2.添加spark-hive依赖
    //3.将hive-site.xml文件复制到module的resources目录下
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("HiveDemo3")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

        import spark.implicits._
    //使用savaAsTable insertinto hivesql-inser 三种方式插入数据
    spark.sql("use spark1128")
    //方式一：使用df.write.savaAsTable插入数据。当表格不存在时，会自动帮我们创建表格
    //使用该配置可以添加mode，设置写入模式
    //    spark
    //      .read
    //      .json("D:\\input\\people.json")
    //      .write
    //追加
    //      .mode(SaveMode.Append)
    //覆盖写，本质是先删表在写数据
    //      .mode(SaveMode.Overwrite)
    //      .saveAsTable("user")

    //使用svaAsTable，不需要关注列的顺序，只需要插入数据的字段和表中的字段相同，且类型一致即可。
        val list = List(("a", 10), ("b", 19))
    //表中的字段顺序是age，name。和我们插入的数据顺序是不一致的
        list

          .toDF("name","age")
          .write
          .mode("overwrite")
          .saveAsTable("user")

    //方式二：使用insertInto
    //使用insertInto时，类名是否一致不重要。但是插入数据中，每一列的数据类型必须要和表中的数据类型保持一致

//    val list = List((88, "cc"), (99, "dd"))
//    list
//      .toDF("我的列名不重要~", "类型一致即可")
//      .coalesce(1)
//      .write
//      .mode("append")
//      .insertInto("user")

    //方式三：使用hivesql的insert语句，当插入的数据和对应列的数据类型匹配不一致时，会使用null填充
    //        spark
    //          .sql(
    //            """
    //              |insert into table user values(99,'马云')
    //              |""".stripMargin)
    //          .show


  }

}

```



#### 注意事项

1）在linux下写完以后需要退出当前shell重新启动spark-shell才能查询到最新的数据。

2）write后可设置mode模式，和sparkSQL数据源的模式设置mode一样。

3）**使用saveAsTable和insertInto写入时的区别**

**saveAsTable**

>  列名：要求列名和表中的列名必须一样。

>  列名的顺序：插入列的顺序是否和表中列的顺序是否一致不重要。

**insertInto**

>  列名：列名是否相同不重要。

> 列名的顺序：插入列的数据类型必须和表中列的数据类型一一对应。

4）只要在SparkSQL中产生了shuffle就会将分区数设置为200。

>  在写入时，可以手动设置coalesce(1) 分区为1

>  或者在SparkSession中设置分区数

```
 .config("spark.sql.shuffle.partitions", 10)
```

5）write.mode(overwrite)

> 如果后面的跟的是`haveAsTable`会先删表，在重新创建一张表将数据存入表中
>
> 如果后面跟的是`insertInto`会先清空数据，再将数据插入到表中。

6）使用loaddata方式向hive中加载数据时，异常

```

scala> sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE user")
org.apache.spark.sql.AnalysisException: LOAD DATA is not supported for datasource tables: `spark1128`.`user`;
  at org.apache.spark.sql.execution.command.LoadDataCommand.run(tables.scala:194)
  at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:58)
  at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:56)
  at org.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(commands.scala:74)
```



# 六 Spark SQL项目实战

## 需求：***各区域热门商品 Top3***

## 需求简介

这里的热门商品是从点击量的维度来看的.

计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。

例如:

| 地区 | 商品名称 |      | 点击次数 | 城市备注                        |
| ---- | -------- | ---- | -------- | ------------------------------- |
| 华北 | 商品A    |      | 100000   | 北京21.2%，天津13.2%，其他65.6% |
| 华北 | 商品P    |      | 80200    | 北京63.0%，太原10%，其他27.0%   |
| 华北 | 商品M    |      | 40000    | 北京63.0%，太原10%，其他27.0%   |
| 东北 | 商品J    |      | 92000    | 大连28%，辽宁17.0%，其他 55.0%  |

## 具体实现

### APP层

```scala
package com.atguigu.spark.sparksql

import com.atguigu.spark.sparksql.util.RemarkUDAF
import org.apache.spark.sql.SparkSession

/**
 * @Classname Top3
 * @Description TODO
 *              Date ${Date} 20:28
 * @Create by childwen
 */
object Top3 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Top3")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()


    //注册函数
    spark.udf.register("remark", new RemarkUDAF)

    //切换数据库
    spark
      .sql("use spark1128")
      .show

    //需求：各区域热门商品 Top3
    //这里的热门商品是从点击量的维度来看的

    //1.联合查询得到地区，商品名称，城市表
    spark
      .sql(
        """
          |SELECT
          |area,product_name,city_name
          |FROM (
          |SELECT
          |product_name,city_id
          |FROM user_visit_action uva
          |JOIN product_info pi
          |on uva.click_product_id =  pi.product_id
          |)t1 join city_info ci
          |on t1.city_id = ci.city_id
          |""".stripMargin)
      .createOrReplaceTempView("t1")

    //2.通过t1表得到每一个地区每一件商品的点击次数
    // 使用自定义UDAF函数对每一个地区每一件商品的城市进行统计，最后返回一个字符串如：北京21.2%，天津13.2%，其他65.6%
    //每进来一组数据，都会重新执行一次UDAF函数
    spark
      .sql(
        """
          |select area,product_name,count(*) count,remark(city_name) remark
          |from t1
          |group by area,product_name
          |""".stripMargin)
      .createOrReplaceTempView("t2")

    //3.通过开窗排名函数，对同一个地区内的商品数进行倒序排序
    //rank：运行并列，并列后跳号 1,1,3,4
    //dense_rank：允许并列，并列不跳号 1,1,2,3
    //row_number：不并列，不跳号 1,2,3,4
    spark
      .sql(
        """
          |select area,product_name,count,rank() over(partition by area order by count desc) rank,remark
          |from t2
          |""".stripMargin)
      .createOrReplaceTempView("t3")

    //4.对开窗函数得到结果进行过滤，只取前3
    spark
      .sql(
        """
          |select area,product_name,count,remark
          |from t3
          |where rank <= 3
          |""".stripMargin)
      .show(100,false)

  }

}

```

### Bean层

```scala
package com.atguigu.spark.sparksql.bean

import java.text.DecimalFormat

/**
 * @Classname RemarkCity
 * @Description TODO
 *              Date ${Date} 21:32
 * @Create by childwen
 */
case class RemarkCity(city:String,rate:Double){
  val foo = new DecimalFormat(".00%")
  override def toString: String = s"$city${foo.format(rate)}"
}

```

### Util层

```scala
package com.atguigu.spark.sparksql.util

import com.atguigu.spark.sparksql.bean.RemarkCity
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
 * @Classname RemarkUDAF
 * @Description TODO
 *              Date ${Date} 21:01
 * @Create by childwen
 */
class RemarkUDAF extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))

  //缓冲区数据类型
  override def bufferSchema: StructType =
  //Map(城市,次数),地区总数
    StructType(Array(StructField("map", MapType(StringType, LongType)), StructField("count", LongType)))

  //输出数据类型
  override def dataType: DataType = StringType

  //确定性
  override def deterministic: Boolean = true

  //缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  //区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //使用模式匹配判断Row的类型
    input match {
      case Row(cityName: String) =>
        val map = buffer.getMap[String, Long](0)
        val count = buffer.getLong(1)
        //城市的总数
        buffer(1) = count + 1L
        //不同城市的个数
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      case _ =>
    }
  }

  //区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //将buffer2中的数据合并到buffer1中
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

    //map合并
    val map1 = buffer1.getMap[String, Long](0)
    val map2 = buffer2.getMap[String, Long](0)

    buffer1(0) = map1.foldLeft(map2) {
      case (map, (cityName, count)) =>
        map + (cityName -> (map.getOrElse(cityName, 0L) + count))
    }

  }

  //最终返回值 北京21.2%，天津13.2%，其他65.6%
  override def evaluate(buffer: Row): Any = {
    val resMap = buffer.getMap[String, Long](0)
    val resCount = buffer.getLong(1)
    //对结果map进行排序
    val resList = resMap
      .toList
      .sortBy(-_._2)
      .map {
        case (cityName, count) => RemarkCity(cityName, count.toDouble / resCount)
      }
      .take(2)
    val otherRate = resList.map(_.rate).sum
    val res = resList :+ RemarkCity("其他", otherRate)
    val str = res.mkString(",")
    str


  }
}

```

## 项目总结

1）碰到复杂的需求时，使用自定义UDF或UDAF函数来完成计算。

2）开窗排名函数

> rank：允许并列，并列后跳号  `1,1,3,4`
>
> denserank：运行并列，并列后不跳号 `1,1,3,4`
>
> rownumber：不并列，不跳号 `1,2,3,4`

注：在连续天数问题求解中，经常使用rownumber开窗函数做差求解。

## 经典Hive-SQL面试题

### 第一题

```
我们有如下的用户访问数据
    userId  visitDate   visitCount
    u01 2017/1/21   5
    u02 2017/1/23   6
    u03 2017/1/22   8
    u04 2017/1/20   3
    u01 2017/1/23   6
    u01 2017/2/21   8
    U02 2017/1/23   6
    U01 2017/2/22   4
要求使用SQL统计出每个用户的累积访问次数，如下表所示：
    用户id    月份  小计  累积
    u01 2017-01 11  11
    u01 2017-02 12  23
    u02 2017-01 12  12
    u03 2017-01 8   8
    u04 2017-01 3   3
```



```scala
package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession


/**
 * @Classname Test1
 * @Description TODO
 *              Date ${Date} 8:51
 * @Create by childwen
 */
object Test1 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[2]")
      .appName("Test1")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()
    //要求使用SQL统计出每个用户的累积访问次数
    spark.sql("use test_sql")
    //按用户和月份进行分组聚合
    spark
      .sql(
        """
          |SELECT userid,date_format(regexp_replace(visitdate,'/','-'),'yyyy-MM')visitdate,visitcount
          |FROM test1
          |""".stripMargin)
      .createOrReplaceTempView("t1")

    spark
      .sql(
        """
          |select userid,visitdate,sum(visitcount) visitcount
          |from t1
          |group by userid,visitdate
          |""".stripMargin)
      .createOrReplaceTempView("t2")

    //如果窗口中指定了partition by 那么窗口大小不能超过分区的范围
    //order by: 对分区或整个数据集中的数据按照某个字段进行排序，窗口大小默认等同于上午边界到当前行
    spark
      .sql(
        """
          |select userid,visitdate,sum(visitcount) over(partition by userid order by visitdate) visitcount
          |from t2
          |order by userid
          |""".stripMargin).show

  }
}

```

### 第二题

有50W个京东店铺，每个顾客访客访问任何一个店铺的任何一个商品时都会产生一条访问日志，
访问日志存储的表名为Visit，访客的用户id为user_id，被访问的店铺名称为shop，数据如下：

```
                u1  a
                u2  b
                u1  b
                u1  a
                u3  c
                u4  b
                u1  a
                u2  c
                u5  b
                u4  b
                u6  c
                u2  c
                u1  b
                u2  a
                u2  a
                u3  a
                u5  a
                u5  a
                u5  a
请统计：
(1)每个店铺的UV（访客数）
(2)每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
```

实现

```scala
package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession

/**
 * @Classname Test2
 * @Description TODO
 *              Date ${Date} 9:25
 * @Create by childwen
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[2]")
      .appName("Test2")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    spark.sql("use test_sql")

    //(1)每个店铺的UV（访客数）
    //先按店铺和用户分组 得到count
    //在按店铺分区，得到sumCount
    spark
      .sql(
        """
          |select shop,user_id,count(*) count
          |from test2
          |group by shop,user_id
          |""".stripMargin)
      .createOrReplaceTempView("t1")

    spark
      .sql(
        """
          |select shop,sum(count) sum_Count
          |from t1
          |group by shop
          |""".stripMargin)


    //(2)每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
    //按用户，店铺分组，求count
    //开窗，对count排名 这里按并列跳号计算
    //取top3
    spark
      .sql(
        """
          |select user_id,shop,count,rank(count) over(partition by shop order by count desc) rank
          |from t1
          |""".stripMargin)
      .createOrReplaceTempView("t2")

    spark
      .sql(
        """
          |select shop,user_id,count,rank
          |from t2
          |where rank <= 3
          |""".stripMargin).show

  }

}

```

### 第三题

需求：

```
已知一个表STG.ORDER，有如下字段:Date，Order_id，User_id，amount。
数据样例:2017-01-01,10029028,1000003251,33.57。
请给出sql进行统计:
(1)给出 2017年每个月的订单数、用户数、总成交金额。
(2)给出2017年11月的新客数(指在11月才有第一笔订单)
```

```
CREATE TABLE test_sql.test3 ( 
            dt string,
            order_id string, 
            user_id string, 
            amount DECIMAL ( 10, 2 ) )
ROW format delimited FIELDS TERMINATED BY '\t';
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','10029028','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','10029029','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','100290288','1000003252',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','10029088','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','100290281','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','100290282','1000003253',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-11-02','10290282','100003253',234);
INSERT INTO TABLE test_sql.test3 VALUES ('2018-11-02','10290284','100003243',234);
```

1 ) 给出 2017年每个月的订单数、用户数、总成交金额。

```scala
package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession

/**
 * @Classname Test3
 * @Description TODO
 *              Date ${Date} 10:56
 * @Create by childwen
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("Test3")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    //(1)给出 2017年每个月的订单数、用户数、总成交金额。
    spark.sql("use test_sql")
    spark
      .sql(
        """
          |select date_format(dt,'yyyy-MM')month,order_id,user_id,amount
          |from test3
          |where year(dt) = 2017
          |""".stripMargin)
      .createOrReplaceTempView("t1")

    spark
      .sql(
        """
          |select month,count(*)total_order,collect_set(user_id)users,sum(amount)total_amount
          |from t1
          |group by month
          |""".stripMargin)
      .createOrReplaceTempView("t2")

    spark
      .sql(
        """
          |select month,total_order,size(users) total_user,total_amount
          |from t2
          |""".stripMargin)
      .show

  }

}

```

2）给出2017年11月的新客数(指在11月才有第一笔订单，笨办法

```scala
package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession

/**
 * @Classname Test3
 * @Description TODO
 *              Date ${Date} 10:56
 * @Create by childwen
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("Test3")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    spark.sql("use test_sql")

    //(2)给出2017年11月的新客数(指在11月才有第一笔订单)
   //思路，找出11月之前的所有客户，和11月的客户进行left join，找出11月新增。
    spark
      .sql(
        """
          |select date_format(dt,'yyyy-MM')month,user_id
          |from test3
          |where year(dt) = 2017 and month(dt) < 11
          |""".stripMargin)
      .createOrReplaceTempView("t1")

    spark
      .sql(
        """
          |select date_format(dt,'yyyy-MM')month,user_id
          |from test3
          |where year(dt) = 2017 and month(dt) = 11
          |""".stripMargin)
      .createOrReplaceTempView("t2")

    spark
      .sql(
        """
          |select t2.month,t2.user_id
          |from
          |t2 left join t1
          |on t2.user_id = t1.user_id
          |where t1.user_id is null
          |""".stripMargin)
      .show

  }

}

```

第二种思路，按用户分组，对分完组以后的日期使用聚合函数min，找最小日期为11月的用户

```scala
package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession

/**
 * @Classname Test3
 * @Description TODO
 *              Date ${Date} 10:56
 * @Create by childwen
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("Test3")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    spark.sql("use test_sql")

    //(2)给出2017年11月的新客数(指在11月才有第一笔订单)
    //思路，按照所有客户进行分组，分组后进行二次过滤。筛选出每一个用户日期中最小值等于11月的满足条件
    spark
      .sql(
        """
          |select collect_set(user_id)
          |from test3
          |where year(dt) = 2017
          |group by user_id
          |having min(month(dt)) = 11
          |""".stripMargin)
      .show


  }

}

```

### 第四题

有一个5000万的用户文件(user_id，name，age)，一个2亿记录的用户看电影的记录文件(user_id，url)，根据年龄段观看电影的次数进行排序？    

```scala
package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession

/**
 * @Classname Test4
 * @Description TODO
 *              Date ${Date} 14:11
 * @Create by childwen
 */
object Test4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("Test4")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    spark.sql("use test_sql")
    //有一个5000万的用户文件(user_id，name，age)，
    // 一个2亿记录的用户看电影的记录文件(user_id，url)，根据年龄段观看电影的次数进行排序？
spark
      .sql(
        """
          |select user_id,count(*)view_count
          |from test4log
          |group by user_id
          |""".stripMargin)
      .createOrReplaceTempView("t1")

    spark
      .sql(
        """
          |select case
          |when age > 0 and age <= 10 then '0-10'
          |when age > 10 and age <= 20 then '10-20'
          |when age > 20 and age <= 30 then '20-30'
          |when age > 30 and age <= 40 then '30-40'
          |when age > 40 and age <= 50 then '40-50'
          |when age > 50 and age <= 60 then '50-60'
          |else '其他' end age,view_count
          |from test4user
          |join t1
          |on test4user.user_id = t1.user_id
          |""".stripMargin)
      .createOrReplaceTempView("t2")

    spark
      .sql(
        """
          |select age,sum(view_count)total_count
          |from t2
          |group by age
          |order by total_count desc
          |""".stripMargin)
      .show

  }

}

```

### 第五题

有日志如下，请写出代码求得所有用户和活跃用户的总数及平均年龄。（活跃用户指连续两天都有访问记录的用户）
日期 用户 年龄

```
2019-02-11,test_1,23
2019-02-11,test_2,19
2019-02-11,test_3,39
2019-02-11,test_1,23
2019-02-11,test_3,39
2019-02-11,test_1,23
2019-02-12,test_2,19
2019-02-13,test_1,23
2019-02-15,test_2,19
2019-02-16,test_2,19
```

```scala
package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession

/**
 * @Classname Test5
 * @Description TODO
 *              Date ${Date} 14:49
 * @Create by childwen
 */
object Test5 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("Test5")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    spark.sql("use test_sql")

    //有日志如下，请写出代码求得所有用户和活跃用户的总数及平均年龄。（活跃用户指连续两天都有访问记录的用户）
    //日期 用户 年龄
  spark
      .sql(
        """
          |select user_id,age
          |from test5
          |group by user_id,age
          |""".stripMargin)
      .createOrReplaceTempView("t1")

    spark
      .sql(
        """
          |select 1 a,count(*) total_count,avg(age)total_avg_age
          |from t1
          |""".stripMargin)
      .createOrReplaceTempView("t2")

    spark
      .sql(
        """
          |select *
          |from test5
          |group by dt,user_id,age
          |""".stripMargin)
      .createOrReplaceTempView("t3")

    spark
      .sql(
        """
          |select dt,user_id,age,row_number() over(partition by user_id order by dt)num
          |from t3
          |""".stripMargin)
      .createOrReplaceTempView("t4")

    spark
      .sql(
        """
          |select user_id,age,date_sub(dt,num)date_sub
          |from t4
          |""".stripMargin)
      .createOrReplaceTempView("t5")

    spark
      .sql(
        """
          |select user_id,avg(age)age
          |from t5
          |group by user_id,date_sub
          |having count(*) >= 2
          |""".stripMargin)
      .createOrReplaceTempView("t6")

    spark
      .sql(
        """
          |select 1 a,count(*) action_count,avg(age)action_avg_age
          |from t6
          |group by user_id
          |""".stripMargin)
      .createOrReplaceTempView("t7")

    spark
      .sql(
        """
          |select *
          |from t2 join t7
          |on t2.a = t7.a
          |""".stripMargin)
      .show

  }

}

```

### 第六题

请用sql写出所有用户中在今年10月份第一次购买商品的金额，
表ordertable字段:
(购买用户：userid，金额：money，购买时间：paymenttime(格式：2017-10-01)，订单id：orderid  

```scala
 package com.atguigu.spark.sparksql.exec

import org.apache.spark.sql.SparkSession

/**
 * @Classname Test6
 * @Description TODO
 *              Date ${Date} 16:24
 * @Create by childwen
 */
object Test6 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("Test6")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    spark.sql("use test_sql")

    //请用sql写出所有用户中在今年10月份第一次购买商品的金额，
    //表ordertable字段:
    //(购买用户：userid，金额：money，购买时间：paymenttime(格式：2017-10-01)，订单id：orderid

    spark
      .sql(
        """
          |select *,FIRST_VALUE(money) over(partition by userid,date_format(paymenttime,'yyyy-MM') order by paymenttime) first_money
          |from test6
          |where date_format(paymenttime,'yyyy-MM')='2017-10'
          |""".stripMargin)
      .show

  }

}

```



# 七 Spark Streaming 

## 7.1 什么是Spark Streaming ？

### 1）流式数据分析，计算

Spark Streaming是SPark核心API的扩展，用于构建弹性，高吞吐量，容错的在线数据流的流式处理程序，简言之：**SPark Streaming用于流式数据的处理。**

sparkStreaming定位就是做数据分析，计算的

### 2）对接多种数据源

Spark Streaming的数据可以来源于多种数据源：Kafka，Flume，Kinesis，或者TCP套接字。接收到的数据可以使用Spark的负责元语来处理，例如：`map，reduce，join和window`，最终，被处理的数据可以发布到FS，数据库或者在线dashboards。

工作中90%的数据都来自于kafka。

<img src="D:\ProgramFiles\Typora\图片备份\image-20200517205300769.png" alt="image-20200517205300769" style="zoom: 50%;" />



### 3）按批处理，按条采集

在Saprk Streaming中，处理数据的单位按批次处理，而不是按条处理。但是数据采集却是逐条进行的，因此Spark Streaming系统需要设置间隔使得数据汇总到一定的量后再一并操作，这个间隔就是批处理间隔。批处理间隔是Spark Streaming的核心概念和关键参数，它决定了Spark Streaming提交作业的频率和处理数据的延迟，同时也影响着数据处理的吞吐量和性能。

批处理，准实时。

低于秒级的处理很难，和集群的性能数据量有关系。

<img src="D:\ProgramFiles\Typora\图片备份\image-20200517210450379.png" alt="image-20200517210450379" style="zoom:67%;" />

### 4）DStream 连续数据流

Spark Streaming提供了一个高级抽象discretized stream(SStream)，DSteam 表示一个连续的数据流。

DStream可以由来自数据源的输入数据流创建，也可以通过在其他的DSrteam上应用一些高阶操作来得到。

流的底层还是RDD，假如三秒一个批次，那么每隔三秒就将这一批数据封装到一个RDD中。

分布式流，底层可以看成是由多个RDD组成，一个流就是一个RDD。



## 7.2 Spark Streaming有哪些特点？

### 1）易用

### 2）容错

### 3）易整合到Spark体系中

### 4）缺点

Spark Streaming 是一种“微量批处理”架构, 和其他基于“一次处理一条记录”架构的系统相比, 它的延迟会相对高一些.

真正的实时是来一条记录处理一条记录。

## 7.3 SparkStreaming 架构和被压机制

<img src="D:\ProgramFiles\Typora\图片备份\image-20200517211731236.png" alt="image-20200517211731236" style="zoom:67%;" />

1）接收器：专门用来接收数据。

> 整个集群中只有一个接收器。



#### 被压机制

接收器：Spark 1.5以前版本，用户如果要限制 Receiver 的数据接收速率，可以通过设置静态配制参数**spark.streaming.receiver.maxRate**的值来实现，此举虽然可以通过限制接收速率，来适配当前的处理能力，防止内存溢出，但也会引入其它问题

1、数据量很大集群处理不过来，接收器OOM

2、数据量很小集群资源浪费

最理想的情况是数据来了，当发现集群处理不过来时，接收器控制数据量来的慢一点达到平衡，这种平衡属于动态平衡，需要对速度做一个动态的调整。

> 到kafka的时候没有接收器，会有一种直连模式。

为了更好的协调数据接收速率与资源处理能力，1.5版本开始 Spark Streaming 可以动态控制数据接收速率来适配集群数据处理能力。

背压机制（即Spark Streaming Backpressure）: 根据 JobScheduler 反馈作业的执行信息来动态调整 Receiver 数据接收率。

通过属性**spark.streaming.backpressure.enabled**来控制是否启用backpressure机制，默认值**false**，即不启用。

## 7.4 基本使用

#### 7.4.1 Spark Streaming对接套接字数据源

需求：使用SparkStreaming读取套接字端口的数据，并做一个简单的wordCount。

添加SparkStreaming依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.11</artifactId>
    <version>2.1.1</version>
</dependency>
```

代码

```scala
package com.atguigu.spark.streaming.day01.selfstudy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname WordCount
 * @Description TODO
 *              Date ${Date} 21:24
 * @Create by childwen
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WordCount")

    //1.创建Spark Streaming上下文对象，传参：Spark配置文件，检查点时间：多少时间取一批数据。
    val sc = new StreamingContext(conf, Seconds(3))

    //2.从数据源获取数据，得到ReceiverInputDStream：接收器的输入流
    val stream: ReceiverInputDStream[String] = sc.socketTextStream("hadoop102", 9999)

        //通过高阶算子对输入流进行操作，得到DStream
        val DStream = stream
          .flatMap(_.split("\\s+"))
          .map((_, 1))
          .reduceByKey(_ + _)
        DStream.print(100)
    
    //3.启动Streaming
    sc.start()

    //4.阻塞线程，保证流所在的线程一直处在开启状态
    sc.awaitTermination()

  }

}

```

#### linux下netcat工具简单使用

下载nc工具：sudo yum -y install nc

使用：nc -lk 9999

## 7.5 注意事项

1）一旦StreamingContext已经启动，就不能在添加新的streaming computations（计算指令）

2）一旦一个StreamingContext已经停止，它就不能再重启。

```scala
    //3.启动Streaming
    sc.start()
    //关闭Streaming
    sc.stop()
    //注意：这时候不可以在启动了
    sc.start()
```

3）在同一个JVM中，同一时间只能启动一个StreamingContext。

4）使用`stop()`的方式停止StreamingContext，会把SparkContext也停掉。如果仅仅想停止StreamingContext，可以使用`stop(false)`

5）一个SparkContext可以重用去创建多个StreamingContext，前提是以前的StreamingContext使用`stop(false)`已经被停掉，并且SparkContext没有被停止。

```scala
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WordCount")

    //1.创建Spark Streaming上下文对象，传参：Spark配置文件，检查点时间：多少时间取一批数据。
    val sc: StreamingContext = new StreamingContext(conf, Seconds(3))
```



## 7.6 DStream的几种创建方式

### 7.6.1 通过socket创建

如上述案例所示。

### 7.6.2 通过RDD队列创建

#### 基本介绍

一般用于压力测试，测试集群的计算能力。

#### 基本使用

```scala
package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @Classname RDDQueue
 * @Description TODO
 *              Date ${Date} 11:31
 * @Create by childwen
 */
object RDDQueue {
  def main(args: Array[String]): Unit = {
    /**
     * _sc: SparkContext,
     * _cp: Checkpoint,
     * _batchDur: Duration
     */
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDQueue")
    //1.创建Streaming对象
    val sc = new StreamingContext(conf, Seconds(3))
    //2.使用streaming对象从数据源读取数据
    /**
     * def queueStream[T: ClassTag](
     * queue: Queue[RDD[T]],
     * oneAtATime: Boolean = true 一次取一次，默认是true
     * )
     */
    val queue = new mutable.Queue[RDD[Int]]()

    val stream = sc.queueStream(queue)

    val resDS = stream.reduce(_ + _)
    resDS.print(100)
    //3.启动Streaming对象
    sc.start()

    while(true){
      val RDD: RDD[Int] = sc.sparkContext.parallelize(1 to 100)
      queue.enqueue(RDD)
      println(queue.size)
      Thread.sleep(2000)
    }

    //4.阻塞主线程，保证streaming一直运行
    sc.awaitTermination()
  }

}

```



### 7.6.3 自定义数据源

#### 基本介绍

自定义数据源的本质就是自定义接收器，需要继承Receiver，实现onStart、Onstop方法。

#### 基本使用

需求：自定义数据源，实现监控某个端口号，获取该端口的内容。

**接收器代码**

```scala
package com.atguigu.spark.streaming.day01.selfstudy


import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * @Classname MySource
 * @Description TODO
 *              Date ${Date} 21:58
 * @Create by childwen
 */

//abstract class Receiver[T](val storageLevel: StorageLevel) extends Serializable
//自定义类继承Receiver：
// 泛型类型为接收器数据源的类型。
//传参为存储级别：系统默认仅存储在内存中。
case class MySource(host: String, port: Int) extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY) {
  var socket: Socket = _
  var reader: BufferedReader = _

  //1)接收器在启动时会调用该方法，
  //2)该方法内部必须初始化一些读取数据必须的资源
  //3)该方法不能阻塞，所以读取数据要在一个新的线程中进行。
  //为什么需要在一个新的线程中进行？
  //答：因为一直在该方法中读取数据，会导致该方法阻塞。
  override def onStart(): Unit = {
    //在子线程中写读取数据的代码
    try {
      newThread {
        // 从socket读数据
        socket = new Socket(host, port)
        reader =
          new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
        //读取的每一行数据
        var line = reader.readLine()

        // 表示读到了数据
        while (line != null) {
          store(line)// 将来spark'会分发其他的executor进行处理
          line = reader.readLine() // 如果没有数据, 这里会阻塞, 等待数据的输入
        }
      }
    } catch {
      case e => println(e.getMessage)
    }
    finally {
      // 先回调onStop, 再回调 onStart
      restart("重启任务")
    }
  }

  //该方法用来关闭资源
  override def onStop(): Unit = {
    if (reader != null) reader.close()
    if (socket != null) socket.close()
  }

  //定义一个新的子线程来接收数据
  //该方法传入一个名调用，该名调用返回值为unit
  def newThread(foo: => Unit): Unit = {
    new Thread() {
      override def run(): Unit = {
        //在子线程的run方法中执行名调用
        foo
      }
    }.start()
  }

}

```

**streaming代码**

```scala
package com.atguigu.spark.streaming.day01.selfstudy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname MySoureceTest
 * @Description TODO
 *              Date ${Date} 22:15
 * @Create by childwen
 */
object MySoureceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MySoureceTest")
      .setMaster("local[2]")

    val sc = new StreamingContext(conf, Seconds(3))

    val stream = sc.receiverStream(new MySource("hadoop102", 9999))

    stream.print(100)

    sc.start()

    sc.awaitTermination()


  }

}

```



#### 注意事项

1）所有在网络上进行传输的数据都是字节。

2）在定义类继承接收器时，需要指定接收器的泛型类为数据源的类型，参数列表中传入存储级别。

3）接收器在内部会调用onstart方法，在onstart内部必须初始化读取数据资源的方法。

4）需要在onstart方法中读取数据，但是该方法又不能阻塞，所以读取数据需要在一个新的线程中进行。

```scala
  //定义一个新的子线程来接收数据
  //该方法传入一个名调用，该名调用返回值为unit
  def newThread(foo: => Unit): Unit = {
    new Thread() {
      override def run(): Unit = {
        //在子线程的run方法中执行名调用
        foo
      }
    }.start()
  }
```



5）restart方法的执行顺序：先回调onStop, 再回调 onStart。该方法一般用在tryCatch中当程序运行遇到异常时，用来重启接收器使用。

6）store方法的作用是将读取的数据分发到其他的executor上进行处理。

7）onstop方法的作用是关闭资源。

### 7.6.4 通过kafka创建（重点）

#### 基本介绍

针对Kafka数据源，SparkStreaming支持两种数据获取方式。

① Receiver + kafka API 高阶函数

② 直连模式

直连模式在Spark1.3版本开始出现，较前者有以下3点优势：

> 1）简化并行处理：直连模式下，SparkStreaming端RDD的分区数与Kafka分区数量直接挂钩，各个分区之间形成一对一映射，可以保证并行读取数据，如果想提高并行度只需要增加Kafka的分区数即可。
>
> 2）高效：为了防止数据丢失，采用第一种Receiver 接收器方式需要预写日志（Write Ahead Log），效率大打折扣。而第二种直连方式，只需要Kafka中还保留有数据，SparkStreaming就可以从Kafka中重新读取丢失的数据，避免了WAL产生二次复制的情况。
>
> 3）*Exactly-once* 语义：第一种方式，使用Kafka的旧API从Zookeeper端获取偏移量数据，如果发生错误，容易出现多次消费的问题。第二种方式使用Kafka的新API直接从Kafka获取偏移量数据进行checkPoint，如果发生错误，SparkStreaming可以从checkPoint中恢复偏移量，以此实现精准一次语义，需要注意的是，SparkStreaming直连模式只是做到了消费时的严格一次，如何向输出端也达到严格一次需要开发者自己保证：输出系统是幂等或者输出系统支持事务。

三个语义:

1. 至多一次(高效, 数据丢失)
2. 正好一次(最理想. 额外的很多工作, 效率最低)
3. 至少一次(保证数据不丢失, 数据重复)

注：生产者可以自动创建 topic，分区数为 /opt/module/kafka/config/server.properties 中定义的默认分区数。

#### Receiver 接收器模式基本使用

添加依赖

```xml
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
        <version>2.1.1</version>
    </dependency>
```

代码

```scala
package com.atguigu.spark.streaming.day01.selfstudy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname ReceiverApi
 * @Description TODO
 *              Date ${Date} 23:11
 * @Create by childwen
 */
object ReceiverApi {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ReceiverApi")

    val sc = new StreamingContext(conf, Seconds(3))

    /**
     * 参数列表
     * ssc: StreamingContext, StreamingContext上下文对象
     * zkQuorum: String, zookeeper中kafka的地址
     * groupId: String, 消费者组ID
     * topics: Map[String, Int], 从kafka的topic到DStream流RDD序列中分区的映射。每个分区在自己的线程中消耗数据。
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     */
    //关于返回值
    // (k, v)  k默认是 null, v 才是真正的数据
    // key的用处: 决定数据的分区. 如果key null, 轮询的分区
    val sourceStream: ReceiverInputDStream[(String, String)] =
    KafkaUtils.createStream(
      sc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181/mykafka",
      "atguigu",
      Map("spark1128" -> 2)
    )
    sourceStream
      //取出真正的数据
      .map(_._2)
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    sc.start()

    sc.awaitTermination()


  }

}

```

#### 0.8 版本Direct 直连模式

##### 1）Direct 直连模式基本使用

```scala
package com.atguigu.spark.streaming.day01.selfstudy

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname DirectMode
 * @Description TODO
 *              Date ${Date} 23:35
 * @Create by childwen
 */
object DirectMode {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DirectMode")
      .setMaster("local[*]")

    val sc = new StreamingContext(conf, Seconds(3))

    /**
     * 参数解读
     * def createDirectStream[
     * K: ClassTag,
     * V: ClassTag,
     * KD <: Decoder[K]: ClassTag,
     * VD <: Decoder[V]: ClassTag] (
     * ssc: StreamingContext, ： SparkStreaming对象
     *
     * kafkaParams: Map[String, String]：需要使用以host1：port1，host2：port2格式指定的Kafka代理（非Zookeeper服务器）
     * 设置“ metadata.broker.list”或“ bootstrap.servers”。
     * 如果不是从检查点开始，则“ auto.offset.reset”可以设置为“最大”或“最小” 确定流从何处开始（默认为“最大”）
     * 设置group.id : 消费者组ID
     *
     * topics: Set[String] : 读取数据的主题
     * )
     */
    val params = Map[String, String](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "group.id" -> "childWen"
        //通过ConsumerConfig对象获取
 //         ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
   // ConsumerConfig.GROUP_ID_CONFIG -> "childWen"
    )
    val topics = Set[String]("spark1128")

    //直连模式必须指定解码器的泛型类型，如果不指定无法推断返回值类型
    // 解码器与反序列化作用类似，但解码器效率比反序列化要高
    val stream = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](
      sc,
      params,
      topics
    )

    stream
      .map(_._2)
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    sc.start()

    sc.awaitTermination()

  }

}

```

##### 2）Direct 直连 + checkPoint基本使用

```scala
package com.atguigu.spark.streaming.day01.selfstudy

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname DirectMode
 * @Description TODO
 *              Date ${Date} 23:35
 * @Create by childwen
 */
object DirectMode {
  def main(args: Array[String]): Unit = {
    /*
      def getActiveOrCreate(
      checkpointPath: String, 检查点路径
      creatingFunc: () => StreamingContext, 该函数创建一个StreamingContext
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
    )
    从checkpoint中恢复一个StreamingContext,
    如果checkpoint不存在, 则调用后面的函数去创建一个StreamingContext
     */
    val sc = StreamingContext.getActiveOrCreate("ck2", createSc _)
    sc.start()
    sc.awaitTermination()
  }

  def createSc() = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("createSc")

    val sc = new StreamingContext(conf, Seconds(3))
    
    //设置检查点
    sc.checkpoint("./ck2")
    
    val parms = Map[String, String](
      "bootstrap.servers" -> "hadoop102:9092",
      "group.id" -> "hello"
    )
    val topics = Set("spark1128")
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      sc,
      parms,
      topics
    )

    stream
      .map(_._2)
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    sc

  }

}

```

#### 0.10 版本Direct 直连模式

##### 基本介绍

平均分布：将kafka分区平均分布交给executor，适用于kafka单独配置的情况。

偏好brokers：适用于当kafka配置在集群中的情况，可以节省IO开销。

##### 基本使用

添加依赖

```xml
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming-kafka-0-10_2.11</artifactId>
        <version>2.1.1</version>
    </dependency>
```

代码

```scala
package com.atguigu.spark.streaming


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Classname DirectKfka0_11
 * @Description TODO
 *              Date ${Date} 9:41
 * @Create by childwen
 */
object DirectKafka0_11 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("DirectKafka0_11")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    /**
     * 参数列表
      ssc: StreamingContext,
      locationStrategy: LocationStrategy, 分区策略
      consumerStrategy: ConsumerStrategy[K, V] 消费者策略
     */


    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer], //key的反序列化器
      "value.deserializer" -> classOf[StringDeserializer], //value的反序列化器
      "group.id" -> "bigdata", //消费者组id
      "auto.offset.reset" -> "latest", //每次从最新的位置开始读
      "enable.auto.commit" -> (true: java.lang.Boolean) //自动提交kafka的offset
    )

    val topics = Array("spark1128")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,//平均分布分区给spark的executor
      Subscribe[String, String](topics, kafkaParams)
    )

    stream
      .map(_.value())
      .print()

    ssc.start()

    ssc.awaitTermination()
  }
}

```

##### 注意事项

![image-20200518100147638](D:\ProgramFiles\Typora\图片备份\image-20200518100147638.png)

位置策略:

1. `PreferConsistent` 大多数用这个
2. `PreferBrokers` `executor`和`broker`在同一个设备
3. `PreferFixed`  当数据发生严重倾斜的时候

#### 注意事项

**直连模式**

1）直接找broker，跟zk没有关系了。

2）该流不使用接收器，直接查询Kafka的偏移量，不需要使用Zookeeper的偏移量，流本身跟踪消耗的偏移量。

3）要从程序故障中恢复上次读取的偏移量，必须在StreamingContext中启用检查点，可以从检查点中恢复有关消耗的偏移量信息。

4）消费者组，没有的话会自己创建。

5）输出端语义：此流确保每个记录都被有效地接收并精确地转换一次，但是不能保证转换后的数据是否被精确地输出一次。对于输出端的精确一次语义，您必须确保输出操作是幂等的，或者使用事务原子地输出记录。





## 7.7 DStream转换算子操作

### 7.7.1 基本介绍

DStream与RDD类似，分为Transformations（转换）和output Operations（输出）两种，此外转换操作中还有一些比较特殊的算子。如：*updateStateByKey()*、*transform()*以及各种*window*相关的算子。

| Transformation                                  | Meaning                                                      |
| ----------------------------------------------- | ------------------------------------------------------------ |
| ***\*map\****(**func**)                         | Return a new DStream by passing each element of the source DStream through a function **func**. |
| flatMap(**func**)                               | Similar to map, but each input item can be mapped to 0 or more output items. |
| ***\*filter\****(**func**)                      | Return a new DStream by selecting only the records of the source DStream on which **func** returns true. |
| ***\*repartition\****(**numPartitions**)        | Changes the level of parallelism in this DStream by creating more or fewer partitions. |
| ***\*union\****(otherStream)                    | Return a new DStream that contains the union of the elements in the source DStream and **otherDStream**. |
| count()                                         | Return a new DStream of single-element RDDs by counting the number of elements in each RDD of the source DStream. |
| ***\*reduce\****(**func**)                      | Return a new DStream of single-element RDDs by aggregating the elements in each RDD of the source DStream using a function **func** (which takes two arguments and returns one). The function should be associative and commutative so that it can be computed in parallel. |
| countByValue()                                  | When called on a DStream of elements of type K, return a new DStream of (K, Long) pairs where the value of each key is its frequency in each RDD of the source DStream. |
| ***\*reduceByKey\****(**func**, [**numTasks**]) | When called on a DStream of (K, V) pairs, return a new DStream of (K, V) pairs where the values for each key are aggregated using the given reduce function. ***\*Note:\**** By default, this uses Spark’s default number of parallel tasks (2 for local mode, and in cluster mode the number is determined by the config property **spark.default.parallelism**) to do the grouping. You can pass an optional **numTasks** argument to set a different number of tasks. |
| ***\*join\****(otherStream, [numTasks])         | When called on two DStreams of (K, V) and (K, W) pairs, return a new DStream of (K, (V, W)) pairs with all pairs of elements for each key. |
| ***\*cogroup\****(otherStream, [numTasks])      | When called on a DStream of (K, V) and (K, W) pairs, return a new DStream of (K, Seq[V], Seq[W]) tuples. |
| transform(**func**)                             | Return a new DStream by applying a RDD-to-RDD function to every RDD of the source DStream. This can be used to do arbitrary RDD operations on the DStream. |
| updateStateByKey(**func**)                      | Return a new “state” DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values for the key. This can be used to maintain arbitrary state data for each key. |

### 7.7.2 无状态转换 

#### 基本介绍

1）无状态转换操作就是把简单的RDD转换操作应用到每个批次上，也就是转化DStream中的每一个RDD。

部分无状态转换操作如下表：

![image-20200518193323118](D:\ProgramFiles\Typora\图片备份\image-20200518193323118.png)

2）实际上每个DStream在内部都是由许多RDD（批次）组成，尽管这些函数看起来像是作用在整个流上，但事实上无状态转换操作是分别应用在每个RDD上的。例如`reduceByKey()`会简化每个时间区间中的数据，但是不会简化不同分区之间的数据。



#### transfrom算子

##### 基本介绍

**transform** 原语允许 **DStream**上执行任意的**RDD-to-RDD**函数。

可以用来执行一些 RDD 操作, 即使这些操作并没有在 SparkStreaming 中暴露出来.

该函数每一批次调度一次。其实也就是对**DStream**中的**RDD**应用转换。

##### 基本使用

```scala
package com.atguigu.spark.streaming.day02.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname TransfromDemo
 * @Description TODO
 *              Date ${Date} 10:16
 * @Create by childwen
 */
object TransfromDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("TransfromDemo")
      .setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val stream = ssc.socketTextStream("hadoop102", 9999)

    stream
      //调用transform无状态转换算子，对进来的每个批次的RDD执行一次代码
      .transform(
        rdd => rdd
          .flatMap(_.split("\\W+"))
          .map((_, 1))
          .reduceByKey(_ + _)
      )
        .print

    ssc.start()

    ssc.awaitTermination()
  }


}

```

##### 注意事项

1）该算子是转换算子，返回值一个DStream对象。不建议在转换算子中做行动操作。

2）transfrom转换算子一般用在当流的算子不够丰富无法解决当前需求时，可以转换成RDD，然后操作RDD来完成需求。

#### 注意事项

1）无状态转换仅仅针对当前批次，批次与批次之间没有关系。

2）针对聚合算子来说，只要是用的和RDD一样的算子都是无状态算子。

### 7.7.3 有状态转换

#### 基本介绍

有状态转换操作，类似于累积型快照事实表。在更新状态的同时还可以保留历史状态，需要设置检查点。

#### updateStateByKey算子

##### 基本使用

```scala
package com.atguigu.spark.streaming.day02.transform.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Classname UpDateStateBykey
 * @Description TODO
 *              Date ${Date} 10:54
 * @Create by childwen
 */
object UpDateStateBykey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UpDateStateBykey").setMaster("local[2]")
    //创建stream
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点
    ssc.checkpoint("D:\\ck1")

    val stream = ssc.socketTextStream("hadoop102", 9999)

    /*
     * def updateStateByKey[S: ClassTag](
     * updateFunc: (Seq[V], Option[S]) => Option[S]
     * )
     * updateStateByKey操作允许在使用新信息不断更新状态的同时能够保留他的状态.
     *定义状态更新函数. 指定一个函数, 这个函数负责使用以前的状态和新值来更新状态.
     * seq中存储的就是以前的状态，option中存储的为新值。
     * 在每个阶段, Spark 都会在所有已经存在的 key 上使用状态更新函数, 而不管是否有新的数据在.
     */
    val resDStream = stream
      .flatMap(_.split("\\W+"))
      .map((_, 1))
      .updateStateByKey((seq: Seq[Int], option: Option[Int]) => Some(seq.sum + option.getOrElse(0)))

    resDStream.print

    ssc.start()

    ssc.awaitTermination()
  }

}

```



##### 注意事项

1）updateStateByKey操作允许在使用新信息不断更新状态的同时能够保留他的状态.

2）seq中存储的就是以前的状态，option中存储的为新值。

3）在每个阶段, Spark 都会在所有已经存在的 key 上使用状态更新函数, 而不管是否有新的数据在.

> ![image-20200518200538359](D:\ProgramFiles\Typora\图片备份\image-20200518200538359.png)



4）该算子的作用可以理解为用来替换无状态的聚合函数。

#### 窗口转换window算子

##### 基本介绍

1）SparkStreaming也提供有窗口计算，允许转换操作在一个窗口内执行。

2）默认情况下，计算只对一个时间段内的RDD进行，有了窗口之后，可以把计算应用到一个指定窗口内的所有RDD上。

3）窗口在DStream上每滑动一次，**落在窗口内的那些RDD会结合在一起，然后在上面操作产生新的RDD，组成Window DStream。**

![image-20200518205459176](D:\ProgramFiles\Typora\图片备份\image-20200518205459176.png)

> 窗口长度 – 窗口的持久时间(执行一次持续多少个时间单位)(图中是 3)
>
> 滑动步长 – 窗口操作被执行的间隔(每多少个时间单位执行一次).(图中是 2 )
>
> 注意: 这两个参数必须是源 DStream 的 interval 的倍数。

##### reduceByKeyAndWindow基本使用

```scala
package com.atguigu.spark.streaming.day02.transform.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname ReduceByKeyAndWindow
 * @Description TODO
 *              Date ${Date} 11:51
 * @Create by childwen
 */
object ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("ReduceByKeyAndWindow")
      .setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //检查点
    ssc.checkpoint("D:\\window")
    val stream = ssc.socketTextStream("hadoop102", 9999)

    /**
     * def reduceByKeyAndWindow(
     * reduceFunc: (V, V) => V, 聚合函数
     * invReduceFunc: (V, V) => V, 优化函数
     * windowDuration: Duration, 窗口大小
     * slideDuration: Duration, 滑动步长
     * partitioner: Partitioner, 分区器
     * filterFunc: ((K, V)) => Boolean 过滤
     * )
     */
    stream
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      //命名函数，窗口大小9，滑动步长6
      .reduceByKeyAndWindow(_ + _, (pre, now) => pre - now, Seconds(9), Seconds(6), filterFunc = _._2 > 0)
      .print

    ssc.start()

    ssc.awaitTermination()

  }

}

```

##### window基本使用

基于对源 DStream 窗化的批次进行计算返回一个新的 Dstream

```scala
package com.atguigu.spark.streaming.day02.transform.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname WindowDemo
 * @Description TODO
 *              Date ${Date} 20:56
 * @Create by childwen
 */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("WindowDemo")
      .setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val stream = ssc.socketTextStream("hadoop102", 9999)

    stream
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .window(Seconds(9))
      .reduceByKey(_ + _)
      .print

    ssc.start()

    ssc.awaitTermination()
  }

}

```

##### countByWindow

返回一个滑动窗口计数流中元素的个数



##### 注意事项

1）窗口一般都是涉及到聚合操作时，才会使用。

2）管道式传输使用窗口没意义。

3）窗口长度必须是批次窗口的整数倍。

4）invReduceFunc优化窗口计算：只有窗口和窗口之间有重叠才有必要优化。

5）创建流时直接加window没办法优化重复运算。

6）**刚开始启动滑窗时，如果没有达到窗口长度，左侧的窗体不会滑动也就是只有新增数据。直到窗体长度达到阈值。左侧的窗体才会开始滑动**

7）文档开篇时说的SparkStreaming可以看做是一个RDD序列，应该是站在整个数据链角度来看的对吗？实际上我们处理起来，不管是无状态转换还是有状态转，最终都是在操作一个RDD，区别在于窗口的转换是把一个时间段内多个批次的DStream合并变成一个DStream来处理，而无状态只是针对一个批次来处理。

### 7.7.4 有状态和无状态的区别

有状态可以理解为，转换算子保存了历史状态和最新状态。而无状态则不会保留历史状态，如map，filter等RDD相似的算子都是无状态算子。

## 7.8 DStream输出算子

### 7.8.1 基本介绍

SparkStreaming输出操作指定了对数据经转化操作得到的数据要执行的操作，例如把结果导入到外部数据或输出到屏幕上。

与RDD的惰性求值类似，如果一个DStream及其派生出的DStream都没有被执行输出操作，那么这些DStream就都不会被求值，如果StreamingContext中没有设定输出操作，整个context就不会启动。

| Output Operation                                      | Meaning                                                      |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| print()                                               | Prints the first ten elements of every batch of data in a DStream on the driver node running the streaming application. This is useful for development and debugging. ***\*Python API\**** This is called ***\*pprint()\**** in the Python API. |
| ***\*saveAsTextFiles\****(**prefix**, [**suffix**])   | Save this DStream’s contents as text files. The file name at each batch interval is generated based on **prefix** and **suffix**: **“prefix-TIME_IN_MS[.suffix]”**. |
| ***\*saveAsObjectFiles\****(**prefix**, [**suffix**]) | Save this DStream’s contents as **SequenceFiles** of serialized Java objects. The file name at each batch interval is generated based on **prefix** and **suffix**: **“prefix-TIME_IN_MS[.suffix]”**. ***\*Python API\**** This is not available in the Python API. |
| ***\*saveAsHadoopFiles\****(**prefix**, [**suffix**]) | Save this DStream’s contents as Hadoop files. The file name at each batch interval is generated based on **prefix** and **suffix**: **“prefix-TIME_IN_MS[.suffix]”**. ***\*Python API\**** This is not available in the Python API. |
| foreachRDD(**func**)                                  | The most generic output operator that applies a function, **func**, to each RDD generated from the stream. This function should push the data in each RDD to an external system, such as saving the RDD to files, or writing it over the network to a database. Note that the function **func** is executed in the driver process running the streaming application, and will usually have RDD actions in it that will force the computation of the streaming RDDs. |

### 7.8.2 foreachRDD基本使用

##### 需求：将DStream中计算的数据写入mysql

方式一：使用RDD的方式写入

```scala
package com.atguigu.spark.streaming.day02.transform.foreach

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Classname ForEachDemo1
 * @Description TODO
 *              Date ${Date} 14:29
 * @Create by childwen
 */
object ForEachDemo1 {

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/test"
  val user = "root"
  val psd = "root"
  val sql = "insert into a values(?,?)"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("ForEachDemo1")
      .setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val DStream = ssc.socketTextStream("hadoop102", 9999)

    DStream
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      //将每一个流写入到mysql中
      .foreachRDD(rdd => {
        rdd.foreachPartition {
          it => {
            //建立到mysql的连接
            //注册驱动
            Class.forName(driver)
            //获取连接
            val con = DriverManager.getConnection(url, user, psd)
            val ps = con.prepareCall(sql)
            //批次深度
            var depth = 0
            it.foreach {
              case (key, count) =>
                ps.setString(1, key)
                ps.setInt(2, count)
                //批处理
                ps.addBatch()
                depth += 1
                if (depth >= 3) {
                  ps.executeBatch()
                  depth = 0
                }
            }
            //遍历完成在提交一次，有可能最后一次不满足批次深度。导致未提交
            ps.executeBatch()
            ps.close()
          }
        }
      })

    ssc.start()

    ssc.awaitTermination()
  }

}

```

方式二：将RDD转换成DataFrame写入

```scala
package com.atguigu.spark.streaming.day02.transform.foreach

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname ForEachRDDDemo2
 * @Description TODO
 *              Date ${Date} 21:48
 * @Create by childwen
 */
object ForEachRDDDemo2 {

  val url = "jdbc:mysql://localhost:3306/test"
  val table = "a"
  private val pro = new Properties
  pro.setProperty("user", "root")
  pro.setProperty("password", "root")

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("ForEachRDDDemo2")
      .setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //使用有状态转换，需要设置检查点
    ssc.checkpoint("/ck1")
    val DStream = ssc.socketTextStream("hadoop102", 9999)

    val sparkSession: SparkSession = SparkSession
      .builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()

    import sparkSession.implicits._
    DStream
      .flatMap(_.split("\\W+"))
      .map((_, 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
      .foreachRDD(rdd => {
        //将rdd转换成df
        rdd
          .toDF
          .write
          .mode(SaveMode.Overwrite)
          .jdbc(url, table, pro)
      })

    ssc.start()

    ssc.awaitTermination()
  }

}

```



### 7.8.3 foreachRDD和transfrom的区别

1）transfrom是转换算子必须要返回一个RDD，在转换算子中执行行动算子是违规的。

2）foreachRDD是行动算子，专门用来把流的操作转换成RDD的操作，不需要返回值。

3）两者本质都是操作RDD。

>  使用RDD转换成DF时，SparkSession中的配置文件不要new需要从SSC中获取。
>
> ```scala
>     val sparkSession: SparkSession = SparkSession
>       .builder()
>       .config(ssc.sparkContext.getConf)
>       .getOrCreate()
> ```



## 7.9 Caching / Persistence

和 RDDs 类似，DStreams 同样允许开发者将流数据保存在内存中。也就是说，在DStream 上使用 **persist()**方法将会自动把**DStreams**中的每个**RDD**保存在内存中。

当**DStream**中的数据要被多次计算时，这个非常有用（如在同样数据上的多次操作）。对于像**reduceByWindow**和**reduceByKeyAndWindow**以及基于状态的**(updateStateByKey)**这种操作，保存是隐含默认的。

因此，即使开发者没有调用**persist()**，由基于窗操作产生的**DStreams**会自动保存在内存中。

# 八 Spark Streaming项目实战

## 8.1 求每天每个地区热门商品Top3

```scala
package com.atguigu.spark.streaming.app

import com.atguigu.spark.streaming.bean.{AdsCount, AdsInfo}
import com.atguigu.spark.streaming.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.TreeSet

/**
 * @Classname Need1
 * @Description TODO
 *              Date ${Date} 22:12
 * @Create by childwen
 */
object Need1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("Need1")
      .setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

    ssc.checkpoint("./ck1")
    //需求：每天每地区热门广告 Top3
    val KafkaDStream = MyKafkaUtil.getDStream(ssc, "ads_log")

    val adsDStream = KafkaDStream
      .map(lines => {
        val splits = lines._2.split(",")
        AdsInfo(
          splits(0).toLong,
          splits(1),
          splits(2),
          splits(3),
          splits(4)
        )
      })

    implicit val ord: Ordering[AdsCount] = new Ordering[AdsCount] {
      override def compare(x: AdsCount, y: AdsCount): Int =
        if (x.count > y.count) -1
        else 1
    }

    adsDStream
      .map {
        ads => ((ads.dayString, ads.area, ads.adsId), 1)
      }
      .updateStateByKey(
        (seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0))
      )
      .map {
        case ((day, area, ads), count) => ((day, area), (ads, count))
      }
      .groupByKey()
      .map {
        case ((day, area), it) => {
          var set = new TreeSet[AdsCount]()
          it.foreach {
            case (ads, count) => set += AdsCount(ads, count)
              if (set.size > 4) set = set.take(3)
          }
          ((day, area), set.toList)
        }
      }
      .print(50)


    ssc.start()

    ssc.awaitTermination()


  }


}

```

## 8.2 最近 1 小时广告点击量实时统计

```scala
package com.atguigu.spark.streaming.app

import java.sql.DriverManager

import com.atguigu.spark.streaming.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.jackson.Serialization

/**
 * @Classname Need2
 * @Description TODO
 *              Date ${Date} 10:30
 * @Create by childwen
 */
object Need2 extends APP {
  val url = "jdbc:mysql://localhost:3306/test"
  val driver = "com.mysql.jdbc.Driver"
  val user = "root"
  val psd = "root"
  val sql = "insert into clickrate values(?,?) on duplicate key update count=?"

  override def doSomeThing(ssc: StreamingContext, adsDStream: DStream[AdsInfo]): Unit = {
    /**
     * 默认情况下，计算只对一个时间段内的RDD进行，有了窗口之后，可以把计算应用到一个指定窗口内的RDD上。
     */
    adsDStream
      //进来一批数据，封装成一个DStream底层本质就是一个RDD
      .map {
        info => ((info.adsId, info.hmString), 1)
      }
      //map处理过后，当前批次的DStream进入窗口，与窗口内的RDD组成一个window DStream。
      .window(Minutes(60), Seconds(6)) //所有计算都在窗口内进行
      //对window DStream做一次聚合运算。
      .reduceByKey(_ + _)
      //对window DStream做一次map操作
      .map {
        case ((ads, hm), count) => (ads, (hm, count))
      }
      //对window DStream做分组聚合操作
      .groupByKey()
      //foreachRDD取出window DStream中的RDD
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          //写入mysql
          Class.forName(driver)
          val conn = DriverManager.getConnection(url, user, psd)
          val ps = conn.prepareStatement(sql)
          var depth = 0
          it.foreach {
            case (ads, it) => {
              import org.json4s.DefaultFormats
              val count = Serialization.write(it.toList.sortBy(_._1).toMap)(DefaultFormats)
              ps.setInt(1, ads.toInt)
              ps.setString(2, count)
              ps.setString(3, count)
              ps.addBatch()
              depth += 1
              //提交批次
              if (depth >= 3) {
                ps.executeBatch()
                depth = 0
              }
            }
          }
          //关闭资源
          ps.executeBatch()
          ps.close()
          conn.close()
        })
      })
  }
}

```

## 8.3 项目总结

### 1）当一个应用中一段代码被用到了两次以上，利用面向接口编程的思想将其封装起来。

```scala
package com.atguigu.spark.streaming.app

import com.atguigu.spark.streaming.bean.AdsInfo
import com.atguigu.spark.streaming.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Classname APP
 * @Description TODO
 *              Date ${Date} 10:16
 * @Create by childwen
 */
trait APP {
  val conf: SparkConf = new SparkConf()
    .setAppName("Need1")
    .setMaster("local[*]")

  val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

  ssc.checkpoint("./ck1")
  //需求：每天每地区热门广告 Top3
  val KafkaDStream = MyKafkaUtil.getDStream(ssc, "ads_log")

  val adsDStream: DStream[AdsInfo] = KafkaDStream
    .map(lines => {
      val splits = lines._2.split(",")
      AdsInfo(
        splits(0).toLong,
        splits(1),
        splits(2),
        splits(3),
        splits(4)
      )
    })

  //抽象方法
  def doSomeThing(ssc:StreamingContext,adsDStream: DStream[AdsInfo]):Unit

  def main(args: Array[String]): Unit = {

    doSomeThing(ssc,adsDStream):Unit

    ssc.start()

    ssc.awaitTermination()
  }

}

```

### 2）使用Spark自带的json4s工具将集合转换成json字符串

```scala
import org.json4s.DefaultFormats

val str = Serialization.write(it.toList.sortBy(_._1).toMap)(DefaultFormats)
```

### 3）向mysql中插入数据时，主键重复的情况下只想更新value值怎么办？

```scala
 val sql = "insert into clickrate values(?,?) on duplicate key update count=?"
```

### 4）自定义方法返回一个从kafka指定分区消费数据的流

```scala
object MyKafkaUtil {

  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "group.id" -> "childWen"
  )

  //返回一个从kafka指定分区读取数据的流
  def getDStream(ssc: StreamingContext, topic: String, topics: String*) = {

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, (topics :+ topic).toSet)

  }
}
```

### 5）创建一个Kafka生产者向分区生产数据

#### 依赖

```xml
    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>0.11.0.2</version>
        </dependency>
    </dependencies>
```

#### 自定义方法返回生产者

```scala
  def createKafkaProducer: KafkaProducer[String, String] = {
    val props: Properties = new Properties
    // Kafka服务端的主机名和端口号
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    // key序列化
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }
```

#### 使用方法向Kafka生产数据

```scala
val topic = "ads_log"
val producer: KafkaProducer[String, String] = createKafkaProducer
    while (true) {
      mockRealTimeData().foreach {
        msg => {
          // 发送到kafka
          println(msg)
          producer.send(new ProducerRecord(topic, msg))
          Thread.sleep(100)
        }
      }
      Thread.sleep(1000)
    }
```



