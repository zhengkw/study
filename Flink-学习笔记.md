# 一、Flink简介------为什么要学！

## 1.大数据开源社区宏观发展

Google3篇论文催生了hadoop生态

《Google File System》->HDFS

《BigTable 》->HBase

《Mapreduce》->Hadoop

MapReduce处理的是海量数据 Hive也是，Hadoop设计初期，移动互联网还没有爆炸式发展，所以没有设计微批处理！

ApacheSpark处理的是微批次（500ms）数据，将计算场景放到了内存，因为在内存中，所以运算速度快。考虑到了微批处理，但是没有考虑的真正的流式处理！无法改变底层原理，所以也就成了自身瓶颈，就算是Structured Streaming 也不能实现真正的实时！

Google Dataflow Model -> Apache Flink

google 2015年发表一个内部处理实时的论文，催生Apache在2018年做出Flink！

## 2.为什么选择Flink

- 流数据能反映我们的生活方式，来一条处理一条，实时的！

- 传统数据架构是基于有限数据集

- 我们的需求

  1. 高吞吐量
  2. 低延迟
  3. 结果准确和容错

  

## 3.flink可以应用的场景

1. 电商和市场营销 ->广告投放，双11阿里实时大屏等
2. 物联网-> 实时数据采集和显示、实时报警，自动驾驶
3. 电信->基站流量调配
4. 银行金融业->实时结算和通知推送，实时监测异常行为

## 4.传统数据处理架构转变为大数据分析

传统数据库，承担了本不应该承担的分析功能，应用大数据场景，传统数据库无法对大量数据进行分析处理（读瓶颈），所以大数据解决方案是通过ETL程序将传统数据库中的数据清洗，再导入到DataWareHouse中！这样把数据分析从关系数据库中解耦！

![image-20200608234333050](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200608234333050.png)

## 5.如何结合传统处理架构和大数据分析优势

传统数据处理架构是基于事务的事件响应的，是属于事实的！但是由于读的瓶颈，无法处理海量数据的分析运算！

![image-20200609000949945](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609000949945.png)

从上图可以看出，传统数据处理架构是基于events的！而且实时响应的！目前学过的架构里面hive可以做到离线的数据分析，如第4节所提到的！但是无法做到实时响应！

但是如今离线的分析无法满足我们对数据分析的需求，所以解决处理海量数据并且实时分析响应成了如今的难题！所以Flink应运而生！他提供了有状态的流式处理！

阿里双11处理每秒4.8PB数据这样的应用场景也侧面的展示了Flink的强大之处！

## 6.有状态的流式处理（第一代流式处理架构）

![image-20200609002212652](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609002212652.png)

传统数据处理框架中，我们需要对数据进行先读取再分析，如果要实时处理流式数据，势必这样有点多此一举。只需要我们将来的数据直接缓存到内存，然后直接处理即可！

- **保存到内存在flink中就称为本地状态**（Local State）！

- 为了容灾所以需要周期性的保存一个内存快照，这里就是存到远程的存储空间中，例如HDFS或者RocksDB中

什么是有状态和无状态

有状态：全局变量随着程序的运行，全局变量会一直变化，每次change都是一个状态！

无状态：函数式编程中的纯函数，无论运行多少次，只要输入是一样的，输出必是一样的！eg -> 1的n次方！

**在并发场景下，如何保证处理数据的顺序性和准确性成了上面架构的难点！因为每个节点各自为政，处理自己保存下来的数据，但是由于分布式处理高并发问题，同一时间处理的数据输出以后能保证顺序还和数据产生时的时间一致吗？？**

优点：高吞吐，低时延

缺点：结果不准确，顺序不一致！

## 7.流式处理的演变

### lambda 架构

![image-20200609004746627](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609004746627.png)

用两套系统，同时保证低延迟和结果准确！

通过给一个较为合理的长时延处理批数据，然后得到的结果和实时数据进行merge来给客户端进行提供数据查询支持！

优点：将第一代的缺点进行了弥补。

缺点：

- 两套系统如果相同业务处理逻辑有偏差那么极可能造成bug！

- 同样的业务逻辑要在2个系统中实现，开发维护成本变高！

### 流批结合

![image-20200609005820953](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609005820953.png)

目前flink的架构可以由上图表现！这里就可以看出flink的强大之处！流批结合，只需要开发一套系统！

这里sparkStreaming并不是流处理的思想！而是微批处理思想！

时间正确性：

- 因为sparkstreaming和storm只支持机器时间，即处理该数据的节点上的机器时间，而和数据产生时间没有关系！如果处理一个离线数据，开窗以后只会读到一个窗口中，时间只会是当前处理该数据的时间！
- 而flink是支持数据生产时间的，这样我们就可以实时分析离线数据，比如10年前的某一天的数据！

## 8.Flink的特点

### 事件驱动

<img src="C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609011605537.png" alt="image-20200609011605537" style="zoom:150%;" />

### 基于流的世界观

1. 在 Flink 的世界观中，一切都是由流组成的，离线数据是有界的流；实时数据是一个没有界限的流：这就是所谓的有界流和无界流！
2. 在spark中微批次组成流！

### 分层API

![image-20200609013007231](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609013007231.png)

- 越顶层越抽象，表达含义越简明，使用越方便
- 越底层越具体，表达能力越丰富，使用越灵活

### 其他特点

![image-20200609013137908](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609013137908.png)

## 9.对比spark和flink

### 处理模型

![image-20200609013330686](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609013330686.png)

### 数据模型与运行架构

![image-20200609013417155](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609013417155.png)

## 10.Flink任务划分

### 任务划分

spark的任务划分是一个shuffle算子一个stage，一个分区一个task。一个行动算子一个job。

flink的任务划分是，并行度相同的划分到一个task中！

### 子任务

一个任务的并行度是N，那么这个任务的子任务有N个！

### 算子

flink job中用于处理数据的一个单元，addSource，addSink，KeyBy等都是一个**数据处理单元**！



# 二、Flink运行架构

## Flink运行时的组件（宏观全局）

![image-20200609082100749](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609082100749.png)

必要组件：JobManger,TaskManager,ResourceManager

非必要组件：Dispacher（这个是一个ui'组件，除了可以用webUI组件提交，也可以使用命令行提交./flink run -c flink.StreamWordCount -p 2 myspark-1.0-SNAPSHOT.jar ）

standalone模式Dispatcher启动Start JobManager 线程！

### 1.JobManger

![image-20200609083948124](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609083948124.png)

简要概括：

- 一个FlinkApp一个JobManager进程进行管理！（JM等于master）
- JM接到图和资源jar包。
- JM将上面的图转换为执行图。
- JM向FlinkRM请求TaskManager上的slot（slot为静态资源，由配置文件配置决定！）
- 如果请求到足够的slot，那么就会将执行图和资源分发到TM中
- JM协调调度资源，协调检查点

### 2.TaskManager

![image-20200609084710302](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609084710302.png)

- 一个TaskManager代表一个可以工作的节点（worker）
- 最少包含1个slot（配置文件中默认是1）
- TM启动以后会想RM注册插槽。
- 接收到RM的指令后会提供给JM调用，JM将资源分发到TM
- **TM可以与其他运行同一FlinkApp的TM进行通讯数据交换**！

### 3.ResourceManager

![image-20200609084646066](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609084646066.png)

- 管理slot（flink中处理资源的单元！限制TM执行并发任务数量）
- 可结合其他的资源管理平台，比如yarn
- 接收JM申请，查看可用存活的slot数量
- 如果数量充足，RM会将有闲置slot的TM分配给JM
- 如果RM没有足够的Slot满足JM那么，RM会向资源提供平台发起会话
- 提供启动TM进程的容器，比如Yarn中的Container

### 注意事项

1. 插槽数是静态资源通过配置文件修改
2. 并行度的动态的，可以全局设置也可以局部设置

3. slot>=并行度

4. taskmanager个数和flink节点个数一致！

## 任务提交流程

#### standalone提交流程

![image-20200609205337171](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609205337171.png)

1. App提交应用，通过Dispatcher的webUI或者用flink命令提交到JM！
2. JM向RM申请slot
3. RM启动TM
4. TM启动以后对RM注册slots
5. RM发出提供slot指令
6. TM提供slots给JM
7. JM将jar，执行图等发给TM
8. TM与TM之间如果执行的是同一个App可以进行数据交换！



#### yarn提交流程（为了避免混淆没有在AM中画出Flink的RM！）

![image-20200609214153984](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609214153984.png)

1. 客户端提交job到yarn的RM
2. RM在某个NM节点上启动一个AM进程
3. AM进程中启动2个线程1个是JobManager，1个是RrsourceManager（FlinkRM）
4. JM会向FlinkRM申请slots，如果申请不到足够的slots，那么会向yarn中的 RM申请，如果申请足够，后面操作从6开始继续进行！
5. RM接到申请会启动Container容器，在容器中启动TaskManager。（一个节点Container最大数量由yarn配置文件决定）
6. 启动TaskManager以后会向FRM注册slots
7. 资源足够以后，TaskManager与JM会建立通讯,JM将资源传输到TM中
8. TM执行任务！

### 总结

一个容器就看作一个JVM ，一个JVM就运行一个**TM进程**，里面多个slot， slot里面多个**task线程**

**slot是一个固定的资源**，每个Task都执行在这个资源中！

TaskManager*slot 为最大并行能力（静态的）

并行度才是能用到能力（动态的）

## 任务调度原理



![image-20200609221517204](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200609221517204.png)



# TODO！！

# 三、编写Flink程序

区别于Spark，Flink没有行动算子这一概念！

![image-20200610200854438](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200610200854438.png)

**主要核心在与 source，transformations，sink**

## 3.1读取输入流

### 3.1.1无线循环产生随机数 （自定义数据源）

自定义数据源需要继承一个class RichParallelSourceFunction[T]

实现里面的run方法和cancel方法！

发送数据到下游flink一般采用的是collect方法，因为是模仿Java8新特性流！在Java8采用的是集合来存放流数据！

```scala
package com.zhengkw.day02

import java.util.Calendar
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import scala.util.Random

/**
 * @ClassName:SensorSource
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 9:30
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
class SensorSource extends RichParallelSourceFunction[SensorReading] {
  // 表示数据源是否正在运行，`true`表示正在运行
  var running: Boolean = true

  // `run`函数会连续不断的发送`SensorReading`数据
  // 使用`SourceContext`来发送数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化随机数发生器，用来产生随机的温度读数
    val rand = new Random

    // 初始化10个(温度传感器ID，温度读数)元组
    // `(1 to 10)`从1遍历到10
    var curFTemp = (1 to 10).map(
      // 使用高斯噪声产生温度读数
      i => ("sensor_" + i, 65 + (rand.nextGaussian() * 20))
    )

    // 无限循环，产生数据流
    while (running) {
      // 更新温度
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))

      // 获取当前的时间戳，单位是ms
      val curTime = Calendar.getInstance.getTimeInMillis

      // 调用`SourceContext`的`collect`方法来发射出数据
      // Flink的算子向下游发送数据，基本都是`collect`方法
      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      // 100ms发送一次数据
      Thread.sleep(100)
    }
  }

  /**
   * @descrption:   当取消任务时，关闭无限循环
   * @return: void
   * @date: 20/06/11 下午 5:04
   * @author: zhengkw
   */

  override def cancel(): Unit = running = false
}

```

**样例类T**，每一条流的数据都是一个sensorreading对象！

```scala
package com.zhengkw.day02

/**
 * @ClassName:SensorReading
 * @author: zhengkw
 * @description: 模拟温度传感器读数
 *              id  传感器id
 *              ts 时间戳
 *              temper 温度
 * @date: 20/06/09上午 9:28
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
case class SensorReading(id: String,
                         timestamp: Long,
                         temperature: Double)

```

#### **添加自定义的流并打印**

 通过调用evn.addSource()

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SourceFromCustomDataSource
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 9:43
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SourceFromCustomDataSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //添加自定义流
    val source = env.addSource(new SensorSource)
    source.print

    env.execute()

  }
}

```

### 3.1.2 从文件中读取数据源

通过env调用readTextFile读取

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SourceFromFile
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 9:47
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SourceFromFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .readTextFile("E:\\IdeaWorkspace\\myflink\\src\\main\\resources\\sensor.txt")
      .map(r => {
        // 使用逗号切割字符串
        val arr = r.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })  // `run`函数会连续不断的发送`SensorReading`数据
    // 使用`SourceContext`来发送数据

    stream.print()
    env.execute()
  }
}

```

### 3.1.3 从列表读取数据源

通过调用fromCollection来完成读取集合类型的数据源

```scala
package com.zhengkw.day02.kafka

import com.zhengkw.day02.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SourceFromCollection
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 1:03
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SourceFromCollection {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718199, 15.402984393403084),
        SensorReading("sensor_7", 1547718199, 6.720945201171228),
        SensorReading("sensor_10", 1547718199, 38.101067604893444)
      )).print()
    env.execute()
  }
}
```

### 3.1.4 从kafka读取数据

```scala
package com.zhengkw.day02.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * @ClassName:KafkaConsumerExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 5:19
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KafkaConsumerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put("group.id", "consumer-group")
    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserialization"
    )
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserialization"
    )
    props.put("auto.offset.reset", "latest")

    val stream = env
      .addSource(
        new FlinkKafkaConsumer011[String](
          "test",
          new SimpleStringSchema(),
          props
        )
      )
//消费到一条数据以后 sink回test继续消费 -》 循环队列！
    stream.addSink(
      new FlinkKafkaProducer011[String](
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "test",
        new SimpleStringSchema()
      )
    )

    stream.print()
    env.execute()

  }
}
```

#### **生产者 发送一条以后由消费者自产自销**

```scala
package com.zhengkw.day02.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
 * @ClassName:KafkaProducer
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 4:59
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    writeToKafka("test")
  }

  def writeToKafka(topic: String) = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)
    producer.send(new ProducerRecord[String, String](topic, "zhengkw"))
   //没有close无法发送成功！
    producer.close()
  }
}
```

#### **POM依赖**

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-0.11_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka_2.11</artifactId>
    <version>2.2.0</version>
</dependency>
	<dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
            <version>1.9.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-scala-bridge_2.11</artifactId>
            <version>1.10.0</version>
            <!--   <scope>provided</scope>-->
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_2.11</artifactId>
            <version>1.10.0</version>
            <!--   <scope>provided</scope>-->
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner-blink_2.11</artifactId>
            <version>1.10.0</version>
            <!--   <scope>provided</scope>-->
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>1.10.0</version>
            <!--   <scope>provided</scope>-->
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-csv</artifactId>
            <version>1.10.0</version>
        </dependency>


	<dependency>     
        <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka-0.11_2.11</artifactId>
            <version>1.10.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.bahir</groupId>
            <artifactId>flink-connector-redis_2.11</artifactId>
            <version>1.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-elasticsearch6_2.11</artifactId>
            <version>1.10.0</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.44</version>
        </dependency>
```

### 3.1.5 从socket读取数据源

```scala
package com.zhengkw.day01

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName:WordCountFromSocket
 * @author: zhengkw
 * @description:
 * @date: 20/06/08上午 11:34
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WordCountFromSocket {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境，类似SparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 并行任务的数量设置为1
    env.setParallelism(1)
    // 从socket获取流
    //使用nc -lk 9999 向9999端口发信息！
      val stream = env.socketTextStream("hadoop102", 9999, '\n')
    // 对数据流进行转换算子操作
    val textStream: DataStream[WordWithCount] = stream
      // 使用空格来进行切割输入流中的字符串
      .flatMap(r => r.split("\\s"))
      // 做map操作, w => (w, 1)
      .map(w => WordWithCount(w, 1))
      // 使用word字段进行分组操作，也就是shuffle
      .keyBy(0)
      // 分流后的每一条流上，开5s的滚动窗口
      .timeWindow(Time.seconds(5))
      // 做聚合操作，类似与reduce
      .sum(1)

    // 将数据流输出到标准输出，也就是打印
    textStream.print()

    // 不要忘记执行！
    env.execute()

  }
}

case class WordWithCount(word: String, count: Int)
```

## 3.2基本转换算子

大部分的流转换操作都基于用户自定义函数UDF。UDF函数打包了一些业务逻辑并定义了输入流的元素如何转换成输出流的元素。像`MapFunction`这样的函数，将会被定义为类，这个类实现了Flink针对特定的转换操作暴露出来的接口。

```scala
    class MyMapFunction extends MapFunction[Int, Int] {
      override def map(value: Int): Int = value + 1
    }
```

大部分函数接口被设计为`Single Abstract Method`（单独抽象方法）接口，并且接口可以使用Java 8匿名函数来实现。Scala DataStream API也内置了对匿名函数的支持。

### 3.2.1 DataStream转换算子

|             Transformation              |                       **Description**                        |
| :-------------------------------------: | :----------------------------------------------------------: |
|   **Map**<br/>DataStream → DataStream   | `DataStream<Integer> dataStream = //... dataStream.map(new MapFunction<Integer, Integer>() {     @Override     public Integer map(Integer value) throws Exception {         return 2 * value;     } });` |
| **FlatMap**<br/>DataStream → DataStream | `dataStream.flatMap(new FlatMapFunction<String, String>() {     @Override     public void flatMap(String value, Collector<String> out)         throws Exception {         for(String word: value.split(" ")){             out.collect(word);         }     } });` |
| **Filter**<br/>DataStream → DataStream  | `dataStream.filter(new FilterFunction<Integer>() {     @Override     public boolean filter(Integer value) throws Exception {         return value != 0;     } });` |
| **KeyBy**<br/>DataStream → KeyedStream  | `dataStream.keyBy("someKey") // Key by field "someKey" dataStream.keyBy(0) // Key by the first element of a Tuple` <br/>注意：KeyBy在以下情况，类型不能为键。1.POJO类型，但是没有重写hashCode()2.Any类型的Array不能作为键！ |

### 3.2.2KeyedStream算子

|              Transformation               |                       **Description**                        |
| :---------------------------------------: | :----------------------------------------------------------: |
|    **Reduce** KeyedStream → DataStream    | `keyedStream.reduce(new ReduceFunction<Integer>() {    @Override    public Integer reduce(Integer value1, Integer value2)    throws Exception {        return value1 + value2;    } });       ` |
|     **Fold** KeyedStream → DataStream     | `DataStream<String> result =  keyedStream.fold("start", new FoldFunction<Integer, String>() {    @Override    public String fold(String current, Integer value) {        return current + "-" + value;    }  });         ` |
| **Aggregations** KeyedStream → DataStream | `keyedStream.sum(0); keyedStream.sum("key"); keyedStream.min(0); keyedStream.min("key"); keyedStream.max(0); keyedStream.max("key"); keyedStream.minBy(0); keyedStream.minBy("key"); keyedStream.maxBy(0); keyedStream.maxBy("key");` |

### 3.2.3多流转换算子

|                      Transformation                      |                       **Description**                        |
| :------------------------------------------------------: | :----------------------------------------------------------: |
|          **Union**<br/>DataStream* → DataStream          |     `dataStream.union(otherStream1, otherStream2, ...);`     |
| **Connect**<br/>DataStream,DataStream → ConnectedStreams | `DataStream<Integer> someStream = //... DataStream<String> otherStream = //...  ConnectedStreams<Integer, String> connectedStreams = someStream.connect(otherStream);     ` |
|  **CoMap, CoFlatMap**<br/>ConnectedStreams → DataStream  | `connectedStreams.map(new CoMapFunction<Integer, String, Boolean>() {     @Override     public Boolean map1(Integer value) {         return true;     }      @Override     public Boolean map2(String value) {         return false;     } }); connectedStreams.flatMap(new CoFlatMapFunction<Integer, String, String>() {     @Override    public void flatMap1(Integer value, Collector<String> out) {        out.collect(value.toString());    }     @Override    public void flatMap2(String value, Collector<String> out) {        for (String word: value.split(" ")) {          out.collect(word);        }    } });` |
|          **Split**<br/>DataStream → SplitStream          | `SplitStream<Integer> split = someDataStream.split(new OutputSelector<Integer>() {     @Override     public Iterable<String> select(Integer value) {         List<String> output = new ArrayList<String>();         if (value % 2 == 0) {             output.add("even");         }         else {             output.add("odd");         }         return output;     } });` |
|         **Select**<br/>SplitStream → DataStream          | `SplitStream<Integer> split; DataStream<Integer> even = split.select("even"); DataStream<Integer> odd = split.select("odd"); DataStream<Integer> all = split.select("even","odd");` |



### 3.2.4分布式转换算子

|                        Transformation                        |                       **Description**                        |
| :----------------------------------------------------------: | :----------------------------------------------------------: |
|       **Custom partitioning** DataStream → DataStream        | `dataStream.partitionCustom(partitioner, "someKey"); dataStream.partitionCustom(partitioner, 0);     ` |
|       **Random partitioning** DataStream → DataStream        |                   `dataStream.shuffle();`                    |
| **Rebalancing (Round-robin partitioning)** DataStream → DataStream |                  `dataStream.rebalance();`                   |
|          **Rescaling**<br/>DataStream → DataStream           |                   `dataStream.rescale();`                    |
|         **Broadcasting**<br/>DataStream → DataStream         |                  `dataStream.broadcast();`                   |

#### Rescalling重缩放和Round-robin轮询区别---- 图例

![image-20200611000205302](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200611000205302.png)

## 3.3  基本转换算子应用demo

这里没有用匿名函数来写，而是实现了接口！当然这里大部分案例都可以用匿名函数来实现！

### 3.3.1 map case

```scala
package com.zhengkw.day02

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:MapExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 11:02
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    // `MyMapFunction`实现了`MapFunction`接口
    stream.map(new MyMapFunction).print()

    // 使用匿名类的方式实现`MapFunction`接口
    stream
      .map(
        new MapFunction[SensorReading, String] {
          override def map(value: SensorReading): String = value.id
        }
      )
      .print()

    // 使用匿名函数的方式抽取传感器ID
    stream.map(r => r.id).print()

    env.execute()
  }

}
```

### 3.3.2  flatmap

```scala
package com.zhengkw.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:FlatMapExampleFromDoc
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 11:00
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FlatMapExampleFromDoc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "white", "black", "white", "gray", "black", "white"
      )

    stream
      .flatMap(
        new FlatMapFunction[String, String] {
          override def flatMap(value: String, out: Collector[String]): Unit = {
            if (value.equals("white")) {
              out.collect(value)
            } else if (value.equals("black")) {
             //复制操作！
              out.collect(value)
              out.collect(value)
            }
          }
        }
      )
      .print()

    env.execute()
  }

}
```

**注意：下面的案例数据源来自3.1.1案例**

```scala
package com.zhengkw.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:FlatMapExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 10:50
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new SensorSource)
    env.setParallelism(1)
    // 用flatMap实现 map操作
    stream.flatMap(new FlatMapFunction[SensorReading, String] {
      override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
        out.collect(value.id)
      }
    }).print
    // 用flatMap实现 filter操作
    stream.flatMap(new FlatMapFunction[SensorReading, String] {
      override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
        if (value.id.equals("sensor_1")) out.collect(value.id)
      }
    }).print

    env.execute()
  }
}
```

**flatmap功能和spark中的一致！**

#### 3.3.2.1 RichFunction case

```scala
package com.zhengkw.day02

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:RichFlatMapExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 4:21
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RichFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(1, 2, 3)
    stream
      .flatMap(new MyFlatMap)
      .print()

    env.execute()
  }

  class MyFlatMap extends RichFlatMapFunction[Int, Int] {
    override def flatMap(value: Int, out: Collector[Int]): Unit = {
      println(s"index：${getRuntimeContext.getIndexOfThisSubtask}")
      out.collect(value + 1)
    }

    override def open(parameters: Configuration): Unit = println("this is init")

    override def close(): Unit = println("it is closed!")
  }

}
```



### 3.3.3  filter

```scala
package com.zhengkw.day02

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:FilterExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:32
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FilterExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    // 将传感器ID为`sensor_1`的过滤出来
    stream
      .filter(
        new FilterFunction[SensorReading] {
          override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
        }
      )
      .print()

    // 使用匿名函数
    stream.filter(r => r.id.equals("sensor_1")).print()

    stream.filter(new MyFilterFunction).print()

    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
  }

}
```



### 3.3.4 keyby

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:KeyByExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 11:50
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KeyByExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream  = env
      .addSource(new SensorSource) // --> DataStream[T]
      .keyBy(r => r.id) // --> KeyedStream[T, K]
      .min(2)  // --> DataStream[T]

    stream.print()
    env.execute()
  }
}
```

keyby如果传入参数为string类型，则后面在实现window方法时，key的类型会返回一个java.tuple。取值会很麻烦，如果传入参数用scala取tuple的形式的话，这不会这么麻烦。

假设这个tuple里只有一个field。

则取值是这样的

```scala
//这样引入，否则默认的是scala的tuple
org.apache.flink.api.java.{tuple，tuple1}
//假设已知tuple里的field是Long
key.asInstanceOf[Tuple1[Long]].fo


/**
//为什么是fo 看这个源码
@Public
public class Tuple1<T0> extends Tuple {

	private static final long serialVersionUID = 1L;

	/** Field 0 of the tuple. */
	public T0 f0;
*/
```



### 3.3.5 reduce

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:KeyByReduceExampleFromDoc
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:30
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KeyByReduceExampleFromDoc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env
      .fromElements(
        ("en", List("tea")),
        ("fr", List("vin")),
        ("en", List("cake"))
      )

    inputStream
      .keyBy(_._1)
      // `:::`用来拼接列表
      .reduce((r1, r2) => (r1._1, r1._2 ::: r2._2))
      .print()

    env.execute()
  }
}
```

### 3.3.6 split&select

split相当于盖章，select来检出

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SplitExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:37
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SplitExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[(Int, String)] = env
      .fromElements(
        (1001, "1001"),
        (999, "999")
      )

    val splitted: SplitStream[(Int, String)] = inputStream
      .split(t => if (t._1 > 1000) Seq("large") else Seq("small"))

    val large: DataStream[(Int, String)] = splitted.select("large")
    val small: DataStream[(Int, String)] = splitted.select("small")
    val all: DataStream[(Int, String)] = splitted.select("small", "large")

    all.print()

    env.execute()
  }
}
```



## 3.4 多流算子应用

### 3.4.1connect&comap

connect只能连接 流，但是输出可以不一致！

```SCALA
package com.atguigu.day2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val one: DataStream[(Int, Long)] = env.fromElements((1, 1L))
    val two: DataStream[(Int, String)] = env.fromElements((1, "two"))

    // 将key相同的联合到一起
    val connected: ConnectedStreams[(Int, Long), (Int, String)] = one.keyBy(_._1)
      .connect(two.keyBy(_._1))

    val printed: DataStream[String] = connected
      .map(new MyCoMap)

    printed.print

    env.execute()
  }

  class MyCoMap extends CoMapFunction[(Int, Long), (Int, String), String] {
    override def map1(value: (Int, Long)): String = value._2.toString + "来自第一条流"

    override def map2(value: (Int, String)): String = value._2 + "来自第二条流"
  }
}
```



### 3.4.2 union

必须合并流的类型一致，但是可以连接多个流！

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:UnionExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:37
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object UnionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 传感器ID为sensor_1的数据为来自巴黎的流
    val parisStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_1"))

    // 传感器ID为sensor_2的数据为来自东京的流
    val tokyoStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_2"))

    // 传感器ID为sensor_3的数据为来自里约的流
    val rioStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_3"))

    val allCities: DataStream[SensorReading] = parisStream
      .union(
        tokyoStream,
        rioStream
      )

    allCities.print()

    env.execute()
  }
}
```

### 3.4.3  CoFlatMap

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:CoFlatMapExample
 * @author: zhengkw
 * @description: 相当于外连接
 * @date: 20/06/09下午 2:36
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CoFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val one: DataStream[(Int, Long)] = env.fromElements((1, 1L))
    val two: DataStream[(Int, String)] = env.fromElements((2, "two"))

    // 将key相同的联合到一起
    val connected: ConnectedStreams[(Int, Long), (Int, String)] = one.keyBy(_._1)
      .connect(two.keyBy(_._1))

    val printed: DataStream[String] = connected
      .flatMap(new MyCoFlatMap)

    printed.print

    env.execute()
  }

  class MyCoFlatMap extends CoFlatMapFunction[(Int, Long), (Int, String), String] {
    override def flatMap1(value: (Int, Long), out: Collector[String]): Unit = {
      out.collect(value._2.toString + "来自第一条流")
      out.collect(value._2.toString + "来自第一条流")
    }

    override def flatMap2(value: (Int, String), out: Collector[String]): Unit = {
      out.collect(value._2 + "来自第二条流")
    }
  }
}
```

### 3.4.4 CoMap

```scala
package com.zhengkw.day02

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:CoMapExample
 * @author: zhengkw
 * @description: 相当于外连接
 * @date: 20/06/09下午 2:35
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val one: DataStream[(Int, Long)] = env.fromElements((1, 1L))
    val two: DataStream[(Int, String)] = env.fromElements((2, "two"))

    // 将key相同的联合到一起
    val connected: ConnectedStreams[(Int, Long), (Int, String)] = one.keyBy(_._1)
      .connect(two.keyBy(_._1))

    val printed: DataStream[String] = connected
      .map(new MyCoMap)

    printed.print

    env.execute()
  }

  class MyCoMap extends CoMapFunction[(Int, Long), (Int, String), String] {
    override def map1(value: (Int, Long)): String = value._2.toString + "来自第一条流"

    override def map2(value: (Int, String)): String = value._2 + "来自第二条流"
  }

}
```



## 3.5 Sink应用

### 3.5.1 sinktoKafka

```scala
package com.zhengkw.day02.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * @ClassName:KafkaConsumerExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 5:19
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KafkaConsumerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put("group.id", "consumer-group")
    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserialization"
    )
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserialization"
    )
    props.put("auto.offset.reset", "latest")

    val stream = env
      .addSource(
        new FlinkKafkaConsumer011[String](
          "test",
          new SimpleStringSchema(),
          props
        )
      )
//消费到一条数据以后 sink回test继续消费 -》 循环队列！
    stream.addSink(
      new FlinkKafkaProducer011[String](
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "test",
        new SimpleStringSchema()
      )
    )

    stream.print()
    env.execute()

  }
}
```

### 3.5.2 sinkToES

```scala
package com.zhengkw.day03

import java.util

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * @ClassName:SinkToES
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 9:36
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    // es的主机和端口
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))

    // 定义了如何将数据写入到es中去
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts, // es的主机名
      // 匿名类，定义如何将数据写入到es中
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading,
                             runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          // 哈希表的key为string，value为string
          val json = new util.HashMap[String, String]()
          json.put("data", t.toString)
          // 构建一个写入es的请求
          val indexRequest = Requests
            .indexRequest()
            .index("sensor") // 索引的名字是sensor
            .`type`("zhengkw")
            //传入的是一个hash表
            .source(json)

          requestIndexer.add(indexRequest)
        }
      }
    )

    // 用来定义每次写入多少条数据
    // 成批的写入到es中去
    esSinkBuilder.setBulkFlushMaxActions(10)

    stream.addSink(esSinkBuilder.build())

    env.execute()

  }
}
```

### 3.5.3 toMysql

```scala
package com.zhengkw.day03

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SinkToMysql
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 10:27
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToMySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    stream.addSink(new MyJDBCSink)
    env.execute()
  }

  class MyJDBCSink extends RichSinkFunction[SensorReading] {
    // 连接
    var conn: Connection = _
    // 插入语句
    var insertStmt: PreparedStatement = _
    // 更新语句
    var updateStmt: PreparedStatement = _

    /**
     * @descrption: 只执行一次
     *              初始化mysql
     * @param parameters
     * @return: void
     * @date: 20/06/10 上午 10:37
     * @author: zhengkw
     */
    override def open(parameters: Configuration): Unit = {
      val conn = DriverManager.getConnection(
        "jdbc:mysql://hadoop102:3306/test",
        "root",
        "sa"
      )
      // 插入语句
      insertStmt = conn.prepareStatement(
        "INSERT INTO temperatures (sensor, temp) VALUES (?, ?)"
      )
      //更新语句
      updateStmt = conn.prepareStatement(
        "UPDATE temperatures SET temp = ? WHERE sensor = ?"
      )
    }

    override def invoke(value: SensorReading): Unit = {
      updateStmt.setDouble(1, value.temperature)
      updateStmt.setString(2, value.id)
      updateStmt.execute()

      if (updateStmt.getUpdateCount == 0) {
        insertStmt.setString(1, value.id)
        insertStmt.setDouble(2, value.temperature)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }

}
```

### 3.5.4 toRedis

官方没有对应的API来写入到Redis

不过Apache有一个第三方API支持

导入依赖

```xml
   <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>flink-connector-redis_2.11</artifactId>
        <version>1.0</version>
    </dependency>
```
```SCALA
package com.zhengkw.day03

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @ClassName:RedisMapperExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 8:52
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToRedis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    // redis的主机
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    stream.addSink(new RedisSink[SensorReading](conf, new MyRedis))
    env.execute()
  }

  class MyRedis extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor")
    }

    override def getKeyFromData(data: SensorReading): String = data.id

    override def getValueFromData(data: SensorReading): String = data.temperature.toString
  }

}
```

### 3.5.5 SinkToFile

```scala
package com.zhengkw.day03

import com.zhengkw.StreamingJob
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @ClassName:SinkToFile
 * @author: zhengkw
 * @description:
 * @date: 20/06/18下午 3:46
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从txt文件中获取数据源
    val stream = env.readTextFile("source/sensor.txt")
    //直接输出到另一个文件中
    stream.addSink(
      StreamingFileSink.forRowFormat(
        //给定文件路径
        new Path("source/out"),//文件夹名字 文件名自动生成！
        new SimpleStringEncoder[String]() //空参默认UTF-8
      ).build()
    )
    env.execute()

  }
}
```

## 3.6 UDF函数

### 3.6.1 函数类(Function Classes)

Flink暴露了很多接口，例如MapFunction，FilterFunction，ProcessFunction等

**参考代码3.3.3**

例子实现了FilterFunction接口

```scala
    class FilterFilter extends FilterFunction[String] {
      override def filter(value: String): Boolean = {
        value.contains("flink")
      }
    }
    
    val flinkTweets = tweets.filter(new FlinkFilter)
```

还可以将函数实现成匿名类

```scala
    val flinkTweets = tweets.filter(
      new RichFilterFunction[String] {
        override def filter(value: String): Boolean = {
          value.contains("flink")
        }
      }
    )
```

我们filter的字符串“flink”还可以当作参数传进去。

```scala
    val tweets: DataStream[String] = ...
    val flinkTweets = tweets.filter(new KeywordFilter("flink"))
    
    class KeywordFilter(keyWord: String) extends FilterFunction[String] {
      override def filter(value: String): Boolean = {
        value.contains(keyWord)
      }
    }
```

### 3.6.2 匿名函数(Lambda Functions)

**代码参考3.3.3**

匿名函数可以实现简单逻辑，但是无法实现高级功能，例如**状态访问**等！

### 3.6.3 富函数(Rich Functions)

如果我们需求是在函数处理数据之前，需要做一些初始化操作，或者是需要在**处理数据时**获得函数的执行上下文信息，那么可以使用已经继承Rich类的一些Function。flink提供了继承Function和Rich的一些RichFunction

- RichMapFunction
- RichFlatMapFunction
- RichFilterFunction
- …

富函数中有2个可以重写的方法一个是open(),一个是close()属于他的生命周期。

- open是RichjFunction的初始化方法，当算子例如map或者filter被调用之前open()会被调用。open()函数一般多用于对状态变量的初始化！一般是通过直接调用getRuntimeContext()方法来获取state！

- close()方法是生命周期中的最后一个调用方法，通常做一些资源释放工作！

- getRuntimeContext() 提供了很多常用的方法，获取分布式缓存，子任务索引，累加器等信息！最重要的提供了访问状态的方法！

- ```
  .getIndexOfThisSubtask  //获取当前子任务的索引
  .getNumberOfParallelSubtasks//获取并行度
  .getState()//获取状态
  
  ```

#### **RichFlatMapExample** 

```scala
package com.zhengkw.day02

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**

 * @ClassName:RichFlatMapExample

 * @author: zhengkw

 * @description:

 * @date: 20/06/09下午 4:21

 * @version:1.0

 * @since: jdk 1.8 scala 2.11.8
   */
   object RichFlatMapExample {
     def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment
   env.setParallelism(1)

   val stream = env.fromElements(1, 2, 3)
   stream
     .flatMap(new MyFlatMap)
     .print()

   env.execute()
     }

  class MyFlatMap extends RichFlatMapFunction[Int, Int] {
    override def flatMap(value: Int, out: Collector[Int]): Unit = {
      println(s"index：${getRuntimeContext.getIndexOfThisSubtask}")
      out.collect(value + 1)
    }

    override def open(parameters: Configuration): Unit = println("this is init")
    
    override def close(): Unit = println("it is closed!")

  }

}
```

## 3.7 总结

1. keyBy

基于key的hashcode重分区，同一个key只能在一个分区内处理，一个分区内可以有不同key的数据

keyBy之后在**keyedStream上的**的所有操作，针对的作用域都是当前的**key**！

keyby以后可以调用一些带状态的方法!例如mapWithState，flatMapWithState

​	2.滚动聚合操作

DataStream没有聚合操作，目前所有的聚合操作都是针对KeyStream

​	3.多流转换算子

split-select，connect-comap/coflatmap  这些算子是成对出现

先转换成SplitStream，ConnectedStreams，然后再通过select/comap操作转换回来DataStream所谓coMap，其实是基于ConnectedStream的map方法，里面传入的参数是coMapFunction

​	4.富函数

是函数类的增强版，有2个生命周期方法，可以获取运行时上下文，可以通过上下文获取state。

进行flink有状态的流式计算，做状态编程，就是基于RichFunction的编程！

富函数不能获取窗口信息和时间戳信息（wm）



### 数据转换模型

![image-20200617230153055](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200617230153055.png)



## 3.8 WaterMark

**StreamExecutionEnvironment**

```scala
public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
   this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
   if (characteristic == TimeCharacteristic.ProcessingTime) {
      getConfig().setAutoWatermarkInterval(0);
   } else {
      getConfig().setAutoWatermarkInterval(200);
   }
}public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
		this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
		if (characteristic == TimeCharacteristic.ProcessingTime) {
			getConfig().setAutoWatermarkInterval(0);
		} else {
			getConfig().setAutoWatermarkInterval(200);
		}
	}
```

从该段代码可以看出，除了机器时间语义没有水位线这个概念以外，其他的都有水位线概念。

默认每隔200ms插入一次水位线！默认的水位线属于周期性插入

可以对这个时间进行修改，下面以500ms一次插入！

```scala
env.getConfig.setAutoWatermarkInterval(500L)
```

### 水位线

在对事件时间窗口，窗口为[start,end)，当水位线>end时，那么该窗口闭合，输出计算结果，销毁窗口！

### 处理时间 vs 事件时间

大家可能会有疑问，既然事件时间已经可以解决我们的所有问题，为什么我们还要对比这两个时间概念？真相是，处理时间在很多情况下依然很有用。处理时间窗口将会带来理论上最低的延迟。因为我们不需要考虑迟到事件以及乱序事件，所以一个窗口只需要简单的缓存窗口内的数据即可，一旦机器时间超过指定的处理时间窗口的结束时间，就会触发窗口的计算。所以对于一些处理速度比结果准确性更重要的流处理程序，处理时间就派上用场了。另一个应用场景是，当我们需要在真实的时间场景下，周期性的报告结果时，同时不考虑结果的准确性。一个例子就是一个实时监控的仪表盘，负责显示当事件到达时立即聚合的结果。最后，处理时间窗口可以提供流本身数据的忠实表达，对于一些案例可能是很必要的特性。例如我们可能对观察流和对每分钟事件的计数（检测可能存在的停电状况）很感兴趣。简单的说，处理时间提供了低延迟，同时结果也取决于处理速度，并且也不能保证确定性。另一方面，事件时间保证了结果的确定性，同时还可以使我们能够处理迟到的或者乱序的事件流。

### 自定义周期性水位线

```scala
package com.zhengkw.day04

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * @ClassName:PeriodicInsertWatermarks
 * @author: zhengkw
 * @description:
 * @date: 20/06/11上午 11:57
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object PeriodicInsertWatermarks {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(
        new MyAssigner
      )
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new MyProcess)

    stream.print()
    env.execute()
  }

  // `BoundedOutOfOrdernessTimestampExtractor`的底层实现
  class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    val bound: Long = 1000L // 最大延迟时间
    var maxTs: Long = Long.MinValue + bound // 观察到的最大时间戳

    // 每来一条元素就要调用一次
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
      maxTs = maxTs.max(element.timestamp)
      element.timestamp
    }

    // 产生水位线的函数，默认200ms调用一次
    override def getCurrentWatermark: Watermark = {
      // 水位线 = 观察到的最大时间戳 - 最大延迟时间
      new Watermark(maxTs - bound)
    }
  }

  class MyProcess extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect(elements.size.toString)
    }
  }
}
```

## 3.9 ProcessFunction

之前的转换算子是无法访问事件的时间戳信息和WaterMark信息。但是在一些应用场景下，这些信息非常重要。

- ProcessFunction不仅提供了这些信息获取方法，还可以注册定时器。
- 因为ProcessFunction继承了RichFunction，所以也可以重新RichFunction里面的所有方法！
- 可以输出侧输出流

### 3.9.1 KeyedProcessFunction

```scala
ctx.timerService().currentProcessingTime()//获取当前处理时间
ctx.timerService().currentWatermark()//获取当前水位线
ctx.timerService().deleteEventTimeTimer(a:Long)//删除事件时间定时器
ctx.timerService().deleteProcessingTimeTimer(b:Long)//删除处理时间定时器
ctx.timerService().registerEventTimeTimer(time)//注册事件时间定时器
ctx.timerService().registerProcessingTimeTimer(time)//注册处理时间定时器
```



## 最大乱序程度

事件时间中水位线

wm=maxTs-lateness

lateness为延迟时间，也可以当做乱序程度，如果将延迟时间和最大乱序程度等价以后，就可以有效控制数据丢失

![image-20200616003308119](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200616003308119.png)

假设窗口为5秒关闭一次，这个时候如果最大乱序程度刚好和延迟时间一致都为4，那么例如8-4=4，代表8后面最多迟到的是4不会出现3，那么认为wm进展到4，那么4之前的数据都到齐了！为了保证正确性，一般情况WM的延迟时间和最大乱序时间一致，就可以有效保证数据不丢失，但也只是理论上！

 

# TableAPI

目前TableAPI还在开发阶段，有些功能不支持。（v 1.10）

官方pom依赖

![image-20200713104212550](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200713104212550.png)

### blink-planner与flink-table-planner区别

老版本planner一套方案是流处理程序转换为DataStream程序。批处理程序转换为DataSet程序。调用相应API处理数据。

而blink实现真正的流批统一。

### 创建表环境

```scala
val env=StreamExecutioneEnvironment.getExecutionEnvironment
env.setParallelism(1)

val tableEnv = StreamTableEnvironment.create(env) //创建表执行环境
```



#### 流转换成表

```scala
伪代码  导入table api 隐式转换

读文件获取流，转换为一个DataStream类型为样例类的流

1基于env创建一个表环境
val tableEnv=StreamTableEnvironmet.create(env)
2.基于tableenv将流转表
val dataTable:Table=tableEnv.fromDataStrem
3.调用table api
val resultTable:Table=dataTable.select("id,temperature")
.filter("id=='sensor_1'")
4.表转流
resultTable.toAppendStream[(String,Double)].print()
env.execute

```

#### 直接创建表不经过流转换

```
tableEnv.connect(xxx).createTemporayTable("TBname")//无返回值  创建一个用于读数据的表/或输出


```

#### 获取表对象

获取了Table类型的对象就可以操作API，如果需要写SQL需要用env环境去注册表！如果表环境里注册里表可以通过下面的方法取得获取表。

```
val table = tableEnv.from("TBname")
```

#### TableAPI之新老版本区别(获取表环境)

```scala
val env=StreamExecutioneEnvironment.getExecutionEnvironment
env.setParallelism(1)

val tableEnv = StreamTableEnvironment.create(env) //创建表执行环境
//老版本
val setting:EnvrionmentSettings = EnvironmentSettings.newInstance()
                                  .useOldPlanner()
                                  .inStreamingMode() //流处理模式
                                  .build()
 val oldStreamTableEnv=  StreamTableEnvrionment.create(env,settings)  //获取流处理的table环境                             
  //老版本不是批流统一 所以批处理方式特殊
val batchEnv:ExecutionEnvrionment = ExecutionEnvironment.getExecutionEnvrionment
val batchTableEnv:BatchTableEnvrionment=BatchTableEnvrionment.create(batchEnv)

//blink版本
val bsSettings=EnvironmentSettings.newInstance()
                                  .useBlinkPlanner()
                                  .inStreamingMode() //流处理模式
                                  .build()
val bsTableEnv=StreamTableEnvironment.create(env,bsSettings)

//blink 批处理
 val bbSettings=var bsSettings=EnvironmentSettings.newInstance()
                                  .useBlinkPlanner()
                                  .inBatchMode() //批处理模式
                                  .build()
val bbTableEnv= TableEnvironment.create(bbSettings)

```



#### 判断字段相等 

![image-20200714100746219](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200714100746219.png)

用的三等号！

#### 追加流输出和回收流输出

```SCALA
table.toAppendStream[case- class] //追加流

table.toRetractSteam[case-class] //回收流

val env=StreamExecutioneEnvironment.getExecutionEnvironment
env.setParallelism(1)

val tableEnv = StreamTableEnvironment.create(env) //创建表执行环境
//使用flinkSQL输出
val table=tableEnv.sqlQuery("xxxx")
//如果是仅仅对表环境中的表进行追加写不修改里面相同id的值就使用追加流，如果有修改就用回收流
```

#### Sink到外部FSsystem  侧输出

```scala

//创建表环境
val env=StreamExecutioneEnvironment.getExecutionEnvironment
env.setParallelism(1)

val tableEnv = StreamTableEnvironment.create(env) //创建表执行环境
//创建表用于输出到文件系统
tableEnv.connect(new FileSystem().path("xxxxx"))
        .withFormat(new Csv())
         .withSchema(new Schema().field("id",DataTypes.STRING())
              .field("temp",DataTypes.DOUBLE())                    
                    )
             .createTemporaryTable("outputTable") //创建表
//table为用环境变量创建的对象
table.insertInto("outputTable")

```



## 2PC两阶段提交原理

分布式事务

```
广义2pc原理
1准备阶段  协调者请求事务，参与者先将操作记录成日志，然后逐条开始执行这些日志里的操作。如果一系列操作执行成功（各个参与者），则像协调者发送同意。如果有失败则返回中止消息。
2提交阶段  协调者分析所有参与者返回的消息，如果全部同意，则发出提交请求。参与者提交成功返回完成信息，如果有一个参与者提交失败，则返回中止，协调者群发回滚。根据之前日志进行回滚。

缺点
1协调者可能单点故障
2可能出现协调者发送提交信息的参与者单点故障了，导致数据的不一致性（不理解，分布式应该有重试机制，如果重试超次则应该判定为失败）

```

分布式系统中，具有多个并发运行的接收器任务想要简单的提交或者是回滚是不安全的。必须要所有的组件在提交或者回滚时“一致”以确保一致的结果。

检查点的启动-->两阶段提交协议的预提交阶段。 当检查点启动时，Flink JobManager会将检查点屏障（将数据流中的记录分隔为进入当前检查点的集合与进入下一个检查点的集合）注入数据流。

检查点屏障对齐以后（v1.10及以前版本），会触发算子的状态后端以获取状态的快照。

## 窗口

#### 全窗口函數

  将数据全量存到一起不做计算，当窗口闭合才做计算。

#### 增量聚合函数

没来一条数据聚合一次，只存放聚合后的结果，存到状态变量中。（这样更节约空间）



### Group Windows

#### 滚动窗口要用Tumble类来定义

原理是给表一个字段！明确指定时间语义

```scala
//event-time window
.window(Tumble over 10.minutes on 'rowtime as 'w)
//processing-time window
.window(Tumble over 10.minutes on 'proctime as 'w)
//row-count window 10行记数窗口
.window(Tumble over 10.rows on 'proctime as 'w)
```

#### 滑动窗口

```scala
//event-time window
.window(Slide over 10.minutes every 5.minutes on 'rowtime as 'w)
//processing-time window
.window(Slide over 10.minutes every 5.minutes on 'proctime as 'w)
//row-count window 10行记数窗口
.window(Slide over 10.rows every 5.minutes on 'proctime as 'w)
```

#### 会话窗口

```scala
//event-time window
.window(Session withGap 10.minutes  on 'rowtime as 'w)
//processing-time window
.window(Session  withGap 10.minutes  on 'proctime as 'w)
```

