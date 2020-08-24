

# 一.Spark-standalone模式下内核分析

## 1.1 Master和Worker启动

在standalone模式下，如果配置了高可用，则必须先启动ZK和HDFS，HDFS里面有历史服务信息！

使用启动命令

start-all.sh启动 ->执行2个脚本  start-master start-slave

### 1.2.1启动master

启动master和worker并不冲突，他们为并行执行。

脚本中执行

```shell
java –cp org.apache.spark.deploy.master.Master
```

相应的传递了master的host，port，webui-port这些参数

**进入Master类**

```scala
//主构造器
private[deploy] class Master(
                                override val rpcEnv: RpcEnv,
                                address: RpcAddress,
                                webUiPort: Int,
                                val securityMgr: SecurityManager,
                                val conf: SparkConf)
    extends ThreadSafeRpcEndpoint with Logging with LeaderElectable
```





> 继承一个线程安全的RpcEndpoint，因为spark2.x.x开始底层通讯用的NettyRpcEnv来取代1.6以前的AKKA！主构造器里面封装了比较重要的几个参数 rpcEnv，conf

**入口函数**

```scala
private[deploy] object Master extends Logging {
    val SYSTEM_NAME = "sparkMaster"
    val ENDPOINT_NAME = "Master"

    // 启动 Master 的入口函数
    def main(argStrings: Array[String]) {
        Utils.initDaemon(log)
        val conf = new SparkConf
        // 构建用于参数解析的实例   --host hadoop201 --port 7077 --webui-port 8080
        val args = new MasterArguments(argStrings, conf)
        // 启动 RPC 通信环境和 MasterEndPoint(通信终端)
        val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
        rpcEnv.awaitTermination()
    }

    /**
      * Start the Master and return a three tuple of:
      * 启动 Master 并返回一个三元组
      * (1) The Master RpcEnv
      * (2) The web UI bound port
      * (3) The REST server bound port, if any
      */
    def startRpcEnvAndEndpoint(
                                  host: String,
                                  port: Int,
                                  webUiPort: Int,
                                  conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
        val securityMgr = new SecurityManager(conf)
        // 创建 Master 端的 RpcEnv 环境   参数: sparkMaster hadoop201 7077 conf securityMgr
        // 实际类型是: NettyRpcEnv
        val rpcEnv: RpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
        // 创建 Master对象, 该对象就是一个 RpcEndpoint, 在 RpcEnv中注册这个RpcEndpoint
        // 返回该 RpcEndpoint 的引用, 使用该引用来接收信息和发送信息
        val masterEndpoint: RpcEndpointRef = rpcEnv.setupEndpoint(ENDPOINT_NAME,
            new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
        // 向 Master 的通信终端发法请求，获取 BoundPortsResponse 对象
        // BoundPortsResponse 是一个样例类包含三个属性: rpcEndpointPort webUIPort restPort
        val portsResponse: BoundPortsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
        (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
    }
}
```

> 脚本传递过来的3个参数可以通过MasterArguments的构造器来对默认值进行修改

```scala
private[master] class MasterArguments(args: Array[String], conf: SparkConf) extends Logging {
  var host = Utils.localHostName()
  var port = 7077
  var webUiPort = 8080
  var propertiesFile: String = null
    ........
}
```

> 将封装好的masterArg作为参数传给startRpcEnvAndEndpoint来启动env环境和endport，在AKKA中节点都是actor，而在Netty中节点是Endport
>
> 且最后执行  rpcEnv.awaitTermination() 来使进程不退出！

```scala
    // 启动 RPC 通信环境和 MasterEndPoint(通信终端) Master伴生对象中！
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
//Class master    
rpcEnv.awaitTermination()
```

### 1.2.2构建RpcEnv

```scala
......
val rpcEnv: RpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr) //创建env
//将endpoint的信息set进去 将Master封装到RpcEndpointRef中
val masterEndpoint: RpcEndpointRef = rpcEnv.setupEndpoint(ENDPOINT_NAME,
            new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))

.....
//Class master 
rpcEnv.awaitTermination() -> dispatcher.awaitTermination()
```

通过create()方法来创建RpcEnv

```scala
//最终调用父类RpcEnv
def create(
                  name: String,
                  bindAddress: String,
                  advertiseAddress: String,
                  port: Int,
                  conf: SparkConf,
                  securityManager: SecurityManager,
                  clientMode: Boolean): RpcEnv = {
        // 保存 RpcEnv 的配置信息
        val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
            clientMode)
        // 创建 NettyRpcEvn
        new NettyRpcEnvFactory().create(config)
    }
```



工厂类里面创建一个env实例

```scala
private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {
def create(config: RpcEnvConfig): RpcEnv = {
        val sparkConf = config.conf
        // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
        // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
        // 用于 Rpc传输对象时的序列化
        val javaSerializerInstance: JavaSerializerInstance = new JavaSerializer(sparkConf)
            .newInstance()
            .asInstanceOf[JavaSerializerInstance]
        // 实例化 NettyRpcEnv
        val nettyEnv = new NettyRpcEnv(
            sparkConf,
            javaSerializerInstance,
            config.advertiseAddress,
            config.securityManager)
        if (!config.clientMode) {
            // 定义 NettyRpcEnv 的启动函数
            val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
                nettyEnv.startServer(config.bindAddress, actualPort)
                (nettyEnv, nettyEnv.address.port)
            }
            try {
                // 启动 NettyRpcEnv
                //返回值是tuple3
                Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
            } catch {
                case NonFatal(e) =>
                    nettyEnv.shutdown()
                    throw e
            }
        }
        nettyEnv
    }
}

```

### 1.2.3 new Master

```scala
private[deploy] class Master(
                                override val rpcEnv: RpcEnv,
                                address: RpcAddress,
                                webUiPort: Int,
                                val securityMgr: SecurityManager,
                                val conf: SparkConf)
    extends ThreadSafeRpcEndpoint with Logging with LeaderElectable{....
         //创建了一个线程池，在后台运行只能跑一个线程的线程池
 private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
       .....
   //封装work的信息2个map 一个set 内容差不多，只是kV的好取值！                                    
   val workers = new HashSet[WorkerInfo]
    private val idToWorker = new HashMap[String, WorkerInfo]
    private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]
                                                                   }
```

> Master是一个线程安全的RPCEndpoint

### 1.2.4 endpoint启动后向自己发送消息触发 onstart

```scala
// 向 Master 的通信终端发法请求，获取 BoundPortsResponse 对象
// BoundPortsResponse 是一个样例类包含三个属性: rpcEndpointPort webUIPort restPort
val portsResponse: BoundPortsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
(rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
```



### 1.2.5 onStart

```scala
// 按照固定的频率去启动线程来检查 Worker 是否超时. 其实就是给自己发信息: CheckForWorkerTimeOut
// 默认是每分钟检查一次.
checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
    }
}, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
```

> 启动后没一分钟检查一次，判断已注册的work是否超时！ 60S！！！！！！！

### 1.2.6receive

Onstart 方法中给master send了一个信息   self.send(CheckForWorkerTimeOut)，一般信息都是由receive接收，所以触发! receive是一个偏函数！

```scala
verride def receive: PartialFunction[Any, Unit] = {
case ...
    //called
 case CheckForWorkerTimeOut =>
    timeOutDeadWorkers()

}
```

```scala
private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
    for (worker <- toRemove) {
        if (worker.state != WorkerState.DEAD) {
            logWarning("Removing %s because we got no heartbeat in %d seconds".format(
                worker.id, WORKER_TIMEOUT_MS / 1000))
            removeWorker(worker)
        } else {
            if (
                //如果超时时间大于16分钟那么将work移除 （默认值）
                worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
                workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
            }
        }
    }
}
```

如果机器老化严重，网络通讯差，可以考虑将park.worker.timeout和spark.dead.worker.persistence 调大些！

### 1.2.7onStop 清理资源

```scala
....
  if (recoveryCompletionTask != null) {
            recoveryCompletionTask.cancel(true)
        }
        if (checkForWorkerTimeOutTask != null) {
            checkForWorkerTimeOutTask.cancel(true)
        }
...
```



### 1.3.1 worker启动

执行脚本后走到workerMain中

```scala
def main(argStrings: Array[String]) {
    Utils.initDaemon(log)
    val conf = new SparkConf
    // 构建解析参数的实例
    val args = new WorkerArguments(argStrings, conf)
    // 启动 Rpc 环境和 Rpc 终端
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
        args.memory, args.masters, args.workDir, conf = conf)
    rpcEnv.awaitTermination()
}
```

和master的极其相似！在脚本传递参数的过程中把master的地址，和master名称等信息也一并传递了过来，对这些参数进行了封装！



```scala
def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              webUiPort: Int,
                              cores: Int,
                              memory: Int,
                              masterUrls: Array[String],
                              workDir: String,
                              workerNumber: Option[Int] = None,
                              conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val securityMgr = new SecurityManager(conf)
    // 创建 RpcEnv 实例  参数: "sparkWorker", "hadoop201", 8081, conf, securityMgr
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    // 根据传入 masterUrls 得到 masterAddresses.  就是从命令行中传递过来的 Master 地址
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
    // 最终实例化 Worker 得到 Worker 的 RpcEndpoint
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
        masterAddresses, ENDPOINT_NAME, workDir, conf, securityMgr))
    rpcEnv
}
```

也调用了startRpcEnvAndEndpoint 唯一的区别是用mater的url来获取了一个地址，然后对其进行封装！ 封装成为一个env！相应的调用onstart方法

### 1.3.2 onstart

```scala
override def onStart() {
 // 第一次启动断言 Worker 未注册
        assert(!registered)
        ......
         // 创建工作目录
        createWorkDir()
        // 启动 shuffle 服务
        shuffleService.startIfEnabled()
        // Worker的 WebUI
        webUi = new WorkerWebUI(this, workDir, webUiPort)
        webUi.bind()
       workerWebUiUrl = s"http://$publicAddress:${webUi.boundPort}"
        // 向 Master 注册 Worker
        registerWithMaster()
        .....
        .....
}
```

### 1.3.3 worker向master注册

```scala
private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
        case None =>
            registered = false
            // 向所有的 Master 注册
            registerMasterFutures = tryRegisterAllMasters()
            connectionAttemptCount = 0
            registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
                new Runnable {
                    override def run(): Unit = Utils.tryLogNonFatalError {
                        Option(self).foreach(_.send(ReregisterWithMaster))
                    }
                },
                // [0.5, 1.5) * 10
                INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
                INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
                TimeUnit.SECONDS))
        case Some(_) =>
            logInfo("Not spawning another attempt to register with the master, since there is an" +
                " attempt scheduled already.")
    }
}
```



#### 1.3.3.1 tryRegisterAllMaster  获取ref对象！

```scala
private def tryRegisterAllMasters(): Array[JFuture[_]] = {
        masterRpcAddresses.map { masterAddress =>
            // 从线程池中启动线程来执行 Worker 向 Master 注册
            registerMasterThreadPool.submit(new Runnable {
                override def run(): Unit = {
                    try {
                        logInfo("Connecting to master " + masterAddress + "...")
      // 根据 Master 的地址得到一个 Master 的 RpcEndpointRef, 然后就可以和 Master 进行通讯了.
      val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
                        // 向 Master 注册
                        registerWithMaster(masterEndpoint)
                    } catch {
                        case ie: InterruptedException => // Cancelled
                        case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                    }
                }
            })
        }
    }
```



返回了一个registrationRetryTimer

```scala
override def receive: PartialFunction[Any, Unit] = synchronized {
    case SendHeartbeat =>
        if (connected) {
            sendToMaster(Heartbeat(workerId, self))
        }
```

### 1.3.4 worker向Master发送自己的信息

```scala
private def registerWithMaster(masterEndpoint: RpcEndpointRef): Unit = {
    // 向 Master 对应的 receiveAndReply 方法发送信息
    // 信息的类型是 RegisterWorker, 包括 Worker 的一些信息: id, 主机地址, 端口号, 内存, webUi
    masterEndpoint.ask[RegisterWorkerResponse](RegisterWorker(
        workerId, host, port, self, cores, memory, workerWebUiUrl))
        .onComplete {
            // This is a very fast action so we can use "ThreadUtils.sameThread"
            case Success(msg) =>
                Utils.tryLogNonFatalError {
                    handleRegisterResponse(msg)
                }
            case Failure(e) =>
                logError(s"Cannot register with master: ${masterEndpoint.address}", e)
                System.exit(1)
        }(ThreadUtils.sameThread)
}
---------------------
//注册成功以后调用  handleRegisterResponse
 registered = true
                // 更新 Master
                changeMaster(masterRef, masterWebUiUrl)
                // 通知自己给 Master 发送心跳信息  默认 1 分钟 4 次
                forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
                    override def run(): Unit = Utils.tryLogNonFatalError {
                        self.send(SendHeartbeat)
                    }
                }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)  //"spark.worker.timeout", 60) * 1000 / 4

```

# 二、yarn模式下 内核分析

## 2.1.1进入sparksubmit

submit一个wordcount 调用了SparkSubmit的main方法

```scala
 def main(args: Array[String]): Unit = {
        /*
            参数
            --master yarn
            --deploy-mode cluster
            --class org.apache.spark.examples.SparkPi
            ./examples/jars/spark-examples_2.11-2.1.1.jar 100
        */
        val appArgs = new SparkSubmitArguments(args)
        if (appArgs.verbose) {
            // scalastyle:off println
            printStream.println(appArgs)
            // scalastyle:on println
        }

        appArgs.action match {
                // 如果没有指定 action, 则 action 的默认值是:
            case SparkSubmitAction.SUBMIT => submit(appArgs)
            case SparkSubmitAction.KILL => kill(appArgs)
            case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
        }
    }
```

模式匹配到submit

```scala
private def submit(args: SparkSubmitArguments): Unit = {
    // 准备提交环境
    val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)

    def doRunMain(): Unit = {
        ......
            runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
      
    }
    
}
```

```scala
//605行 如果depoly mode是cluster模式的话
childMainClass = "org.apache.spark.deploy.yarn.Client"
 //454
sysProps("SPARK_SUBMIT") = "true"

//650   Ignore invalid spark.driver.host in cluster modes.
       if (deployMode == CLUSTER) {
            sysProps -= "spark.driver.host"
        }


//509行 如果是client模式 childmainclass为参数里传来的mainclass即userclass
 if (deployMode == CLIENT || isYarnCluster) {
     //确定childmainclass为usermainclass
     childMainClass = args.mainClass
            if (isUserJar(args.primaryResource)) {
                childClasspath += args.primaryResource
            }
            if (args.jars != null) {
                childClasspath ++= args.jars.split(",")
            }
        }

        if (deployMode == CLIENT) {
            if (args.childArgs != null) {
                //将参数封装 510行左右
                childArgs ++= args.childArgs
            }
        }

//执行完以后就直接返回tuple4
  (childArgs, childClasspath, sysProps, childMainClass)
```

> 上面确定了反射mainClass执行的时候加载的类到底是Client的main方法还是user的mainClass

**runMain**

```scala
//732 反射出 client类
mainClass = Utils.classForName(childMainClass)
// 反射出来 Client 的 main 方法  759
  val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)        
//776 执行client类的main方法
 mainMethod.invoke(null, childArgs.toArray)
```

> 这里就只谈下如果反射的是用户的mainclass会怎么样，后面则的对cluster模式的延续说明！
>
> 这里的childArgs是Client类510行左右封装的数据，是用户执行提交命令时提交的用户类参数！



```scala
// 231 rows
doRunMain()
//732 反射出 用户类
mainClass = Utils.classForName(childMainClass)
// 反射出来 用户类 的 main 方法  759
  val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)        
//776 执行用户类的main方法
 mainMethod.invoke(null, childArgs.toArray)
```

> 用户类中开始对sparkcontext的创建，然后操作sc对象来对数据进行处理！
>
> 所以这里我们要到sparkcontext里看看

### 2.1.1.1 sparkcontext （client模式）

```scala
class SparkEnv(
                  val executorId: String,
                  private[spark] val rpcEnv: RpcEnv,//对RPCENV的再次封装
                  .....)
         
//394 判断参数 即 用户的sparkcontext参数
        if (!_conf.contains("spark.master")) {
            throw new SparkException("A master URL must be set in your configuration")
        }
        if (!_conf.contains("spark.app.name")) {
            throw new SparkException("An application name must be set in your configuration")
        }
//449
 _env = createSparkEnv(_conf, isLocal, listenerBus)
//268
  SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master))

```

![image-20200527081733186](C:\Users\swime\AppData\Roaming\Typora\typora-user-images\image-20200527081733186.png)

```scala
//517
// 创建 TaskSheduler 返回(SchedulerBackend, TaskScheduler)
val (sched, ts): (SchedulerBackend, TaskScheduler) = SparkContext.createTaskScheduler(this, master, deployMode)

```

> 代码运行到这里后会在submit进程里面启动一个 YarnClientSchedulerBackend （CoarseGrainedSchedulerBackend的子类 即也是一个进程）

## 2.1.2  进入client的main方法

```scala
def main(argStrings: Array[String]) {
    if (!sys.props.contains("SPARK_SUBMIT")) {
        logWarning("WARNING: This client is deprecated and will be removed in a " +
            "future version of Spark. Use ./bin/spark-submit with \"--master yarn\"")
    }

    // Set an env variable indicating we are running in YARN mode.
    // Note that any env variable with the SPARK_ prefix gets propagated to all (remote) processes
    // 设置环境变量 SPARK_YARN_MODE 表示运行在 YARN mode
    // 注意: 任何带有 SPARK_ 前缀的环境变量都会分发到所有与的进程, 也包括远程进程
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf
    // SparkSubmit would use yarn cache to distribute files & jars in yarn mode,
    // so remove them from sparkConf here for yarn mode.
    sparkConf.remove("spark.jars")
    sparkConf.remove("spark.files")
    // 对传递来的参数进一步封装
    val args = new ClientArguments(argStrings)
    new Client(args, sparkConf).run()
}
```

> jars和files应该通过分布式缓存加载，所以这里直接可以在conf中移除。对client参数进行封装！然后new一个client将参数conf传入后执行run方法！

## 2.1.3 创建client

```scala
private[spark] class Client(
                               val args: ClientArguments,
                               val hadoopConf: Configuration,
                               val sparkConf: SparkConf)
    extends Logging {

    import Client._
    import YarnSparkHadoopUtil._

    def this(clientArgs: ClientArguments, spConf: SparkConf) =
        this(clientArgs, SparkHadoopUtil.get.newConfiguration(spConf), spConf)

    private val yarnClient = YarnClient.createYarnClient
    private val yarnConf = new YarnConfiguration(hadoopConf)

    private val isClusterMode = sparkConf.get("spark.submit.deployMode", "client") == "cluster"
.... 
    
 //554  模式匹配  把jars通过分布式缓存加载，但是模式必须是cluster模式！
         sparkConf.get(SPARK_JARS) match  

}
```

> 从上面的构造方法中可以看出来，启动的是一个yarnclient 在执行 submitApplication()会对client进行初始化和启动

## 2.1.4执行run方法

```scala
/**
  * Submit an application to the ResourceManager.
  * If set spark.yarn.submit.waitAppCompletion to true, it will stay alive
  * reporting the application's status until the application has exited for any reason.
  * Otherwise, the client process will exit after submission.
  * If the application finishes with a failed, killed, or undefined status,
  * throw an appropriate SparkException.
  *
  * 向 RM 提交应用
  *
  * 如果设置了spark.yarn.submit.waitAppCompletion 为 true,
  * 则 Client 会一直保持 alive 来报告应用的状态, 直到应用退出
  * 否则, 应用提交之后, 客户端会自动退出.
  *
  * 如果应用由于失败, 被杀或者其他未知转态, 则会抛出异常
  */
def run(): Unit = {
    // 提交应用, 返回应用的 id
    this.appId = submitApplication()
    if (!launcherBackend.isConnected() && fireAndForget) {
        val report = getApplicationReport(appId)
        val state = report.getYarnApplicationState
        logInfo(s"Application report for $appId (state: $state)")
        logInfo(formatReportDetails(report))
        if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
            throw new SparkException(s"Application $appId finished with status: $state")
        }
    } else {
        val (yarnApplicationState, finalApplicationStatus) = monitorApplication(appId)
        if (yarnApplicationState == YarnApplicationState.FAILED ||
            finalApplicationStatus == FinalApplicationStatus.FAILED) {
            throw new SparkException(s"Application $appId finished with failed status")
        }
        if (yarnApplicationState == YarnApplicationState.KILLED ||
            finalApplicationStatus == FinalApplicationStatus.KILLED) {
            throw new SparkException(s"Application $appId is killed")
        }
        if (finalApplicationStatus == FinalApplicationStatus.UNDEFINED) {
            throw new SparkException(s"The final status of application $appId is undefined")
        }
    }
}
```

>  submitApplication() 是这里最关键的方法。他不仅对client进行初始化和启动。还对RM进行了通讯，并且创建了一个application，获取到 applicationID等操作

```scala
//149
def submitApplication(): ApplicationId = {
    var appId: ApplicationId = null
    try {
        launcherBackend.connect()
        // Setup the credentials before doing anything else,
        // so we have don't have issues at any point.
        setupCredentials()
        // 初始化 yarn 客户端
        yarnClient.init(yarnConf)
        // 启动 yarn 客户端
        yarnClient.start()

        logInfo("Requesting a new application from cluster with %d NodeManagers"
            .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

        // Get a new application from our RM
        // 从 RM 创建一个应用程序
        val newApp = yarnClient.createApplication()
        val newAppResponse = newApp.getNewApplicationResponse()
        // 获取到 applicationID
        appId = newAppResponse.getApplicationId()
        reportLauncherState(SparkAppHandle.State.SUBMITTED)
        launcherBackend.setAppId(appId.toString)

        new CallerContext("CLIENT", Option(appId.toString)).setCurrentContext()

        // Verify whether the cluster has enough resources for our AM
        verifyClusterResources(newAppResponse)

        // Set up the appropriate contexts to launch our AM
        // 设置正确的上下文对象来启动 ApplicationMaster 
        //核心代码！
        val containerContext = createContainerLaunchContext(newAppResponse)
        // 创建应用程序提交任务上下文
        val appContext = createApplicationSubmissionContext(newApp, containerContext)

        // Finally, submit and monitor the application
        logInfo(s"Submitting application $appId to ResourceManager")
        // 提交应用给 ResourceManager
       //187 rows
        yarnClient.submitApplication(appContext)
        appId
    } catch {
        case e: Throwable =>
            if (appId != null) {
                cleanupStagingDir(appId)
            }
            throw e
    }
}
```

## 2.1.5 提交指令到RM并创建AM

提交了指令（创建AM的指令--cluster模式）,开始进行对AM的创建及初始化。所以我们直接到Application一探究竟。





