# 一. redis的安装

 1. 解压
 2. 安装gcc
	gcc-c++-4.4.7-23.el6.x86_64
```shell
yum install -y gcc-c++
```

3.编译redis

a: 进入src   b: 编译make

```shell
cd src
make
```

4.安装	
要么使用sudo, 要么直接切换到root

```shell
make install
```

5.脚本所在

> /usr/local/bin

# 二. redis 基本操作



> 开启服务器:
>
> redis-server [配置文件]
> 			
>
> 如果没有配置文件, 使用默认配置    端口号6379
> 	
>
>   开启客户端
> 	redis-cli   [--raw]   raw是解决中文无法显示的问题
> 	
>
> 测试联通:
> 	ping   返回pong
> 	
>
>  退出客户端
> 	exit
>
> 关闭服务器 在客户端输入
>
>  shutdown
>
>    碰到的问题
> 	不能优雅的关闭服务器?
> 	  在关闭服务器的时候, 需要内存的数据默认存储到rdb文件中.
> 	  rdb默认是在启动服务器时的当前目录中(注意目录所属权限有没有写入权限！)
>
>    	  1. 使用root用户启动
>            或者
>    	  2. 启动的时候切换到atgugiu有权限的目录
>          或者
>    	  3. 指定固定死rdb的写的目录(配置)





# 三. 数据库操作

   redis也有数据库的概念
   默认有16个数据库(可以配置)  编号0-15
   默认进入到了0号数据库

   1. 切换数据库

```shell
 select num
```
   2. flushdb  清空当前数据库
   3. flushall 清空所有数据库
   4. dbsize 查看当前数据中的键值对的个数

# 四. key的基本操作

​	key   
​		key不能重复. 如果重复定义, 则后面值的会覆盖前面的值
​		set k1 v1
​		set k1 v11  最终k1的值是v11
​		

		1. keys pattern    获取满足表达式所有key
		2. type k1  获取k1的value数据类型
		3. EXISTS k1 判断key是否存在.
				如果存在返回 1, 不存在返回0
		4. del k1 删除指定的key
		5. RANDOMKEY 随机获取key
		
		6. 给key设置过期时间
			redis所有的数据在工作的时候都是在内存中, 有些数据, 有一定的时效性. 当超过一个时间
			自动从内存删除.
			如果没有设置过期时间, 则永远有效.
			
			expire key seconds
	    7. ttl key 查看key的过期时间
			返回时 -2 表示已经过期
			返回时 -1 永不过期
			
		8. 给key重新命名
			RENAME k4 k44
			
			如果k44已经存在, 则会用k4的值去覆盖k44的值
		9. RENAMENX k6 k3
			给key命名. 只有k3不存在才会成功


​     	
​	
	value
		五大数据类型

# 五 基本数据类型

### 特别注意

> **由于redis用c写的，所以在判断是否存在时沿用C的判断方式！0表示false，非0表示true！**

##   5.1  string

​		set k1 v1  添加或修改k1
​		setnx k1 v11 只有当k1不存在的时候, 才会成功
​		get k1 获取k1的值
​		APPEND k1 text 在k1的值的尾部追加新的数据
​		STRLEN k1 获取value的长度
​		

		INCR k1 k1的值 +1, 注意value必须是数字
		DECR k1 k1 值 -1
		INCRBY k1 10 把k1的值 +10
		DECRBY k1 3 把k1的值 -3
		mset a1 b1 a2 b2 a3 b3  同时设置多个键值对
		mget k1 k2 a1 a2 同时获取4个key的值
		GETRANGE k1 1 3 获取 key的指定范围的值  [1, 3] 前后都是闭区间
		SETRANGE k1 2 xx 从指定的位置开始替换
			不影响替换不到的位置
			底层存储字符串时候使用的数组
			
			[0,0,0,0,0,0]
	    GETSET k1 v1 设置k1值, 并把k1的旧值返回
		
		SETEX k100 20 v200 设置值的同时, 添加过期时间

##   5.2 list

​	 是一个链表. 双向链表, 链表的两端都可以高效的操作.
​	 链表的的元素还有索引
​	 

	 lputh  k  10   从左侧插入元素
	 lputh  k  20
	 rpush list1 1000 从有侧插入   


​		   
		   20  10   100  200   300
		   
	 llen list1  测试list1的长度
	 lindex list1 -4 获取指定索引位置的值
		每个元素有两个索引, 用哪个都可以
	
	 lpop list1	从左边删除一个元素
	 rpop list1	从右边删除一个元素
	 
	 LRANGE list1 -2 -1  获取指定索引范围内的所有元素 [-2, -1]
			正的索引和负的索引可以配合使用
	LINSERT list1 before 100 20  从左边数在第一个`100的前面插入20
	LINSERT list1 after 100 40  从左边数在第一个`100的后面插入40
	LREM list1 2 100 从左边开始删除最多2个100
	lset list1 1 300 把下标为1的元素更换陈个300
	LTRIM list1 2 -2 保留指定索引范围的数据
	RPOPLPUSH list1 list2 把 list2的右边弹出数据, 然后push到list2的左边.

​	

## 	5.3 set

```
sadd set1 a b c d 向set中添加元素
SMEMBERS set1 set中所有 成员
SISMEMBER set1 a 判断元素在set中是否存在
1 表示存在
0 表示不存在
SREM set1 a 删除set中指定的元素

SCARD set1 返回set集合中元素的个数
SPOP set1 2 随机删除两个元素
SRANDMEMBER set1 2 从set中, 返回2个随机的元素 用于抽奖
SINTER set1 set2 计算set的交集
SINTERSTORE set3 set1 set2 对set1和set2求交集, 然后存储到set3中.
SUNION set1 set2 并集
SDIFF set1 set2 计算set1和set2差集
```

 

## 	5.4hash操作

```
key value(hash)
field value

hset hash1 name lisi 向hash中添加 field和value
hsetnx hash1 name ww 如果field name 不存在, 成功. 否则失败


hget hash1 name 获取指定field的值
HGETALL hash1 获取hash1下面所有的field和value
HMSET hash1 f1 v1 f2 v2 f3 v3 一次设置多个field value

hkeys hash1 获取指定key的hash的所有的field
hvals hash1 获取指定key的hash的所有value
HLEN hash1 获取field的个数
HMGET hash1 f1 f2 获取多个field的值
HEXISTS hash1 f1 判断f1这个field是否存在. 返回1表示存在, 0 表示不存在
HINCRBY hash1 n1 100 对n1这个field的值+100, 如果不存在, 则直接把100赋值给field	 
```

​	 
​	 

## 5.5 zset

```
比set多了一个分数, 每个元素多一个分数

zadd z1 10 a 向z1这个zset中添加元素. 10是分数 a是元素只
zcard 返回元素的个数

ZRANGE z1 0 -1 ][withscores] 返回指定范围的元素 按照score升序的顺序
ZREVRANGE z1 0 -1 withscores 降序

ZRANGEBYSCORE z1 10 15 withscores 按照分数的范围获取元素 升序
ZREVRANGEBYSCORE z1 15 10 withscores 降序

ZRANGEBYSCORE z1 10 15 limit 1 2 从结果中获取的指定偏移量开始获取值

ZSCORE z1 a 获取指定值的分数
ZCOUNT z1 10 15 返回指定分数段之间的元素个数
ZINCRBY z1 100 aa 给aa的分数+100		
```


​		
​		




















​	  
​		
​		
​		
​		
​		
​		
​		
​		