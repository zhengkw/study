# Scala集合操作算子与sparkRdd操作算子区别

## 单value操作

foreach 遍历  

| method   | 调用对象 | option     |
| -------- | -------- | ---------- |
| iterator | list     | 迭代器     |
| foreach  | list     | 遍历       |
| mkstring | list     | 指定分隔符 |
| contain  | list     | 是否包含   |
| sum      | list     | 求和       |
| product  | list     | 求乘积     |
| max      | list     | 最大       |
| min      | list     | 最小       |

| method       | 调用对象 | option               |
| ------------ | -------- | -------------------- |
| head         | list     | 取头部               |
| tail         | list     | 取除头部以外的所有   |
| last         | list     | 取最后一个           |
| init         | list     | 取除开最后一个的所有 |
| reverse      | list     | 元素反转             |
| take(n)      | list     | 取前N个              |
| takeRight(n) | list     | 取后N个              |
| drop(n)      | list     | 去掉前n个            |
| dropRight    | List     | 去掉后N个            |



## **单value高级**

spark中的glom()根据分区来聚合成一个array，一个分区一个array最后将分区聚合成一个集合，即泛型为array的rdd

spark中的 foldbykey 中的zero类型必须是kv类型中的v一致！而且zero只在分区内聚合参与一次运算（预聚合map端）！分区间聚合不参与运算（reduce端聚合）！对于一个key，zero最多参与N次，N为分区数，因为有的分区可能没有该k！

而scala中的zero可以是任意类型，而且影响了输出类型，输出类型必须和zero一致！











## 双value操作，交并补差

scala算子针对的是集合，spark算子针对的是Rdd。spark更为全局化！

| method           | 调用对象 | option                                                       |
| ---------------- | -------- | ------------------------------------------------------------ |
| union(list2)     | list     | 并集                                                         |
| intersect(list2) | list     | 交集                                                         |
| diff(list2)      | list     | 差集                                                         |
| zip(list2)       | list     | 拉链                                                         |
| zipAll(list2)    | list     | 拉链补位！                                                   |
| zipWithIndex     | list     | 与自身索引拉链                                               |
| sliding          | list     | .sliding(3, 1)每3个为一个组滑动1个步长。当往右没有新元素停止 |

**注意在scala中没有笛卡尔积！**

## spark行动算子

reduce,fold,aggreate

对rdd操作如果返回值是rdd那么为转换算子，如果返回值不是rdd，则为行动算子。

## job，stage，task（线程级）产生条件

遇到一个shuffle算子（会自动缓存运算结果）就会产生一个新的stage，一个分区产生一个task，一个行动算子产生一个job！

