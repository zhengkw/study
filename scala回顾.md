# scala纯函数

```
纯函数:
    1. 没有副作用，以下均为副作用！
        - 向控制台打印东西
        - 向文件写入数据
        - 更改了外部的变量（append）
        - 向其他存储系统写入数据
        
    2. 引用透明
        这个函数执行的结果, 只和参数有关, 不依赖于其他的任何值!
        
 好处:
    1. 不用考虑线程安全问题
    2. 可以通过缓存技术, 来提升计算的效率
    
过程:
    没有返回值, 只有符副作用的函数, 就叫过程
```

# Scala中下划线的高频作用

```scala
/*

1. 中置运算符
   1 + 2
2. 一元运算符
   后置
       1 toString
   前置
       +5
       -5
       !false  取反
       ~2  按位取反
3. apply方法
   任何对象都可以 调用
   对象(...)   // 对象.apply(...)
   伴生对象
   普通的对象
   函数对象
4. update方法
       user(0) = 100 // user.update(0, 100)

5. _ 总结
   https://stackoverflow.com/questions/8000903/what-are-all-the-uses-of-an-underscore-in-scala
   1. 导包, 通配符 _
          import java.util.Math._
   2. 屏蔽类
          import java.util.{HashMap => _, _}
   3. 给可变参数传值的时候, 展开
          foo(arr:_*)
   4. 元组元素访问
          t._1
   5. 函数参数的占位符
          reduce(_ + _)
   6. 方法转函数
      val f = foo _

   7. 给属性设置默认值
          class A{
              var a: Int = _  // 给a赋值默认的0
          }
   8. 模式匹配的通配符
      case _ =>   // 匹配所有

   9. 模式匹配集合
      Array(a, b, rest@_*)

   10. 部分应用函数
       math.pow(_, 2)

   11. 在定义标识符的时候, 把字符和运算符隔开
           val a_+ = 10
           a+ // 错误
   12. List[_]
           泛型通配符

   13. 自身类型
       _: Exception =>


 */
```



# Scala泛型界定

## Scala视图界定

视图界定是对象***隐式转换函数***的封装!!!

```scala

def max[T <% Ordered[T]](x: T, y: T) :T = {
    if(x > y) x
    else y
}


/*def max[T](x: T, y: T)(implicit f:  T => Ordered[T]) :T = {
        if(x > y) x
        else y
    }*/

```

## 上下文界定(泛型)

```scala
def max[T](x: T,y:T)(implicit ord: Ordering[T]) = {
    if(ord.gt(x, y)) x
    else y
}
```

```scala
//
def max[T:Ordering[T]](x: T,y:T) = {
  //Ordering[T]是个隐式值需要 从冥界召唤！
   // for summoning implicit values from the nether world -- TODO: when dependent method types are on by default, give this result type `e.type`, so that inliner has better chance of knowing which method to inline in calls like `implicitly[MatchingStrategy[Option]].zero`
    val ord:Ordering=implicitly [Ordering[T]]
    if(ord.gt(x, y)) x
    else y
}
```

```scala
[T:Ordering]
这就是泛型上下文.
表示: 一定有一个隐式值  Ordering[T] 类型的隐式值
```

上下文泛型的本质其实是对***隐式参数和隐式值***的封装!!!