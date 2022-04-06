多读上下层结构有关代码，获取结构相关全貌

## 调用链追溯

使用ide查看definition、reference，对于接口使用需要查看implementation

通过查看构造函数的调用和引用、可以较快确定类上下层关系

## 异步追踪

对于异步的管道、goroutine等， 通常可以通过查询引用相关的new或start函数获取sender和receiver关系

## 调试

日志中可以通过获取堆栈， 来输出调用者相关信息

设置各种条件稳定复现, 通过log定位问题， 慢慢可以通过log去猜测问题并设计场景所在处加速debug

go defer作用域为协程


remove peer 如果是leader，会出现leader提交commit 并且 apply后，如果之前发送的信息丢失，则会导致其它peer没有正常删除。

输出一些合适的日志