多读上下层结构有关代码，获取结构相关全貌

## 调用链追溯

使用ide查看definition、reference，对于接口使用需要查看implementation

通过查看构造函数的调用和引用、可以较快确定类上下层关系

## 异步追踪

对于异步的管道、goroutine等， 通常可以通过查询引用相关的new或start函数获取sender和receiver关系

