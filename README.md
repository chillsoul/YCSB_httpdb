# YCSB HTTP Database Benchmark 


基于YCSB提供的Workload、DB接口修改，目的是扩展YCSB通过HTTP API访问自定义的索引结构。

通过调用`site.ycsb.db.MyClient`利用`HttpClient`实现HTTP访问，测试操作较多时建议在Linux上运行，Windows可能出现socket端口不足的问题。

例如，要访问LevelDB数据库，可以在任意语言的LevelDB Client的基础上封装HTTP API，然后修改本项目实现支持。

项目中删除了一些用不到的数据库binding使项目结构简单，留下了Redis的官方实现以供二次开发参考。

## 二次开发

修改工作负载：可以参照`site.ycsb.workload.CoreWorkload`修改`site.ycsb.workload.MyWorkload`，`MyWorkload`代码中默认为基于个人需要修改的key和value为int类型。

修改HTTP访问：修改MyDB文件夹的`site.ycsb.db.MyClient`。默认只实现了Insert和Read操作。




### Thanks to:

#### YCSB 0.17.0

#### Apache HttpComponents 4.5.13

