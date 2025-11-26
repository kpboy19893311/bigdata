
Flink
    是一个框架
    是一个分布式处理引擎
    主要对有界以及无界数据进行处理
    是有状态(程序运行的中间结果)
离线和实时
    离线
        批处理
        在处理数据前，数据是固定的，在处理的过程中，数据不会发生变化
        数据量大
        处理时间长
        T + 1
    实时
        流处理
        在处理数据前，数据不是固定的，在处理的过程中，数据源源不断的进来
        数据量小
        处理时间短
        T + 0
Flink和Sparkstreaming对比
                        Flink                       Sparkstreaming
    处理方式            纯流式处理                       微批次的流处理
    窗口               灵活                            不够灵活
    时间语义            事件时间、处理时间                 处理时间
    状态               有状态                           无状态
    流式SQL            支持(流表的二象性)                 不支持

Flink分层API
    SQL
    TableAPI
    DataStream|DataSet   从Flink1.12后，DataSetAPI基本不用，因为DataStream可以实现流批一体
    处理函数-process

集群角色
    客户端
        主要负责作业的提交操作
    JobManager
        主要对作业进行调度管理，将作业上的任务交给TaskManager执行
    TaskManager
        执行任务

Flink运行模式(指的是Flink程序在什么地方运行)
    standalone、yarn、k8s、mesos....

Flink部署模式
    会话模式-session
    单作业模式-per-job
    应用模式-application

Flink-On-yarn
    会话模式-session
        需要提前启动集群
            bin/yarn-session.sh -nm test
        可以通过WebUI提交作业
        可以通过命令行提交作业
            bin/flink run -d -c 类 jar包路径
        当取消作业的时候对集群没有影响的
    单作业模式-per-job
        不需要提前启动集群
        只能通过命令行提交作业
            bin/flink run -d -t yarn-per-job -c 类 jar包路径
        当取消作业的时候集群也会停止
    应用模式-application
        不需要提前启动集群
        只能通过命令行提交作业
            bin/flink run-application -d -t yarn-application -c 类 jar包路径
        当取消作业的时候集群也会停止

    会话模式和per-job模式一些图的转换操作是在客户端完成
    应用模式一些图的转换操作不是在客户端完成，而是在服务器端的JobManager中完成