
一致性级别
    至多一次 At-Most-Once
        可能会丢数据
    至少一次 At-Least-Once
        可能会出现数据重复
    精准一次 Exactly-Once

Flink端到端的一致性
    source端(数据源)
        重置偏移量
    Flink框架本身
        检查点
    sink端(目的地)
        幂等
        事务
            预写日志
            两阶段提交 2pc
两阶段提交
    当第一条数据或者barrier到达sink的时候，开启事务
    将数据写到外部系统---标记为预提交
    当检查点完成之后，真正提交事务

结合实际情况，说说Flink的端到端的一致性
    Source端:从kafka读取数据
        kafka本身具备重置偏移量能力
        KafkaSource->KafkaSourceReader->手动维护消费的偏移量->状态
    Flink框架:检查点
    Sink端:向kafka写数据
        KafkaSink->KafkaWriter->实现了2pc接口
        需要做如下设置:
            开启检查点
            设置一致性级别为精准一次
            设置事务id的前缀
            检查点超时时间 < 事务超时时间 <= 事务最大超时时间(默认15min)
            在消费端设置消费的隔离级别为读已提交



