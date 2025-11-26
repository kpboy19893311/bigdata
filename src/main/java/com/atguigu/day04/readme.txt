
富函数
    在对流中数据进行处理的时候，算子需要将处理的逻辑作为参数进行传递
    默认情况下，参数都是以接口的形式进行声明的
    每个处理逻辑接口都有一个对应的实现类----Rich + 接口名
    这样的类称之为富函数类
富函数和普通的处理函数类多了如下工嗯呢
    可以通过运行时上下文getRuntimeContext() 获取更丰富的信息
    带有声明周期方法open和close
    以map算子为例        map---MapFunction---RichMapFunction
        open
            每一个算子子任务执行初始化的时候调用一次
            一般用于创建连接
        map
            流中每来一条数据都会被调用一次
        close
            每一个算子子任务执行完毕的时候调用一次
            一般用于资源的释放
            注意：如果处理的是无界数据，程序用于不会结束，close方法就不会被调用

分区"算子"
    shuffle
    rebalance
    rescale
    broadcast
    global
    keyby
    forward
    custom
分流
    filter
        本质工作是做数据过滤
        如果通过filter进行分流的话，同一条流可能被重复处理多次，效率低
        所以分流的时候，虽然filter可以实现，但是一般不用
    侧输出流
        原理:在对流中数据进行处理的时候，给元素打标签
        所以在使用侧输出流的时候，需要先创建标签对象
        创建标签对象的时候，需要注意，会存在泛型擦除问题
            以匿名内部类方式创建对象
            在创建对象的时候，指定泛型类型
        主流.getSideOutput() 获取侧输出流
合流
    union
        可以合并2条或者多条流
        参与合并的流的数据类型必须一致
    connect
        对2条流进行合并
        参与合并的流的数据类型可以不一致
        合并之后得到结果是 ConnectedStreams
        需要调用map、flatMap、process对连接后的数据进行处理
        ConnectedStreams也可以直接调用.keyBy()进行按键分组的操作，得到的还是一个ConnectedStreams

在Flink1.12前
    从数据源读取数据
        env.addSource()
    将流的数据向外部系统写
        流.addSink()
从哪Flink1.12开始
    从数据源读取数据
        env.fromSource
    将流的数据向外部系统写
        流.sinkTo
Sink输出算子
    file
    kafka
        如果Flink从kafka主题中读取数据的话，创建KafkaSource
            要想保证读取的一致性，需要手动维护偏移量
                kafkaSource->KafkaSourceReader->SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit
        如果Flink向kafka主题中写入数据的话，创建KafkaSink
            要想保证写入的一致性
                ack=-1
                开启幂等
                事务(2pc)
                    设置事务一致性级别
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    设置事务id前缀
                         .setTransactionalIdPrefix("xxxx")
                    开启检查点
                        env.enableCheckpointing(5000L);
                    设置事务的超时时间
                        检查点超时时间 < 事务的超时时间 <= 事务最大超时时间(15min)
                    在消费端需要设置消费的隔离级别为读已提交


