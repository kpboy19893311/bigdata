Flink程序执行流程
    环境准备->Source->Transform->Sink->提交作业

环境准备
    创建本地执行环境
        StreamExecutionEnvironment.createLocalEnvironment();
    创建本地执行环境带WebUI
        需要在pom.xml文件中添加flink-rumtime-web依赖
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    创建远程执行环境
         StreamExecutionEnvironment.createRemoteEnvironment(远程JM服务器地址,端口号,jar)
    根据实际情况自动创建执行环境
        StreamExecutionEnvironment.getExecutionEnvironment()
        注意：要在自动创建环境的时候带webUI，需要指定WebUI的端口号
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment.getExecutionEnvironment(conf)
指定运行模式
    BATCH
        以批的形式对数据进行处理，一般处理的是有界数据
    STREAMING
        以流的形式对数据进行处理，一般处理的是无界数据(默认)
    AUTOMATIC
        根据数据源类型进行指定，如果有界数据-BATCH，如果无界数据-STREAMING
    在代码中指定
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    在提交作业的时候，在命令行参数中指定
        -Dexecution.runtime-mode=BATCH
提交作业
    同步提交
        env.execute()
    异步提交
        env.executeAsync()
源算子
    在Flink1.12前
        env.addSource(SourceFunction)
    从Flink1.12开始
        env.fromSource()
    从集合中读取数据
        env.fromCollection(集合)
        env.fromElements(元素....)
    从文件中读取数据
        env.readTextFile---已过时
        FileSource<String> source  = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(文件路径)).build();
        env.fromSource(source,水位线,source名)
    从kafka主题中读取数据(重点)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            //kafka集群地址
            .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
            //消费的主题
            .setTopics("first")
            //消费者组
            .setGroupId("test")
            // 从最早位点开始消费
            //.setStartingOffsets(OffsetsInitializer.earliest())
            // 从最末尾位点开始消费
            //.setStartingOffsets(OffsetsInitializer.latest());
            // 从时间戳大于等于指定时间戳（毫秒）的数据开始消费
            //.setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))
            // 从消费组提交的位点开始消费，不指定位点重置策略
            //.setStartingOffsets(OffsetsInitializer.committedOffsets())
            // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
            //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setStartingOffsets(OffsetsInitializer.latest())
            //反序列化方式
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        KafkaSource->KafkaSourceReader->SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit
            底层由Flink维护消费的偏移量，可以保证消费的精准一次
    从数据生成器读取数据(主要用于测试)
        new DataGeneratorSource<>(
                        //生成数据的逻辑
                        new GeneratorFunction<Long, String>() {
                            @Override
                            public String map(Long value) throws Exception {
                                return "数据->" + value;
                            }
                        },
                        //生成数据的条数
                        100,
                        //生成速率
                        RateLimiterStrategy.perSecond(10),
                        //生成数据的类型
                        TypeInformation.of(String.class)
                )
    自定义数据源
基本转换算子
    map
        通过return返回值将处理后的结果传递到下游，只能传递一次
    filter
    flatmap
        通过collector将数据传递下游，可以传递多次
聚合算子
    在使用聚合算子前，需要先进行keyBy
    keyBy后得到的流称之为分组流或者键控流
    sum、max、maxBy、min、minBy
    这些聚合算子底层维护了状态，用于存放中间的聚合结果

    reduce--规约聚合
        如果流中只有一条数据，reduce方法不会被调用
        reduce(value1,value2)
            value1-规约结果
            value2-新来的数据




