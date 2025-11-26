
JdbcSink
    SinkFunction sk = JdbcSink.sink(
        "要执行的insert语句",
        给问号占位符赋值,
        执行选项(攒批),
        连接选项(url、driver、user、password)
    );
    流.addSink(sk)
自定义Sink
    定义类 implements SinkFunction{
        invoke:将流中数据写到外部系统
    }

时间语义
    摄入时间
        元素进入到Source算子的时间
    事件时间
        元素产生的时间
    处理时间
        元素到了算子，算子对其进行处理的时间
    Flink1.12开始，默认的时间语义是事件时间语义

窗口
    将无限的数据划分为有限的数据块

窗口分类
    按照驱动形式分
        时间窗口
            按照时间定义窗口的起始以及结束
        计数窗口
            按照窗口中元素的个数定义窗口的起始以及结束

    按照数据划分的方式分
        滚动窗口
            窗口大小固定
            窗口和窗口之间不会重叠
            窗口和窗口是首尾相接
            元素只会属于一个窗口
        滑动窗口
            窗口大小固定
            窗口和窗口可能会重叠
            窗口和窗口不是首尾相接
            同一个元素会属于多个窗口   窗口大小/滑动步长
        会话窗口(只针对时间)
            窗口大小不是固定
            窗口和窗口之间不会重叠
            窗口和窗口不是首尾相接，窗口之间会存在时间间隔(size)
            元素只会属于一个窗口

        全局窗口
            将流中数据放到同一个窗口中，默认情况下，是没有结束的
            需要手动指定触发器，指定窗口的结束时机
            计数窗口的底层就是全局窗口

窗口API
    是否在开窗前进行了keyBy
        keyBy
            针对keyBy之后的每一个组进行开窗，组和组之间相互不影响
            时间窗口
                window()
                windowAll()
            计数窗口
                countWindow()
                countWindowAll(窗口大小)
                countWindowAll(窗口大小,滑动步长)
        no-keyBy
            针对整条流进行开窗，相当于并行度设置为1
            时间窗口
                windowAll()
            计数窗口
                countWindowAll(窗口大小)
                countWindowAll(窗口大小,滑动步长)

    窗口分配器
        开什么类型的窗口
        计数窗口
            滚动计数窗口
                countWindow[All](窗口大小)
            滑动计数窗口
                countWindow[All](窗口大小,滑动步长)
        时间窗口
            .window()
            .windowAll()
            滚动处理时间窗口
                TumblingProcessingTimeWindows
            滑动处理时间窗口
                SlidingProcessingTimeWindows
            处理时间会话窗口
                ProcessingTimeSessionWindows
            滚动事件时间窗口
                TumblingEventTimeWindows
            滑动事件时间窗口
                SlidingEventTimeWindows
            事件时间会话窗口
                EventTimeSessionWindows
        全局窗口
            stream.keyBy(...)
                   .window(GlobalWindows.create());

        以滚动处理时间为例，说明窗口的生命周期
            窗口对象什么时候创建
                属于窗口的第一个元素到来的时候，创建窗口对象

            窗口对象的起始和结束时间
                起始时间:向下取整
                结束时间:起始时间  +  窗口大小
                [起始时间,结束时间)
                最大时间: 结束时间 - 1ms

            窗口什么时候触发计算和关闭
                系统时间到了窗口的最大时间

    窗口处理函数
        如何对窗口中数据进行处理
        增量处理
            窗口数据来一条计算一次，不会缓存数据  优点：省空间      缺点：不能获取更丰富的窗口信息
            reduce
                窗口中元素类型以及向下游传递的类型必须一致
                reduce(value1,value2)
                    value1:中间累加的结果
                    value2:新来的数据
                注意：如果窗口中只有一条数据，reduce方法不会被调用
            aggregate
                窗口中的元素类型、累加器类型以及向下游传递的类型可以不一致
                createAccumulator:窗口中第一条数据到来的时候执行
                add:窗口中每来一条数据都会执行
                getResult:窗口触发计算的时候执行的方法
                merge:只有会话才需要重写
        全量处理
            窗口数据到来的时候不会马上计算，等窗口触发计算的时候，在整体进行计算  优点：可以获取更丰富的窗口信息  缺点：费空间
            apply
            process
                更底层，可以通过上下文对象获取窗口对象以及其它一些信息

        在实际开发的过程中，可以增量 + 全量


    窗口触发器
        什么时候触发窗口的计算

    窗口移除器
        在窗口触发计算之后，在处理函数执行之前(之后)，需要执行的移除操作是什么
