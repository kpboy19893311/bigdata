FlinkAPI双流join
    基于窗口的实现
        滚动窗口
        滑动窗口
        会话窗口
        语法
            ds1
                .join(ds2)
                .where(提取ds1中关联字段)
                .equalTo(提取ds2中关联字段)
                .window()
                .apply()
    基于状态的实现
        intervalJoin
        语法
            keyedA
                .intervalJoin(keyedB)
                .between(下界,上界)
                .process()
        底层实现
            connect + 状态

        底层处理流程
            判断是否迟到
            将当前数据放到状态中缓存起来
            用当前这条数据和另外一条流中缓存的数据进行关联
            清除状态
    注意：FlinkAPI不管是通过哪种方式实现双流join， 都支持内连接
         如果要想实现外连接效果，可以通过FlinkSQL或者自己通过connect实现
处理函数
    处于Flink分层API的最底层
    可以更加灵活的对流中数据进行处理(处理逻辑全靠自己实现)
    处理函数不是接口，是抽象类，并且继承了AbstractRichFunction，所以富函数拥有的功能，处理函数都有
    一般在处理函数的方法如下
        processElement：对流中数据进行处理的方法,是抽象的
        onTimer:定时器触发的时候执行的方法，非抽象的


处理函数分类
    ProcessFunction
        最基本的处理函数，基于DataStream直接调用.process()时作为参数传入。
    KeyedProcessFunction
        对流按键分组后的处理函数，基于KeyedStream调用.process()时作为参数传入。要想使用定时器，必须基于KeyedStream。
    ProcessWindowFunction
        开窗之后的处理函数，也是全窗口函数的代表。基于WindowedStream调用.process()时作为参数传入。
    ProcessAllWindowFunction
        同样是开窗之后的处理函数，基于AllWindowedStream调用.process()时作为参数传入。
    CoProcessFunction
        合并（connect）两条流之后的处理函数，基于ConnectedStreams调用.process()时作为参数传入。关于流的连接合并操作，我们会在后续章节详细介绍。
    ProcessJoinFunction
        间隔连接（interval join）两条流之后的处理函数，基于IntervalJoined调用.process()时作为参数传入。
    BroadcastProcessFunction
        广播连接流处理函数，基于BroadcastConnectedStream调用.process()时作为参数传入。这里的“广播连接流”BroadcastConnectedStream，是一个未keyBy的普通DataStream与一个广播流（BroadcastStream）做连接（conncet）之后的产物。关于广播流的相关操作，我们会在后续章节详细介绍。
    KeyedBroadcastProcessFunction
        按键分组的广播连接流处理函数，同样是基于BroadcastConnectedStream调用.process()时作为参数传入。与BroadcastProcessFunction不同的是，这时的广播连接流，是一个KeyedStream与广播流（BroadcastStream）做连接之后的产物。

只有在KeyedProcessFunction的processElement方法中，才能使用定时服务
    在处理函数中，常用的操作
        获取当前分组的key
            ctx.getCurrentKey()
        获取当前元素的事件时间
            ctx.timestamp()
        将数据"放到"侧输出流中
            注意：如果要使用侧输出流，必须用process算子对流中数据进行处理，
            ctx.output()
        获取定时服务
            ctx.timerService()
        获取当前处理时间
             timerService.currentProcessingTime()
        获取当前水位线
            timerService.currentWatermark()
        注册处理时间定时器   定时器的触发是由系统时间触发的
            timerService.registerProcessingTimeTimer(currentProcessingTime + 10000)
        注册事件时间定时器   定时器的触发是由水位线(逻辑时钟)触发的
            timerService.registerEventTimeTimer(10)
        删除处理时间定时器
            timerService.deleteProcessingTimeTimer(10);
        删除事件时间定时器
            timerService.deleteEventTimeTimer(10)