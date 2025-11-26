水位线(Watermark)
    前提:事件时间语义
    用于衡量事件时间进展的标记
    是一个逻辑时钟
    水位线也会作为流中的元素向下游传递
    水位线的值是根据流中元素的事件时间得到的
    flink在做处理的时候，认为水位线之前的数据都已经处理过了
    主要用于窗口的触发计算、关闭以及定时器的执行
    水位线是递增的，不会变小(相当于时间不会倒流)
    为了处理流中的迟到数据，设置水位线的延迟时间，目的其实让窗口或者定时器延迟触发

Flink内置水位线生成策略
    单调递增
        WatermarkStrategy.<流中数据类型>forMonotonousTimestamps()
    有界乱序
        WatermarkStrategy.<流中数据类型>forBoundedOutOfOrderness(乱序程度)
    单调递增是有界乱序的子类
    单调递增是有界乱序的一种特殊情况，乱序程度是0
    class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator{
        onEvent:流中每条数据到来的时候都会执行方法
            取出流中元素最大事件时间
        onPeriodicEmit:周期性执行的方法，默认周期200ms  可以通过如下代码修改默认周期 env.getConfig().setAutoWatermarkInterval()
            创建水位线对象，发射到流中   wm = 最大时间 - 乱序程度 - 1ms
    }

以滚动事件时间窗口为例
    窗口对象什么时候创建
        当属于当前窗口的第一个元素进来的时候
    窗口的起始时间
        向下取整
    窗口的结束时间
        起始时间  + 窗口大小
    窗口的最大时间
        结束时间 - 1ms
    窗口什么时候触发计算
        水位线 >= 窗口最大时间
        (window.maxTimestamp() <= ctx.getCurrentWatermark()
    窗口什么时候关闭
        水位线到了window.maxTimestamp() + allowedLateness

水位线的传递
    如果上游一个并行度，下游多个并行度，广播
    如果上游多个并行度，下游一个并行度，将上游所有并行度的水位线拿过来取最小
    如果上游多个并行度，下游多个并行度，先广播再取最小

迟到数据的处理
    指定wm的生成策略为有界乱序，指定乱序程度(延迟窗口触发计算以及关闭时间)
    设置窗口的允许迟到时间
    侧输出流








