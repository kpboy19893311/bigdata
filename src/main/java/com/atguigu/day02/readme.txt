
并行度
    当要处理的数据量非常大时，我们可以把一个算子操作，“复制”多份到多个节点，数据来了之后就可以到其中任意一个执行。
    这样一来，一个算子任务就被拆分成了多个并行的“子任务”（subtasks）,可以提升程序的处理能力
    一个算子并行子任务的个数称之为并行度

    *一个Flink应用程序并行度 = 并行度最大的算子的并行度的个数

    如果在IDEA开发程序过程中，没有指定并行度，默认并行度是CPU的线程数
设置并行度的方式
    针对某一个算子单独设置并行度
        算子.setParallelism(4)
    在代码中全局设置并行度
        env.setParallelism(4)
    在命令行提交作业的时候通过-p参数设置
    在flink-conf.yaml配置文件中执行
        parallelism.default: 1
优先级
    针对某一个算子单独设置并行度 > 在代码中全局设置并行度 > 在命令行提交作业的时候通过-p参数设置 > 在flink-conf.yaml配置文件中执行

算子链
    前提
        两个算子间并行度相同
        one-to-one
            不存在重分区以及数据打乱重组

    在Flink中，如果两个算子并行度相同的一对一（one to one）算子操作，
    可以直接链接在一起形成一个“大”的任务（task），这样原来的算子就成为了真正任务里的一部分
    这样的技术被称为“算子链”（Operator Chain）

    算子链是Flink提供的非常有效的优化手段，可以减少算子之间的网络IO
禁用算子链
    算子.disableChaining()
    算子.startNewChain()
    全局禁用算子链  env.disableOperatorChaining();

任务槽
    任务槽slot是TaskManager上资源分配的最小单位
    Flink的TaskManager负责运行任务(提供任务运行需要的资源)
    为了让TaskManager上的资源更合理的进行分配
    TaskManager进程上，可以开启多个线程，每个线程我们称之为任务槽(slot)
    任务槽可以均分TaskManager上的内存资源
    taskManager的内存大小可以在配置文件以及提交作业的时候指定
        taskmanager.memory.process.size: 1728m
    默认情况下，每一个TaskManager上面，slot数量是1个，可以在配置文件以及提交参数中指定
        taskmanager.numberOfTaskSlots: 1

    在实际设置任务槽数量的时候，也需要参考cpu的核数，一般slot数量和cpu核数的关系为1:1或2:1

任务槽共享
    只要属于同一个作业，那么对于不同任务节点（算子）的并行子任务，就可以放到同一个slot上执行

    注意：那么对于同一个任务的子任务必须放到不同的slot上
          所以一个Flink应用程序的并行度 = 并行度最大的算子的并行度的个数




