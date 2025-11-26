
状态
    用于保存程序运行的中间结果
状态分类
    原始状态
        由程序员自己开辟内存，自己负责状态的序列化以及容错恢复等
    托管状态
        由Flink框架管理状态的存储、序列化以及容错恢复等
        算子状态
            作用范围: 算子的每一个并行子任务上(分区、并行度、slot)
            ListState
            UnionListState
            BroadcastState(重点)
            使用步骤
                类必须实现checkpointedFunction接口
                    initializeState:初始化状态
                    snapshotState:对状态进行备份
                算子状态和普通成员变量声明的位置一样，作用范围范围一样，但是不同的是算子状态可以被持久化
                其实算子状态底层在snapshotState就是将普通的变量放到状态中进行的持久化，相当于普通的变量也被持久保存了


        键控状态
            作用范围:经过keyBy之后的每一个组，组和组之间状态是隔离的
            ValueState
            ListState
            MapState
            ReducintState
            AggregatingState
            使用步骤
                在处理函数类成员变量位置声明状态，注意：虽然在成员变量位置声明，但是作用范围是keyBy后的每一个组
                在open方法中对状态进行初始化
                在具体的处理函数中使用状态

广播状态BroadcastState
    也算是算子状态的一种，作用范围也是算子子任务
    广播状态的使用方式是固定的

状态后端
    管理本地状态的存储方式和位置以及检查点的存储位置
    检查点是对状态做的备份，是状态的一个副本
    Flink1.13前
                                      状态                     检查点
        Memory                      TM堆内存                 JM的堆内存
        Fs                          TM堆内存                 文件系统
        RocksDB                     RocksDB库                文件系统

    从Flink1.13开始
                                      状态                     检查点
        HashMap                     TM堆内存               JM的堆内存|文件系统
        RocksDB                     RocksDB库              文件系统

