package chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Description：批处理——wordcount
 * @Author：YJ
 * @Createtime 2021/5/13 21:57
 */
public class Flink01_WC_Batch {
    public static void main(String[] args) throws Exception {
        // 0、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1、从文件（有界）读取数据
        DataSource<String> fileDS = env.readTextFile("input/word.txt");

        // 2、处理数据
        // 2.1、切分，转换成元组(word, 1)
//        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOneTuple = fileDS.flatMap(new MyFlatMapFunction());

        // lamda 表达式缺少明确的返回值类型信息，需要使用 return 去指定 .returns(new TypeHint<Tuple2<String, Integer>>() {})

        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOneTuple = fileDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            // 1、切分
            String[] words = s.split(" ");
            // 2、转换成二元组
            for (String word : words) {
                Tuple2<String, Integer> tuple = new Tuple2<>(word, 1); // 创建元组
                // collector.collect() 往下游发送数据
                collector.collect(tuple);
            }

        }).returns(new TypeHint<Tuple2<String, Integer>>() {});

//        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOneTuple = fileDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                // 1、切分
//                String[] words = s.split(" ");
//                // 2、转换成二元组
//                for (String word : words) {
//                    Tuple2<String, Integer> tuple = new Tuple2<>(word, 1); // 创建元组
//                    // collector.collect() 往下游发送数据
//                    collector.collect(tuple);
//                }
//
//            }
//        });
        // 2.2、按照 word 分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 2.3、按照分组聚合
        AggregateOperator<Tuple2<String, Integer>> result = wordAndOneGroup.sum(1);

        // 3、输出，保存
        result.print();

        // 4、启动（批处理不需要）

    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 1、切分
            String[] words = s.split(" ");
            // 2、转换成二元组
            for (String word : words) {
                Tuple2<String, Integer> tuple = new Tuple2<>(word, 1); // 创建元组
                // collector.collect() 往下游发送数据
                collector.collect(tuple);
            }
        }
    }
}



































