package spark;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by teots on 3/16/14.
 */
public class SparkTester {
    public static void main(String[] args) {
        JavaSparkContext spark = new JavaSparkContext("local", "JavaWordCount",
                System.getenv("SPARK_HOME"), new String[] {"/home/teots/IdeaProjcts/TechnologyTests/target/TechnologyTests-1.0-SNAPSHOT.jar"});

        JavaRDD<String> file = spark.textFile("file:///home/teots/IdeaProjcts/TechnologyTests/src/main/resources/text/");
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String, Integer> pairs = words.map(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        //counts.saveAsTextFile("file:///home/teots/Desktop/wc");
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

    }
}
