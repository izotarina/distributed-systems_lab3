package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class AirportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportsFile = sc.textFile("L_AIRPORT_ID.csv");
        JavaRDD<String> airportsRows = airportsFile.flatMap(s -> Arrays.stream(s.split("\n")).iterator());
        JavaPairRDD<String, Long> wordsWithCount =
                airportsRows.mapToPair(
                        s -> new Tuple2<>(s, 1l)
                );
    }
}
