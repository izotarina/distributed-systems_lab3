package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportsFile = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<String, String> airportsPairs =
            airportsFile.mapToPair(
                s -> {
                    s = s.replace("\"", "");
                    String[] columns = s.split(",");
                    return new Tuple2<>(columns[0], columns[1]);
                }
            );

        JavaRDD<String> flightsFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<String, String> flightsPairs =
            airportsFile.mapToPair(
                s -> {
                    s = s.replace("\"", "");
                    String[] columns = s.split(",");
                    return new Tuple2<>(columns[0], columns[1]);
                }
            );
    }
}
