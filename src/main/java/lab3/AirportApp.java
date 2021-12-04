package lab3;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
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
        JavaPairRDD<Tuple2<String, String>, FlightSerializable> flightsPairs =  airportsFile
            .filter(s -> !s.contains("YEAR"))
            .mapToPair(
                s -> {
                    s = s.replace("\"", "");
                    String[] columns = s.split(",");
                    double delay = Double.parseDouble(columns[18]);
                    boolean isCancelled = Double.parseDouble(columns[19]) == 1;
                    FlightSerializable flight = new FlightSerializable(delay, isCancelled);
                    return new Tuple2<>(new Tuple2<>(columns[11], columns[14]), flight);
                }
            );
        JavaPairRDD<Tuple2<String, String>, FlightSerializable> collectedFlights = flightsPairs.reduceByKey(
                (a, b) -> new Tuple2<Double, Integer>(Math.max(a.getDelay(), b.getDelay()), a.getIsCancelled() ? 1 : 0 + (b.getIsCancelled() ? 1 : 0))
        );
    }
}
