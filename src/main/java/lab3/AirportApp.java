package lab3;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

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
        JavaPairRDD<Tuple2<String, String>, AirportStatisticSerializable> flightsPairs =  airportsFile
            .filter(s -> !s.contains("YEAR"))
            .mapToPair(
                s -> {
                    s = s.replace("\"", "");
                    String[] columns = s.split(",");
                    double delay = Double.parseDouble(columns[18]);
                    double isCancelled = Double.parseDouble(columns[19]);
                    AirportStatisticSerializable flight = new AirportStatisticSerializable(delay, isCancelled);
                    return new Tuple2<>(new Tuple2<>(columns[11], columns[14]), flight);
                }
            );
        JavaPairRDD<Tuple2<String, String>, AirportStatisticSerializable> collectedFlights = flightsPairs.reduceByKey(
                (a, b) -> new AirportStatisticSerializable(Math.max(a.getMaxDelay(), b.getMaxDelay()),
a.getCancelledAndDelayedFlightsPart() + b.getCancelledAndDelayedFlightsPart())
        );

        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportsPairs.collectAsMap());
        JavaPairRDD<Tuple2<String, String>, AirportStatisticSerializable> flightsWithAirportNames = collectedFlights.map(
                s -> {
                    Map<String, String> airports = airportsBroadcasted.value();
                    String result = "";

//                    return new Tuple2<>(new Tuple2<>(airports.get(s._1()._1()), airports.get(s._1()._2())),
//                            s._2());

                }
        )
    }
}
