package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class AirportApp {
    private final static int AIRPORT_CODE_COLUMN_INDEX = 0;
    private final static int AIRPORT_NAME_COLUMN_INDEX = 1;

    private final static int FLIGHT_DELAY_COLUMN_INDEX = 18;
    private final static int FLIGHT_CANCELLED_COLUMN_INDEX = 19;
    private final static int FLIGHT_DEPARTURE_COLUMN_INDEX = 11;
    private final static int FLIGHT_DESTINATION_COLUMN_INDEX = 14;
    private final static String DELIMITER = ",";
    private final static int FLIGHT_INSTANCE = 1;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportsFile = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<String, String> airportsPairs =
            airportsFile
                    .filter(s -> !s.contains("Code"))
                    .mapToPair(
                s -> {
                    s = s.replace("\"", "");
                    String[] columns = s.split(DELIMITER);
                    return new Tuple2<>(columns[AIRPORT_CODE_COLUMN_INDEX], columns[AIRPORT_NAME_COLUMN_INDEX]);
                }
            );

        JavaRDD<String> flightsFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<Tuple2<String, String>, AirportStatisticSerializable> flightsPairs =  flightsFile
            .filter(s -> !s.contains("YEAR"))
            .mapToPair(
                s -> {
                    s = getStringWithoutQuotes(s);
                    String[] columns = s.split(",");
                    double delay = getValidNumber(columns[FLIGHT_DELAY_COLUMN_INDEX]);
                    double isCancelled = Double.parseDouble(columns[FLIGHT_CANCELLED_COLUMN_INDEX]);
                    AirportStatisticSerializable flight = new AirportStatisticSerializable(delay, isDelayedFlight(delay), isCancelled, FLIGHT_INSTANCE);
                    return new Tuple2<>(new Tuple2<>(columns[FLIGHT_DEPARTURE_COLUMN_INDEX], columns[FLIGHT_DESTINATION_COLUMN_INDEX]), flight);
                }
            );
        JavaPairRDD<Tuple2<String, String>, AirportStatisticSerializable> collectedFlights = flightsPairs.reduceByKey(
                (a, b) -> new AirportStatisticSerializable(Math.max(a.getMaxDelay(), b.getMaxDelay()),
                a.getDelayedFlights() + b.getDelayedFlights(),
                        a.getCancelledFlights() + b.getCancelledFlights(),
a.getCountFlights() + b.getCountFlights())
        );

        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportsPairs.collectAsMap());
        JavaRDD<String> flightsWithAirportNames = collectedFlights.map(
                s -> {
                    Map<String, String> airports = airportsBroadcasted.value();
                    String departure = airports.get(s._1()._1());
                    String destination = airports.get(s._1()._2());
                    AirportStatisticSerializable statistic = s._2();

                    String result = "departure: " + departure + ", destination: " + destination;
                    result += ", maxDelay: " + statistic.getMaxDelay() + ", delayedPart: " + getProcent(statistic.getDelayedFlights(), statistic.getCountFlights());
                    result += "cancelledPart: " + getProcent(statistic.getCancelledFlights(), statistic.getCountFlights());
                  return result;
                }
        );

        flightsWithAirportNames.saveAsTextFile("output");
    }

    private static double getValidNumber(String value) {
        return value.length() > 0 ? Double.parseDouble(value) : 0;
    }

    private static String getStringWithoutQuotes(String string) {
        return string.replace("\"", "");
    }

    private static int isDelayedFlight(double delay) {
        return delay > 0 ? 1 : 0;
    }

    private static String getProcent(double value, int numberOfFlights) {
        return value / numberOfFlights * 100 + "% ";
    }
}
