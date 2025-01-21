import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StreamingEventTime {
    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();
            //streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            streamEnv.setParallelism(1);

            /****************************************************************************
             *                  Read CSV File Stream into a DataStream.
             ****************************************************************************/
            ///////Read TCC Data
            DataStream<Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>> TCC = streamEnv.socketTextStream("localhost", 9090)
                    .map(new MapFunction<String, Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>>() {
                        public Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer> map(String value) throws ParseException {
                            String[] tcc = value.split(",");
                            return new Tuple7<>(tcc[0], new Long(tcc[1]), new Integer(tcc[2]), new Integer(tcc[3]), new Integer(tcc[4]), new Integer(tcc[5]), new Integer(tcc[6]));
                        }
                    });

            ///////Read TP Data
            DataStream<Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>> TP = streamEnv.socketTextStream("localhost", 9091)
                    .map(new MapFunction<String, Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>>() {
                        public Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer> map(String value) throws ParseException {
                            String[] tp = value.split(",");
                            return new Tuple7<>(tp[0], new Long(tp[1]), new Integer(tp[2]), new Integer(tp[3]), new Integer(tp[4]), new Integer(tp[5]), new Integer(tp[6]));
                        }
                    });

            ///////////////////////Join Traffic, Crash, Crime, and Park
            DataStream<Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>> TCCP = TCC
                    .join(TP)
                    .where(new KeySelector<Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>, Integer>() {
                        @Override
                        public Integer getKey(Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                            return in.f2;
                        }
                    })
                    .equalTo(new KeySelector<Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>, Integer>() {
                        @Override
                        public Integer getKey(Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer> inn) throws Exception {
                            return inn.f2;
                        }
                    })
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                    .apply(new JoinFunction<Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>, Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>, Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer>>() {
                        @Override
                        public Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer> join(Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer> tcc, Tuple7<String, Long, Integer, Integer, Integer, Integer, Integer> tp) throws Exception {
                            return new Tuple7<>(tcc.f0, tcc.f1, tcc.f2, tcc.f3, tcc.f4, tcc.f5, tp.f6);
                        }
                    });



            //TrafficCrash.union(TrafficCrime).print();
            System.out.println("----------------------------------------------------");
            TCC.print();





            streamEnv.execute();

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }


}