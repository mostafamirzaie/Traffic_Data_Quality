package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.mesos.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.mesos.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ModelBuilder3 {
    public static void main(String[] args) throws SQLException, IOException {

        SQLDatabaseConnection MySql = new SQLDatabaseConnection();
        String filePathInputTrain = "Experiments/train_1121.csv";
        String filePathOutput = "Experiments/results.csv";

        FileWriter OutputOrigin = new FileWriter(filePathOutput);
        OutputOrigin.append("============ Start Of Experiment at :" + java.time.LocalDate.now() + " " + java.time.LocalTime.now() + " ============= \n");
        OutputOrigin.flush();
        OutputOrigin.close();

        ResultSet SegmentsId = MySql.Select(" SELECT id, gIndex FROM Segments where id = 812");
        while (SegmentsId.next())
        {
            FileWriter InputTrainOrigin = new FileWriter(filePathInputTrain);

            InputTrainOrigin.append("preCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("fCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("count");
            InputTrainOrigin.append("\n");

            InputTrainOrigin.flush();
            InputTrainOrigin.close();

            int id = Integer.parseInt(SegmentsId.getString(1));
            String gIndex = SegmentsId.getString(2);

            ResultSet Ans = MySql.Select("  SELECT  t1.gIndex, t2.id, t3.time as sTime, " +
                    "                                       DATEADD(mi, 10 ,t3.time) as eTime, " +
                    "                                       (t3.bus_count + t3.message_count) as count, " +
                    "                                       t4.temperature, t4.weatherType, t4.wind, t4.humidity, t4.barometer, t4.visibility," +
                    "                                       t5.light, t5.surface, t5.defect," +
                    "                                       t6.situation, t7.parkNumber, " +
                    "                                       t8.bus_count as fcount "+
                    "                               FROM Grid as t1 INNER JOIN Segments as t2 ON t1.gIndex = t2.gIndex " +
                    "                                       INNER JOIN Traffics as t3 ON t3.segment_id = t2.id " +
                    "                                       INNER JOIN Traffics as t8 On t8.segment_id = t2.fNeighbor AND t8.time BETWEEN DATEADD(mi, -1 ,t3.time) AND DATEADD(mi, 1 ,t3.time) "+
                    "                                       LEFT OUTER JOIN Weather as t4 ON t4.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time)" +
                    "                                       LEFT OUTER JOIN Crashes as t5 ON t5.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time) AND t5.gIndex = t1.gIndex " +
                    "                                       LEFT OUTER JOIN Crimes as t6 ON t6.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time) AND t6.gIndex = t1.gIndex " +
                    "                                       LEFT OUTER JOIN ParkEvents as t7 ON t3.time BETWEEN t7.start_date AND t7.end_date AND t7.gIndex = t1.gIndex " +

                    "                               WHERE t2.id = "+id+" " +
                    "                               ORDER BY t3.time ASC");

            double temp = 0.;
            double windD = 0.;
            double humidD = 0.;
            double weatherT = 0.;
            double visibleD = 0.;
            double nullD = -1000;
            double preCount = -1;
            int duplicateDate = 0;
            double barometerD = 0.;
            String uniqueDate = null;
            StringBuilder TrainString = new StringBuilder();

            while (Ans.next()) {
                String date = Ans.getString(3);

                if(uniqueDate == null){ uniqueDate = date; }
                else if(uniqueDate.equals(date)){duplicateDate++; continue;}
                uniqueDate = date;

                double count = Double.parseDouble(Ans.getString(5));
                preCount = (preCount < 0)? count : preCount; // For first Row value

                double fCount = (Ans.getString(17) == null)? 0 : Double.parseDouble(Ans.getString(17));

                double temperature = (Ans.getString(6) == null)? temp : Double.parseDouble(Ans.getString(6));
                temp = temperature;

                double weatherType = (Ans.getString(7) == null)? weatherT : Double.parseDouble(Ans.getString(7));
                weatherT = weatherType;

                double wind = (Ans.getString(8) == null)? windD : Double.parseDouble(Ans.getString(8));
                windD = wind;

                double humidity = (Ans.getString(9) == null)? humidD : Double.parseDouble(Ans.getString(9));
                humidD = humidity;

                double barometer = (Ans.getString(10) == null)? barometerD : Double.parseDouble(Ans.getString(10));
                barometerD = barometer;

                double visibility = (Ans.getString(11) == null)? visibleD : Double.parseDouble(Ans.getString(11));
                visibleD = visibility;

                double light = (Ans.getString(12) == null)? nullD : Double.parseDouble(Ans.getString(12));
                double surface = (Ans.getString(13) == null)? nullD : Double.parseDouble(Ans.getString(13));
                double defect = (Ans.getString(14) == null)? nullD : Double.parseDouble(Ans.getString(14));
                double crash = (Ans.getString(12) == null && Ans.getString(13) == null && Ans.getString(14) == null)? 0 : 1;

                double crime = (Ans.getString(15) == null)? 0 : 1;
                double parkEvent = (Ans.getString(16) == null)? 0 : 1;

                TrainString.append(preCount +","+ fCount +","+ count +"\n");
                preCount = count;
            }

            FileWriter InputTrain = new FileWriter(filePathInputTrain, true);
            InputTrain.append(TrainString);
            InputTrain.flush();
            InputTrain.close();

            FileWriter Output = new FileWriter(filePathOutput, true);
            Output.append("\n*********************************************************************\n");
            Output.append("***********   Segment id: "+ id + " | Grid Index: "+ gIndex + "   **************\n");
            Output.append("*********************************************************************\n");
            Output.flush();
            Output.close();

            MultipleLinearRegression2 regression = new MultipleLinearRegression2(filePathInputTrain,filePathInputTrain,filePathOutput,1);

            System.out.println("Segment ID : "+id+" Was Processed. Duplicated date-time : "+duplicateDate);
        }

        FileWriter OutputFinal = new FileWriter(filePathOutput, true);
        OutputFinal.append("\n\n============ END Of Experiment at :" + java.time.LocalDate.now() + " " + java.time.LocalTime.now() + " ============= \n");
        OutputFinal.flush();
        OutputFinal.close();
    }
}
