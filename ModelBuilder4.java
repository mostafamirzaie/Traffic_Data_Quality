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

public class ModelBuilder4 {
    public static void main(String[] args) throws SQLException, IOException {

        SQLDatabaseConnection MySql = new SQLDatabaseConnection();
        String filePathInputTrain = "Experiments/trainGrid_11213.csv";
        String filePathOutput = "Experiments/results.csv";
        String filePathInputTest = "Experiments/train413.csv";
        int id = 413;
        int gIndex = 11213;

        FileWriter OutputOrigin = new FileWriter(filePathOutput);
        OutputOrigin.append("============ Start Of Experiment at :" + java.time.LocalDate.now() + " " + java.time.LocalTime.now() + " ============= \n");
        OutputOrigin.flush();
        OutputOrigin.close();

            FileWriter InputTrainOrigin = new FileWriter(filePathInputTrain);

            InputTrainOrigin.append("preCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("fCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("count");
            InputTrainOrigin.append("\n");

            InputTrainOrigin.flush();
            InputTrainOrigin.close();

            ResultSet Ans = MySql.Select("  SELECT  t1.gIndex, t2.id, t3.time as sTime, " +
                    "                                       DATEADD(mi, 10 ,t3.time) as eTime, " +
                    "                                       (t3.bus_count + t3.message_count) as count, " +
                    "                                       (t8.bus_count + t8.message_count) as fcount "+
                    "                               FROM Grid as t1 INNER JOIN Segments as t2 ON t1.gIndex = t2.gIndex " +
                    "                                       INNER JOIN Traffics as t3 ON t3.segment_id = t2.id " +
                    "                                       INNER JOIN Traffics as t8 On t8.segment_id = t2.fNeighbor AND t8.time BETWEEN DATEADD(mi, -1 ,t3.time) AND DATEADD(mi, 1 ,t3.time) "+
                    "                               WHERE t2.id = 410 or t2.id = 411 or t2.id = 412 or t2.id = 413 or t2.id = 414 or t2.id = 419 or t2.id = 420 or t2.id = 421 or t2.id = 422 or t2.id = 423 or t2.id = 686 or t2.id = 687 or t2.id = 688 or t2.id = 689 or t2.id = 705 or t2.id = 706 or t2.id = 707 or t2.id = 708 or t2.id = 800 or t2.id = 801 or t2.id = 802 or t2.id = 803 or t2.id = 811 or t2.id = 812 or t2.id = 813 or t2.id = 814" +
                    "                               ORDER BY t3.time ASC");


            double preCount = -1;
            int duplicateDate = 0;
            String uniqueDate = null;
            StringBuilder TrainString = new StringBuilder();

            while (Ans.next()) {
                String date = Ans.getString(3);

                if(uniqueDate == null){ uniqueDate = date; }
                else if(uniqueDate.equals(date)){duplicateDate++; continue;}
                uniqueDate = date;

                double count = Double.parseDouble(Ans.getString(5));
                preCount = (preCount < 0)? count : preCount; // For first Row value

                double fCount = (Ans.getString(6) == null)? 0 : Double.parseDouble(Ans.getString(6));

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
            MultipleLinearRegression2 regression = new MultipleLinearRegression2(filePathInputTrain,filePathInputTest,filePathOutput,1);

            System.out.println("Grid Index: "+ gIndex +" Was Processed. Duplicated date-time : "+duplicateDate);

        FileWriter OutputFinal = new FileWriter(filePathOutput, true);
        OutputFinal.append("\n\n============ END Of Experiment at :" + java.time.LocalDate.now() + " " + java.time.LocalTime.now() + " ============= \n");
        OutputFinal.flush();
        OutputFinal.close();
    }
}

