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

public class ModelBuilderContext {
    public static void main(String[] args) throws SQLException, IOException {

        SQLDatabaseConnection MySql = new SQLDatabaseConnection();


        ResultSet SegmentsId = MySql.Select(" SELECT id from Segments");
        while (SegmentsId.next())
        {
            int id = Integer.parseInt(SegmentsId.getString(1));
            ResultSet Ans = MySql.Select("  SELECT  t1.gIndex, t2.id, t3.time as sTime, " +
                    "                                          DATEADD(mi, 10 ,t3.time) as eTime," +
                    "                                          (t3.bus_count + t3.message_count) as count, " +
                    "                                          (t8.bus_count + t8.message_count) as fcount, " +
                    "                                          t4.weatherType, t4.visibility, " +
                    "                                          t5.light, t5.surface, t5.defect, " +
                    "                                            t6.situation, t7.parkNumber " +
                    "                                  FROM Grid as t1 INNER JOIN Segments as t2 ON t1.gIndex = t2.gIndex " +
                    "                                          INNER JOIN Traffics as t3 ON t3.segment_id = t2.id " +
                    "                                          INNER JOIN Traffics as t8 ON t8.segment_id = t2.fNeighbor AND t8.time BETWEEN DATEADD(mi, -1 ,t3.time) AND DATEADD(mi, 1 ,t3.time) " +
                    "                                          LEFT OUTER JOIN WeatherEndDate as t4 ON t3.time BETWEEN t4.start_date AND t4.end_date " +
                    "                                          LEFT OUTER JOIN Crashes as t5 ON t5.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time) AND t5.gIndex = t1.gIndex " +
                    "                                          LEFT OUTER JOIN Crimes as t6 ON t6.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time) AND t6.gIndex = t1.gIndex " +
                    "                                          LEFT OUTER JOIN ParkEvents as t7 ON t3.time BETWEEN t7.start_date AND t7.end_date AND t7.gIndex = t1.gIndex " +
                    "                                  WHERE t2.id = "+ id +" and ( "+
                    "                                    t5.defect is not null or t5.light is not null or t5.surface is not null "+
                    "                                      or t6.situation is not null or t7.parkNumber is not null) "+
                    "                                  ORDER BY t3.time ASC");

            double weatherT = 0.;
            double visibleD = 0.;
            String uniqueDate = null;
            double nullD = -1000;
            StringBuilder TrainString = new StringBuilder();


            while (Ans.next()) {
                double count = Double.parseDouble(Ans.getString(5));
                double fCount = Double.parseDouble(Ans.getString(6));
                double weatherType = (Ans.getString(7) == null)? weatherT : Double.parseDouble(Ans.getString(7));
                weatherT = weatherType;
                double visibility = (Ans.getString(8) == null)? visibleD : Double.parseDouble(Ans.getString(8));
                visibleD = visibility;
                double crash = (Ans.getString(9) == null && Ans.getString(10) == null && Ans.getString(11) == null)? 0 : 1;
                double crime = (Ans.getString(12) == null)? 0 : 1;
                double parkEvent = (Ans.getString(13) == null)? 0 : 1;

                TrainString.append(fCount + ","+ weatherType +","+ visibility +","+ crash +","+ crime +","+ parkEvent +","+ count +"\n");
            }
            String filePath = "Experiments/Context_Train/Segments/"+id+".csv";
            FileWriter csvWriterOrigin = new FileWriter(filePath);

            csvWriterOrigin.append("fCount");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("weatherType");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("visibility");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("crash");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("crime");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("parkEvents");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("count");
            csvWriterOrigin.append("\n");
            csvWriterOrigin.append(TrainString);
            csvWriterOrigin.flush();
            csvWriterOrigin.close();

            System.out.println("Segment id : "+id+" file is created!");
            //System.out.println("Segment ID : "+id+" Was Inserted into DB. Duplicated date-time : "+duplicateDate);
        }
    }
}
