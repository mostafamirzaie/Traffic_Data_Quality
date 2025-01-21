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

public class ModelBuilder2 {
    public static void main(String[] args) throws SQLException, IOException {

        SQLDatabaseConnection MySql = new SQLDatabaseConnection();


        ResultSet SegmentsId = MySql.Select(" SELECT id from Segments where id = 129 or id = 166 or id = 670 or id = 673 or id = 674 or id = 1000 or id = 1003 or id = 1011 or id = 1055 or id = 1124 or id = 1193 or id = 1196 or id = 1267 or id = 1276 or id = 1277 or id = 1286 or id = 1291 or id = 1300 or id = 1304 or id = 1306 or id = 1307");
        while (SegmentsId.next())
        {
            int id = Integer.parseInt(SegmentsId.getString(1));
            ResultSet Ans = MySql.Select("  SELECT  t1.gIndex, t2.id, t3.time as sTime, " +
                    "                                          DATEADD(mi, 10 ,t3.time) as eTime," +
                    "                                          (t3.bus_count + t3.message_count) as count, " +
                    "                                          (t8.bus_count + t8.message_count) as fcount " +
                    "                                  FROM Grid as t1 INNER JOIN Segments as t2 ON t1.gIndex = t2.gIndex " +
                    "                                          INNER JOIN Traffics as t3 ON t3.segment_id = t2.id " +
                    "                                          INNER JOIN Traffics as t8 On t8.segment_id = t2.fNeighbor AND t8.time BETWEEN DATEADD(mi, -1 ,t3.time) AND DATEADD(mi, 1 ,t3.time) " +
                    "                                  WHERE t2.id = "+ id +" "+
                    "                                  ORDER BY t3.time ASC");

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
            String filePath = "Experiments/train_"+id+".csv";
            FileWriter csvWriterOrigin = new FileWriter(filePath);

            csvWriterOrigin.append("preCount");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("fCount");
            csvWriterOrigin.append(",");
            csvWriterOrigin.append("count");
            csvWriterOrigin.append("\n");
            csvWriterOrigin.append(TrainString);
            csvWriterOrigin.flush();
            csvWriterOrigin.close();

            System.out.println("Segment ID : "+id+" Was Inserted into DB. Duplicated date-time : "+duplicateDate);
        }
    }
}