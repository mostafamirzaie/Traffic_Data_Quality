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

public class ModelGridTracker {
    public static void main(String[] args) throws SQLException, IOException {

        SQLDatabaseConnection MySql = new SQLDatabaseConnection();
        String filePathInputTrain = "";
        String filePathInputTest = "";
        //StringBuilder sqlCommand = new StringBuilder();
        String sqlCommand = "";

        //int level = 14;
        ResultSet SegmentsId = MySql.Select(" SELECT id, gIndex FROM Segments where id > 3 and id < 10");
        while (SegmentsId.next())
        {
            int id = Integer.parseInt(SegmentsId.getString(1));
            String gIndex = SegmentsId.getString(2);
            String gIndex_temp = "";
            filePathInputTest = "Experiments/Context_Train/Segments/"+id+".csv";
            //gIndex_temp = gIndex.substring(0, level);
            //filePathInputTrain = "Experiments/train/"+gIndex_temp+".csv";
            for (int i = 0; i < gIndex.length(); i++){
                gIndex_temp = gIndex.substring(0, gIndex.length() - i);
                if (gIndex_temp.length() > 3){
                    filePathInputTrain = "Experiments/Context_Train/Grids_with_weatherType=3_6_7/"+gIndex_temp+".csv";
                    MultipleLinearRegression4 regression = new MultipleLinearRegression4(filePathInputTrain,filePathInputTest,5);
                    sqlCommand = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_weatherType, model_visibility, model_crash, model_crime, model_parkEvent, model_rSquared, accuracy_B, model_type) VALUES ("+id+", "+gIndex_temp+", "+gIndex_temp.length()+", "+regression.parameters.get("Intercept")+", "+regression.parameters.get("weatherType")+", "+regression.parameters.get("visibility")+", "+regression.parameters.get("crash")+", "+regression.parameters.get("crime")+", "+regression.parameters.get("parkEvents")+", "+regression.rSquared+", "+regression.accuracy+", 1);";
                    //System.out.println(sqlCommand);
                    MySql.Command(sqlCommand);
                    System.out.println("id = "+id+" and level "+gIndex_temp.length()+" is inserted!");
                    //System.out.println("segment id = "+id+" and grid index = "+gIndex_temp+" is inserted!");
                }
            }
        }
        //sqlCommand = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_weatherType, model_visibility, model_crash, model_crime, model_parkEvent, model_rSquared, accuracy_B, model_type) VALUES (10, 11111111, 12, -100, -100, -100, -100, -100, -100, -100, -100, 1);";
        //MySql.Command(sqlCommand.toString());

    }
}