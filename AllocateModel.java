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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AllocateModel {
    public static void main(String[] args) throws SQLException, IOException {

        SQLDatabaseConnection MySql = new SQLDatabaseConnection();
        //StringBuilder sqlCommand = new StringBuilder();
        String sqlCommand = "";

        ResultSet GridModel = MySql.Select("select\tmodel_gIndex, model_intercept, model_fCount, model_weatherType, model_visibility, model_crash, model_crime, model_parkEvent, model_rSquared\t\t\n" +
                "from RegressionModel3 \n" +
                "where model_type = 1 and model_intercept != -100 and model_gLevel < 4 \n" +
                "group by model_gIndex,model_crash, model_crime, model_fCount, model_intercept, model_parkEvent, model_rSquared, model_visibility, model_weatherType");
        while (GridModel.next())
        {
            String gIndex = GridModel.getString(1);
            double intercept = Double.parseDouble(GridModel.getString(2));
            double fCount = Double.parseDouble(GridModel.getString(3));
            double weatherType = Double.parseDouble(GridModel.getString(4));
            double visibility = Double.parseDouble(GridModel.getString(5));
            double crash = Double.parseDouble(GridModel.getString(6));
            double crime = Double.parseDouble(GridModel.getString(7));
            double parkEvent = Double.parseDouble(GridModel.getString(8));
            double rSquared = Double.parseDouble(GridModel.getString(9));

            MySql.Command("Update RegressionModel3 set model_intercept = "+intercept+" , model_fCount = "+fCount+" , model_weatherType = "+weatherType+" , model_visibility = "+visibility+" , model_crash = "+crash+" , model_crime = "+crime+" , model_parkEvent = "+parkEvent+" , model_rSquared = "+rSquared+", accuracy_B = -99 where model_gIndex = '"+gIndex+"' and model_type = 1 and accuracy_A = -100");
           /* System.out.println("Update RegressionModel set model_intercept = "+intercept+" and model_fCount = "+fCount+" and model_weatherType = "+weatherType+"" +
                    " and model_visibility = "+visibility+" and model_crash = "+crash+" and model_crime = "+crime+" and model_parkEvent = "+parkEvent+" and model_rSquared = "+rSquared+" where model_gIndex = '"+gIndex+"' ");*/
        }
    }
}
