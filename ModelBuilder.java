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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ModelBuilder {
    public static void main(String[] args) throws SQLException, IOException {
        String path = "E:/Mirzaie/Data Sets/2020/my.txt";
        String num = "1";

        //PCA pca = new PCA(path , num);


        /*SQLDatabaseConnection MySql = new SQLDatabaseConnection();
        ResultSet Ans = MySql.Select("  SELECT  t1.gIndex, " +
                "                                       t2.id, " +
                "                                       t3.time as sTime, " +
                "                                       DATEADD(mi, 10 ,t3.time) as eTime, " +
                "                                       (t3.bus_count + t3.message_count) as count, " +
                "                                       t4.temperature, t4.weatherType, t4.wind, t4.humidity, t4.barometer, t4.visibility," +
                "                                       t5.light, t5.surface, t5.defect," +
                "                                       t6.situation, t7.parkNumber " +
                "                                       FROM Grid as t1 INNER JOIN Segments as t2 ON t1.gIndex = t2.gIndex " +
                "                                       INNER JOIN Traffics as t3 ON t3.segment_id = t2.id " +
                "                                       LEFT OUTER JOIN Weather as t4 ON t4.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time)" +
                "                                       LEFT OUTER JOIN Crashes as t5 ON t5.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time) AND t5.gIndex = t1.gIndex " +
                "                                       LEFT OUTER JOIN Crimes as t6 ON t6.date BETWEEN t3.time AND DATEADD(mi, 10 ,t3.time) AND t6.gIndex = t1.gIndex " +
                "                                       LEFT OUTER JOIN ParkEvents as t7 ON t3.time BETWEEN t7.start_date AND t7.end_date AND t7.gIndex = t1.gIndex " +
                "                                       WHERE t2.id = 793 or t2.id = 1003" +
                "                                       ORDER BY t3.time ASC");

        double temp = 0.;
        double weatherT = 0.;
        double windD = 0.;
        double humidD = 0.;
        double barometerD = 0.;
        double visibleD = 0.;
        double nullD = -1000;
        double preCount = -1;

        List<double []> xAR = new ArrayList<>();
        //ArrayList<Double> yAR = new ArrayList<>();

        //int o = 1000;
        while (Ans.next()) {
            String gIndex = Ans.getString(1);
            int id = Integer.parseInt(Ans.getString(2));
            String date = Ans.getString(3);

            double count = Double.parseDouble(Ans.getString(5));
            preCount = (preCount < 0)? count : preCount; // For first Row value

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

            //System.out.println( gIndex +" | "+ id +" | "+ date +" | "+ count +" | "+ temperature +" | "+ weatherType +" | "+ wind +" | "+ humidity +" | "+ barometer +" | "+ visibility +" | "+ light +" | "+ surface +" | "+ defect +" | "+ crash +" | "+ crime +" | "+ parkEvent);

            xAR.add(new double[] { preCount, temperature, weatherType, wind, humidity, barometer, visibility, crash, crime, parkEvent, count });
            //yAR.add(count);
            preCount = count;

            //o--;
        }

        FileWriter csvWriter = new FileWriter("resources/train.csv");

        csvWriter.append("preCount");
        csvWriter.append(",");
        csvWriter.append("temperature");
        csvWriter.append(",");
        csvWriter.append("weatherType");
        csvWriter.append(",");
        csvWriter.append("wind");
        csvWriter.append(",");
        csvWriter.append("humidity");
        csvWriter.append(",");
        csvWriter.append("barometer");
        csvWriter.append(",");
        csvWriter.append("visibility");
        csvWriter.append(",");
        csvWriter.append("crash");
        csvWriter.append(",");
        csvWriter.append("crime");
        csvWriter.append(",");
        csvWriter.append("parkEvent");
        csvWriter.append(",");
        csvWriter.append("count");
        csvWriter.append("\n");

        for (double[] xs : xAR) {
            csvWriter.append(String.join(",", Arrays.toString(xs).replace("[", "").replace("]", "")));
            csvWriter.append("\n");
        }

        csvWriter.flush();
        csvWriter.close();
*/

        /*double[][] x = new double[xAR.size()][];
        for (int i = 0; i < xAR.size(); i++) {
            double[] row = xAR.get(i);
            x[i] = row;
        }
        double[] y = new double[yAR.size()];
        for (int i = 0; i < xAR.size(); i++) {
            y[i] = yAR.get(i);
        }

        System.out.println(Arrays.deepToString(x));
        System.out.println(Arrays.toString(y));*/

        /*MultipleLinearRegression regression = new MultipleLinearRegression(x,y);
        System.out.printf("%.2f + %.2f beta1 + %.2f beta2 + %.2f beta3   (R^2 = %.2f)\n",
                regression.beta(0), regression.beta(1), regression.beta(2), regression.beta(3), regression.R2());*/
    }
}