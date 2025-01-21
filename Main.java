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

import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import static java.lang.Math.*;

public class Main
{
    public static void main(String[] args) throws IOException, ParseException, SQLException, InterruptedException {

        String line = "";
        String splitBy = ",";
        ArrayList<Sensor> sos = new ArrayList<Sensor>();
        BufferedReader br = new BufferedReader(new FileReader("resources/Segments.csv"));
        StringBuilder sqlCommand = new StringBuilder();
        String sqlCommand2 = "";
        StringBuilder sqlCommand1 = new StringBuilder();
        SQLDatabaseConnection MySql = new SQLDatabaseConnection();

        while ((line = br.readLine()) != null)   //returns a Boolean value
        {
            String[] sensor = line.split(splitBy);    // use comma as separator
            int id = Integer.parseInt(sensor[0]);
            Double lat = Double.parseDouble(sensor[12]);
            Double lon = Double.parseDouble(sensor[13]);
            sos.add(new Sensor(id, lat, lon));
            /*
            sqlCommand.append("INSERT INTO Segments (id, street, direction, from_street, to_street, street_headings, minLat, minLong, maxLat, maxLong, meanLat, meanLong) "
                            + "VALUES ("+id+", \'"+sensor[1]+"\', \'"+sensor[2]+"\', \'"+sensor[3]+"\', \'"+sensor[4]+"\', \'"+sensor[5]+"\', "+sensor[6]+", "+sensor[7]+", "+sensor[8]+", "+sensor[9]+", "+lat+", "+lon+");");
            */
        }

        Grid outputGrid = new Grid(sos, 1, 1, new double[]{41.62, -87.84, 42.02, -87.01});

        ////Save Context Models in each Grid

        /*ResultSet Ans = MySql.Select("SELECT [model_gIndex]" +
                                    "      ,[model_gLevel]" +
                                    "      ,[model_intercept]" +
                                    "      ,[model_fCount]" +
                                    "      ,[model_weatherType]" +
                                    "      ,[model_visibility]" +
                                    "      ,[model_crash]" +
                                    "      ,[model_crime]" +
                                    "      ,[model_parkEvent]" +
                                    "  FROM [GridTest].[dbo].[RegressionModel3]" +
                                    "  where model_type = 1 and model_gLevel < 4" +
                                    "  group by [model_gIndex]" +
                                    "      ,[model_gLevel]" +
                                    "      ,[model_intercept]" +
                                    "      ,[model_fCount]" +
                                    "      ,[model_weatherType]" +
                                    "      ,[model_visibility]" +
                                    "      ,[model_crash]" +
                                    "      ,[model_crime]" +
                                    "      ,[model_parkEvent]" +
                                    "   order by model_gLevel");
        while (Ans.next()) {
            long gIndex = Long.parseLong(Ans.getString(1));
            int gLevel = Integer.parseInt(Ans.getString(2));
            double intercept = Double.parseDouble(Ans.getString(3));
            double fCount = Double.parseDouble(Ans.getString(4));
            double wType = Double.parseDouble(Ans.getString(5));
            double visibility = Double.parseDouble(Ans.getString(6));
            double crash = Double.parseDouble(Ans.getString(7));
            double crime = Double.parseDouble(Ans.getString(8));
            double parkEvent = Double.parseDouble(Ans.getString(9));
            GModelContext gModelContext = new GModelContext(intercept, fCount, wType, visibility, crash, crime, parkEvent);
            Grid gridTest = outputGrid.SearchByIndex(gIndex);
            gridTest.gModelContexts.add(gModelContext);
        }*/

        //////////////
        /////// Save Time Statistics in each Segment
        BufferedReader statistics = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/Statistics_All.csv"));
        statistics.readLine();
        while ((line = statistics.readLine()) != null)   //returns a Boolean value
        {
            String[] stat = line.split(splitBy);    // use comma as separator
            int segID = Integer.parseInt(stat[0]);
            int min = Integer.parseInt(stat[1]);
            int max = Integer.parseInt(stat[2]);
            int day = Integer.parseInt(stat[3]);
            int month = Integer.parseInt(stat[4]);
            Statistics statistics1 = new Statistics(min, max, day, month);
            Grid gridTest = outputGrid.SearchById(segID);
            gridTest.statistics.add(statistics1);
        }

        ///////////
        /////// Save Context Model in each Segment
        ResultSet SegmentsId = MySql.Select(" SELECT id from Segments");
        while (SegmentsId.next()) {
            int id = Integer.parseInt(SegmentsId.getString(1));

            ResultSet SegOwnContext = MySql.Select("SELECT top 1 [model_gLevel]" +
                    "      ,[model_intercept]" +
                    "      ,[model_fCount]" +
                    "      ,[model_weatherType]" +
                    "      ,[model_visibility]" +
                    "      ,[model_crash]" +
                    "      ,[model_crime]" +
                    "      ,[model_parkEvent]" +
                    "      ,[accuracy_A]" +
                    "  FROM [GridTest].[dbo].[RegressionModel3]" +
                    "  where [segment_id] = " + id + " and model_type = 1" +
                    "  group by" +
                    "  [model_gLevel]" +
                    "      ,[model_intercept]" +
                    "      ,[model_fCount]" +
                    "      ,[model_weatherType]" +
                    "      ,[model_visibility]" +
                    "      ,[model_crash]" +
                    "      ,[model_crime]" +
                    "      ,[model_parkEvent]" +
                    "      ,[accuracy_A]" +
                    "  having accuracy_A = max(accuracy_A) and model_gLevel = min(model_gLevel)" +
                    "  order by model_gLevel desc");
            while (SegOwnContext.next()) {
                int gLevel = Integer.parseInt(SegOwnContext.getString(1));
                int parentGLevel = ((gLevel - 1) == 0) ? 1 : gLevel - 1;
                double intercept = Double.parseDouble(SegOwnContext.getString(2));
                double fCount = Double.parseDouble(SegOwnContext.getString(3));
                double wType = Double.parseDouble(SegOwnContext.getString(4));
                double visibility = Double.parseDouble(SegOwnContext.getString(5));
                double crash = Double.parseDouble(SegOwnContext.getString(6));
                double crime = Double.parseDouble(SegOwnContext.getString(7));
                double parkEvent = Double.parseDouble(SegOwnContext.getString(8));
                GModelContext gModelContext = new GModelContext(intercept, fCount, wType, visibility, crash, crime, parkEvent);
                Grid gridTest = outputGrid.SearchById(id);
                gridTest.gModelContexts.add(gModelContext);
                if (parentGLevel == gLevel) {
                    GParentModelContext gParentModelContext = new GParentModelContext(intercept, fCount, wType, visibility, crash, crime, parkEvent);
                    gridTest.gParentModelContexts.add(gParentModelContext);
                } else {
                    ResultSet SegParentContext = MySql.Select("SELECT top 1 [model_gLevel]" +
                            "      ,[model_intercept]" +
                            "      ,[model_fCount]" +
                            "      ,[model_weatherType]" +
                            "      ,[model_visibility]" +
                            "      ,[model_crash]" +
                            "      ,[model_crime]" +
                            "      ,[model_parkEvent]" +
                            "      ,[accuracy_A]" +
                            "  FROM [GridTest].[dbo].[RegressionModel3]" +
                            "  where [segment_id] = " + id + " and model_type = 1 and model_gLevel = " + parentGLevel + "" +
                            "  group by" +
                            "  [model_gLevel]" +
                            "      ,[model_intercept]" +
                            "      ,[model_fCount]" +
                            "      ,[model_weatherType]" +
                            "      ,[model_visibility]" +
                            "      ,[model_crash]" +
                            "      ,[model_crime]" +
                            "      ,[model_parkEvent]" +
                            "      ,[accuracy_A]" +
                            "  order by model_gLevel desc");
                    while (SegParentContext.next()) {
                        double P_intercept = Double.parseDouble(SegParentContext.getString(2));
                        double P_fCount = Double.parseDouble(SegParentContext.getString(3));
                        double P_wType = Double.parseDouble(SegParentContext.getString(4));
                        double P_visibility = Double.parseDouble(SegParentContext.getString(5));
                        double P_crash = Double.parseDouble(SegParentContext.getString(6));
                        double P_crime = Double.parseDouble(SegParentContext.getString(7));
                        double P_parkEvent = Double.parseDouble(SegParentContext.getString(8));
                        GParentModelContext gParentModelContext = new GParentModelContext(P_intercept, P_fCount, P_wType, P_visibility, P_crash, P_crime, P_parkEvent);
                        outputGrid.SearchById(id);
                        gridTest.gParentModelContexts.add(gParentModelContext);
                    }
                }

            }
        }

        Grid grid = outputGrid.SearchById(645);
        long gIndex = grid.gIndex;
        System.out.println(gIndex);
        /////// Save Traffic Model in each Grid

        /*ResultSet Ans2 = MySql.Select("SELECT [model_gIndex]" +
                "      ,[model_gLevel]" +
                "      ,[model_intercept]" +
                "      ,[model_preCount]" +
                "      ,[model_fCount]" +
                "  FROM [GridTest].[dbo].[RegressionModel3]" +
                "  where model_type = 0 and [model_gLevel] > 3" +
                "  group by [model_gIndex]" +
                "      ,[model_gLevel]" +
                "      ,[model_intercept]" +
                "      ,[model_preCount]" +
                "      ,[model_fCount]" +
                "  order by model_gLevel");
        while (Ans2.next()) {
            long gIndex2 = Long.parseLong(Ans2.getString(1));
            int gLevel = Integer.parseInt(Ans2.getString(2));
            double intercept = Double.parseDouble(Ans2.getString(3));
            double preCount = Double.parseDouble(Ans2.getString(4));
            double fCount = Double.parseDouble(Ans2.getString(5));
            GModelTraffic gModelTraffic = new GModelTraffic(intercept, preCount, fCount);
            Grid gridTest = outputGrid.SearchByIndex(gIndex2);
            gridTest.gModelTraffics.add(gModelTraffic);
        }*/

        ///////////////
        /////// Save Traffic Model in each Segments

        ResultSet SegmentsId2 = MySql.Select(" SELECT id from Segments");
        while (SegmentsId2.next()) {
            int id = Integer.parseInt(SegmentsId2.getString(1));

            ResultSet SegOwnTraffic = MySql.Select("SELECT top 1 [model_gLevel]" +
                    "      ,[model_intercept]" +
                    "      ,[model_preCount]" +
                    "      ,[model_fCount]" +
                    "      ,[accuracy_A]" +
                    "  FROM [GridTest].[dbo].[RegressionModel3]" +
                    "  where [segment_id] = " + id + " and model_type = 0" +
                    "  group by" +
                    "  [model_gLevel]" +
                    "      ,[model_intercept]" +
                    "      ,[model_preCount]" +
                    "      ,[model_fCount]" +
                    "      ,[accuracy_A]" +
                    "  having accuracy_A = max(accuracy_A) and model_gLevel = min(model_gLevel)" +
                    "  order by model_gLevel desc");
            while (SegOwnTraffic.next()) {
                int gLevel = Integer.parseInt(SegOwnTraffic.getString(1));
                int parentGLevel = ((gLevel - 1) == 4) ? 5 : gLevel - 1;
                double intercept = Double.parseDouble(SegOwnTraffic.getString(2));
                double preCount = Double.parseDouble(SegOwnTraffic.getString(3));
                double fCount = Double.parseDouble(SegOwnTraffic.getString(4));
                GModelTraffic gModelTraffic = new GModelTraffic(intercept, preCount, fCount);
                Grid gridTest = outputGrid.SearchById(id);
                gridTest.gModelTraffics.add(gModelTraffic);
                if (parentGLevel == gLevel) {
                    GParentModelTraffic gParentModelTraffic = new GParentModelTraffic(intercept, preCount, fCount);
                    gridTest.gParentModelTraffics.add(gParentModelTraffic);
                } else {
                    ResultSet SegParentTraffic = MySql.Select("SELECT top 1 [model_gLevel]" +
                            "      ,[model_intercept]" +
                            "      ,[model_preCount]" +
                            "      ,[model_fCount]" +
                            "      ,[accuracy_A]" +
                            "  FROM [GridTest].[dbo].[RegressionModel3]" +
                            "  where [segment_id] = " + id + " and model_type = 0 and model_gLevel = " + parentGLevel + "" +
                            "  group by" +
                            "  [model_gLevel]" +
                            "      ,[model_intercept]" +
                            "      ,[model_preCount]" +
                            "      ,[model_fCount]" +
                            "      ,[accuracy_A]" +
                            "  order by model_gLevel desc");
                    while (SegParentTraffic.next()) {
                        double P_intercept = Double.parseDouble(SegParentTraffic.getString(2));
                        double P_preCount = Double.parseDouble(SegParentTraffic.getString(3));
                        double P_fCount = Double.parseDouble(SegParentTraffic.getString(4));

                        GParentModelTraffic gParentModelTraffic = new GParentModelTraffic(P_intercept, P_preCount, P_fCount);
                        outputGrid.SearchById(id);
                        gridTest.gParentModelTraffics.add(gParentModelTraffic);
                    }
                }

            }
        }

        /*ArrayList<Statistics> test = outputGrid.SearchById(1).statistics;
        for (Statistics t: test){
            if (t.day == 15 && t.month == 2)
                System.out.println("Min: "+ t.min + " and Max: "+ t.max);
        }*/

        // Grid newGrid = outputGrid.SearchByIdLevel(41, 6);
        //System.out.println(newGrid.gModelTraffics.toString());
        //System.out.println(newGrid.gModelTraffics.get(0).intercept);



        /*File output = new File("resources/Grids_2.txt");
        FileWriter outputWriter = new FileWriter(output);
        ArrayList<Grid> endPoints = outputGrid.SearchByLevel(2);
        int ids;
        for (Grid endPoint: endPoints) {
            if (endPoint.sos.size() >= 1){
                sqlCommand1.append(endPoint.gIndex).append(": ");
                for (int i=0; i<endPoint.sos.size(); i++){
                    //sqlCommand1.append("t2.id = ");
                    ids = endPoint.sos.get(i).id;
                    sqlCommand1.append(ids).append(" | ");
                }
                sqlCommand1.append("\n");
                //sqlCommand.append(endPoint.gIndex).append(", ").append(endPoint.level).append(", ").append(endPoint.border[0]).append(", ").append(endPoint.border[1]).append(", ").append(endPoint.border[2]).append(", ").append(endPoint.border[3]).append(sqlCommand1).append("\n");
            }
        }
        outputWriter.write(sqlCommand1.toString());
        outputWriter.close();*/


        /*ArrayList<Grid> endPoints = outputGrid.SearchByLevel(1);
        for (Grid endPoint: endPoints) {
            if (endPoint.sos.size() > 0){
                System.out.println("The Grid index = "+ endPoint.gIndex +" is started!");
                String filePathInputTrain = "Experiments/Context_Train/Grids/"+endPoint.gIndex+".csv";
                FileWriter InputTrainOrigin = new FileWriter(filePathInputTrain);
                InputTrainOrigin.append("fCount");
                InputTrainOrigin.append(",");
                InputTrainOrigin.append("weatherType");
                InputTrainOrigin.append(",");
                InputTrainOrigin.append("visibility");
                InputTrainOrigin.append(",");
                InputTrainOrigin.append("crash");
                InputTrainOrigin.append(",");
                InputTrainOrigin.append("crime");
                InputTrainOrigin.append(",");
                InputTrainOrigin.append("parkEvents");
                InputTrainOrigin.append(",");
                InputTrainOrigin.append("count");
                InputTrainOrigin.append("\n");
                for (int i = 0; i < endPoint.sos.size(); i++){
                    BufferedReader data = new BufferedReader(new FileReader("Experiments/Context_Train/Segments/"+endPoint.sos.get(i).id+".csv"));
                    data.readLine();
                    while ((line = data.readLine()) != null)   //returns a Boolean value
                    {
                        String[] train = line.split(splitBy);    // use comma as separator
                        double fCount = Double.parseDouble(train[0]);
                        double weatherType = Double.parseDouble(train[1]);
                        double visibility = Double.parseDouble(train[2]);
                        double crash = Double.parseDouble(train[3]);
                        double crime = Double.parseDouble(train[4]);
                        double parkEvent = Double.parseDouble(train[5]);
                        double count = Double.parseDouble(train[6]);
                        InputTrainOrigin.append(fCount +","+ weatherType +","+ visibility +","+ crash +","+ crime +","+ parkEvent +","+ count +"\n");
                    }
                    System.out.println("The segment id = "+ endPoint.sos.get(i).id +" is inserted!");
                }
                InputTrainOrigin.flush();
                InputTrainOrigin.close();
                System.out.println("The Grid index = "+ endPoint.gIndex +" is completed!");
            }
        }*/
        /*int level = 14;
        ArrayList<Grid> endPoints = outputGrid.SearchByLevel(level);
        for (Grid endPoint: endPoints) {
            if (endPoint.sos.size() > 0){
                System.out.println("The Grid index = "+ endPoint.gIndex +" is started!");
                String filePathInputTrain = "Experiments/Context_Train/Grids/"+endPoint.gIndex+".csv";

                for (int i = 0; i < endPoint.sos.size(); i++){
                    System.out.println("The segment id = "+ endPoint.sos.get(i).id +" is started!");
                    String filePathInputTest = "Experiments/Context_Train/Segments/"+endPoint.sos.get(i).id+".csv";
                    MultipleLinearRegression4 regression = new MultipleLinearRegression4(filePathInputTrain,filePathInputTest,5);
                    //sqlCommand2 = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_preCount, model_fCount, model_rSquared, accuracy_A) VALUES ("+endPoint.sos.get(i).id+", "+endPoint.gIndex+", "+level+", "+regression.parameters.get("Intercept")+", "+regression.parameters.get("preCount")+", "+regression.parameters.get("fCount")+", "+regression.rSquared+", "+regression.accuracy+");";
                    sqlCommand2 = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_fCount, model_weatherType, model_visibility, model_crash, model_crime, model_parkEvent, model_rSquared, accuracy_B, model_type) VALUES ("+endPoint.sos.get(i).id+", "+endPoint.gIndex+", "+level+", "+regression.parameters.get("Intercept")+", "+regression.parameters.get("fCount")+","+regression.parameters.get("weatherType")+", "+regression.parameters.get("visibility")+", "+regression.parameters.get("crash")+", "+regression.parameters.get("crime")+", "+regression.parameters.get("parkEvents")+", "+regression.rSquared+", "+regression.accuracy+", 1);";
                    System.out.println(sqlCommand2);
                    //MySql.Command(sqlCommand2);
                    System.out.println("The segment id = "+ endPoint.sos.get(i).id +" is inserted!");
                }
                System.out.println("The Grid index = "+ endPoint.gIndex +" is completed!");
            }
        }*/

        /*int level = 4;
        ArrayList<Grid> endPoints = outputGrid.SearchByLevel(level);
        for (Grid endPoint: endPoints) {
            if (endPoint.sos.size() > 0){
                System.out.println("The Grid index = "+ endPoint.gIndex +" is started!");
                String filePathInputTrain = "Experiments/Traffic_Train_just_precount/Grids/Level4/"+endPoint.gIndex+".csv";

                for (int i = 0; i < endPoint.sos.size(); i++){
                    System.out.println("The segment id = "+ endPoint.sos.get(i).id +" is started!");
                    String filePathInputTest = "Experiments/Traffic_Train_just_precount/Segments/"+endPoint.sos.get(i).id+".csv";
                    MultipleLinearRegression5 regression = new MultipleLinearRegression5(filePathInputTrain,filePathInputTest,5);
                    //sqlCommand2 = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_preCount, model_fCount, model_rSquared, accuracy_A) VALUES ("+endPoint.sos.get(i).id+", "+endPoint.gIndex+", "+level+", "+regression.parameters.get("Intercept")+", "+regression.parameters.get("preCount")+", "+regression.parameters.get("fCount")+", "+regression.rSquared+", "+regression.accuracy+");";
                    sqlCommand2 = "INSERT INTO RegressionModel3 (segment_id, model_gIndex, model_gLevel, model_intercept, model_preCount, model_rSquared, accuracy_A, model_type) VALUES ("+endPoint.sos.get(i).id+", "+endPoint.gIndex+", "+level+", "+regression.parameters.get("Intercept")+", "+regression.parameters.get("preCount")+", "+regression.rSquared+", "+regression.accuracy+", 3);";
                    //System.out.println(sqlCommand2);
                    MySql.Command(sqlCommand2);
                    System.out.println("The segment id = "+ endPoint.sos.get(i).id +" is inserted!");
                }
                System.out.println("The Grid index = "+ endPoint.gIndex +" is completed!");
            }
        }*/


        /*int level = 3;
        ArrayList<Grid> endPoints = outputGrid.SearchByLevel(level);
        String filePathOutput = "";
        for (Grid endPoint: endPoints) {
            if (endPoint.sos.size() > 0){
                System.out.println("The Grid index = "+ endPoint.gIndex +" is started!");
                filePathOutput = "Experiments/Test Data Set/"+endPoint.gIndex+".csv";
                StringBuilder sCommand = new StringBuilder();

                for (int i = 0; i < endPoint.sos.size(); i++){
                    ResultSet SegOwnContext = MySql.Select("SELECT segmentId, accuracy FROM [GridTest].[dbo].[FinalModelAccuracy] where segmentId="+endPoint.sos.get(i).id+"");
                    while (SegOwnContext.next()){
                        int id = Integer.parseInt(SegOwnContext.getString(1));
                        double accuracy = Double.parseDouble(SegOwnContext.getString(2));
                        sCommand.append(id + "," + accuracy +"\n");
                    }
                }
                FileWriter csvWriterOrigin = new FileWriter(filePathOutput);
                csvWriterOrigin.append("id");
                csvWriterOrigin.append(",");
                csvWriterOrigin.append("accuracy");
                csvWriterOrigin.append("\n");
                csvWriterOrigin.append(sCommand);
                csvWriterOrigin.flush();
                csvWriterOrigin.close();
                System.out.println("The Grid index = "+ endPoint.gIndex +" is completed!");
            }
        }*/


       /* int level = 1;
        ArrayList<Grid> endPoints = outputGrid.SearchByLevel(level);
        for (Grid endPoint: endPoints) {
            if (endPoint.sos.size() > 0){
                System.out.println("The Grid index = "+ endPoint.gIndex +" is started!");
                String filePathInputTrain = "Experiments/Context_Train/Grids/"+endPoint.gIndex+".csv";

                for (int i = 0; i < endPoint.sos.size(); i++){
                    //String filePathOutput = "Experiments/Context_Train/Results/"+endPoint.sos.get(i).id+"_"+endPoint.gIndex+".csv";
                    System.out.println("The segment id = "+ endPoint.sos.get(i).id +" is started!");
                    String filePathInputTest = "Experiments/Context_Train/Segments/"+endPoint.sos.get(i).id+".csv";
                    BufferedReader brGrid = new BufferedReader(new FileReader(filePathInputTrain));
                    brGrid.readLine();
                    BufferedReader brSegment = new BufferedReader(new FileReader(filePathInputTest));
                    brSegment.readLine();
                    if (brSegment.readLine() == null || brGrid.readLine() == null)
                        sqlCommand2 = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_fCount, model_weatherType, model_visibility, model_crash, model_crime, model_parkEvent, model_rSquared, accuracy_A, model_type) VALUES ("+endPoint.sos.get(i).id+", "+endPoint.gIndex+", "+level+", -100, -100, -100, -100, -100, -100, -100, -100, -100, 1);";
                    else {
                        brGrid.readLine();
                        if (brGrid.readLine() == null)
                            sqlCommand2 = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_fCount, model_weatherType, model_visibility, model_crash, model_crime, model_parkEvent, model_rSquared, accuracy_A, model_type) VALUES ("+endPoint.sos.get(i).id+", "+endPoint.gIndex+", "+level+", -100, -100, -100, -100, -100, -100, -100, -100, -100, 1);";
                        else{
                            MultipleLinearRegression4 regression = new MultipleLinearRegression4(filePathInputTrain,filePathInputTest,5);
                            sqlCommand2 = "INSERT INTO RegressionModel (segment_id, model_gIndex, model_gLevel, model_intercept, model_fCount, model_weatherType, model_visibility, model_crash, model_crime, model_parkEvent, model_rSquared, accuracy_A, model_type) VALUES ("+endPoint.sos.get(i).id+", "+endPoint.gIndex+", "+level+", "+regression.parameters.get("Intercept")+", "+regression.parameters.get("fCount")+","+regression.parameters.get("weatherType")+", "+regression.parameters.get("visibility")+", "+regression.parameters.get("crash")+", "+regression.parameters.get("crime")+", "+regression.parameters.get("parkEvents")+", "+regression.rSquared+", "+regression.accuracy+", 1);";
                        }
                    }
                    //System.out.println(sqlCommand2);
                    MySql.Command(sqlCommand2);
                    //System.out.println("The segment id = "+ endPoint.sos.get(i).id +" is inserted!");
                }
                System.out.println("The Grid index = "+ endPoint.gIndex +" is completed!");
            }
        }*/


        /*int level = 14;
        File output = new File("Experiments/Grids/"+level+".txt");
        FileWriter outputWriter = new FileWriter(output);
        ArrayList<Grid> endPoints = outputGrid.SearchByLevel(level);
        for (Grid endPoint: endPoints) {
            sqlCommand1.append("Grid Index = "+endPoint.gIndex+": \n");
            for (int i = 0; i < endPoint.sos.size(); i++){
                sqlCommand1.append(endPoint.sos.get(i).id).append("| ");
            }
            sqlCommand1.append("\n");

           *//* if (endPoint.sos.size() >= 1)
                sqlCommand1.append("'");
            sqlCommand.append("INSERT INTO Grids (gIndex, segments) VALUES (").append(endPoint.gIndex).append(", ").append(sqlCommand1).append(");");
            sqlCommand1.setLength(0);*//*
        }
        outputWriter.write(sqlCommand1.toString());
        outputWriter.close();*/
        //System.out.println(sqlCommand.toString());
        //MySql.Command(sqlCommand.toString());


        /*
        ArrayList<Grid> endPoints = outputGrid.FindEndPoint();
        for (Grid endPoint: endPoints) {
            if (endPoint.sos.size() == 1) {
                int sId = endPoint.sos.get(0).id;
                sqlCommand.append("UPDATE Segments SET gIndex = ").append(endPoint.gIndex).append(" WHERE id = ").append(sId).append("; ");
            }
        }
        */


       /* BufferedReader brCrashes = new BufferedReader(new FileReader("resources/Crashes.csv"));
        while ((line = brCrashes.readLine()) != null)   //returns a Boolean value
        {
            String[] crash = line.split(splitBy);    // use comma as separator
            Date date = toDate(crash[0], "MM/dd/yyyy HH:mm");
            int light = Integer.parseInt(crash[1]);
            int surface = Integer.parseInt(crash[2]);
            int defect = Integer.parseInt(crash[3]);
            Double lat = (crash[4] != null)? Double.parseDouble(crash[4]) : null;
            Double lon = (crash[5] != null)? Double.parseDouble(crash[5]) : null;
            if(lat != null && lon != null) {
                Crash newCrash = new Crash(date, light, surface, defect, lat, lon);
                if (outputGrid.SearchByLocation(new double[]{lat, lon}) != null){
                    Grid gridTest = outputGrid.SearchByLocation(new double[]{lat, lon});
                    gridTest.crashes.add(newCrash);
                    long ind = gridTest.gIndex;
                    java.sql.Timestamp sqlDate = new java.sql.Timestamp( date.getTime() );
                    sqlCommand.append("INSERT INTO Crashes (date, light, surface, defect, latitude, longitude, gIndex) VALUES (\'"+sqlDate+"\', "+light+", "+surface+", "+defect+", "+lat+", "+lon+", "+ind+"); ");
                }
            }
        }*/


      /*  BufferedReader brParkEvents = new BufferedReader(new FileReader("resources/Park_Events.csv"));
while ((line = brParkEvents.readLine()) != null)   //returns a Boolean value
{
    String[] parkEvent = line.split(splitBy);    // use comma as separator
    int parkNum = Integer.parseInt(parkEvent[0]);
    Date sDate = toDate(parkEvent[1], "MM/dd/yyyy");
    Date eDate = toDate(parkEvent[2], "MM/dd/yyyy");
    String permitStatus = parkEvent[3];
    Double lat = (parkEvent[4] != null)? Double.parseDouble(parkEvent[4]) : null;
    Double lon = (parkEvent[5] != null)? Double.parseDouble(parkEvent[5]) : null;
    if(permitStatus.equals("Approved")){
        if(lat != null && lon != null){
            ParkEvent newParkEvent = new ParkEvent(parkNum, sDate, eDate, lat, lon);
            Grid gridTest = outputGrid.SearchByLocation(new double[]{lat, lon});
            if(gridTest != null){
                gridTest.parksEvents.add(newParkEvent);
                java.sql.Timestamp sqlSDate = new java.sql.Timestamp( sDate.getTime() + (9 * 60 * 60 *1000 ));
                java.sql.Timestamp sqlEDate = new java.sql.Timestamp( eDate.getTime() + (21 * 60 * 60 *1000 ));
                sqlCommand.append("INSERT INTO ParkEvents (parkNumber, start_date, end_date, latitude, longitude, gIndex) VALUES ("+parkNum+", \'"+sqlSDate+"\', \'"+sqlEDate+"\', "+lat+", "+lon+", "+gridTest.gIndex+"); ");
            }
        }
    }
}
MySql.Command(sqlCommand.toString());*/



        /*
        BufferedReader brCrimes = new BufferedReader(new FileReader("resources/Crimes.csv"));
        while ((line = brCrimes.readLine()) != null)   //returns a Boolean value
        {
            String[] crime = line.split(splitBy);    // use comma as separator
            if(crime.length >= 4) {
                Date date = toDate(crime[0], "MM/dd/yyyy HH:mm");
                String location = crime[2];
                Double lat = (crime[3] != null)? Double.parseDouble(crime[3]) : null;
                Double lon = (crime[4] != null)? Double.parseDouble(crime[4]) : null;
                if(location.equals("ALLEY") || location.equals("STREET") || location.equals("HIGHWAY/EXPRESSWAY")){
                    if(lat != null && lon != null){
                        Crime newCrime = new Crime(date, location, lat, lon);
                        if(outputGrid.SearchByLocation(new double[]{lat, lon}) != null){
                            Grid gridTest = outputGrid.SearchByLocation(new double[]{lat, lon});
                            gridTest.crimes.add(newCrime);
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp( date.getTime() );
                            sqlCommand.append("INSERT INTO Crimes (date, situation, latitude, longitude, gIndex) VALUES (\'"+sqlDate+"\', \'"+location+"\', "+lat+", "+lon+", "+gridTest.gIndex+");");

                        }

                    }
                }
            }
        }
        */


        /*String start_date = null;
        //ArrayList<Weather> weathers = new ArrayList<>();
        int temperature1 = -1000;
        int wType1 = -1000;
        int wind1 = -1000;
        Double humidity1 = -1000.0;
        Double barometer1= -1000.0;
        int visibility1= -1000;
        BufferedReader brWeather = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/Weather2020.csv"));
        while ((line = brWeather.readLine()) != null)   //returns a Boolean value
        {
            String[] weather = line.split(splitBy);    // use comma as separator
            if(weather.length == 8) {
                Date edate = toDate(weather[0], "MM/dd/yyyy HH:mm");
                Date sdate = (start_date != null)?toDate(start_date, "MM/dd/yyyy HH:mm"):toDate(weather[0], "MM/dd/yyyy HH:mm");
                start_date = weather[0];
                ////////////////
                ///////////////   IF DATASET IS NOT CLEAN e.g visibility = 12 km not 12
                //////////////
                String wTempreture = weather[1].substring(0, weather[1].length() - 3);
                temperature1 = Integer.parseInt(wTempreture);
                int temperature = (temperature1 != -1000)?temperature1:Integer.parseInt(wTempreture);
                int wType = (wType1 != -1000)? wType1:Integer.parseInt(weather[3]);
                wType1 = Integer.parseInt(weather[3]);
                String wWind = weather[4].substring(0, weather[4].length() - 5);
                int wind = (wind1 != -1000)?wind1:Integer.parseInt(wWind);
                wind1 = Integer.parseInt(wWind);
                Double humidity = (humidity1 != -1000.0)? humidity1: Double.parseDouble(weather[5]);
                humidity1 = Double.parseDouble(weather[5]);
                String wBarometer = weather[6].substring(0, weather[6].length() - 5);
                Double barometer = (barometer1 != -1000.0)? barometer1:Double.parseDouble(wBarometer);
                barometer1 = Double.parseDouble(wBarometer);
                String wVisible = weather[7].substring(0, weather[7].length() - 3);
                int visibility = (visibility1 != -1000)?visibility1:Integer.parseInt(wVisible);
                visibility1 = Integer.parseInt(wVisible);
                //Weather newWeather = new Weather(date, temperature, wType, wind, humidity, barometer, visibility);
                //weathers.add(newWeather);
                java.sql.Timestamp sDate = new java.sql.Timestamp( sdate.getTime() );
                java.sql.Timestamp eDate = new java.sql.Timestamp( edate.getTime() );
                sqlCommand.append("INSERT INTO WeatherEndDate (start_date, end_date, temperature, weatherType, wind, humidity, barometer, visibility) VALUES (\'"+sDate+"\', \'"+eDate+"\', "+temperature+", "+wType+", "+wind+", "+humidity+", "+barometer+", "+visibility+");");
                //System.out.println("INSERT INTO WeatherEndDate (start_date, end_date, temperature, weatherType, wind, humidity, barometer, visibility) VALUES (\'"+sDate+"\', \'"+eDate+"\', "+temperature+", "+wType+", "+wind+", "+humidity+", "+barometer+", "+visibility+");");

            }
        }
        MySql.Command(sqlCommand.toString());*/


       /* int id=0;
        String sqlCommand5 = "";
        BufferedReader brTraffic = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Chicago_Traffic_Tracker_-01122020_31122020.csv"));
        brTraffic.readLine();

        while ((line = brTraffic.readLine()) != null)   //returns a Boolean value
        {
            String[] traffic = line.split(splitBy);    // use comma as separator
            if(traffic.length == 22) {
                Date date = toDate(traffic[0], "MM/dd/yyyy hh:mm:ss a");
                long segId = Long.parseLong(traffic[1]);
                int bus = Integer.parseInt(traffic[10]);
                int message= Integer.parseInt(traffic[11]);
                //Traffic newTraffic = new Traffic(date, segId, vCount);
                //outputGrid.SearchById(segId).traffics.add(newTraffic);
                java.sql.Timestamp sqlDate = new java.sql.Timestamp( date.getTime() );
                id++;
                System.out.println(id);
                sqlCommand5 = "INSERT INTO Traffics (time, segment_id, bus_count, message_count) VALUES (\'"+sqlDate+"\', "+segId+", "+bus+" , "+message+");";
                //sqlCommand.append("INSERT INTO Traffic (date, segment_id, bus_count, message_count) VALUES (\'"+sqlDate+"\', "+segId+", "+bus+", "+message+");");
                MySql.Command(sqlCommand5);
            }
        }*/

       /* ////////////////DATA SERVER

        ServerSocket listener = new ServerSocket(9090);
        br = null;
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            br = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/wtlab2021/Traffic-small.csv"));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                while((line = br.readLine()) != null){
                    String[] inputs = line.split(",");
                    if(inputs.length == 22){
                        //Date date = toDate(inputs[0], "MM/dd/yyyy hh:mm:ss a");
                        long segId = Long.parseLong(inputs[1]);
                        //int bus = Integer.parseInt(inputs[10]);
                        //int message= Integer.parseInt(inputs[11]);
                        Grid grr = outputGrid.SearchByIdLevel(segId, 5);
                        long gIndex = grr.gIndex;
                        String outputs = inputs[0] + "," + inputs[1] + "," + inputs[10] + "," + inputs[11] + "," + gIndex;

                        //Input input = new Input(date, segId, bus, message);
                        out.println(outputs);
                        Thread.sleep(10);
                    }
                }

            } finally{
                socket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            listener.close();
            if (br != null)
                br.close();
        }*/

        ////////////////DATA SERVER 2

        /*ServerSocket listener = new ServerSocket(9090);
        br = null;
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            br = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/01_2020/combind.csv"));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                br.readLine();
                while((line = br.readLine()) != null){
                    String[] inputs = line.split(",");
                    if(inputs.length == 9){
                        int id = Integer.parseInt(inputs[0]);
                        String date = inputs[1];
                        Date date2 = toDate(date, "yyyy-MM-dd hh:mm:ss");
                        int day = date2.getDay();
                        int month = date2.getMonth();
                        double fCount = Double.parseDouble(inputs[2]);
                        double wType = Double.parseDouble(inputs[3]);
                        double visibility = Double.parseDouble(inputs[4]);
                        double crash = Double.parseDouble(inputs[5]);
                        double crime = Double.parseDouble(inputs[6]);
                        double parkEvent = Double.parseDouble(inputs[7]);
                        double count = Double.parseDouble(inputs[8]);
                        /// gIndex extraction
                        Grid grr = outputGrid.SearchById(id);
                        long gIndex = grr.gIndex;
                        ///Own Context model extraction
                        double M_intercept = grr.gModelContexts.get(0).intercept;
                        double M_fCount = grr.gModelContexts.get(0).fCount;
                        double M_wType = grr.gModelContexts.get(0).wType;
                        double M_visibility = grr.gModelContexts.get(0).visibility;
                        double M_crash = grr.gModelContexts.get(0).crash;
                        double M_crime = grr.gModelContexts.get(0).crime;
                        double M_parkEvent = grr.gModelContexts.get(0).parkEvent;

                        ///Parent Context model extraction
                        double P_intercept = grr.gParentModelContexts.get(0).intercept;
                        double P_fCount = grr.gParentModelContexts.get(0).fCount;
                        double P_wType = grr.gParentModelContexts.get(0).wType;
                        double P_visibility = grr.gParentModelContexts.get(0).visibility;
                        double P_crash = grr.gParentModelContexts.get(0).crash;
                        double P_crime = grr.gParentModelContexts.get(0).crime;
                        double P_parkEvent = grr.gParentModelContexts.get(0).parkEvent;

                        ///Own Traffic model extraction
                        double TM_intercept = grr.gModelTraffics.get(0).intercept;
                        double TM_preCount = grr.gModelTraffics.get(0).preCount;
                        double TM_fCount = grr.gModelTraffics.get(0).fCount;

                        ///Parent Traffic model extraction
                        double TP_intercept = grr.gParentModelTraffics.get(0).intercept;
                        double TP_preCount = grr.gParentModelTraffics.get(0).preCount;
                        double TP_fCount = grr.gParentModelTraffics.get(0).fCount;

                        ///// Min and Max of the last year
                        double min = 0;
                        double max = 50;
                        ArrayList<Statistics> test = outputGrid.SearchById(id).statistics;
                        for (Statistics t: test){
                            if (t.day == day && t.month == month){
                                min = t.min;
                                max = t.max;
                            }
                        }

                        String outputs = gIndex + "," + id + "," + date + "," + fCount + "," + wType + "," + visibility + "," + crash + "," + crime + "," + parkEvent + "," + count + "," + M_intercept + "," + M_fCount + "," + M_wType + "," + M_visibility + "," + M_crash + "," + M_crime + "," + M_parkEvent + "," + P_intercept + "," + P_fCount + "," + P_wType + "," + P_visibility + "," + P_crash + "," + P_crime + "," + P_parkEvent + "," + TM_intercept + "," + TM_preCount + "," + TM_fCount + "," + TP_intercept + "," + TP_preCount + "," + TP_fCount + "," + min + "," + max;
                        //System.out.println(outputs);

                        //Input input = new Input(date, segId, bus, message);
                        out.println(outputs);
                        //Thread.sleep(1);
                    }
                }

            } finally{
                socket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            listener.close();
            if (br != null)
                br.close();
        }*/

        /////////////////////////////
        ///////////////DATA SERVER 3
        /*Integer[] preCount = new Integer[1310];
        for (int i =1; i<1310; i++)
            preCount[i] = 0;

        String sDate = "2020-01-01 00:00:00.0000000";
        //String sDate = "2020-01-06 08:50:00.0000000";
        Date startDate = toDate(sDate, "yyyy-MM-dd HH:mm:ss");

        long timeInSecs = startDate.getTime();
        Date endDate = new Date(timeInSecs + (10 * 60 * 200));
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("1st time = " + startDate + " , last time = " + endDate + "");
        String neighborLine = "";
        String trafficLine = "";
        String CrashLine = "";
        String CrimeLine = "";
        String ParkLine = "";
        String WeatherLine = "";

        /////////Define Sockets
        ServerSocket trafficListener = new ServerSocket(9000);
        ServerSocket crashListener = new ServerSocket(9001);
        ServerSocket crimeListener = new ServerSocket(9002);
        ServerSocket parkListener = new ServerSocket(9003);
        ServerSocket weatherListener = new ServerSocket(9004);
        Socket weatherSocket = weatherListener.accept();
        System.out.println("Got new connection: " + weatherSocket.toString());
        Socket parkSocket = parkListener.accept();
        System.out.println("Got new connection: " + parkSocket.toString());
        Socket crimeSocket = crimeListener.accept();
        System.out.println("Got new connection: " + crimeSocket.toString());
        Socket crashSocket = crashListener.accept();
        System.out.println("Got new connection: " + crashSocket.toString());
        Socket trafficSocket = trafficListener.accept();
        System.out.println("Got new connection: " + trafficSocket.toString());
        long startTime = System.nanoTime();
        System.out.println("Start Time: " +startTime);

        String ss = "2020-01-01 10:11:00.0000000";
        //String ss = "2020-01-06 09:11:00.0000000";
        Date dd = toDate(ss, "yyyy-MM-dd HH:mm:ss");
        long tt = dd.getTime();
        StringBuilder tCommand = new StringBuilder();
        StringBuilder nCommand = new StringBuilder();
        ////////////////Do til data is finished
        while (timeInSecs <= tt) {
            int minA = 1;
            int maxA = 3;
            double a = Math.random()*(maxA-minA+1)+minA;
            //int randA = (int) Math.round(a);
            int randA = 4;
            System.out.println(randA);
            int minB = 1;
            int maxB = 1308;
            double b = Math.random()*(maxB-minB+1)+minB;
            int randB = (int) Math.round(b);
            int res = 12000;
            Date sd = new Date(timeInSecs);
            SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String sFormat = formatter2.format(sd);
            Date ed = new Date(timeInSecs + (10 * 60 * 900));
            Date ed2 = new Date(timeInSecs + (10 * 60 * 1000));
            timeInSecs = ed2.getTime();

            BufferedReader brCrash = null;
            BufferedReader brTraffic = null;
            BufferedReader brCrime = null;
            BufferedReader brPark = null;
            BufferedReader brWeather = null;

            brCrash = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Crash.csv"));
            brCrime = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Crime.csv"));
            brPark = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/ParkEvents.csv"));
            brWeather = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Weather.csv"));
            brTraffic = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Traffic_01_2020.csv"));

            ////////// Weather Data Ingestion
            PrintWriter weatherOut = new PrintWriter(weatherSocket.getOutputStream(), true);
            brWeather.readLine();
            while ((WeatherLine = brWeather.readLine()) != null) {
                String[] weather = WeatherLine.split(",");
                String ssDate = weather[1];
                Date cDate = toDate(ssDate, "yyyy-MM-dd HH:mm:ss");
                String eeDate = weather[2];
                Date eDate = toDate(eeDate, "yyyy-MM-dd HH:mm:ss");
                if (!sd.before(cDate) && !sd.after(eDate)) {
                    int wType = Integer.parseInt(weather[4]);
                    int visibility = Integer.parseInt(weather[8]);
                    //System.out.println("Weather: "+wType + " , " + visibility + " , " + sFormat);
                    String outputs = wType + "," + visibility + "," + sFormat;
                    weatherOut.println(outputs);
                }
            }
            brWeather.close();

            ////////// Park Data Ingestion
            PrintWriter parkOut = new PrintWriter(parkSocket.getOutputStream(), true);
            brPark.readLine();
            while ((ParkLine = brPark.readLine()) != null) {
                String[] park = ParkLine.split(",");
                String ssDate = park[2];
                Date cDate = toDate(ssDate, "yyyy-MM-dd HH:mm:ss");
                String eeDate = park[3];
                Date eDate = toDate(eeDate, "yyyy-MM-dd HH:mm:ss");
                if (!sd.before(cDate) && !sd.after(eDate)) {
                    double lat = Double.parseDouble(park[4]);
                    double lon = Double.parseDouble(park[5]);
                    Grid newGrid = outputGrid.SearchByLocationLevel(new double[]{lat, lon} , 5);
                    long gIndex = newGrid.gIndex;
                    //System.out.println("Park: "+gIndex + " , " + sFormat);
                    String outputs = gIndex + "," + sFormat;
                    parkOut.println(outputs);
                }
            }
            brPark.close();


            ////////// Crime Data Ingestion
            PrintWriter crimeOut = new PrintWriter(crimeSocket.getOutputStream(), true);
            brCrime.readLine();
            while ((CrimeLine = brCrime.readLine()) != null) {
                String[] crime = CrimeLine.split(",");
                String ccDate = crime[1];
                Date cDate = toDate(ccDate, "yyyy-MM-dd HH:mm:ss");
                if (!cDate.before(sd) && !cDate.after(ed)) {
                    double lat = Double.parseDouble(crime[3]);
                    double lon = Double.parseDouble(crime[4]);
                    Grid newGrid = outputGrid.SearchByLocationLevel(new double[]{lat, lon} , 5);
                    long gIndex = newGrid.gIndex;
                    //System.out.println(gIndex + " , " + sFormat);
                    String outputs = gIndex + "," + sFormat;
                    crimeOut.println(outputs);
                }
            }
            brCrime.close();

            ////////// Crash Data Ingestion
            PrintWriter crashOut = new PrintWriter(crashSocket.getOutputStream(), true);
            brCrash.readLine();
            while ((CrashLine = brCrash.readLine()) != null) {
                String[] crash = CrashLine.split(",");
                String ccDate = crash[1];
                Date cDate = toDate(ccDate, "yyyy-MM-dd HH:mm:ss");
                if (!cDate.before(sd) && !cDate.after(ed)) {
                    double lat = Double.parseDouble(crash[5]);
                    double lon = Double.parseDouble(crash[6]);
                    Grid newGrid = outputGrid.SearchByLocationLevel(new double[]{lat, lon} , 5);
                    long gIndex = newGrid.gIndex;
                    //System.out.println(gIndex + " , " + sFormat);
                    String outputs = gIndex + "," + sFormat;
                    crashOut.println(outputs);
                }
            }
            brCrash.close();

            /////////////////Traffic Data Ingestion

            PrintWriter trafficOut = new PrintWriter(trafficSocket.getOutputStream(), true);

            brTraffic.readLine();
            while ((trafficLine = brTraffic.readLine()) != null) {
                res = 12000;
                String[] traffic = trafficLine.split(",");
                String ttDate = traffic[0];
                Date tDate = toDate(ttDate, "yyyy-MM-dd HH:mm:ss");

                if (!tDate.before(sd) && !tDate.after(ed)) {
                    int id = Integer.parseInt(traffic[1]);
                    int bus_count = Integer.parseInt(traffic[2]);
                    int message_count = Integer.parseInt(traffic[3]);
                    int count = bus_count + message_count;
                    if (id == randB){
                        if (randA == 1){
                            int t = completeness(randB);
                            res = 10230;
                        }
                        else if (randA == 2)
                            res = validity(randB, count);
                        else if (randA == 3)
                            res = accuracy(randB, count);
                        else if (randA == 4){
                            System.out.println("pointA Function! for segID: "+randB+"");
                            int pre = preCount[randB];
                            res = pointA(randB, pre);
                        }
                    }
                    preCount[id] = count;
                    //long trafficEnrichTime = System.nanoTime();
                    Grid newGrid = outputGrid.SearchByIdLevel(id , 5);
                    long gIndex = newGrid.gIndex;
                    //long trafficEnrichEndTime = System.nanoTime();
                    //long duration = (trafficEnrichEndTime - trafficEnrichTime);
                    //tCommand.append(id+","+duration+"\n");

                    BufferedReader brNeighbor = null;
                    brNeighbor = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Neighbors.csv"));
                    brNeighbor.readLine();
                    int nId = 0;
                    long neighborEnrichStartTime = System.nanoTime();
                    while ((neighborLine = brNeighbor.readLine()) != null) {

                        String[] neighbor = neighborLine.split(",");
                        int myId = Integer.parseInt(neighbor[0]);

                        if (id == myId){
                            nId = Integer.parseInt(neighbor[1]);
                            long neighborEnrichEndTime = System.nanoTime();
                            long neiDuration = (neighborEnrichEndTime - neighborEnrichStartTime);
                            nCommand.append(id+","+neiDuration+"\n");
                        }
                    }
                    //System.out.println(gIndex + " , " + sFormat + " , " + id + " , " + nId + " , " + count);
                    String outputs = "";
                    if (res == 12000)
                        outputs = gIndex + ","+sFormat+","+id+","+nId+","+ count+",0,"+ count;
                    else if (res == 10230) {
                        outputs = gIndex + "," + sFormat + "," + id + "," + nId + ",,1," + count;
                        System.out.println(outputs);
                    }
                    else{
                        outputs = gIndex + ","+sFormat+","+id+","+nId+","+ res+",1,"+ count;
                        System.out.println(outputs);
                    }

                    trafficOut.println(outputs);

                }
            }
            *//*FileWriter csvWriterOrigin = new FileWriter("E:/Mirzaie/Data Sets/2020/mine.csv", true);
                    csvWriterOrigin.append(tCommand);
                    csvWriterOrigin.append("\n");
                    csvWriterOrigin.flush();
                    csvWriterOrigin.close();
            FileWriter csvWriterOrigin2 = new FileWriter("E:/Mirzaie/Data Sets/2020/myNei.csv", true);
            csvWriterOrigin2.append(nCommand);
            csvWriterOrigin2.append("\n");
            csvWriterOrigin2.flush();
            csvWriterOrigin2.close();*//*
            brTraffic.close();
            Thread.sleep(5000);
            System.out.println(sFormat);
        }
        long endTime = System.nanoTime();
        long dur = endTime - startTime;
        System.out.println("Finished!");
        //System.out.println("Duration: "+dur+"");
        weatherSocket.close();
        weatherListener.close();
        parkSocket.close();
        parkListener.close();
        crimeSocket.close();
        crimeListener.close();
        crashSocket.close();
        crashListener.close();
        trafficSocket.close();
        trafficListener.close();*/

        ///////////////////
        ///////////////////////////Data Server 4

        /*BufferedReader brTraffic = null;
        ResultSet seg = MySql.Select("SELECT id from Segments");
        while (seg.next()) {
            String trafficLine = "";
            int id = Integer.parseInt(seg.getString(1));
            double res = 12000.0;
            brTraffic = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/01_2020/"+id+".csv"));
            brTraffic.readLine();
            double preCount = 0;
            while ((trafficLine = brTraffic.readLine()) != null) {
                String outputs = "";
                int minA = 1;
                int maxA = 3;
                double a = Math.random()*(maxA-minA+1)+minA;
                int randA = (int) Math.round(a);
                int minB = 1;
                int maxB = 4;
                double b = Math.random()*(maxB-minB+1)+minB;
                int randB = (int) Math.round(b);
                res = 12000;
                String[] traffic = trafficLine.split(",");
                int idd = Integer.parseInt(traffic[0]);
                String ttDate = traffic[1];
                double fCount = Double.parseDouble(traffic[2]);
                double wType = Double.parseDouble(traffic[3]);
                double visibility = Double.parseDouble(traffic[4]);
                double crash = Double.parseDouble(traffic[5]);
                double crime = Double.parseDouble(traffic[6]);
                double park = Double.parseDouble(traffic[7]);
                double count = Double.parseDouble(traffic[8]);
                if (randB == 1){
                    if (randA == 1){
                        int t = completeness(randB);
                        res = 10230;
                    }
                    else if (randA == 2)
                        res = validity(randB, count);
                    else if (randA == 3)
                        res = accuracy(randB, count);
                    else if (randA == 4){
                        System.out.println("pointA Function! for segID: "+randB+"");
                        double pre = preCount;
                        res = pointA(randB, pre);
                    }
                }
                if (res == 12000)
                    outputs = idd + ","+ttDate+","+preCount+","+fCount+","+ wType+","+ visibility+","+ crash+","+ crime+","+ park+","+ count+",0,"+ count;
                else if (res == 10230) {
                    outputs = idd + ","+ttDate+","+preCount+","+fCount+","+ wType+","+ visibility+","+ crash+","+ crime+","+ park+",,1,"+ count;
                    System.out.println(outputs);
                }
                else{
                    outputs = idd + ","+ttDate+","+preCount+","+fCount+","+ wType+","+ visibility+","+ crash+","+ crime+","+ park+","+ res+",1,"+ count;
                    System.out.println(outputs);
                }
                FileWriter csvWriterOrigin = new FileWriter("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/01_2020/a_"+id+".csv", true);
                csvWriterOrigin.append(outputs);
                csvWriterOrigin.append("\n");
                csvWriterOrigin.flush();
                csvWriterOrigin.close();
                preCount = count;
            }
        }

        System.out.println("Finished!");*/

        //////Data Server 5
        /*double total = 0.0;
        double totalAnomalous = 0.0;
        double truePredict = 0.0;
        double falsePredict = 0.0;
        int cnt = 0;
        StringBuilder cleanRow = new StringBuilder();

        String filePathCleanOutput = "Experiments/Test Data Set/cleanReport.csv";
        FileWriter csvWriterCleanRow = new FileWriter(filePathCleanOutput, true);
        csvWriterCleanRow.append("id");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("date");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("preCount");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("fCount");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("wType");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("visibility");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("crash");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("crime");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("park");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("newCount");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("isAnomalous");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("realCount");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("ownCModel");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("parCModel");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("ownTModel");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("parTModel");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("min");
        csvWriterCleanRow.append(",");
        csvWriterCleanRow.append("max");
        csvWriterCleanRow.append("\n");

        br = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/01_2020/a_combined.csv"));
        br.readLine();
        while((line = br.readLine()) != null){
            total += 1;
            String[] inputs = line.split(",");
            if(inputs.length == 12){
                int id = Integer.parseInt(inputs[0]);
                String date = inputs[1];
                String[] dd = date.split("-");
                int month = Integer.parseInt(dd[1]);
                String[] ddd = dd[2].split(" ");
                int day = Integer.parseInt(ddd[0]);
                double preCount = Double.parseDouble(inputs[2]);
                double fCount = Double.parseDouble(inputs[3]);
                double wType = Double.parseDouble(inputs[4]);
                double visibility = Double.parseDouble(inputs[5]);
                double crash = Double.parseDouble(inputs[6]);
                double crime = Double.parseDouble(inputs[7]);
                double parkEvent = Double.parseDouble(inputs[8]);
                double new_count = (inputs[9].isEmpty())?-1120.0:Double.parseDouble(inputs[9]);
                double isAnomalous = Double.parseDouble(inputs[10]);
                double origin_count = Double.parseDouble(inputs[11]);
                /// gIndex extraction
                Grid grr = outputGrid.SearchById(id);
                long gIndex = grr.gIndex;
                ///Self Context model extraction
                double M_intercept = grr.gModelContexts.get(0).intercept;
                double M_fCount = grr.gModelContexts.get(0).fCount;
                double M_wType = grr.gModelContexts.get(0).wType;
                double M_visibility = grr.gModelContexts.get(0).visibility;
                double M_crash = grr.gModelContexts.get(0).crash;
                double M_crime = grr.gModelContexts.get(0).crime;
                double M_parkEvent = grr.gModelContexts.get(0).parkEvent;
                double ownCModel = M_intercept + (M_fCount * fCount) + (M_wType * wType) + (M_visibility * visibility) + (M_crash * crash) + (M_crime * crime) + (M_parkEvent * parkEvent);

                ///Parent Context model extraction
                double P_intercept = grr.gParentModelContexts.get(0).intercept;
                double P_fCount = grr.gParentModelContexts.get(0).fCount;
                double P_wType = grr.gParentModelContexts.get(0).wType;
                double P_visibility = grr.gParentModelContexts.get(0).visibility;
                double P_crash = grr.gParentModelContexts.get(0).crash;
                double P_crime = grr.gParentModelContexts.get(0).crime;
                double P_parkEvent = grr.gParentModelContexts.get(0).parkEvent;
                double parCModel = P_intercept + (P_fCount * fCount) + (P_wType * wType) + (P_visibility * visibility) + (P_crash * crash) + (P_crime * crime) + (P_parkEvent * parkEvent);

                ///Own Traffic model extraction
                double TM_intercept = grr.gModelTraffics.get(0).intercept;
                double TM_preCount = grr.gModelTraffics.get(0).preCount;
                double TM_fCount = grr.gModelTraffics.get(0).fCount;
                double ownTModel = TM_intercept + (TM_preCount * preCount) + (TM_fCount * fCount);

                ///Parent Traffic model extraction
                double TP_intercept = grr.gParentModelTraffics.get(0).intercept;
                double TP_preCount = grr.gParentModelTraffics.get(0).preCount;
                double TP_fCount = grr.gParentModelTraffics.get(0).fCount;
                double parTModel = TP_intercept + (TP_preCount * preCount) + (TP_fCount * fCount);

                ///// Min and Max of the last year
                double min = 0;
                double max = 50;
                ArrayList<Statistics> test = outputGrid.SearchById(id).statistics;
                for (Statistics t: test){
                    if (t.day == day && t.month == month){
                        min = t.min;
                        max = t.max;
                        System.out.println("Min and Max = " + t.min + "," + t.max);
                    }
                }
                if (isAnomalous != 1.0){
                    if (abs(new_count - ownCModel) < 6 || abs(new_count - parCModel) < 6 || abs(new_count - ownTModel) < 6 || abs(new_count - parTModel) < 6 || (new_count <= min && new_count >= max)){
                        truePredict += 1;
                    }
                }
                else{
                    totalAnomalous += 1;
                    if (new_count > 0) {
                        if (abs(new_count - ownCModel) < 6)
                            falsePredict += 1;
                    }
                }
                cleanRow.append(
                        id + "," +
                        date + "," +
                        preCount + "," +
                        fCount + "," +
                        wType + "," +
                        visibility + "," +
                        crash + "," +
                        crime + "," +
                        parkEvent + "," +
                        new_count + "," +
                        isAnomalous + "," +
                        origin_count + "," +
                        ownCModel + "," +
                        parCModel + "," +
                        ownTModel + "," +
                        parTModel + "," +
                        min + "," +
                        max + "\n");
                cnt += 1;
                System.out.println(cnt);
                if ((cnt % 50000) == 0){
                    csvWriterCleanRow.append(cleanRow);
                    cleanRow.setLength(0);
                }
            }
        }


        System.out.println(total+","+(total-totalAnomalous)+","+totalAnomalous+","+truePredict+","+(totalAnomalous - falsePredict));
        csvWriterCleanRow.flush();
        csvWriterCleanRow.close();*/


        ///////////////DATA SERVER 6 for paper new2

       /* String sDate = "2020-01-01 00:00:00.0000000";
        //String sDate = "2020-01-06 08:50:00.0000000";
        Date startDate = toDate(sDate, "yyyy-MM-dd HH:mm:ss");

        long timeInSecs = startDate.getTime();
        Date endDate = new Date(timeInSecs + (10 * 60 * 200));
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("1st time = " + startDate + " , last time = " + endDate + "");
        String trafficLine = "";*/

        /////////Define Sockets
 /*       ServerSocket trafficListener = new ServerSocket(9090);
        Socket trafficSocket = trafficListener.accept();
        System.out.println("Got new connection: " + trafficSocket.toString());

        String ss = "2020-02-01 00:11:00.0000000";
        //String ss = "2020-01-06 09:11:00.0000000";
        Date dd = toDate(ss, "yyyy-MM-dd HH:mm:ss");
        long tt = dd.getTime();
        StringBuilder tCommand = new StringBuilder();
        StringBuilder nCommand = new StringBuilder();
        ////////////////Do til data is finished
        while (timeInSecs <= tt) {

            Date sd = new Date(timeInSecs);
            SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String sFormat = formatter2.format(sd);
            Date ed = new Date(timeInSecs + (10 * 60 * 900));
            Date ed2 = new Date(timeInSecs + (10 * 60 * 1000));
            timeInSecs = ed2.getTime();

            BufferedReader brTraffic = null;
            brTraffic = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/01_2020/a_combined.csv"));

            /////////////////Traffic Data Ingestion

            PrintWriter trafficOut = new PrintWriter(trafficSocket.getOutputStream(), true);

            brTraffic.readLine();
            String outputs = "";
            while ((trafficLine = brTraffic.readLine()) != null) {
                String[] traffic = trafficLine.split(",");
                String ttDate = traffic[1];
                Date tDate = toDate(ttDate, "yyyy-MM-dd HH:mm:ss");

                if (!tDate.before(sd) && !tDate.after(ed)) {
                    int id = (int) Double.parseDouble(traffic[0]);
                    int new_count = (traffic[9].isEmpty())?-1120: (int) Double.parseDouble(traffic[9]);
                    int isAnomalous = (int) Double.parseDouble(traffic[10]);
                    double c = (new_count + 15) / 10;
                    int g = (int) Math.round(c);

                    outputs = ttDate+","+g+","+id+","+new_count+","+ isAnomalous;
                    //System.out.println(outputs);

                    trafficOut.println(outputs);

                }
            }
            brTraffic.close();
            Thread.sleep(300);
            System.out.println(sFormat);
        }
        System.out.println("Finished!");
        trafficSocket.close();
        trafficListener.close();*/

 ////////////////DATA SERVER 7

        /*ServerSocket listener = new ServerSocket(9090);
        br = null;
        String trafficLine = "";
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            br = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/01_2020/a_combined.csv"));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                br.readLine();
                int f = 0;
                while((trafficLine = br.readLine()) != null){
                    String[] traffic = trafficLine.split(",");
                    String ttDate = traffic[1];
                    int id = (int) Double.parseDouble(traffic[0]);
                    int new_count = (traffic[9].isEmpty())?-1120: (int) Double.parseDouble(traffic[9]);
                    int isAnomalous = (int) Double.parseDouble(traffic[10]);
                    double c = (new_count + 15) / 10;
                    int g = (int) Math.round(c);

                    String outputs = ttDate+","+g+","+id+","+new_count+","+ isAnomalous;
                    f += 1;
                    System.out.println(f);

                    out.println(outputs);
                }

            } finally{
                socket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            listener.close();
            if (br != null)
                br.close();
        }*/


        /*File output = new File("resources/Output.txt");
        FileWriter outputWriter = new FileWriter(output);
        outputWriter.write(outputGrid.toString());
        outputWriter.close();

        File outputEndpoints = new File("resources/Output-endpoints.txt");
        FileWriter outputEndpointsWriter = new FileWriter(outputEndpoints);
        outputEndpointsWriter.write(outputGrid.FindEndPoint().toString());
        outputEndpointsWriter.close();*/

        /* Mini test
        ArrayList<Sensor> sos = new ArrayList<Sensor>();
        sos.add(new Sensor(1, 2, 2));
        sos.add(new Sensor(2, 2, 3));
        sos.add(new Sensor(3, 4, 4));
        sos.add(new Sensor(4, 5, 3));
        sos.add(new Sensor(5, 4, 2));

        Grid outputGrid = new Grid(sos, 1, 1, new double[]{1,1,10,10});

        System.out.println(outputGrid);


        long heapMaxSize = Runtime.getRuntime().maxMemory();
        long heapSize = Runtime.getRuntime().totalMemory();
        System.out.println(heapMaxSize/(1024*1024));

        Date Dc1 = toDate("1/2/2019 12:00:00 AM", "MM/dd/yyyy hh:mm:ss a");
        Date Dc2 = toDate("1/2/2019 11:59:59 PM", "MM/dd/yyyy hh:mm:ss a");

        Date D1 = toDate("1/3/2019 12:00:00 PM", "MM/dd/yyyy hh:mm:ss a");
        Date D2 = toDate("1/2/2019 12:00:00 AM", "MM/dd/yyyy hh:mm:ss a");
        Date D3 = toDate("1/2/2019 12:10:00 AM", "MM/dd/yyyy hh:mm:ss a");
        System.out.println(isBetweenDateTime(D1, Dc1, Dc2));

         */
    }

    public static Date toDate(String dateString, String pattern) throws ParseException {
        SimpleDateFormat dateFormat = (!pattern.isEmpty())?new SimpleDateFormat(pattern, Locale.US):new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        return dateFormat.parse(dateString);
    }

    static boolean isBetweenDateTime(Date date, Date startDate, Date endDate)
    {
        return date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0;
        //return date.after(startDate) && date.before(endDate);
    }
    static int completeness (int segId){
        System.out.println("Completeness Function! for segId: \"+segId+\"");
        return segId;
    }
    static double validity (int segId, double count){
        System.out.println("Validity Function! for segId: "+segId+"");
        int minA = -1;
        int maxA = -10;
        double a = Math.random()*(maxA-minA+1)-minA;
        int randA = (int) Math.round(a);
        double cnt = randA;
        return cnt;
    }
    static double accuracy (int segId, double count){
        System.out.println("Accuracy Function! for segID: "+segId+"");
        double cnt = count + 6;
        return cnt;
    }
    static double pointA (int segId, double pre){
        System.out.println("pointA Function! for segID: "+segId+"");
        double cnt = pre + 4500;
        return cnt;
    }
}