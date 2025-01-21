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


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DataServer2 {
    public static void main(String[] args) throws IOException, ParseException, InterruptedException {

        String sDate = "2020-01-01 00:00:00.0000000";
        Date startDate = toDate(sDate, "yyyy-MM-dd HH:mm:ss");

        long timeInSecs = startDate.getTime();
        Date endDate = new Date(timeInSecs + (10 * 60 * 200));
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sDateFormat = formatter.format(startDate);
        Date startDate2 = new Date(timeInSecs + (10 * 60 * 1000));
        Date endDate2 = new Date(timeInSecs + (10 * 60 * 1200));
        System.out.println("1st time = "+startDate+" , last time = "+endDate+"");
        String trafficLine = "";
        String CrashLine = "";

        /////////Define Sockets
        ServerSocket trafficListener = new ServerSocket(9000);
        ServerSocket crashListener = new ServerSocket(9001);
        Socket crashSocket = crashListener.accept();
        System.out.println("Got new connection: " + crashSocket.toString());
        Socket trafficSocket = trafficListener.accept();
        System.out.println("Got new connection: " + trafficSocket.toString());

        String ss = "2020-01-01 00:21:00.0000000";
        Date dd = toDate(ss, "yyyy-MM-dd HH:mm:ss");
        long tt = dd.getTime();
        ////////////////Do til data is finished
        while (timeInSecs <= tt){
            Date sd = new Date(timeInSecs);
            SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String sFormat = formatter2.format(sd);
            Date ed = new Date(timeInSecs + (10 * 60 * 1000));
            timeInSecs = ed.getTime();
            BufferedReader brCrash = null;
            BufferedReader brTraffic = null;
            brCrash = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Crash.csv"));
            brTraffic = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Traffic_01_2020.csv"));

            ////////// Crash Data Ingestion
            PrintWriter crashOut = new PrintWriter(crashSocket.getOutputStream(), true);
            brCrash.readLine();
            while((CrashLine = brCrash.readLine()) != null) {
                String[] crash = CrashLine.split(",");
                String ccDate = crash[1];
                Date cDate = toDate(ccDate, "yyyy-MM-dd HH:mm:ss");
                if (!cDate.before (sd) && !cDate.after (ed)){
                    long gIndex = Long.parseLong(crash[7]);
                    System.out.println(gIndex + " , " + sFormat);
                    String outputs = gIndex+","+sFormat;
                    crashOut.println(outputs);
                }
            }
            brCrash.close();
            /////////////////Traffic Data Ingestion
            PrintWriter trafficOut = new PrintWriter(trafficSocket.getOutputStream(), true);

            brTraffic.readLine();
            while((trafficLine = brTraffic.readLine()) != null) {
                String[] traffic = trafficLine.split(",");
                String ttDate = traffic[0];
                Date tDate = toDate(ttDate, "yyyy-MM-dd HH:mm:ss");

                if (!tDate.before (sd) && !tDate.after (ed)){
                    int id = Integer.parseInt(traffic[1]);
                    int bus_count = Integer.parseInt(traffic[2]);
                    int message_count = Integer.parseInt(traffic[3]);
                    int count = bus_count + message_count;
                    long gIndex = Long.parseLong(traffic[3]);
                    System.out.println(gIndex + " , " + sFormat + " , " + id + " , " + count);
                    String outputs = gIndex+","+sFormat+","+id+","+count;
                    trafficOut.println(outputs);

                }
            }
            brTraffic.close();
            Thread.sleep(10000);
        }
        System.out.println("Finished!");
        crashSocket.close();
        crashListener.close();
        trafficSocket.close();
        trafficListener.close();

        /////////Read Streaming Crash Data
    /*    BufferedReader brCrash = null;
        try {
            //Socket crashSocket = crashListener.accept();
            //System.out.println("Got new connection: " + crashSocket.toString());
            brCrash = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Crash_sample.csv"));

            try {
                PrintWriter crashOut = new PrintWriter(crashSocket.getOutputStream(), true);
                brCrash.readLine();
                while((CrashLine = brCrash.readLine()) != null) {
                    String[] crash = CrashLine.split(",");
                    String ccDate = crash[0];
                    Date cDate = toDate(ccDate, "yyyy-MM-dd HH:mm:ss");
                    if (!cDate.before (startDate) && !cDate.after (endDate)){
                        long gIndex = Long.parseLong(crash[6]);
                        System.out.println(gIndex + " , " + sDateFormat);
                        String outputs = gIndex+","+sDateFormat;
                        crashOut.println(outputs);
                    }
                }
            }
            finally{
                crashSocket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            crashListener.close();
            if (brCrash != null)
                brCrash.close();
        }

        /////////Read Streaming Traffic Data

        BufferedReader brTraffic = null;
        try {
            *//*Socket trafficSocket = trafficListener.accept();
            System.out.println("Got new connection: " + trafficSocket.toString());*//*

            brTraffic = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/Context/New folder/Traffic_small.csv"));

            try {
                PrintWriter trafficOut = new PrintWriter(trafficSocket.getOutputStream(), true);

                brTraffic.readLine();
                while((trafficLine = brTraffic.readLine()) != null) {
                    String[] traffic = trafficLine.split(",");
                    String ttDate = traffic[0];
                    Date tDate = toDate(ttDate, "yyyy-MM-dd HH:mm:ss");

                    if (!tDate.before (startDate) && !tDate.after (endDate)){
                        int id = Integer.parseInt(traffic[1]);
                        int count = Integer.parseInt(traffic[2]);
                        long gIndex = Long.parseLong(traffic[3]);
                        System.out.println(gIndex + " , " + sDateFormat + " , " + id + " , " + count);
                        String outputs = gIndex+","+sDateFormat+","+id+","+count;
                        trafficOut.println(outputs);

                    }
                }
            }
            finally{
                trafficSocket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            trafficListener.close();
            if (brTraffic != null)
                brTraffic.close();
        }*/
    }
    public static Date toDate(String dateString, String pattern) throws ParseException {
        SimpleDateFormat dateFormat = (!pattern.isEmpty())?new SimpleDateFormat(pattern, Locale.US):new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        return dateFormat.parse(dateString);
    }

}
