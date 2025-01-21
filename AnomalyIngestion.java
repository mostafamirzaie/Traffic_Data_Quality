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

public class AnomalyIngestion {
    public static void main(String[] args) throws ParseException, IOException, InterruptedException {

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
    static int validity (int segId, int count){
        System.out.println("Validity Function! for segId: "+segId+"");
        int cnt = count - 1000;
        return cnt;
    }
    static int accuracy (int segId, int count){
        System.out.println("Accuracy Function! for segID: "+segId+"");
        int cnt = count + 6;
        return cnt;
    }
    static int pointA (int segId, int pre){
        System.out.println("pointA Function! for segID: "+segId+"");
        int cnt = pre + 4500;
        return cnt;
    }
}
