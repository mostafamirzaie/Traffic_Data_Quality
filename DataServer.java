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

public class DataServer {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9090);
        BufferedReader br = null;
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            br = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/wtlab2021/Traffic-small.csv"));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;

                while((line = br.readLine()) != null){
                    String[] inputs = line.split(",");
                    if(inputs.length == 22){
                        /*Date date = toDate(inputs[0], "MM/dd/yyyy hh:mm:ss a");
                        long segId = Long.parseLong(inputs[1]);
                        int bus = Integer.parseInt(inputs[10]);
                        int message= Integer.parseInt(inputs[11]);*/
                        String outputs = inputs[0] + "," + inputs[1] + "," + inputs[10] + "," + inputs[11];

                        //Input input = new Input(date, segId, bus, message);
                        out.println(outputs);
                        Thread.sleep(100);
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
        }
    }
    public static Date toDate(String dateString, String pattern) throws ParseException {
        SimpleDateFormat dateFormat = (!pattern.isEmpty())?new SimpleDateFormat(pattern, Locale.US):new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        return dateFormat.parse(dateString);
    }
}
