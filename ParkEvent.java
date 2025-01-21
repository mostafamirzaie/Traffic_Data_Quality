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

import java.util.Date;

public class ParkEvent {
    public int parkNum;
    public Date sDate;
    public Date eDate;
    public double latitude;
    public double longitude;

    public ParkEvent(int id, Date sDate, Date eDate, double latitude, double longitude)
    {
        this.parkNum = id;
        this.sDate = sDate;
        this.eDate = eDate;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String toString()
    {
        return    "  < " + this.parkNum
                + " |" + this.sDate
                + " |" + this.eDate
                + " |" + this.latitude
                + " | " + this.longitude + "> ";
    }
}
