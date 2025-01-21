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
import java.util.LinkedHashMap;
import java.util.List;

// Represents a fitted multiple linear regression model. 
public class Model {
    public LinkedHashMap<String, Double> parameters;
    public String dependent;
    public double rSquared;

    public Model(LinkedHashMap<String, Double> parameters, String dependent, double rSquared){ 
        this.parameters = parameters;
        this.dependent = dependent;
        this.rSquared = rSquared;
    }

    // Takes an Observation matching the dimensions of the input data and calculates a value for the dependent variable. 
    public double predict(Observation input){
        double yhat = parameters.get("Intercept");
        for (String feature: parameters.keySet())
            if (!feature.equals("Intercept")) { yhat += parameters.get(feature) * input.getFeature(feature); } 

        return yhat;
    }
    
    public String toString(){
        String output = "Multiple linear regression predicting " + dependent + " using " + (parameters.size() - 1) + " features.\n"
            + "R-Squared: " + rSquared + "\n\nFeature\t\t\t\tParameter\n-------------------------------------------------------------\n";

        for (String feature : parameters.keySet()){
            String formattedName = feature;
            while (formattedName.length() < 16) { formattedName = formattedName + " "; } // formatting
            output = output + formattedName + "\t\t" + parameters.get(feature) + "\n";
        }

        return output;
    }
}
