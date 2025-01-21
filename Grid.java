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

import java.util.ArrayList;
import java.util.Stack;

class Grid
{
    public int level;
    public long gIndex;
    public double[] border;
    public ArrayList<Sensor> sos;
    public ArrayList<Grid> subGrids;
    //Contextual Attributes
    public ArrayList<ParkEvent> parksEvents = new ArrayList<>();
    public ArrayList<Crash> crashes = new ArrayList<>();
    public ArrayList<Crime> crimes = new ArrayList<>();
    public ArrayList<Traffic> traffics = new ArrayList<>();
    public ArrayList<Context> contexts = new ArrayList<>();
    public ArrayList<GModelContext> gModelContexts = new ArrayList<GModelContext>();
    public ArrayList<GParentModelContext> gParentModelContexts = new ArrayList<GParentModelContext>();
    public ArrayList<GModelTraffic> gModelTraffics = new ArrayList<GModelTraffic>();
    public ArrayList<GParentModelTraffic> gParentModelTraffics = new ArrayList<>();
    public ArrayList<Statistics> statistics = new ArrayList<>();

    public Grid(ArrayList<Sensor> sos, int level, long gIndex, double[] border)
    {
        this.sos = (sos != null)? sos : new ArrayList<>();
        this.level = (level > 0)? level : 1;
        this.gIndex = (gIndex > 0)? gIndex : 1;
        this.border = (border != null)? border : selfBorder();
        selfEvaluate();
        this.subGrids = (this.sos.size() > 1)? selfFracture() : new ArrayList<>();
        //selfModelize();
    }

    public double[] selfBorder()
    {
        double maxLat = 0, minLat = 0, maxLong = 0, minLong = 0;

        if( this.sos.size() > 0){
            maxLat = sos.get(0).latitude;
            minLat = sos.get(0).latitude;
            maxLong = sos.get(0).longitude;
            minLong = sos.get(0).longitude;

            for(Sensor sensor : this.sos){
                if(maxLat <= sensor.latitude){ maxLat = sensor.latitude; }
                else if(minLat >= sensor.latitude) { minLat = sensor.latitude; }
                if(maxLong <= sensor.longitude){ maxLong = sensor.longitude; }
                else if(minLong >= sensor.longitude) { minLong = sensor.longitude; }
            }
        }

        return new double[]{minLat, minLong, maxLat, maxLong};
    }

    public ArrayList<Grid> selfFracture()
    {
        if(this.sos.size() < 1)
            return new ArrayList<>(); //double checking sensor existence and preventing infinite loop

        double meanLat = (this.border[0] + this.border[2])/2;
        double meanLong = (this.border[1] + this.border[3])/2;

        ArrayList<double[]> subBorders = new ArrayList<>();
        subBorders.add(new double[]{this.border[0], this.border[1], meanLat, meanLong});
        subBorders.add(new double[]{meanLat, meanLong, this.border[2], this.border[3]});
        subBorders.add(new double[]{meanLat, this.border[1], this.border[2], meanLong});
        subBorders.add(new double[]{this.border[0], meanLong, meanLat, this.border[3]});

        ArrayList<Grid> sGrids = new ArrayList<>();

        String s1 = Long.toString(this.gIndex);
        String s2 = Integer.toString(1);

        // Concatenate both strings
        String s = s1 + s2;

        // Convert the concatenated string
        // to integer
        long cIndex = Long.parseLong(s);
        int sLevel = this.level + 1;

        sGrids.add(new Grid(this.sos, sLevel, cIndex++, subBorders.get(0)));
        sGrids.add(new Grid(this.sos, sLevel, cIndex++, subBorders.get(1)));
        sGrids.add(new Grid(this.sos, sLevel, cIndex++, subBorders.get(2)));
        sGrids.add(new Grid(this.sos, sLevel, cIndex++, subBorders.get(3)));

        return sGrids;
    }

    public void selfEvaluate()
    {
        if(this.sos.size() < 1) return;
        ArrayList<Sensor> newSos = new ArrayList<>();
        for(Sensor sensor : this.sos)
            if(IsInBorder(new double[]{sensor.latitude, sensor.longitude})) newSos.add(sensor);

        this.sos = newSos;
    }

   /* public void selfModelize()
    {
        // Check DB and extract relative model
        // Check if has child reCall itself
        if(this.sos.size() < 1) return;
        ArrayList<Sensor> newSos = new ArrayList<>();
        for(Sensor sensor : this.sos)
            if(IsInBorder(new double[]{sensor.latitude, sensor.longitude})) newSos.add(sensor);

        this.sos = newSos;
    }*/

    public Grid SearchByIndex(long index){
        if(this.gIndex == index) return this;

        // Removes current index from the first position of searched index
        String subIndex = Long.toString(index).substring(Long.toString(this.gIndex).length(), Long.toString(index).length());

        // Decides which subGrid to open and search;
        int nextChild = Character.getNumericValue(subIndex.charAt(0));
        return this.subGrids.get(nextChild - 1).SearchByIndex(index);
    }

    public Grid SearchById(long id){
        if(this.sos.get(0).id == id && this.sos.size() == 1) return this;
        for(Grid subGrid : this.subGrids)
            for(Sensor sensor : subGrid.sos)
                if(sensor.id == id) return subGrid.SearchById(id);
        return null;
    }

    public Grid SearchByIdLevel(long id, int level){
        for (Sensor sensor: this.sos)
            if(sensor.id == id && this.level == level) return this;
        for(Grid subGrid : this.subGrids)
            for(Sensor sensor : subGrid.sos)
                if(sensor.id == id) return subGrid.SearchByIdLevel(id, level);
        return null;
    }

    public Grid SearchByLocation(double[] location){
        if(this.IsInBorder(location) && this.sos.size() <= 1) return this;
        for(Grid subGrid : this.subGrids)
            if(subGrid.IsInBorder(location)) return subGrid.SearchByLocation(location);
        //System.out.println("Out of boundaries error: "+location[0] + "|" + location[1]);
        return null;
    }
    public Grid SearchByLocationLevel(double[] location, int level){
        if(this.IsInBorder(location) && this.level == level) return this;
        for(Grid subGrid : this.subGrids)
            if(subGrid.IsInBorder(location)) return subGrid.SearchByLocationLevel(location, level);
        //System.out.println("Out of boundaries error: "+location[0] + "|" + location[1]);
        return null;
    }

    public Grid SearchByBorder(double[] border){
        // Check if All 4 points are in this grid
        if( IsInBorder(new double[]{border[0], border[1]})
                && IsInBorder(new double[]{border[2], border[3]}) )
        {
            // Check if All 4 points are in any child of this grid
            Grid isInOneChild = null;
            for(Grid subGrid : this.subGrids)
                if( subGrid.IsInBorder(new double[]{border[0], border[1]})
                        && subGrid.IsInBorder(new double[]{border[2], border[3]}) ) isInOneChild = subGrid;

            //if there is a child which can hold all 4 points then do the same for that child
            //else if no child can hold all 4 points then this grid is the minimum closest answer
            if(isInOneChild != null) return isInOneChild.SearchByBorder(border);
            else return this;
        }
        else return null;
    }

    public ArrayList<Grid> SearchByLevel(int level){
        ArrayList<Grid> levelPoints = new ArrayList<>();
        Stack<Grid> gridStack = new Stack<>();
        gridStack.push(this);

        while (!gridStack.empty()){
            Grid iGrid = gridStack.pop();
            if(iGrid.level == level) levelPoints.add(iGrid);
            else if(iGrid.level < level)
                for(Grid subGrid : iGrid.subGrids)
                    gridStack.push(subGrid);
        }

        return levelPoints;
    }

    public boolean IsInBorder(double[] location)
    {
        if(    location[0] >= this.border[0]
                && location[0] < this.border[2]
                && location[1] >= this.border[1]
                && location[1] < this.border[3] ) return true;
        else return false;
    }

    public ArrayList<Grid> FindEndPoint(){
        ArrayList<Grid> endPoints = new ArrayList<>();
        Stack<Grid> gridStack = new Stack<>();
        gridStack.push(this);

        while (!gridStack.empty()){
            Grid iGrid = gridStack.pop();
            if(iGrid.subGrids.size() == 0) endPoints.add(iGrid);
            else
                for(Grid subGrid : iGrid.subGrids)
                    gridStack.push(subGrid);
        }

        return endPoints;
    }

    public String toString()
    {
        String indent = "";
        for(int i = 0; i < level - 1; i++){ indent = indent + "\t"; }

        String lastIndent = (indent.length() > 1)? indent.substring(0, indent.length() - 1) : "";

        return  "\n" + indent + "Grid Level: " + this.level + "\n"
                + indent + "Grid Index: " + this.gIndex + "\n"
                + indent + "Borders: <" + this.border[0]
                + " | " + this.border[1]
                + "> | <" + this.border[2]
                + " | " + this.border[3] + ">\n"
                + indent + "Sensor(s): " + this.sos + "\n"
                + indent + "ParkEvent(s): " + this.parksEvents + "\n"
                + indent + "Crash(es): " + this.crashes + "\n"
                + indent + "Crime(s): " + this.crimes + "\n"
                + indent + "Traffic: " + this.traffics + "\n"
                + indent + "Context(s): " + this.contexts + "\n"
                + indent + "ContextModel(s): " + this.gModelContexts + "\n"
                + indent + "ParentContextModel(s): " + this.gParentModelContexts + "\n"
                + indent + "TrafficModel(s): " + this.gModelTraffics + "\n"
                + indent + "ParentTrafficModel(s): " + this.gParentModelTraffics + "\n"
                + indent + "Statistic(s): " + this.statistics + "\n"
                + indent + "subGrids: \n" + indent + this.subGrids + "\n" + lastIndent;
    }
}