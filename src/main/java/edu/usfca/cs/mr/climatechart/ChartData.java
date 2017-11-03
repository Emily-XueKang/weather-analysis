package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xuekang on 11/2/17.
 */
public class ChartData implements WritableComparable {
    private FloatWritable temperature;
    private FloatWritable precipitation;

    public ChartData(FloatWritable temperature, FloatWritable precipitation){
        this.temperature = temperature;
        this.precipitation = precipitation;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        temperature.readFields(in);
        precipitation.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        temperature.write(out);
        precipitation.write(out);
    }

    public FloatWritable getTemperature() {
        return temperature;
    }

    public FloatWritable getPrecipitation() {
        return precipitation;
    }

    @Override
    public int compareTo(Object o) {
        ChartData cs = (ChartData)o;
        int cmp = temperature.compareTo(cs.temperature);

        if (cmp != 0) {
            return cmp;
        }

        return precipitation.compareTo(cs.precipitation);
    }

    @Override
    public String toString() {
        return temperature + " " + precipitation;
    }

    @Override
    public int hashCode(){
        return temperature.hashCode()*163 + precipitation.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof ChartData)
        {
            ChartData cd = (ChartData) o;
            return temperature.equals(cd.temperature) && precipitation.equals(cd.precipitation);
        }
        return false;
    }
}
