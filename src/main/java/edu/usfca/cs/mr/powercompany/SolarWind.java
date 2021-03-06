package edu.usfca.cs.mr.powercompany;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * Created by xuekang on 11/7/17.
 */
public class SolarWind implements WritableComparable<SolarWind>{
    private DoubleWritable wind_gust = null;
    private DoubleWritable cloud_coverage = null;

    public SolarWind()   {
        this.wind_gust = new DoubleWritable();
        this.cloud_coverage = new DoubleWritable();
    }

    public SolarWind(DoubleWritable wind, DoubleWritable cloud){
        this.wind_gust = wind;
        this.cloud_coverage = cloud;
    }

    public void set(DoubleWritable wind, DoubleWritable cloud){
        this.wind_gust = wind;
        this.cloud_coverage = cloud;
    }

    public DoubleWritable getCloudcover() {
        return cloud_coverage;
    }

    public DoubleWritable getWindgust() {
        return  wind_gust;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        cloud_coverage.readFields(in);
        wind_gust.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        cloud_coverage.write(out);
        wind_gust.write(out);
    }

    @Override
    public int hashCode()
    {
        return cloud_coverage.hashCode()+wind_gust.hashCode();
    }
    @Override
    public int compareTo(SolarWind other) {

        int cmp = cloud_coverage.compareTo(other.cloud_coverage);
        if(cmp!=0){
            return cmp;
        }
        return wind_gust.compareTo(other.wind_gust);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SolarWind) {
            SolarWind other = (SolarWind) o;
            return cloud_coverage.equals(other.cloud_coverage) && wind_gust.equals(other.wind_gust);
        }
        return false;
    }

    @Override
    public String toString() {
        return wind_gust.toString() + "\t" + cloud_coverage.toString() + "\t" ;
    }


}
