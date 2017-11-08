package edu.usfca.cs.mr.powercompany;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * Created by xuekang on 11/7/17.
 */
public class SolarWind implements WritableComparable<SolarWind>{
    private Text wind_gust = null;
    private Text cloud_coverage = null;
    private Text geohash = null;

    public SolarWind()   {
        this.wind_gust = new Text();
        this.cloud_coverage = new Text();
        this.geohash = new Text();
    }

    public SolarWind(Text wind, Text cloud, Text geohash){
        this.wind_gust = wind;
        this.cloud_coverage = cloud;
        this.geohash = geohash;
    }

    public void set(Text wind, Text cloud, Text geo){
        this.wind_gust = wind;
        this.cloud_coverage = cloud;
        this.geohash = geo;
    }

    public Text getCloudcover() {
        return cloud_coverage;
    }

    public Text getWindgust() {
        return  wind_gust;
    }

    public Text getGeohash() {
        return geohash;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cloud_coverage.readFields(in);
        wind_gust.readFields(in);
        geohash.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        cloud_coverage.write(out);
        wind_gust.write(out);
        geohash.write(out);
    }

    @Override
    public int hashCode()
    {
        return cloud_coverage.hashCode()+wind_gust.hashCode();
    }
    @Override
    public int compareTo(SolarWind other) {
        if (cloud_coverage.compareTo(other.cloud_coverage)==0) {
            if (wind_gust.compareTo(other.wind_gust) == 0)   {
                return (geohash.compareTo(other.geohash ));
            } else  {
                return (wind_gust.compareTo(other.wind_gust));
            }
        } else {
            return (cloud_coverage.compareTo(other.cloud_coverage));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SolarWind) {
            SolarWind other = (SolarWind) o;
            return cloud_coverage.equals(other.cloud_coverage) && cloud_coverage.equals(other.wind_gust)
                    && geohash.equals( other.geohash );
        }
        return false;
    }

    @Override
    public String toString() {
        return wind_gust.toString() + "\t" + cloud_coverage.toString() + "\t" ;
    }


}
