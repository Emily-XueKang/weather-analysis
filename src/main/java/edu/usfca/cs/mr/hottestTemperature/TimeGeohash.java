package edu.usfca.cs.mr.hottestTemperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
/**
 * Created by xuekang on 10/31/17.
 */
public class TimeGeohash implements WritableComparable {
    private Text timestamp;
    private Text geohash;

    public TimeGeohash() {
        timestamp = new Text();
        geohash = new Text();
    }

    public TimeGeohash(Text ts, Text gh){
        timestamp=ts;
        geohash=gh;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp.readFields(in);
        geohash.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        timestamp.write(out);
        geohash.write(out);
    }

    @Override
    public String toString() {
        return timestamp + " : " + geohash;
    }

    @Override
    public int hashCode(){
        return timestamp.hashCode()*163 + geohash.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof TimeGeohash)
        {
            TimeGeohash tg = (TimeGeohash) o;
            return timestamp.equals(tg.timestamp) && geohash.equals(tg.geohash);
        }
        return false;
    }

    @Override
    public int compareTo(Object o) {
        TimeGeohash other = (TimeGeohash)o;
        int cmp = timestamp.compareTo(other.timestamp);

        if (cmp != 0) {
            return cmp;
        }

        return geohash.compareTo(other.geohash);
    }
}
