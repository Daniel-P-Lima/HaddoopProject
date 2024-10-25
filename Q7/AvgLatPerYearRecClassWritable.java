package tde;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgLatPerYearRecClassWritable implements Writable {
    private float latitude;
    private int count;

    public AvgLatPerYearRecClassWritable() {
    }

    public AvgLatPerYearRecClassWritable(float latitude, int count) {
        this.latitude = latitude;
        this.count = count;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(latitude);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.latitude = dataInput.readFloat();
        this.count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "Avg Latitude: " + latitude + ", Count: " + count;
    }

}
