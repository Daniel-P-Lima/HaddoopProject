package tde;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearAvgWritable implements Writable {
    private String year;
    private float avgMass;

    public YearAvgWritable() {}

    public YearAvgWritable(String year, float avgMass) {
        this.year = year;
        this.avgMass = avgMass;
    }

    public String getYear() {
        return year;
    }

    public float getAvgMass() {
        return avgMass;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeFloat(avgMass);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        avgMass = in.readFloat();
    }
}

