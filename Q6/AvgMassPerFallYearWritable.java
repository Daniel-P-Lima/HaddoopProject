package tde;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgMassPerFallYearWritable implements Writable {
    private float mass;
    private String year;
    private int count;

    public AvgMassPerFallYearWritable() {
    }

    public AvgMassPerFallYearWritable(float weightMass, int count, String year) {
        this.mass = weightMass;
        this.count = count;
        this.year = year;
    }



    public float getMass() {
        return mass;
    }

    public void setMass(float mass) {
        this.mass = mass;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(mass);
        dataOutput.writeInt(count);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.mass = dataInput.readFloat();
        this.count = dataInput.readInt();
        this.year = dataInput.readUTF();
    }
}
