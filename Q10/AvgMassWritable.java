package tde;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgMassWritable implements Writable {
    private float mass;
    private int count;

    public AvgMassWritable() {
    }

    public AvgMassWritable(float mass, int count) {
        this.mass = mass;
        this.count = count;
    }

    public float getMass() {
        return mass;
    }

    public void setMass(float mass) {
        this.mass = mass;
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
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.mass = dataInput.readFloat();
        this.count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "Massa total: " + mass + "\t" + count;
    }
}
