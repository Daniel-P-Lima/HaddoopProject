package tde;

import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MeteorWritable implements Writable {
    private float weight;
    private int count;


    public MeteorWritable(){}

    public MeteorWritable(float weight, int count) {
        this.weight = weight;
        this.count = count;
    }

    public float getWeight() { return weight; }
    public void setWeight(float weight) { this.weight = weight; }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(weight); // Escreve o peso
        out.writeInt(count);    // Escreve o contador
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        weight = in.readFloat(); // Lê o peso
        count = in.readInt();    // Lê o contador
    }



}
