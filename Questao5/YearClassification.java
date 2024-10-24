package ProjetoMapReduceHadoop.Questao5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class YearClassification implements WritableComparable<YearClassification> {

    private String year;
    private String classification;


    public YearClassification() {}

    public YearClassification(String year, String classification) {
        this.year = year;
        this.classification = classification;
    }


    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getClassification() {
        return classification;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(classification);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        classification = in.readUTF();
    }


    @Override
    public int compareTo(YearClassification other) {
        int cmp = year.compareTo(other.getYear());
        if (cmp != 0) {
            return cmp;
        }
        return classification.compareTo(other.getClassification());
    }

    @Override
    public String toString() {
        return "Year: " + year + "," + "RecClass: " + classification;
    }
}
