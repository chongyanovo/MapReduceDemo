package WenduSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {
    private int year;
    private int wendu;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getWendu() {
        return wendu;
    }

    public void setWendu(int wendu) {
        this.wendu = wendu;
    }

    @Override
    public String toString() {
        return "" + year + '\t' + wendu;
    }

    @Override
    public int compareTo(Bean bean) {
        int result = this.year - bean.getYear();
        if (result == 0) {
            result = bean.getWendu() - this.wendu;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(wendu);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.wendu = dataInput.readInt();
    }
}
