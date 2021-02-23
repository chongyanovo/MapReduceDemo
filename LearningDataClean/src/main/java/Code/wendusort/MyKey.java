package Code.wendusort;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable {
    private int year;
    private int month;
    private double air;

    public MyKey(){

    }

    public int getYear(){
         return this.year;
    }

    public void setYear(int year) {
        this.year = year;
    }
    public double getAir() {
        return air;
    }

    public void setAir(double air) {
        this.air = air;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getMonth(){
        return this.month;
    }

    public void write(DataOutput out)throws IOException{
        out.writeInt(year);
        out.writeInt(month);
        out.writeDouble(air);
    }

    public void readFields(DataInput in) throws IOException{
        year = in.readInt();
        month = in.readInt();
        air = in.readDouble();
    }

    public int compareTo(Object o){
        return this == o?0:-1;
    }

}
