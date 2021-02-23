package 考试;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *    区域	采集时间	           气温	采集地点
 *  * 仙林	2019.12.16 14:00	12	学则路
 */
public class QiXiangBean implements WritableComparable {
    private String time;//采集时间
    private String location;//采集地点

    public QiXiangBean(){

    }

    public void write(DataOutput out)throws IOException {

        out.writeUTF(time);
        out.writeUTF(location);
    }

    public void readFields(DataInput in) throws IOException{
        time = in.readUTF();
        location = in.readUTF();
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int compareTo(Object o){
        return this == o?0:-1;
    }
}
