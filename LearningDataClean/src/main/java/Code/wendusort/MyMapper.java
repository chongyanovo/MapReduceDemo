package Code.wendusort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object,Text,MyKey,Text> {

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String [] strArray = line[0].split("-");
        context.getCounter("in map text = ", value.toString());
        MyKey k = new MyKey();
        k.setYear(Integer.parseInt(strArray[0]));
        k.setMonth(Integer.parseInt(strArray[1]));
        k.setAir(Double.parseDouble(line[1]));
        context.write(k,value);
    }
}
