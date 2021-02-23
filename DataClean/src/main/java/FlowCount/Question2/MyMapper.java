package FlowCount.Question2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String phoneNum = line[0];
        Integer upFlow = Integer.parseInt(line[1]);
        Integer downFlow = Integer.parseInt(line[2]);
        Integer upCountFlow = Integer.parseInt(line[3]);
        Integer downCountFlow = Integer.parseInt(line[4]);

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        context.write(flowBean, new Text(phoneNum));
    }
}
