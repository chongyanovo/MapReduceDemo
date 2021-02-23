package FlowCount.Question3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String phoneNum = line[1];
        Integer upFlow = Integer.parseInt(line[6]);
        Integer downFlow = Integer.parseInt(line[7]);
        Integer upCountFlow = Integer.parseInt(line[8]);
        Integer downCountFlow = Integer.parseInt(line[9]);

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        context.write(new Text(phoneNum), flowBean);
    }
}
