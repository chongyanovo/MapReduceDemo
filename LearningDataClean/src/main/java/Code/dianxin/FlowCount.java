package dianxin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 电信数据统计：统计同一个用户的上行总流量和，下行总流量和以及上下总流量和
 * dianxin.txt的数据格式如下：
 * 手机号       上行流量  下行流量
 * 13560439658    20       50
 */
public class FlowCount {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "FlowCount");
        job.setJarByClass(FlowCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("H:\\MapReducer\\电信例题1\\in"));

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //job.setPartitionerClass(Mypartition.class);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(3);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("H:\\MapReducer\\电信例题1\\out"));
        job.waitForCompletion(true);
    }
}

class FlowBean implements WritableComparable<FlowBean> {
    private Integer upFlow;
    private Integer downFlow;

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return upFlow + " " + downFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
    }

    @Override
    public int compareTo(FlowBean flowBean) {
        return -flowBean.getDownFlow().compareTo(this.upFlow);
        //return this.upFlow.compareTo(flowBean.getUpFlow());
    }
}


class FlowCountReducer extends Reducer<Text, FlowBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Integer upFlow = 0;
        Integer downFlow = 0;
        for (FlowBean i : values) {
            upFlow += i.getUpFlow();
            downFlow += i.getDownFlow();
        }
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        context.write(key, new Text(flowBean.toString()));
    }

}

class Mypartition extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text text, Text text2, int i) {
        String[] split = text.toString().split(" ");
        if (split[0].startsWith("135")) {
            return 0;
        } else if (split[0].startsWith("138")) {
            return 1;
        } else {
            return 2;
        }
    }
}

class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text lines, Context context) throws IOException, InterruptedException {
        String[] split = lines.toString().split(" ");
        String phoneNum = split[0];
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        context.write(new Text(phoneNum), flowBean);
    }
}


