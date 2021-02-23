package 电信例题;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SortCount");conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        job.setJarByClass(SortCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("H:\\MapReducer\\SortCount\\in"));

        job.setMapperClass(SortFlowMapper.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SortFlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SortBean.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("H:\\MapReducer\\SortCount\\out"));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

class SortBean implements WritableComparable<SortBean> {
    private Integer upFlow;
    private Integer downFlow;
    private Integer upCountFlow;
    private Integer downCountFlow;

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

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + upCountFlow + "\t" + downCountFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCountFlow);
        dataOutput.writeInt(downCountFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.upCountFlow = dataInput.readInt();
        this.downCountFlow = dataInput.readInt();
    }

    @Override
    public int compareTo(SortBean sortBean) {
        return -this.upFlow.compareTo(sortBean.getUpFlow());
    }
}

class SortFlowMapper extends Mapper<LongWritable, Text, SortBean, Text> {
    @Override
    protected void map(LongWritable key, Text lines, Context context) throws IOException, InterruptedException {
        String[] line = lines.toString().split("\t");
        SortBean sortBean = new SortBean();
        sortBean.setUpFlow(Integer.parseInt(line[1]));
        sortBean.setDownFlow(Integer.parseInt(line[2]));
        sortBean.setUpCountFlow(Integer.parseInt(line[3]));
        sortBean.setDownCountFlow(Integer.parseInt(line[4]));
        context.write(sortBean, new Text(line[0]));
    }
}

class SortFlowReducer extends Reducer<SortBean, Text, Text, SortBean> {
    @Override
    protected void reduce(SortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            context.write(i, key);
        }
    }
}