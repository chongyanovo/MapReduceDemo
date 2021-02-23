package dianxin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;

public class Demo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "DemoCount");
        job.setJarByClass(Demo.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("H:\\Test\\in"));

        job.setMapperClass(DemoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DemoBean.class);

        job.setReducerClass(DemoReucer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("H:\\Test\\out"));

        //job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class DemoBean implements Writable {
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
}

class DemoMapper extends Mapper<LongWritable, Text, Text, DemoBean> {
    @Override
    protected void map(LongWritable key, Text lines, Context context) throws IOException, InterruptedException {
        String[] split = lines.toString().split(" ");
        DemoBean demoBean = new DemoBean();
        demoBean.setUpFlow(Integer.parseInt(split[1]));
        demoBean.setDownFlow(Integer.parseInt(split[2]));
        context.write(new Text(split[0]), demoBean);
    }
}

class DemoReucer extends Reducer<Text, DemoBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<DemoBean> values, Context context) throws IOException, InterruptedException {
        Integer upFlow = 0;
        Integer downFlow = 0;

        for (DemoBean i : values) {
            upFlow += i.getUpFlow();
            downFlow += i.getDownFlow();
        }
        DemoBean demoBean = new DemoBean();
        demoBean.setUpFlow(upFlow);
        demoBean.setDownFlow(downFlow);
        context.write(key, new Text(demoBean.toString())) ;
    }
}
