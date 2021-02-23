package Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class T1_1 {
    private static Path INPUT_PATH1 = new Path("hdfs://localhost:9000/Test11/input/sales.txt");
    private static Path INPUT_PATH2 = new Path("hdfs://localhost:9000/Test11/input/order.txt");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test11/output1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(T1_1.class);

        MultipleInputs.addInputPath(job, INPUT_PATH1, TextInputFormat.class, SalesMapper.class);
        MultipleInputs.addInputPath(job, INPUT_PATH2, TextInputFormat.class, OrderMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueBean.class);

        job.setReducerClass(R1_1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class SalesMapper extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        ValueBean valueBean = new ValueBean();
        String str = line[0] + "\t" + line[1] + "\t" + line[2] + "\t" + line[3];
        valueBean.setValue(str);
        valueBean.setFlag("first");
        context.write(new Text(line[4]), valueBean);
    }
}

class OrderMapper extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        ValueBean valueBean = new ValueBean();
        String str = line[1] + "\t" + line[2] + "\t" + line[3] + "\t" + line[4] + "\t" + line[5];
        valueBean.setValue(str);
        valueBean.setFlag("second");
        context.write(new Text(line[0]), valueBean);
    }
}

class R1_1 extends Reducer<Text, ValueBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        String str = "";
        String first = "";
        String second = "";
        for (ValueBean i : values) {
            if (i.getFlag().equals("first")) {
                first = i.getValue();
            } else {
                second = i.getValue();
            }
        }
        str = first + "\t" + key.toString() + "\t" + second;
        context.write(new Text(str), NullWritable.get());
    }
}