package Test;

import org.apache.hadoop.conf.Configuration;
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

public class T1_2 {
    private static Path INPUT_PATH1 = new Path("hdfs://localhost:9000/Test11/output1/part-r-00000");
    private static Path INPUT_PATH2 = new Path("hdfs://localhost:9000/Test11/input/product.txt");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test11/output2");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(T1_2.class);

        MultipleInputs.addInputPath(job, INPUT_PATH1, TextInputFormat.class, MapMapper.class);
        MultipleInputs.addInputPath(job, INPUT_PATH2, TextInputFormat.class, ProductMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueBean.class);

        job.setReducerClass(R1_2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class MapMapper extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        String str = line[0] + "\t" + line[1] + "\t" + line[2];
        ValueBean valueBean = new ValueBean();
        String tmp = "";
        for (int i = 3; i < line.length; i++) {
            tmp += line[i] + "\t";
        }
        valueBean.setValue(tmp);
        valueBean.setFlag("first");
        context.write(new Text(str), valueBean);
    }
}

class ProductMapper extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String str = line[0] + "\t" + line[1] + "\t" + line[2];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(line[3]);
        valueBean.setFlag("second");
        context.write(new Text(str), valueBean);
    }
}

class R1_2 extends Reducer<Text, ValueBean, Text, NullWritable> {
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
        str = key.toString() + "\t" + first + second;
        context.write(new Text(str), NullWritable.get());
    }
}