package Demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

public class Q3 {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Demo1/Q3/input");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Demo1/Q3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q3");
        job.setJarByClass(Q2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(HotBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(R3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M3 extends Mapper<LongWritable, Text, HotBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        HotBean hotBean = new HotBean();
        hotBean.setTime(line[1]);
        hotBean.setHot(Integer.parseInt(line[2]));
        context.write(hotBean, new Text(line[3]));
    }
}

class R3 extends Reducer<HotBean, Text, Text, Text> {
    int K = 1;
    int count = 0;
    @Override
    protected void reduce(HotBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            if (count < K) {
                context.write(i, new Text(key.getTime()));
                count++;
            }
        }
    }
}


class HotBean implements WritableComparable<HotBean> {
    String time;
    int hot;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    @Override
    public String toString() {
        return time + "\t" + hot;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(time);
        dataOutput.writeInt(hot);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.time = dataInput.readUTF();
        this.hot = dataInput.readInt();
    }

    @Override
    public int compareTo(HotBean hotBean) {
        return hotBean.getHot() - this.hot;
    }
}
