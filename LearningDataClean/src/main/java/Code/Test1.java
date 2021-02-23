import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Test1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "MeteorologicalDataCount");
        job.setJarByClass(Test1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("H:\\考试\\in"));

        job.setMapperClass(Mapper1.class);
        job.setMapOutputKeyClass(Text.class);//k2
        job.setMapOutputValueClass(IntWritable.class);//v2

        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("H:\\考试\\out1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text lines, Context context) throws IOException, InterruptedException {
        String replaceAll = lines.toString().replaceAll(" ", "\t");
        String[] split = replaceAll.split("\t");
        context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[3])));
    }
}

class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer sum = 0;
        Integer count = 0;
        for (IntWritable i : values) {
            sum += i.get();
            count++;
        }
        context.write(key, new IntWritable(sum / count));
    }
}

//class DataBean implements Writable {
//    private String region;
//    private String time;
//    private Integer temperature;
//    private String location;
//
//    public String getRegion() {
//        return region;
//    }
//
//    public void setRegion(String region) {
//        this.region = region;
//    }
//
//    public String getTime() {
//        return time;
//    }
//
//    public void setTime(String time) {
//        this.time = time;
//    }
//
//    public Integer getTemperature() {
//        return temperature;
//    }
//
//    public void setTemperature(Integer temperature) {
//        this.temperature = temperature;
//    }
//
//    public String getLocation() {
//        return location;
//    }
//
//    public void setLocation(String location) {
//        this.location = location;
//    }
//
//    @Override
//    public String toString() {
//        return region + " " + time + " " + temperature + " " + location;
//    }
//
//    @Override
//    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeUTF(region);
//        dataOutput.writeUTF(time);
//        dataOutput.writeInt(temperature);
//        dataOutput.writeUTF(location);
//    }
//
//    @Override
//    public void readFields(DataInput dataInput) throws IOException {
//        this.region = dataInput.readUTF();
//        this.time = dataInput.readUTF();
//        this.temperature = dataInput.readInt();
//        this.location = dataInput.readUTF();
//    }
//}