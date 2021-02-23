//package sort;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import javax.security.auth.callback.TextOutputCallback;
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//
//public class SortText {
//    public static void main(String[] args) throws IOException {
//        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
//        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
//        Job job = Job.getInstance(conf, "Sort");
//        job.setJarByClass(SortText.class);
//        //job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.setInputPaths(job, new Path("H:\\sort1\\in"));
//        job.setMapperClass(SortMapper.class);
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(NullWritable.class);
//        job.setReducerClass(SortReducer.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(NullWritable.class);
//        //job.setOutputFormatClass(TextOutputFormat.class);
//        FileOutputFormat.setOutputPath(job, new Path("H:\\sort1\\out"));
//    }
//
//}
//
//class SortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
//    private static IntWritable data = new IntWritable();
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        int i = Integer.parseInt(value.toString());
//        data.set(i);
//        context.write(data, NullWritable.get());
//    }
//}
//
//class SortReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
//    @Override
//    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        context.write(key, NullWritable.get());
//    }
//}
//
//class data implements WritableComparable<data> {
//    private int num;
//
//    public int getNum() {
//        return num;
//    }
//
//    public void setNum(int num) {
//        this.num = num;
//    }
//
//
//    @Override
//    public int compareTo(data data) {
//        return 0;
//    }
//
//    @Override
//    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeInt(num);
//    }
//
//    @Override
//    public void readFields(DataInput dataInput) throws IOException {
//        this.num = dataInput.readInt();
//    }
//}
