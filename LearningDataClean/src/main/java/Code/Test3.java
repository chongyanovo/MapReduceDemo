import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
import java.util.TreeMap;

public class Test3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "Test3");
        job.setJarByClass(Test3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("H:\\考试\\in"));

        job.setMapperClass(Mapper3.class);
        job.setMapOutputKeyClass(DataBean.class);//k2
        job.setMapOutputValueClass(Text.class);//v2

        job.setReducerClass(Reducer3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("H:\\考试\\out3"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

class Mapper3 extends Mapper<LongWritable, Text, Text, DataBean> {
    TreeMap<String, DataBean> map = new TreeMap<String, DataBean>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //区域0,采集时间1 2,气温3,采集地点4
        String replaceAll = value.toString().replaceAll(" ", "\t");
        String[] split = replaceAll.split("\t");

        Integer temperature = Integer.parseInt(split[3]);
        String time = split[1] + " " + split[2];
        String location = split[4];

        DataBean dataBean = new DataBean();
        dataBean.setTemperature(temperature);
        dataBean.setLocation(location);

        map.put(time, dataBean);
        if (map.size() > 1) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String i : map.keySet()) {
            context.write(new Text(i), map.get(i));
        }
    }
}

class Reducer3 extends Reducer<Text, DataBean, Text, Text> {
    TreeMap<Text, DataBean> map = new TreeMap<Text, DataBean>();

    @Override
    protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
        for (DataBean i : values) {
            map.put(key, i);
            if (map.size() > 1) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        for (Text i : map.keySet()) {
//            context.write(i, new Text(map.get(i).getLocation()));
//        }
        DataBean dataBean = map.get(map.firstKey());
        context.write(new Text(dataBean.getLocation()), map.firstKey());
    }
}

class DataBean implements WritableComparable<DataBean> {
    private Integer temperature;
    private String location;

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public int compareTo(DataBean dataBean) {
        return -this.temperature.compareTo(dataBean.temperature);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(temperature);
        dataOutput.writeUTF(location);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.temperature = dataInput.readInt();
        this.location = dataInput.readUTF();
    }
}