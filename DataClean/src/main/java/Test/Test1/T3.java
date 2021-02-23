package Test.Test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class T3 {
    public static Path IN_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test1/T3/input");
    public static Path OUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test1/T3/output");
//    public static Path OUT_PATH = new Path("hdfs://localhost:9000/MapReduce/Test1/T3/output");
//    public static Path IN_PATH = new Path("hdfs://localhost:9000/MapReduce/Test1/T3/input");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T3");
        job.setJarByClass(T3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, IN_PATH);

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(R3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fileSystem = OUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUT_PATH)) {
            fileSystem.delete(OUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M3 extends Mapper<LongWritable, Text, Bean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        Bean bean = new Bean();
        bean.setHot(Integer.parseInt(line[2]));
        bean.setLocation(line[3]);
        context.write(bean, new Text(line[1]));
    }
}

class R3 extends Reducer<Bean, Text, Text, Text> {
    /*
        int K = 1;
    int count = 0;
    @Override
    protected void reduce(Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            if (count < K) {
               context.write(new Text(key.getLocation()),i);
            }
            count++;
        }
    }
     */

    int K = 1;
    TreeMap<Integer, String> map = new TreeMap<Integer, String>();

    @Override
    protected void reduce(Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            map.put(key.getHot(), key.getLocation() + "\t" + i.toString());
            if (map.size() > K) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            context.write(new Text(map.get(i)), new Text(""));
        }
    }
}

class Bean implements WritableComparable<Bean> {
    private int hot;
    private String location;

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return location + "\t" + hot;
    }

    @Override
    public int compareTo(Bean bean) {
        return bean.hot - this.hot;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(hot);
        dataOutput.writeUTF(location);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hot = dataInput.readInt();
        this.location = dataInput.readUTF();
    }
}