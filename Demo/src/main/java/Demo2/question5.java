package Demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

public class question5 {
    private static Path INPATH = new Path("hdfs://localhost:9000/demo2/out1_2");
    private static Path OUTPATH = new Path("hdfs://localhost:9000/demo2/out5");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(question5.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPATH);

        job.setMapperClass(mapper5.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(reducer5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//5、找出南京市1月销售量最高的是哪个商品
class mapper5 extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String good = lines[2];
        int sales = Integer.parseInt(lines[3]);
        context.write(new Text(good), new IntWritable(sales));
    }
}

class reducer5 extends Reducer<Text, IntWritable, Text, NullWritable> {
    int K;
    TreeMap<Integer, String> map = new TreeMap<>();
    ArrayList<String> list = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        K = 1;
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        map.put(sum, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            list.add(map.get(i));
        }
        Collections.reverse(list);
        for (int i = 0; i < K; i++) {
            context.write(new Text(list.get(i)), NullWritable.get());
        }
    }
}


