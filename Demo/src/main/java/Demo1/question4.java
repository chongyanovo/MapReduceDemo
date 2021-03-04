package Demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

public class question4 {
    private static Path INPATH = new Path("hdfs://localhost:9000/demo1/in");
    private static Path OUTPATH = new Path("hdfs://localhost:9000/demo1/out4");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(question4.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPATH);

        job.setMapperClass(mapper4.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//4、找出2013年山西销售量最高的前3位的汽车品牌
class mapper4 extends Mapper<LongWritable, Text, Text, IntWritable> {
    TreeMap<String, Integer> map = new TreeMap<>();
    int K = 3;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split("\t");
        String brand = lines[7];
        if (map.containsKey(brand)) {
            int t = 1;
            t += map.get(brand);
            map.put(brand, t);
        } else {
            map.put(brand, 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<>();
        for (String i : map.keySet()) {
            list.add(i);
        }
        Collections.reverse(list);
        for (int i = 0; i < K; i++) {
            String str = list.get(i);
            context.write(new Text(str), new IntWritable(map.get(str)));
        }
    }
}

