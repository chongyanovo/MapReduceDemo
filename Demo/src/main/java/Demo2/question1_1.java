package Demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class question1_1 {
    private static Path INPATH = new Path("hdfs://localhost:9000/demo2/in");
    private static Path OUTPATH = new Path("hdfs://localhost:9000/demo2/out1_1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(question1_1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPATH);

        job.setMapperClass(mapper1_1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//1）将“缺货”改为0，将小数点的数据按四舍五入的方式改为整数，比如“15.48”改为15。
class mapper1_1 extends Mapper<LongWritable, Text, Text, NullWritable> {
    private String splitter = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        splitter = ",";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(splitter);
        String flag = "缺货";
        HashMap<Integer, String> map = new HashMap<>();
        map.put(3, lines[3]);
        map.put(5, lines[5]);
        for (Integer i : map.keySet()) {
            if (lines[i].equals(flag)) {
                map.put(i, "0");
            } else {
                float j = Float.parseFloat(map.get(i));
                map.put(i, "" + Math.round(j));
            }
        }
        for (Integer j : map.keySet()) {
            lines[j] = map.get(j);
        }
        String str = "";
        for (String k : lines) {
            str = str + "\t" + k;
        }
        context.write(new Text(str), NullWritable.get());
    }
}
