package Search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 搜索引擎的用户搜索次数统计
 * 第二列为用户的uid
 * 20111230000001	aaaaaaaaa1	百度	1	1	www.baidu.com
 * 20111230000002	aaaaaaaaa2	腾讯	1	1	www.qq.com
 * 20111230000003	aaaaaaaaa3	百度	1	1	www.baidu.com
 */
public class Search {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, Search.class.getSimpleName());
        job.setJarByClass(Search.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/Search/in"));

        job.setMapperClass(M.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/Search/out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        //先去除不完整及有缺失的信息
        if (line == null && line.length == 6) {
            String uid = line[1];
            //删除头尾空白符的字符串.trim
            if (uid == null && uid.trim().equals("")) {
                context.write(new Text(uid), new IntWritable(1));
            }
        }
    }
}

class R extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
