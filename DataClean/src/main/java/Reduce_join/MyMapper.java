package Reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        String fileName = split.getPath().getName();
        if (fileName.equals("product.txt")) {
            String[] line = value.toString().split(",");
            String productId = line[0];

            context.write(new Text(productId), new Text(value));
        } else {
            String[] line = value.toString().split(",");
            String productId = line[2];

            context.write(new Text(productId), new Text(value));
        }
    }
}
