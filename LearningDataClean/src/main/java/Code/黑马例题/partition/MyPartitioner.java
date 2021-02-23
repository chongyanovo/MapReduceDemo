package 黑马例题.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] split = text.toString().split("\t");
        //String numstr = split[5];
        if (Integer.parseInt(split[5]) > 15) {
            return 1;
        } else {
            return 0;
        }

    }
}
