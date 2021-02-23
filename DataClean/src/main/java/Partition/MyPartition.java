package Partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] line = text.toString().split("\t");
        String str = line[5];
        if (Integer.parseInt(str) > 15) {
            return 1;
        } else {
            return 0;
        }
    }
}
