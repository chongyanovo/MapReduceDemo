package Code.wendusort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, Text> {
    public int getPartition(MyKey key,Text value,int numPartitions){
        return (key.getYear()-1949)%numPartitions;
    }
}
