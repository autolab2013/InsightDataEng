import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {

    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {
        //Implement
        System.out.println("Trust mapper get node id: " + value.nodeid+", page rank is "+ value.getPageRank());
        double rank = value.getPageRank();
        int[] outlink = value.outgoing;

        for(int i=0;i<outlink.length;i++){
            double loss = rank/value.outgoingSize();
            IntWritable nid = new IntWritable(outlink[i]);
            context.write(nid, new NodeOrDouble(loss));
            value.setPageRank(value.getPageRank() - loss);
        }
        if(value.outgoing.length == 0) {//dangling node
            long long_rank = (long)(value.getPageRank() * PageRank.precision);
            context.getCounter(HadoopCounter.LEFTOVER).increment(long_rank);
            System.out.println("dangling node: "+ context.getCounter(HadoopCounter.LEFTOVER).getValue());
            value.setPageRank(0);
        }
        context.write(new IntWritable(value.nodeid), new NodeOrDouble(value));
    }
}
