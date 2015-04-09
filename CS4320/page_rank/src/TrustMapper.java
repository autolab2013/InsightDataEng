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
        double loss = value.getPageRank()/value.outgoingSize();
        int[] outlink = value.outgoing;
        for(int i=0;i<outlink.length;i++){
            IntWritable nid = new IntWritable(outlink[i]);
            context.write(nid, new NodeOrDouble(loss));
        }
        context.write(new IntWritable(value.nodeid), new NodeOrDouble(value));
    }
}
