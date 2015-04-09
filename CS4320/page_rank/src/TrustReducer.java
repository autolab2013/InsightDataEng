import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustReducer extends Reducer<IntWritable, NodeOrDouble, IntWritable, Node> {
    public void reduce(IntWritable key, Iterable<NodeOrDouble> values, Context context)
            throws IOException, InterruptedException {
        //Implement
        Node n = null;
        double rank = 0;
        for(NodeOrDouble val : values){
            if(val.isNode())
                n = val.getNode();
            else
                rank += val.getDouble();
        }
        n.setPageRank(rank);
        context.write(new IntWritable(n.nodeid), n);
        System.out.println("Trust reducer get node id: " + n.nodeid+", page rank is "+ n.getPageRank());
    }
}
