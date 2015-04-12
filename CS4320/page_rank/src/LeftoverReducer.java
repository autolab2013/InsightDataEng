import java.io.IOException;
import java.lang.Double;
import java.lang.System;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {

    public static double alpha = 0.85;

    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
        //Implement
        Node node = null;
        for(Node n: Ns){
            node = n;
            System.out.println("should only be 1 node!");
        }
        long leftover_cnt = Long.parseLong(context.getConfiguration().get("leftover"));
        double leftover = (double)leftover_cnt / PageRank.precision;
        System.out.println("leftover: "+ leftover);
        double size = (double)Long.parseLong(context.getConfiguration().get("size"));
        System.out.println("size: "+ size);
        double rank = alpha/size + (1-alpha) * (leftover/size + node.getPageRank());
        node.setPageRank(rank);
        context.write(new IntWritable(node.nodeid), node);
        System.out.println("Leftover reducer pagerank is: "+ rank);
    }
}
