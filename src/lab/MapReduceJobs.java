package lab;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/** Merge/deduplicate records, globally rank integers, and join child-parent edges. */
public final class MapReduceJobs extends Configured implements Tool {
    public static class DedupeMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String line=v.toString().trim();
            if (!line.isEmpty()) c.write(new Text(line.replaceAll("\\s+", "\t")), NullWritable.get());
        }
    }
    public static class DedupeReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
        protected void reduce(Text k, Iterable<NullWritable> vs, Context c) throws IOException, InterruptedException {
            c.write(k, NullWritable.get());
        }
    }
    public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,NullWritable> {
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String s=v.toString().trim();
            if (s.isEmpty()) return;
            try { c.write(new LongWritable(Long.parseLong(s)), NullWritable.get()); }
            catch (NumberFormatException e) { throw new IOException("Invalid integer: " + s, e); }
        }
    }
    public static class SortReducer extends Reducer<LongWritable,NullWritable,LongWritable,LongWritable> {
        private long rank=0;
        protected void reduce(LongWritable value, Iterable<NullWritable> vs, Context c) throws IOException, InterruptedException {
            for (NullWritable ignored : vs) c.write(new LongWritable(++rank), value);
        }
    }
    public static class FamilyMapper extends Mapper<LongWritable,Text,Text,Text> {
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String s=v.toString().trim();
            if (s.isEmpty()) return;
            String[] a=s.split("\\s+");
            if (a.length!=2) throw new IOException("Expected child parent: " + s);
            if (a[0].equalsIgnoreCase("child") && a[1].equalsIgnoreCase("parent")) return;
            // Join on the intermediate person: their children and their parents.
            c.write(new Text(a[1]),new Text("C\t"+a[0]));
            c.write(new Text(a[0]),new Text("P\t"+a[1]));
        }
    }
    public static class FamilyReducer extends Reducer<Text,Text,Text,Text> {
        // A single reducer is used; avoid duplicate pairs reached through different parents.
        private final Set<String> emitted = new HashSet<>();
        protected void setup(Context c) throws IOException, InterruptedException {
            c.write(new Text("grandchild"),new Text("grandparent"));
        }
        protected void reduce(Text k, Iterable<Text> vs, Context c) throws IOException, InterruptedException {
            Set<String> children=new TreeSet<>(), parents=new TreeSet<>();
            for (Text value:vs) {
                String[] pair=value.toString().split("\t",2);
                (pair[0].equals("C")?children:parents).add(pair[1]);
            }
            for(String child:children) for(String parent:parents) {
                if (emitted.add(child+"\t"+parent)) c.write(new Text(child),new Text(parent));
            }
        }
    }
    public int run(String[] a) throws Exception {
        if (a.length!=3) throw new IllegalArgumentException("dedupe|sort|family INPUT OUTPUT");
        Job j=Job.getInstance(getConf(),"experiment1-"+a[0]);
        j.setJarByClass(MapReduceJobs.class);
        j.setNumReduceTasks(1); // Global order/rank and exactly one family header.
        switch(a[0]) {
        case "dedupe":
            j.setMapperClass(DedupeMapper.class); j.setReducerClass(DedupeReducer.class);
            j.setCombinerClass(DedupeReducer.class);
            j.setMapOutputKeyClass(Text.class); j.setMapOutputValueClass(NullWritable.class);
            j.setOutputKeyClass(Text.class); j.setOutputValueClass(NullWritable.class); break;
        case "sort":
            j.setMapperClass(SortMapper.class); j.setReducerClass(SortReducer.class);
            j.setMapOutputKeyClass(LongWritable.class); j.setMapOutputValueClass(NullWritable.class);
            j.setOutputKeyClass(LongWritable.class); j.setOutputValueClass(LongWritable.class); break;
        case "family":
            j.setMapperClass(FamilyMapper.class); j.setReducerClass(FamilyReducer.class);
            j.setMapOutputKeyClass(Text.class); j.setMapOutputValueClass(Text.class);
            j.setOutputKeyClass(Text.class); j.setOutputValueClass(Text.class); break;
        default: throw new IllegalArgumentException("Unknown job: "+a[0]);
        }
        FileInputFormat.addInputPath(j,new Path(a[1]));
        FileOutputFormat.setOutputPath(j,new Path(a[2]));
        return j.waitForCompletion(true)?0:1;
    }
    public static void main(String[] a) throws Exception {
        System.exit(ToolRunner.run(new Configuration(),new MapReduceJobs(),a));
    }
}
