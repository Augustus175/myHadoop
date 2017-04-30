package org.conan.myhadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by zhangzhibo on 17-4-27.
 */



public class WordCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text vaule, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(vaule.toString());
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable resault = new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Context contest)throws IOException,InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            resault.set(sum);
            contest.write(key,resault);
        }
    }
    public static void main(String[] args) throws Exception {
//        TokenizerMapper tokenizermapper = new TokenizerMapper();
//        IntSumReducer intsumreducer = new IntSumReducer();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length <2)
        {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapOutputKeyClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        Path outputpath = new Path(otherArgs[1]);
        outputpath.getFileSystem(conf).delete(outputpath);
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length-1]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }

}
