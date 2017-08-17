package org.conan.myhadoop.mapreduce;
// org.conan.myhadoop.mapreduce.InvertedIndex

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by zhangzhibo on 17-5-1.
 */
public class InvertedIndex {
    public static class WordToFileMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                context.write(new Text(itr.nextToken()), new Text(fileName));
            }
        }
    }

    public static class WordToFileCountReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text fileText : values) {
                String file = fileText.toString();
                if (map.containsKey(file)) {
                    map.put(file, map.get(file) + 1);
                } else {
                    map.put(file, 1);
                }
            }
            context.write(key, new Text(map.toString()));
        }
    }


    public static void main(String[] args) throws Exception  {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: invertedindex <inDir> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "invert index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(WordToFileMapper.class);
        job.setReducerClass(WordToFileCountReducer.class);

        // Defining the output key and value class for the mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Defining the output key and value class for the reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        Path outputPath = new Path(otherArgs[1]);

        outputPath.getFileSystem(conf).delete(outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
