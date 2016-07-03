package herzen;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class App {

    public static class Maper
            extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter report)
                throws IOException {
            String line = value.toString();
            String lastToken = null;
            StringTokenizer str = new StringTokenizer(line, "\t");
            String year = str.nextToken();

            while (str.hasMoreTokens()) {
                lastToken = str.nextToken();
            }
            int avgprice = Integer.parseInt(lastToken);
            output.collect(new Text(year), new IntWritable(avgprice));

        }
    }

    public static class Redducer
            extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text keyIn, Iterator<IntWritable> valuesIn, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {

            int maxavg = 30;
            int val = Integer.MIN_VALUE;

            while (valuesIn.hasNext()) {
                if ((val = valuesIn.next().get()) > maxavg) {
                    output.collect(keyIn, new IntWritable(val));
                }
            }
        }

        public static void main(String[] args) throws Exception {

            JobConf conf = new JobConf(App.class);
            conf.setJobName("максимальное электричество");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setMapperClass(Maper.class);
            conf.setReducerClass(Redducer.class);
            conf.setCombinerClass(Redducer.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            JobClient.runJob(conf);
        }
    }
}
