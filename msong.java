
import java.io.IOException;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  


public class msong {  
  
    public static class msongMap extends  
            Mapper<LongWritable, Text, Text, IntWritable> {  
  
        private final IntWritable one = new IntWritable(99);  
        private Text word = new Text();  
  
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {  
            String line = value.toString();  

            String[] array = line.split(",");
            String lis = array[0] + "," +array[1];
            word.set(lis);  
            context.write(word, one);  
            }  
        }  
      
  
    public static class msongReduce extends  
            Reducer<Text, IntWritable, Text, Text> {  
  
        public void reduce(Text key, Iterable<IntWritable> values,  
                Context context) throws IOException, InterruptedException {  
 
            context.write(key, new Text(" "));  
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(msong.class);  
        job.setJobName("msong");  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
  
        job.setMapperClass(msongMap.class);  
        job.setReducerClass(msongReduce.class);  
        job.setNumReduceTasks(1); 

        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
  
        job.waitForCompletion(true);  
    }  
}  