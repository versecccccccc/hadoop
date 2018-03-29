
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
import org.apache.hadoop.io.NullWritable; 
  
public class SongForm {  
  
    public static class SongFormMap extends  
            Mapper<LongWritable, Text, Text, IntWritable> {  
  
        private final IntWritable one = new IntWritable(1);  
        private Text word = new Text();  
  
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {  
            String line = value.toString();  
            String[] st = line.split(",");
            if(!st[6].equals("") && !st[7].equals("") && !st[8].equals("")){
                int len5 = st[5].length();
                char[] ch5 = new char[len5];  
                st[5].getChars(2, len5 - 1, ch5, 0);
                String test5 = new String(ch5);
                test5 = test5.trim();
                int len7 = st[7].length();
                char[] ch7 = new char[len7];  
                st[7].getChars(2, len7 - 1, ch7, 0);
                String test7 = new String(ch7);
                String f = test5 + "," + st[6] + "," + test7 + "," + st[8] + "," + st[10] + "," + st[11] + st[12] + "," + st[13] + "," + st[14] + "," + st[15];
            word.set(f); 
            context.write(word, one); 
         }
        }  
    }  
  
    public static class SongFormReduce extends  
            Reducer<Text, IntWritable, Text, Text> {  
  
        public void reduce(Text key, Iterable<IntWritable> values,  
                Context context) throws IOException, InterruptedException {  
            int sum = 0;  
            for (IntWritable val : values) {  
                sum += val.get();  
            }  
            context.write(key, new Text(" ")); 
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(SongForm.class);  
        job.setJobName("songform");  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
  
        job.setMapperClass(SongFormMap.class);  
        job.setReducerClass(SongFormReduce.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
  
        job.waitForCompletion(true);  
    }  
}  
