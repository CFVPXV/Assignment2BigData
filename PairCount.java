import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public String order_strings(String str){
        
            String[] words = str.split(",");

            if(words[0].compareTo(words[1]) > 0){
            
                String temp = words[0];
                words[0] = words[1];
                words[1] = temp;
            
            }

            return words[0] + "," + words[1];
        
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

               StringTokenizer itr = new StringTokenizer(order_strings(value.toString()));

               while (itr.hasMoreTokens()) {

                   word.set(itr.nextToken());

                   context.write(word, one);

               }

        }

    }

    public static class IntSumReducer

            extends Reducer<Text,IntWritable,Text,IntWritable> {

            private IntWritable result = new IntWritable();

            private IntWritable uniqCounter = new IntWritable();

            private Text uniqLabel = new Text("Unique Counter");
            
            int counter = 0;

            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

                int sum = 0;

                for (IntWritable val : values) {

                    sum += val.get();

                }

                counter++;

                uniqCounter.set(counter);

                result.set(sum);

                context.write(key, result);

                context.write(uniqLabel, uniqCounter);

            }
            
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(PairCount.class);

        job.setMapperClass(TokenizerMapper.class);

        //job.setCombinerClass(IntSumReducer.class);

        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
