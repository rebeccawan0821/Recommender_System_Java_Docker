import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataDividerByUser {
    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input data:  user_id,movie_id,rating
            String line = value.toString().trim();
            String[] user_movie_rating = line.split(",");
            String userId = user_movie_rating[0];
            String movieId = user_movie_rating[1];
            String rating = user_movie_rating[2];
            //divide data by user and write output: <userId, "movieId : rating">
            context.write(new IntWritable(Integer.parseInt(userId)), new Text(movieId + ":" + rating));
        }
    }

    public static class DivideByUserReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        // reduce method, output: key = userId, value = list<movie:rating>
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key = userId
            //value = list<movie:rating>
            StringBuilder sb = new StringBuilder();
            while (values.iterator().hasNext()) {
                sb.append(values.iterator().next() + ",");
            }
            //merge data for one user, list all movie:rating for one user
            context.write(key, new Text(sb.deleteCharAt(sb.length() - 1).toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DivideByUserReducer.class);

        job.setJarByClass(DataDividerMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
