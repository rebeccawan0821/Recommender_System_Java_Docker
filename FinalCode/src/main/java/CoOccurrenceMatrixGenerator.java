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

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input data: value = userId \t movie1: rating, movie2: rating...
			//what to do: separate input data into the following format
			//userId
			//movieList --> <1,2,3> --> <1:1, 1:2, 1:3, 2:1, 2:2, 2:3, ...>
			String line = value.toString().trim();
			String[] user_movieRating = line.split("\t");

			//Corner case: for each user, must see more than 2 movie
			if (user_movieRating.length != 2) {
				return;
			}

			String[] movie_rating = user_movieRating[1].split(",");
			for (int i = 0; i < movie_rating.length; i++) {
				String movie1 = movie_rating[i].split(":")[0];
				for (int j = 0; j < movie_rating.length; j++) {
					String movie2 = movie_rating[j].split(":")[0];
					String outputKey = movie1 + ":" + movie2;
					context.write(new Text(outputKey), new IntWritable(1));
				}
			}
			
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//input data: <"movie1:movie2", <1,1,1,...>>
			//calculate each two movies have been watched by how many people
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
