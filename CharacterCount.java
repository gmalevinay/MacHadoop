
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


	public class CharacterCount {
		@SuppressWarnings("deprecation")
		public static void main(String[] args) throws Exception,InterruptedException
		{
			// Configurations w.r.t JOB, JAR ..
			Configuration conf= new Configuration();
			Job j = new Job(conf,"VowelCount-MyFirstjob");
			j.setJarByClass(WordCount.class);
			
			//Mapper, Reducer, Combiner Classes
			j.setMapperClass(VowelsCountMap.class);
			j.setReducerClass(VowelsCountRed.class);
			
			
			
			//HDFS IO Paths
			FileInputFormat.addInputPath(j,new Path(args[0]));
			FileOutputFormat.setOutputPath(j,new Path(args[1]));
			
			//Final Output k,v data types
			j.setOutputKeyClass(Text.class); 
			j.setOutputValueClass(IntWritable.class);
			
			//System exit process
			System.exit(j.waitForCompletion(true)?0:1);

		}
		public static class VowelsCountMap extends Mapper <Object,Text,Text,IntWritable>
		{
			private static Text word=new Text();
			private static final IntWritable one=new IntWritable(1);
			public static String vowels = "aeiou";

			public void map(Object key,Text value, Context context) throws IOException,InterruptedException
			{
				char[] chararray = value.toString().toCharArray();
				for ( char c : chararray)
				{
					if (vowels.indexOf(Character.toLowerCase(c), 0) > -1)
					{
						word.set(Character.toString(c));
						context.write(word, one);
					}
				}
				
			}
		}
		public static class VowelsCountRed extends Reducer <Text,IntWritable,Text,IntWritable>
		{
			private IntWritable Results=new IntWritable();
			public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
			{
				int sum= 0;
				for (IntWritable x:values)
				{
					sum+=x.get();
				}
				Results.set(sum);
				context.write(key,Results);
			}
		}


}
