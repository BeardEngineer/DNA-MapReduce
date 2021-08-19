package wordcount;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount
{
	public static class MotifInSeq implements Writable
	{
		private int score;
	    private int sequenceId;
		private int index;
		
		private int total;
		public String candidate;
		
		
		//default Constructor
		public MotifInSeq()
		{

		}
		
		//overide constructor
		public MotifInSeq(MotifInSeq temp)
		{
			this.score = temp.score;
			this.sequenceId = temp.sequenceId;
			index = temp.index;
			total = temp.total;
			candidate = temp.candidate;
		}
		//reads input data
		public void readFields(DataInput in) throws IOException
		{
			score = in.readInt();
			sequenceId = in.readInt();
			index = in.readInt();
			candidate = in.readUTF();
		}
		//writes output data
		public void write(DataOutput out) throws IOException
		{
			out.writeInt(score);
			out.writeInt(sequenceId);
			out.writeInt(index);
			out.writeUTF(candidate);
		}
		
		//custom toString to print results
		@Override
		public String toString()
		{
			return candidate + "\t" + sequenceId+ "\t" + score + "\t" + index + "\t " + total;
		}
	}

	public static class MotifMapper extends Mapper<LongWritable, Text, Text, MotifInSeq>
	{
		private MotifInSeq value = new MotifInSeq();
		private Text word  = new Text();
		//
		public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException
		{
			String str = inputValue.toString();
			int strLen = str.length();

			char basePairs[] = "acgt".toCharArray();
			int[] l = new int[8];
			
			
		
			// 8 nested loops to find all possible motif
			for(l[0]=0; l[0]<4; l[0]++)
				for(l[1]=0; l[1]<4; l[1]++)
					for(l[2]=0; l[2]<4; l[2]++)
						for(l[3]=0; l[3]<4; l[3]++)
							for(l[4]=0; l[4]<4; l[4]++)
								for(l[5]=0; l[5]<4; l[5]++)
									for(l[6]=0; l[6]<4; l[6]++)
										for(l[7]=0; l[7]<4; l[7]++)
										{
											int minD = 10;
											int minIndex= 0;
											String motif = "";
											
											//write our found pairs to our motif
											for(int i = 0; i < 8; i++)
												motif += basePairs[l[i]];
											
											//finds the distance 
											for(int i = 0; i < strLen - 8; i++)
											{
												int dist = 0;
												for(int j = 0; j < 8; j++)
												{
													if(str.charAt(i + j) != motif.charAt(j))
														dist++;
												}
												if(dist < minD)
												{
													minD = dist;
													minIndex= i;
												}
												if(minD == 0)
													break;
											}

											word.set(motif);

											value.score = minD;
											value.sequenceId = (int)inputKey.get();
											value.sequenceId /= 59;
											value.index = minIndex; 
											value.candidate = str.substring(minIndex, minIndex + 8);

											context.write(word, value);
										}
		}
	}

	public static class SequenceReducer extends Reducer<Text, MotifInSeq, Text, MotifInSeq> 
	{
		

		public void reduce(Text key, Iterable<MotifInSeq> values, Context context) throws IOException, InterruptedException 
		{
			ArrayList<MotifInSeq> motif = new ArrayList<MotifInSeq>();
			int sum = 0;

			for (MotifInSeq val : values)
			{
				motif.add(new MotifInSeq(val));
				sum += val.score;
				
			}

			for (MotifInSeq val : motif)
			{
				val.total = sum;
				context.write(key, val);
			}
		}

		
	}

	public static void main(String[] args) throws Exception
	{
		
 		Configuration conf = new Configuration();
 		
 		//throws error message if there are not 2 arguments
 		if(args.length != 2)
 		{
 			System.err.println("Usage: [input] [output]");
 			System.exit(100);
 		}
 		//create our job for reduce
		Job job = Job.getInstance(conf, "wordcount");

		job.setJarByClass(WordCount.class);

		//set mapper, combiner and reducer class
		job.setMapperClass(MotifMapper.class);
		job.setCombinerClass(SequenceReducer.class);
		job.setReducerClass(SequenceReducer.class);

		//set output for key and value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MotifInSeq.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MotifInSeq.class);

		//set input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//pause for job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}