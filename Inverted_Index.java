import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;

public class Inverted_Index {

public static class MapperClass extends Mapper<LongWritable , Text, Text, Text>
{
	

  public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
     {
         
     	String line= value.toString();
     	StringTokenizer st = new StringTokenizer(line,"\t");	
     	String docid= st.nextToken();
     	String data= st.nextToken();
     	StringTokenizer tokenizer = new StringTokenizer(data," ");

     	

     	    while(tokenizer.hasMoreTokens())
     	   {
                String word= tokenizer.nextToken();
             //word.set(tokenizer.nextToken());
//write the word with the doc in which it is present.
             context.write(new Text(word), new Text(docid));
     	    }


     }

}
//Reducer class
public static class ReducerClass extends Reducer<Text, Text, Text, Text>
{
	

	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	  {
	   	int count=0;
	   	HashMap hm=new HashMap();
	   	
	     	for (Text value : values ) 
	    	{
	    		String str= value.toString();
              if(!hm.isEmpty() && hm.get(str)!=null)
              {
               count=(int)hm.get(str);
               hm.put(str,++count); //count the number of occurrences of the word in a particular document.
              }
              else
              {
              	hm.put(str,1);
              }
	   		//sum+= value.get();

	   	    }
	   	    String word_count=hm.toString();
	   	    String colon = word_count.replaceAll("=", ":");
	   	    String openBraces=colon.replaceAll("\\{", "");
	   	    String closeBraces=openBraces.replaceAll("\\}", "");
	   	    String comma=closeBraces.replaceAll("\\,", " ");
	   	    String valueFinal= comma + "\n";

	   	    //context.write(key, new IntWritable(sum));
	   	    context.write(key, new Text(valueFinal));
	  }
	   	
	   }


public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException

	{
	   if (args.length!=2) {
	   		System.err.println("Usage: Word Count <input path> <output path>");
	   		System.exit(-1);
	   	}	
	   	
	   	Job job = new Job();
	   	job.setJarByClass(Inverted_Index.class);
	   	job.setJobName("InvertedIndex");
	   	FileInputFormat.addInputPath(job, new Path(args[0]));
	   	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);

	}
}
