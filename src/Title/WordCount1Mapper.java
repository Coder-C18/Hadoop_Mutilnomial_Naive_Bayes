package Title;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;

public class WordCount1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text word = new Text();
	private final static DoubleWritable one = new DoubleWritable(1);
	public int dem = 0;

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String[] part = value.toString().split(",");
		word.set(part[0]);
		output.collect(word, one);
		word.set("@");
		output.collect(word, one);
	}
}
