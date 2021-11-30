

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text word = new Text();
	private final static DoubleWritable one = new DoubleWritable(1);
	public int dem = 0;

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String[] part = value.toString().split(",");
		StringTokenizer itr = new StringTokenizer(part[1]);
		while (itr.hasMoreTokens()) {
			word.set(part[0]);
			output.collect(word, one);
			String x=itr.nextToken();
			String value_ = part[0] + ',' + x;
			word.set(value_);
			output.collect(word, one);
			 value_ = part[0] + "!," + x;
			word.set(value_);
			output.collect(word, one);
		}
	}
}
