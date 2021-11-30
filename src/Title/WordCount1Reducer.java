package Title;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
int dem=0;
	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		Text key = t_key;
		int sum = 0;
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			DoubleWritable value = (DoubleWritable) values.next();
			sum += value.get();
			if (key.toString().equals("@"))dem++;
		}
		if(key.toString().equals("@")==false )output.collect(key, new DoubleWritable((double)sum/dem));
	}
}
