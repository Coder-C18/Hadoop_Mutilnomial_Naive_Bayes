
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
 
public class WordCount1Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private int dem = 0;
	private int test = 0;
	private int dem_kindof = 0;
	private double total=0.0;

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		Text key = t_key;
		int sum = 0;
		String[] part = key.toString().split(",");
		if (part.length == 1) {
			test=0;
			dem_kindof = 0;
			dem = 0;
		}
		while (values.hasNext()) {
			DoubleWritable value = (DoubleWritable) values.next();
			sum += value.get();
			if (part.length == 1)
				dem++;
		}
		if ((part.length > 1) & (key.toString().indexOf('!') != -1)) {
			dem_kindof++;
		}
		
		if ((part.length > 1) & (key.toString().indexOf('!') == -1)&(test==0)) {
			Text keyk = new Text();
			keyk.set(part[0]+"'k");
			output.collect(keyk, new DoubleWritable(dem_kindof));
			test++;
			
		}
		
		if (key.toString().indexOf('!') == -1) {
			if (part.length == 1) {
				output.collect(key, new DoubleWritable(sum));
			}
			if (part.length > 1) {
				output.collect(key, new DoubleWritable((double) (sum+1) / (dem+dem_kindof)));
				total+=(double) (sum+1) / (dem+dem_kindof);
				System.out.println(total);
			}
		}
	}
}