package uber.tracking;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	public Text basement = new Text();
	public int trips;
	Calendar calendar = Calendar.getInstance();
	Date date = null;
	SimpleDateFormat format = new SimpleDateFormat("MM/DD/YYYY");
	String[] days = { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };

	public void map(Object key, Text record, Context context) throws IOException, InterruptedException {
		String[] parts = record.toString().split("[,]");
		basement.set(parts[0]);

		try {
			date = format.parse(parts[1]);
			calendar.setTime(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		trips = new Integer(parts[3]);
		String Keys = basement.toString() + " " + days[Calendar.DAY_OF_WEEK];
		context.write(new Text(Keys), new IntWritable(trips));
	}

}
