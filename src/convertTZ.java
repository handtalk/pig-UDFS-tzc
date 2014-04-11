package mydatetime;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.TimeZone;
import java.util.Date;

//import org.apache.pig.impl.util.WrappedIOException;

public class convertTZ extends EvalFunc<String> {
	SimpleDateFormat in_parser;
	SimpleDateFormat out_parser;
	Date in_Date = null;
	String in_str;
	String out_str;
	public String exec(Tuple input) throws IOException {
		// public String exec(Tuple input) throws Exception {
		if (input == null || input.size() == 0)
			return null;
		// try {
// 		String in_str = (String) input.get(0);
		in_str = (String) input.get(0);
		// String in_format = (String) input.get(1);
		// String in_tz = (String) input.get(2);
		// String out_format = (String) input.get(3);
		// String out_tz = (String) input.get(4);

		in_parser = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.S");
		in_parser.setTimeZone(TimeZone.getTimeZone("UTC"));
//		Date in_Date = null;
		try {
			in_Date = in_parser.parse(in_str);
			// }
		} catch (ParseException e) {
			System.out.println("Parse Error!" + e);
			throw new IOException("Caught exception processing input row " + e);
		}

		out_parser = new SimpleDateFormat( //out_parser.setDateFormatSymbols(
				"yyyy-MM-dd HH:mm:ss");
		out_parser.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		out_str = out_parser.format(in_Date);

		return out_str;
	}
}