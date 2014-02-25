package queryworkload.generator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.yahoo.ycsb.generator.IntegerGenerator;

public class DateMappedIntegerGenerator extends MappedIntegerGenerator {

	private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
	
	private final Date startDate;
	
	public DateMappedIntegerGenerator(IntegerGenerator intGenerator, String startDateString) throws ParseException {
		super(intGenerator);
		SimpleDateFormat parserSDF = new SimpleDateFormat(DATE_FORMAT_STRING);
		startDate = parserSDF.parse(startDateString);
	}
	
	
	@Override
	public String map(int nextInt) {
		SimpleDateFormat parserSDF = new SimpleDateFormat(DATE_FORMAT_STRING);
		String lastMappedInt = mapDate(parserSDF, startDate, nextInt);
		this.setLastMappedInt(lastMappedInt);
		return lastMappedInt;
	}
	
	public static String mapNoSideEffects(String sd, int nextInt) throws ParseException {
		SimpleDateFormat parserSDF = new SimpleDateFormat(DATE_FORMAT_STRING);
		Date tempStartDate = parserSDF.parse(sd);
		return mapDate(parserSDF, tempStartDate, nextInt);
	}

	private static String mapDate(SimpleDateFormat parserSDF, Date baseDate, int daysToAdd) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(baseDate);
		calendar.add(Calendar.DATE, daysToAdd);
		String lastMappedInt = parserSDF.format(calendar.getTime());
		return lastMappedInt;
	}

}
