package queryworkload;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

/**
 * Synchronizes writes to file.
 *
 */
public class DetailedMeasurementsWriter {

	private static DetailedMeasurementsWriter instance;

	private static Properties properties = null;

	private static PrintWriter out;

	public static void setProperties(Properties props) {
		properties = props;
	}

	public synchronized static DetailedMeasurementsWriter getInstance() {
		if (instance == null) {
			if (properties == null) {
				throw new RuntimeException("need to specify properties file");
			}
			try {
				instance = new DetailedMeasurementsWriter(properties);
			} catch (IOException e) {
				throw new RuntimeException(
						"error initializing synchronized writer", e);
			}
		}
		return instance;
	}

	private DetailedMeasurementsWriter(Properties properties) throws IOException {
		String fileName = properties.getProperty("detailsfile");
		out = new PrintWriter(new BufferedWriter(new FileWriter(fileName)));
	}

	public synchronized void write(String s) {
		out.println(s);
	}
	
	public void close() {
		out.close();
	}
}
