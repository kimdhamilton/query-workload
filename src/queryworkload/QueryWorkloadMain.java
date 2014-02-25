package queryworkload;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import queryworkload.sample.SampleConnector;
import queryworkload.sample.SampleRequest;
import queryworkload.sample.SampleWorkload;
import com.google.common.collect.Lists;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;

public class QueryWorkloadMain {

	public static final int DEFAULT_TEST_LENGTH = 120;
	public static final int DEFAULT_THINK_TIME_MS = 1000;
	public static final int DEFAULT_NUM_CLIENTS = 10;

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws WorkloadException
	 * @throws ApiException
	 * @throws ExecutionException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ExecutionException, ApiException,
			WorkloadException {
		Options options = constructOptions();
		if (args.length < 1) {
			printUsage(options, System.out);
			return;
		}
		CommandLineParser parser = new PosixParser();
		try {

			CommandLine cmd = parser.parse(options, args);
			String propFile = null;
			if (cmd.hasOption("props")) {
				propFile = cmd.getOptionValue("props");
			}
			String appName = null;
			if (cmd.hasOption("app")) {
                appName = cmd.getOptionValue("app");
            }
			
			Properties props = new Properties();
			props.load(new FileInputStream(propFile));

			execute(appName,props);
		} catch (ParseException e) {
			printUsage(options, System.out);
		}

	}

	private static void execute(String appName, Properties props) throws InterruptedException,
			ExecutionException, ApiException, WorkloadException {
		int threadCount = Integer.parseInt(props.getProperty("numClients",
				Integer.toString(DEFAULT_NUM_CLIENTS)));
		int thinkTimeMs = Integer.parseInt(props.getProperty("thinkTimeMs",
				Integer.toString(DEFAULT_THINK_TIME_MS)));
		int opCount = Integer.parseInt(props.getProperty("testLength",
				Integer.toString(DEFAULT_TEST_LENGTH)));

		System.out.println("Loading workload...");

		// show a warning message that creating the workload is taking a while
		// but only do so if it is taking longer than 2 seconds
		// (showing the message right away if the setup wasn't taking very long
		// was confusing people)
		Thread warningThread = new Thread() {
			public void run() {
				try {
					sleep(2000);
				} catch (InterruptedException e) {
					return;
				}
				System.err
						.println(" (might take a few minutes for large data sets)");
			}
		};

		warningThread.start();

		// set up measurements
		Measurements.setProperties(props);
		
		// set up detailed measurements
		DetailedMeasurementsWriter.setProperties(props);

		// load the workload
		ApiWorkload workload = null;
		if(appName.equals("bml")) {
		    workload = new BmlWorkload();
		} else if(appName.equals("ducati")) {
		    workload = new DucatiWorkload();
		}
		
		
		workload.init(props);

		warningThread.interrupt();

		// run the workload
		System.out.println("Starting test.");
		List<RateLimiter<ClientTask>> toDo = Lists.newArrayList();
		
        // TODO: request instantiation
		for (int threadId = 0; threadId < threadCount; threadId++) {

			ApiConnector connector = null;
			// TODO: instantiate here
			ApiWrapper wrapper = 
					new ApiWrapper(connector, request);
			ClientTask task = 
					new ClientTask(wrapper, workload, threadId, threadCount, props);
			toDo.add(new RateLimiter<ClientTask>((double) opCount, thinkTimeMs, task));
		}

		long start = System.currentTimeMillis();

		ExecutorService pool = Executors.newFixedThreadPool(threadCount);
		List<Future<Integer>> r = pool.invokeAll(toDo);

		long end = System.currentTimeMillis();

		int n = 0;
		for (Future<Integer> result : r) {
			n += result.get();
		}
		pool.shutdown();

		workload.cleanup();

	 	try {
			exportMeasurements(props, opCount * threadCount, end - start);
			DetailedMeasurementsWriter.getInstance().close();
		} catch (IOException e) {
			System.err.println("Could not export measurements, error: "
					+ e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * 
	 * @throws IOException
	 *             Either failed to write to output stream or failed to close
	 *             it.
	 */
	private static void exportMeasurements(Properties props, int opcount,
			long runtime) throws IOException {
		MeasurementsExporter exporter = null;
		try {
			// if no destination file is provided the results will be written to
			// stdout
			OutputStream out;
			String exportFile = props.getProperty("exportfile");
			if (exportFile == null) {
				out = System.out;
			} else {
				out = new FileOutputStream(exportFile);
			}

			// if no exporter is provided the default text one will be used
			String exporterStr = props
					.getProperty("exporter",
							"com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
			try {
				exporter = (MeasurementsExporter) Class.forName(exporterStr)
						.getConstructor(OutputStream.class).newInstance(out);
			} catch (Exception e) {
				System.err.println("Could not find exporter " + exporterStr
						+ ", will use default text reporter.");
				e.printStackTrace();
				exporter = new TextMeasurementsExporter(out);
			}

			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * ((double) opcount)
					/ ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			Measurements.getMeasurements().exportMeasurements(exporter);
		} finally {
			if (exporter != null) {
				exporter.close();
			}
		}
	}

	/**
	 * Construct command line options.
	 * 
	 * @return command line options
	 */
	private static Options constructOptions() {
		Options options = new Options();
		Option propertiesFileOption = OptionBuilder.withArgName("fileName")
				.hasArg().withDescription("configuration properties").create("props");
		
		Option appName = OptionBuilder.withArgName("appName")
                .hasArg().withDescription("application name").create("app");
		
		options.addOption(propertiesFileOption);
		options.addOption(appName);
		return options;
	}

	/**
	 * Print usage information to provided OutputStream.
	 * 
	 * @param options
	 *            Command-line options to be part of usage.
	 * @param out
	 *            OutputStream to which to write the usage information.
	 */
	public static void printUsage(final Options options, final OutputStream out) {
		final String applicationName = "java -cp query-workload.jar queryworkload.QueryWorkloadMain";
		final HelpFormatter usageFormatter = new HelpFormatter();
		usageFormatter.printHelp(applicationName, options);
	}

}
