package queryworkload;

import java.util.Properties;
import java.util.concurrent.Callable;

import com.yahoo.ycsb.WorkloadException;

/**
 * Describes the overall structure of a workload task. This is where we delegate
 * operations to the actual workload object.
 */
class ClientTask implements Callable<Integer> {
	private volatile int opsDone = 0;

	private ApiConnector api;
	private ApiWorkload workload;

	private Properties properties;
	private Object workloadState;

	// for testing
	protected ClientTask() {
	}

	public ClientTask(ApiConnector api, ApiWorkload workload,
			int threadId, int threadCount, Properties props)
			throws ApiException, WorkloadException {
		this.api = api;
		this.workload = workload;
		this.properties = props;

		api.init(props);
		workloadState = workload.initThread(properties, threadId, threadCount);
	}

	//public int getOpsDone() {
	//	return opsDone;
	//}

	public Integer call() throws ApiException {
		boolean success = false;

		try {
			success = workload.doCall(api, workloadState);
		} catch (WorkloadException e) {
			throw new ApiException(e); // TODO
		}

		if (!success) {
			throw new ApiException("Workload says we are done");
		}
		//opCompleted();
		return opsDone;
	}

	//public void opCompleted() {
	//	opsDone++;
	//}

	public int finish() throws ApiException {
		api.cleanup();
		return opsDone;
	}
}
