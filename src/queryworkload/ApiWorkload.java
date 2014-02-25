
package queryworkload;

import java.util.Properties;

import com.yahoo.ycsb.WorkloadException;

public abstract class ApiWorkload {

  /**
   * Initialize the scenario. Create any generators and other shared objects here. Called once,
   * in the main client thread, before any operations are started.
   */
  public void init(Properties properties) throws ApiException {
  }

  public Object initThread(Properties properties, int myThreadId, int threadCount) throws ApiException {
    return null;
  }

  public void cleanup() throws ApiException {
  }

  public abstract boolean doCall(ApiConnector apiConnector, Object threadState) throws WorkloadException;


}
