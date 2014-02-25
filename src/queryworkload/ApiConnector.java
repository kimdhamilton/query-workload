package queryworkload;

import java.util.Map;
import java.util.Properties;

public abstract class ApiConnector {

	public void init(Properties props) throws ApiException {
	}

	public void cleanup() throws ApiException {
	}

	public abstract int call(Request request, Map<String, String> result);

}
