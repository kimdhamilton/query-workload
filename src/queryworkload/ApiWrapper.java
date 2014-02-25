/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package queryworkload;

import java.util.Map;
import java.util.Properties;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" API connector that measures latencies and counts
 * return codes.
 */
public class ApiWrapper extends ApiConnector {

	private static final String SEPARATOR = ",";
	private static volatile boolean INITIALIZED_HEADER = false;
	ApiConnector apiConnector;
	Measurements measurements;
	DetailedMeasurementsWriter details;

	public ApiWrapper(ApiConnector apiConnector, Request instance)
			throws ApiException {
		this.apiConnector = apiConnector;
		measurements = Measurements.getMeasurements();

		details = DetailedMeasurementsWriter.getInstance();
		if (!INITIALIZED_HEADER) {
			details.write(
					instance.getFormattedHeader() + 
					SEPARATOR + 
					"resultCount" + 
					SEPARATOR +
					"time");
			INITIALIZED_HEADER = true;
		}
	}

	/**
	 * Initialize any state for this ApiConnector. Called once per ApiConnector
	 * instance; there is one ApiConnector instance per client thread.
	 */
	public void init(Properties props) throws ApiException {

	}

	/**
	 * Cleanup any state for this ApiConnector. Called once per ApiConnector
	 * instance; there is one ApiConnector instance per client thread.
	 */
	public void cleanup() throws ApiException {
		apiConnector.cleanup();
	}

	@Override
	public int call(Request request, Map<String, String> result) {
		long st = System.currentTimeMillis();
		int res = apiConnector.call(request, result);
		long en = System.currentTimeMillis();
		measurements.measure("UPDATE", (int) (en - st));
		measurements.reportReturnCode("UPDATE", res);

		details.write(request.getFormattedFields() + 
				SEPARATOR + 
				res + 
				SEPARATOR + 
				(int) (en - st));
		return res;
	}
}
