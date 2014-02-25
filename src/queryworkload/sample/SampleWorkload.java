package queryworkload.sample;

import java.util.HashMap;
import java.util.Properties;

import queryworkload.ApiConnector;
import queryworkload.ApiException;
import queryworkload.ApiWorkload;
import queryworkload.Request;
import queryworkload.generator.GeneratorUtils;
import queryworkload.generator.UniformLongGenerator;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;

public class SampleWorkload extends ApiWorkload {

    public static final String DIMENSION="dim";
    public static final String GROUP_BY="groupby";
    public static final String MINTIMESTAMP_RANGE="mintimestampRange";
    public static final String TIMERANGE="timerange_in_seconds";
    public static final String SELECT="select";
    
    private Generator dimGenerator;
    private Generator groupByGenerator;
    private UniformLongGenerator minTimestampGenerator;
    private UniformIntegerGenerator timerangeGenerator;
    
    private String select;
    
    @Override
    public void init(Properties properties) throws ApiException {
        String dimString = properties.getProperty(DIMENSION);
        String groupByString = properties.getProperty(GROUP_BY);
        
        dimGenerator = GeneratorUtils.getUniformGenerator(dimString);
        groupByGenerator = GeneratorUtils.getUniformGenerator(groupByString);
        
        select = properties.getProperty(SELECT);
        
        String minTimestampRange = properties.getProperty(MINTIMESTAMP_RANGE);
        String rangeSplit[] = minTimestampRange.split("-");
        long start=new Long(rangeSplit[0]);
        long end=new Long(rangeSplit[1]);
        int interval =  ((int)end-(int)start);
        minTimestampGenerator = new UniformLongGenerator(start, interval);
        
        
        String timerange = properties.getProperty(TIMERANGE);
        // generate ranges between 1 minute -- 31 days
        timerangeGenerator = new UniformIntegerGenerator(60, new Integer(timerange));
        
    }
    
    
    @Override
    public boolean doCall(ApiConnector apiConnector, Object threadState)
            throws WorkloadException {
        Request request = generateNextRequest();
        apiConnector.call(request, new HashMap<String, String>());
        return true;
    }


    private DucatiRequest generateNextRequest() {
        long minTimestamp = minTimestampGenerator.nextLong();
        long range = timerangeGenerator.nextInt();
        String groupby = groupByGenerator.nextString();
        String dim = dimGenerator.nextString();
        return new DucatiRequest(""+minTimestamp, ""+(minTimestamp+range), groupby, select, dim);
    }

}
