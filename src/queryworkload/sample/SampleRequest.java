package queryworkload.sample;

public class SampleRequest implements queryworkload.Request {

    String minTimestamp, maxTimestamp, groupby, select, dim;

    public String getMinTimestamp() {
        return minTimestamp;
    }

    public SampleRequest(String minTimestamp, String maxTimestamp,
                         String groupby, String select, String dim) {
        super();
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.groupby = groupby;
        this.select = select;
        this.dim = dim;
    }

    public SampleRequest() {
        // TODO Auto-generated constructor stub
    }

    public String getMaxTimestamp() {
        return maxTimestamp;
    }

    public String getGroupby() {
        return groupby;
    }

    public String getSelect() {
        return select;
    }

    public String getDim() {
        return dim;
    }

    public String getFormattedFields() {
        StringBuilder sb = new StringBuilder();
        sb.append("\"")
        .append(minTimestamp)
        .append("\"")
        .append(BmlRequest.CSV_SEPARATOR)
        .append("\"")
        .append(maxTimestamp)
        .append("\"")
        .append(BmlRequest.CSV_SEPARATOR)
        .append("\"")
        .append(dim)
        .append("\"")
        .append(BmlRequest.CSV_SEPARATOR)
        .append("\"")
        .append(groupby)
        .append("\"")
        .append(BmlRequest.CSV_SEPARATOR)
        .append("\"")
        .append(select)
        .append("\"");
    return sb.toString();
    }

    public String getFormattedHeader() {
        StringBuilder sb = new StringBuilder();
        sb.append("mintimestamp")
            .append(BmlRequest.CSV_SEPARATOR) 
            .append("maxtimestamp")
            .append(BmlRequest.CSV_SEPARATOR) 
            .append("dim")
            .append(BmlRequest.CSV_SEPARATOR) 
            .append("groupby")
            .append(BmlRequest.CSV_SEPARATOR) 
            .append("select");
        return sb.toString();
        
    }

    public String getUrl(String apiHost, String apiPort) {
        String url = String
                .format("http://%s:%s/ducati_api?mintimestamp=%s&maxtimestamp=%s&dim=%s&groupby=%s&select=%s",
                        apiHost, apiPort, minTimestamp, maxTimestamp,
                        dim, groupby, select);
        return url;
    }

    public static DucatiRequest getInstance() {
        return new DucatiRequest();
    }

}
