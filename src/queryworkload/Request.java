package queryworkload;

public interface Request {

	String getFormattedFields();
	String getFormattedHeader();
    String getUrl(String apiHost, String apiPort);
}