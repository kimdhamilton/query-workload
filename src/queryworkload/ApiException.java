package queryworkload;

/**
 * Something bad happened while interacting with the API.
 */
public class ApiException extends Exception {

	/**
	 * Serial version UID.
	 */
	private static final long serialVersionUID = -6623804289269916109L;

	public ApiException(String message) {
		super(message);
	}

	public ApiException() {
		super();
	}

	public ApiException(String message, Throwable cause) {
		super(message, cause);
	}

	public ApiException(Throwable cause) {
		super(cause);
	}

}
