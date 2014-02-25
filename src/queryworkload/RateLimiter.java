package queryworkload;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class RateLimiter<T extends ClientTask> implements Callable<Integer> {
	static Random random = new Random();

	private double opCount;

	private volatile int opsDone;
	private ClientTask task;
	private AtomicBoolean done = new AtomicBoolean(false);
	private AtomicBoolean shouldContinue = new AtomicBoolean(true);
	private int thinkTimeMs;

	/**
	 * 
	 * @param opCount
	 *            Desired number of operations except that opCount == 0 implies
	 *            no end.
	 * @param thinkTimeMs
	 *            Think time in ms
	 * @param task
	 *            The task to call.
	 */
	public RateLimiter(double opCount, int thinkTimeMs, ClientTask task) {
		this.opCount = opCount;
		this.thinkTimeMs = thinkTimeMs;
		this.task = task;
	}

	public Integer call() throws ApiException {
		// soft start over a 10ms period
		try {
			Thread.sleep(random.nextInt(10));
		} catch (InterruptedException e) {
			// do nothing
		}

		while (shouldContinue.get() && ((opCount == 0) || (opsDone < opCount))) {
			// do the work
			task.call();

			opsDone++;

			try {
				Thread.sleep(thinkTimeMs);
			} catch (InterruptedException e) {
				// ignore interruptions
			}

		}

		done.set(true);
		return task.finish();
	}

	/**
	 * Causes at most one more operation to be done.
	 */
	public void cancel() {
		shouldContinue.set(false);
	}

	/**
	 * Returns number of operations already completed.
	 * 
	 * @return The number of operations done.
	 */
	//public int getOpsDone() {
//	
	//	return task.getOpsDone();
	//}

	/**
	 * @return true if no more tasks are to be done.
	 */
	public boolean isFinished() {
		return done.get();
	}
}
