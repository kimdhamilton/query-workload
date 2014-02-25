package queryworkload.generator;

import java.util.Random;
import com.yahoo.ycsb.Utils;

/**
 * Generates integers randomly uniform from an interval.
 */
public class UniformLongGenerator extends LongGenerator 
{
	long _lb,_ub;
	int _interval;
	Random random;
	/**
	 * Creates a generator that will return integers uniformly randomly from the interval [lb,ub] inclusive (that is, lb and ub are possible values)
	 *
	 * @param lb the lower bound (inclusive) of generated values
	 * @param ub the upper bound (inclusive) of generated values
	 */
	public UniformLongGenerator(long lb, int interval)
	{
	    random = new Random();
		_lb=lb;
		_interval=interval;
		_ub = _lb+_interval;
	}
	
	@Override
	public long nextLong() 
	{
		long ret=_lb+random.nextInt(_interval);
		setLastLong(ret);
		return ret;
	}

	@Override
	public double mean() {
		return ((double)((long)(_lb + (long)_ub))) / 2.0;
	}
}