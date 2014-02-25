package queryworkload.generator;

import java.lang.reflect.InvocationTargetException;
import java.util.Vector;

import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.UniformGenerator;

public class GeneratorUtils {

	public static UniformGenerator getUniformGenerator(String s) {
		s = s.trim();
		String[] splits = s.split(";");
		Vector<String> values = new Vector<String>();
		for (String value : splits) {
			value = value.trim();
			values.add(value.equals("null") ? null : value);
		}
		return new UniformGenerator(values);
	}

	public static DiscreteGenerator<String> getDiscreteGenerator(String s) {
		s = s.trim();
		DiscreteGenerator<String> generator = new DiscreteGenerator<String>();
		String[] splits = s.split(";");
		for (String split : splits) {
			split = split.trim();
			String pair[] = split.split(":");
			Double weight = (pair.length == 1) ? 1.0 / splits.length
					: new Double(pair[0].trim());
			String value = (pair.length == 1) ? pair[0] : pair[1];
			value = (value.equals("null") ? null : value.trim());
			generator.addValue(weight, value);
		}
		return generator;
	}

	public static <T extends Number> DiscreteGenerator<T> getDiscreetNumberGenerator(
			String s, Class<T> type) {
		s = s.trim();
		DiscreteGenerator<T> generator = new DiscreteGenerator<T>();
		String[] splits = s.split(";");
		for (String split : splits) {
			split = split.trim();
			String pair[] = split.split(":");
			Double weight = (pair.length == 1) ? 1.0 / splits.length
					: new Double(pair[0].trim());
			String value = (pair.length == 1) ? pair[0] : pair[1];
			value = (value.equals("null") ? null : value.trim());
			T t = null;
			try {
				if (value == null)
					t = null;
				else
					t = type.getConstructor(String.class).newInstance(
							value.trim());

			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			generator.addValue(weight, t);
		}
		return generator;
	}

	public static Generator getGenerator(String s) {

		if (s.indexOf(':') == -1) {
			return getUniformGenerator(s);
		} else
			return getDiscreteGenerator(s);
	}

}
