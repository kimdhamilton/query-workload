package queryworkload.generator;

import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.IntegerGenerator;

public abstract class MappedIntegerGenerator extends Generator {
	
	private IntegerGenerator intGenerator;
	private String lastMappedInt;
	
	public MappedIntegerGenerator(IntegerGenerator intGenerator) {
		this.intGenerator = intGenerator;
	}

	public void setLastMappedInt(String last) {
		lastMappedInt=last;
	}
	
	public abstract String map(int nextInt);

	@Override
	public String nextString() {
		return map(intGenerator.nextInt());
	}
	
	@Override
	public String lastString() {
		return lastMappedInt;
	}

}
