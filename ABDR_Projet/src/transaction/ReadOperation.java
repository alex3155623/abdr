package transaction;

import oracle.kv.Key;

public class ReadOperation implements Operation {

	private Data data;

	public ReadOperation(Data d) {
		data = d;
	}
	
	@Override
	public Data getData() {
		// TODO Auto-generated method stub
		return data;
	}

	@Override
	public String toString() {
		return "ReadOperation [data=" + data + "]";
	}
}
