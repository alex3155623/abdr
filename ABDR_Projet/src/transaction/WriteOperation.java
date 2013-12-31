package transaction;

import oracle.kv.Key;

public class WriteOperation implements Operation {

	private Data data;

	public WriteOperation(Data d) {
		data = d;
	}


	@Override
	public Data getData() {
		return data;
	}
}
