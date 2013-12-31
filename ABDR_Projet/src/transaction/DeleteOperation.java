package transaction;

import oracle.kv.Key;

public class DeleteOperation implements Operation {

	private Data data;

	public DeleteOperation(Data d) {
		data = d;
	}

	@Override
	public Data getData() {
		// TODO Auto-generated method stub
		return data;
	}
}
