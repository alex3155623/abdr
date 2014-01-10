package transaction;

import java.io.Serializable;

public class DeleteOperation implements Operation, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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
