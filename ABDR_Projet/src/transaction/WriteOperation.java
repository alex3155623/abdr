package transaction;

import java.io.Serializable;

public class WriteOperation implements Operation, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Data data;

	public WriteOperation(Data d) {
		data = d;
	}


	@Override
	public Data getData() {
		return data;
	}


	@Override
	public String toString() {
		return "WriteOperation [data=" + data + "]";
	}
}
