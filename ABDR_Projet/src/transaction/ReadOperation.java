package transaction;

import java.io.Serializable;

public class ReadOperation implements Operation, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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
