package transaction;

import java.io.Serializable;

/**
 * Contient le résultat d'une transaction, ainsi que la valeur de retour
 * @author 2600705
 *
 */
public class OperationResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private boolean isSuccess = false;
	private Data data;

	public OperationResult() {}
	
	public OperationResult(boolean isSuccess, Data data) {
		this.isSuccess = isSuccess;
		this.data = data;
	}
	
	
	public boolean isSuccess() {
		return isSuccess;
	}

	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	
	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

}
