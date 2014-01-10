package transaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Data implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int id;
	private int category;
	
	private List<Integer> listNumber = new ArrayList<Integer>();
	private List<String> listString = new ArrayList<String>();
	public int sourceId;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getCategory() {
		return category;
	}
	public void setCategory(int category) {
		this.category = category;
	}
	public List<Integer> getListNumber() {
		return listNumber;
	}
	public void setListNumber(List<Integer> listNumber) {
		this.listNumber = listNumber;
	}
	public List<String> getListString() {
		return listString;
	}
	public void setListString(List<String> listString) {
		this.listString = listString;
	}
	
	@Override
	public String toString() {
		return "id = " + id + ", category = " + category + "  " + listNumber.toString() + "  " + listString.toString();
	}
}
