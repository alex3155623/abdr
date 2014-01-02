package db;

import java.util.ArrayList;
import java.util.List;

public class SleepingToken implements TokenInterface {
	private int id;
	private KVDBInterface kvdb;
	private List<Integer> profiles = new ArrayList<Integer>();
	
	public SleepingToken(int id, KVDBInterface kvdb) {
		this.id = id;
		this.kvdb = kvdb;
	}
	
	@Override
	public int getId() {
		return id;
	}
	
	@Override
	public KVDBInterface getKvdb() {
		return kvdb;
	}
	
	@Override
	public List<Integer> getProfiles() {
		return profiles;
	}
	
	@Override
	public void setProfiles(List<Integer> profiles) {
		this.profiles = profiles;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SleepingToken other = (SleepingToken) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
}
