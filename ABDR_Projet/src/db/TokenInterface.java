package db;

import java.util.ArrayList;
import java.util.List;

public interface TokenInterface {
	int getId();

	KVDBInterface getKvdb();

	void setProfiles(List<Integer> profiles);

	List<Integer> getProfiles();
	
}
