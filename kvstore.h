#include <vector>
#include <fstream>
#include <string>
#include <algorithm>
#include <map>
#include <iostream>

#include "utils.h"
#include "kvstore_api.h"
#include "fuctions_in_kvstore.h"
#include "MemTable.h"


using namespace std;


class KVStore : public KVStoreAPI {
private:
	MemTable memtable;

	/* The timestamp stored here has yet been used, so, ++ after having been used */
	uint64_t timestamp;

	string dir_path;

	/* Very important! Every time you create a level subfolder, 
	* you need to add a element to abstract_dir */
	vector<vector<sstable_header*>> abstract_dir;
	


public:
	KVStore(const string &dir);

	~KVStore();

	void put(uint64_t key, const string &s) override;

	string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void toSSTable();

	bool need_compaction(unsigned int level);

	void compaction();

	void read_dir();
	
	/* compacte from level A to level B, levelA > levelB */
	void compaction_from_levelA_to_levelB(const unsigned long levelA, const unsigned long levelB);


};