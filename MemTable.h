#include <string>
#include <vector>
#include <random>
#include <time.h>

#include "fuctions_in_kvstore.h"
#include "file_operation.h"
#include <iostream>
#include "fuctions_in_kvstore.h"
using namespace std;

//#define TEST_2

struct SkipNode {
	uint64_t key;
	string str;
	vector<SkipNode*> next;
	
	SkipNode(uint64_t k, string s, int level): key(k), str(s) {
		for (int i = 0; i < level; i++)
			next.push_back(nullptr);
	}
};



class MemTable {
private:
	SkipNode* head;
	SkipNode* tail;
	int maxLevel;

	int randomLevel();
	void clear();
	SkipNode* find(uint64_t key);
	int nodeLevel(vector<SkipNode*> p);
public:
	MemTable(int maxlevel);
	MemTable();//maxLevel=100

	~MemTable();

	unsigned long size_bits();

	bool put(uint64_t key, const string& s);

	string get(uint64_t key);

	bool del(uint64_t key);

	void reset();

	void printNode();

	vector<uint64_t> tovector_keys();

	vector<uint32_t> tovector_offsets();

	vector<string> tovector_strs();

	sstable_header* toSSTable(uint64_t tamp, string filepath,unsigned int serial_number);

	// #ifdef TEST_2
	// vector<vector<uint64_t>> temp_keys_vectors;
	// vector<vector<uint32_t>> temp_offsets_vectors;
	// vector<string> paths;
	// vector<uint64_t> find_keys_vector(string path); 
	// vector<string> filter_vector;
	// string find_filter_vector(string path); 
	// #endif

};