#include "MemTable.h"

using namespace std;

//#define TEST_2

MemTable::MemTable() {
	maxLevel=100;
	head = new SkipNode(INT64_MIN, "~DELETED~", maxLevel);
	tail = new SkipNode(INT64_MAX, "~DELETED~", maxLevel);
	for (int i = 0; i < maxLevel; i++)
		head->next[i] = tail;
}

MemTable::MemTable(int maxlevel):maxLevel(maxlevel) {
	head = new SkipNode(INT64_MIN, "~DELETED~", maxLevel);
	tail = new SkipNode(INT64_MAX, "~DELETED~", maxLevel);
	for (int i = 0; i < maxLevel; i++)
		head->next[i] = tail;
}

MemTable::~MemTable() {
	clear();
}

void MemTable::clear(){
	SkipNode* tmp = head;
	SkipNode* tmp1 = head;
	while (tmp) {
		tmp1 = tmp->next[0];
		delete tmp;
		tmp = tmp1;
	}
}


/*This function is used to generate the height of the node */
int MemTable::randomLevel() {
	int random_level = 1;
	int seed = time(NULL);
	static default_random_engine e(seed);
	static uniform_int_distribution<int> u(0, 1);
	while (u(e) && random_level < maxLevel)
		random_level++;
	return random_level;
}

/*Returns the maximum height of head */
int MemTable::nodeLevel(vector<SkipNode*> next) {
	int node_level = 0;
	if (next[0]->key == INT64_MAX)
		return node_level;
	for (unsigned int i = 0; i < next.size(); i++) {
		if (next[i] != nullptr && next[i]->key != INT64_MAX)
			node_level++;
		else
			break;
	}
	return node_level;
}
/* This function is used to count the size of the generated SSTable */
unsigned long MemTable::size_bits() {
	unsigned long bits = 10240+32;
	SkipNode* tmp = head->next[0];
	int str_size = 0;
	while(tmp->key!=INT64_MAX) {
		str_size = tmp->str.size() + 1 ;
		bits += 8;
		bits += str_size;
		bits += 4;
		tmp = tmp->next[0];
	}
	return bits;
}

/* Returns the pointer to the node to find (if it exists) 
* or a pointer to the location where it should exist (if it does not exist)*/ 
SkipNode* MemTable::find(uint64_t key){
	SkipNode* tmp = head;
	int current_level = nodeLevel(tmp->next);
	for (int i = (current_level - 1); i > -1; i--)
		while (tmp->next[i] != nullptr && tmp->next[i]->key < key)
			tmp = tmp->next[i];
	tmp = tmp->next[0];
	if (tmp->key == key)
		return tmp;
	return nullptr;
}

/* Insert a node ,if the key has already exists, change its value*/
bool MemTable::put(uint64_t key, const string& s) {
	int x_level = randomLevel();
	SkipNode* new_node = nullptr;
	SkipNode* tmp = head;
	new_node = find(key);

	if (new_node) {
		new_node->str = s;
		return true;
	}
	new_node = new SkipNode(key, s, x_level);
	
	for (int i = (x_level - 1); i > -1; i--) {
		while (tmp->next[i] != nullptr && tmp->next[i]->key < key)
			tmp = tmp->next[i];
		new_node->next[i] = tmp->next[i];
		tmp->next[i] = new_node;
	}
	return true;
}

/* get the value of a key*/
string MemTable::get(uint64_t key) {
	SkipNode* node = find(key);
	if (node)return node->str;
	return "";
}

/* delete a key*/
bool MemTable::del(uint64_t key) {
	SkipNode* node = find(key);
	if (!node)
		return false;
	else {
		SkipNode* tmp = head;
		int x_level = node->next.size();
		for (int i = (x_level - 1); i > -1; i--) {
			while (tmp->next[i] != nullptr && tmp->next[i]->key < key)
				tmp = tmp->next[i];

			tmp->next[i] = tmp->next[i]->next[i];
		}
		return true;
	}
}

void MemTable::reset() {
	clear();
	head = new SkipNode(INT64_MIN, "~DELETED~", maxLevel);
	tail = new SkipNode(INT64_MAX, "~DELETED~", maxLevel);
	for (int i = 0; i < maxLevel; i++)
		head->next[i] = tail;
}

vector<uint64_t> MemTable::tovector_keys(){
	
	vector<uint64_t> keys;
	SkipNode* node=head->next[0];
	while(node->key!=INT64_MAX){
		keys.push_back(node->key);

		node=node->next[0];
	}
	return keys;
}
vector<string> MemTable::tovector_strs(){

	vector<string> strs;	
	SkipNode* node=head->next[0];
	while(node->key!=INT64_MAX){
		strs.push_back(node->str);
	
		node=node->next[0];
	}
	return strs;
}

vector<uint32_t> MemTable::tovector_offsets(){

	uint32_t bits = 10240+32;
	SkipNode* tmp = head->next[0];
	while(tmp->key!=INT64_MAX) {
		bits += (8+4);
		tmp = tmp->next[0];
	}
	vector<uint32_t> offsets;
	SkipNode* node=head->next[0];
	offsets.push_back(bits);
	uint32_t size=0;
	while(node->key!=INT64_MAX){
		
		if(node->next[0]->key!=INT64_MAX){
			size=node->str.size() +1;
			bits+=size;
			offsets.push_back(bits);
		}
			

		node=node->next[0];
	}
	return offsets;
}
sstable_header* MemTable::toSSTable(uint64_t tamp, string filepath,unsigned int serial_number){

	/* Prepare the required data for SSTable */
	vector<uint64_t> keys=tovector_keys();
	vector<uint32_t> offsets=tovector_offsets();
	vector<string> strs=tovector_strs();
	
	// #ifdef TEST_2
	// temp_keys_vectors.push_back(keys);
	// temp_offsets_vectors.push_back(offsets);
	// paths.push_back(filepath);
	// filter_vector.push_back(buildBloomFilter(keys));
	// #endif

	uint64_t min_key = *min_element(keys.begin(), keys.end());
	uint64_t max_key = *max_element(keys.begin(), keys.end());
	uint64_t num=keys.size();
	string filter=buildBloomFilter(keys);
	
	/* Write file */
	write_sst_file(filepath,tamp,num,min_key,max_key,filter,keys,offsets,strs);

	/* refresh cache */
	sstable_header* newheader=new sstable_header(tamp,min_key,max_key,num,serial_number);
	

	/* Update the timestamp and reset the memtable */
	reset();

	return newheader;
}

// #ifdef TEST_2
// vector<uint64_t> MemTable::find_keys_vector(string path){
// 	// cout<<path<<endl;
// 	int i=paths.size(),index=0;
// 	for(;i>=0;i--){
// 		// cout<<paths[i]<<endl;
// 		if(paths[i]==path){index=i; break;}
// 	}
// 	// cout<<index<<" "<<temp_keys_vectors.size()<<endl;
// 	return temp_keys_vectors[index];
// }
// string MemTable::find_filter_vector(string path){
// 	// cout<<path<<endl;
// 	int i=paths.size(),index=0;
// 	for(;i>=0;i--){
// 		// cout<<paths[i]<<endl;
// 		if(paths[i]==path){index=i; break;}
// 	}
// 	// cout<<index<<" "<<temp_keys_vectors.size()<<endl;
// 	return filter_vector[index];
// }
// #endif
