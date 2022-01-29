#include "fuctions_in_kvstore.h"

using namespace std;

string buildBloomFilter(vector<uint64_t> keys){
	string filter(10240*8,'0');
	uint32_t hash[4] = {0};
	uint64_t size=keys.size();
	for(uint64_t i = 0; i < size; i++){
		MurmurHash3_x64_128(&keys[i], sizeof(keys[i]), 1, hash);
		for(int k=0;k<4;k++)
			if(hash[k]<10240*8&&hash[k]>=0)
				filter[hash[k]]='1';
	}
	return filter;
}


bool checkBloomFilter(string BloomFilter,uint64_t key){
	uint32_t hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);

	for(int k=0;k<4;k++){
		if(hash[k]<10240*8&&hash[k]>=0){
			if(BloomFilter[hash[k]]=='0')
			    return false;
		}
	}

	return true;
	
}

int min_timestamp_index(vector<uint64_t> &files_tobe_read_timestamp){
	vector<uint64_t>::iterator smallest = min_element(begin(files_tobe_read_timestamp), end(files_tobe_read_timestamp));
    int index=distance(begin(files_tobe_read_timestamp), smallest);
	files_tobe_read_timestamp[index]=UINT64_MAX;
	return index;
}

uint64_t max_timestamp(vector<uint64_t> files_tobe_read_timestamp){
	vector<uint64_t>::iterator largest = max_element(begin(files_tobe_read_timestamp), end(files_tobe_read_timestamp));
    int index=distance(begin(files_tobe_read_timestamp), largest);
	return files_tobe_read_timestamp[index];
}

