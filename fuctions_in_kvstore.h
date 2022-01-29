# include <fstream>
# include <vector>
# include <algorithm>
# include <string>
# include "MurmurHash3.h"

using namespace std;

string buildBloomFilter(vector<uint64_t> keys);

bool checkBloomFilter(string BloomFilter,uint64_t key);

int min_timestamp_index(vector<uint64_t> &files_tobe_read_timestamp);

uint64_t max_timestamp(vector<uint64_t> files_tobe_read_timestamp);