# include <fstream>
# include <vector>
# include <algorithm>
# include <string>
# include <bitset>
# include "MurmurHash3.h"

# include "sstable_header.h"
using namespace std;

/* write a sst file */
void write_sst_file(string filepath,
	uint64_t tamp, uint64_t num, uint64_t min_key, uint64_t max_key,
	string filter,
	vector<uint64_t> keys, vector<uint32_t> offsets, vector<string> strs);

/* read keys in a sst file */
vector<uint64_t> read_sst_keys(string path_to_read,  uint64_t size_of_keys);

/* read strs in a sst file */
vector<string> read_sst_strs(string path_to_read, uint64_t size_of_keys) ;

/* read BloomFilter in a sst file */
string read_sst_filter(string path_to_read, uint64_t size_of_keys);

/* read ssstable-header in a sst file */
sstable_header* read_sst_header(string path_to_read, unsigned int serial_number);

string read_a_str(string path_to_read, uint64_t index_search,uint64_t size_of_keys);