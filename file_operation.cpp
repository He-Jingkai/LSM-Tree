#include "file_operation.h"

using namespace std;


void write_sst_file(string filepath,
	uint64_t tamp, uint64_t num, uint64_t min_key, uint64_t max_key,
	string filter,
	vector<uint64_t> keys, vector<uint32_t> offsets, vector<string> strs) {
	ofstream outfile(filepath, ios::out | ios::binary);
	if (!outfile){
		exit(0);
	}
	outfile.write((char*)&tamp, 8);
	outfile.write((char*)&num, 8);
	outfile.write((char*)&min_key, 8);
	outfile.write((char*)&max_key, 8);

	string tmp;
	uint32_t subfilter;
	for (int k = 0; k < 10240*8; k += 32) {
		tmp = filter.substr(k, 32);
		subfilter = stoi(tmp, nullptr,2);
		outfile.write((char*)&subfilter, 4);
	}

	for (uint64_t i = 0; i < num; i++) {
		
		outfile.write((char*)&keys[i], 8);
		outfile.write((char*)&offsets[i], 4);
	}

	for (uint64_t i = 0; i < num; i++)
		outfile.write(strs[i].c_str(), (strs[i].length() + 1));

	outfile.close();
}


/* Read the sst file and store the keys and strings into the vectors;
 * @path_to_read: the path of the sst file;
 * @size_of_keys:the number of the keys */
vector<uint64_t> read_sst_keys(string path_to_read,  uint64_t size_of_keys) {

	vector<uint64_t> keys(size_of_keys);
	uint64_t key=0;
	
	ifstream ifs(path_to_read, ios::in | ios::binary);

	ifs.seekg(32+10240, ifs.beg);

	for (int i = 0; i < (int)size_of_keys; i++) {
		/* read a key */
		ifs.read((char*)&key, sizeof(uint64_t));
		keys[i]=key;
		ifs.seekg(4, ifs.cur);
	}
	ifs.close();
	return keys;
}


vector<string> read_sst_strs(string path_to_read, uint64_t size_of_keys) {

	vector<string> strs(size_of_keys);
	char* strt;
	string str;
	uint32_t offset_left = 0;
	uint32_t offset_right = 0;
	uint32_t prev_offset = 0;
	uint32_t file_length;

	ifstream ifs(path_to_read, ios::in | ios::binary);
	
	/* caculate the length of the file */
	ifs.seekg(0, ifs.end);
	file_length = ifs.tellg();

	ifs.seekg(32+10240, ifs.beg);

	for (uint64_t i = 0; i < size_of_keys; i++) {

		ifs.seekg(8, ifs.cur);

		/* read the offset of the key */
		ifs.read((char*)&offset_left, sizeof(uint32_t));

		/* read the offset to return to */
		prev_offset = ifs.tellg();

		/* read the offset of the next key */
		if (prev_offset == (32 + 10240 + 12 * size_of_keys)) {
			offset_right = (uint32_t)file_length;
		}
		else {
			ifs.seekg(8, ifs.cur);
			ifs.read((char*)&offset_right, sizeof(uint32_t));
		}

		/* read the string of the key */
		ifs.seekg(offset_left, ifs.beg);
		
		strt = new char[offset_right - offset_left];//char* strt 

		ifs.read(strt, (offset_right - offset_left));

		strs[i]=strt;
		delete[] strt;

		/* return to previous offset */
		ifs.seekg(prev_offset, ifs.beg);
	}

	ifs.close();
	return strs;
}

string read_sst_filter(string path_to_read, uint64_t size_of_keys) {

	ifstream ifs(path_to_read, ios::in | ios::binary);
	uint32_t filter_part=0;
	string filter;

	/* read BloomFilter */
	ifs.seekg(32, ifs.beg);
	for (unsigned int i = 0; i < 10240 * 8; i += 32) {

		ifs.read((char*)&filter_part, sizeof(uint32_t));
		bitset<32> t(filter_part);
		filter += t.to_string();
		
	}

	ifs.close();
	return filter;
}

string read_a_str(string path_to_read, uint64_t index_search,uint64_t size_of_keys) {

	char* strt;
	uint32_t offset_left = 0;
	uint32_t offset_right = 0;
	
	uint32_t file_length;

	ifstream ifs(path_to_read, ios::in | ios::binary);
	
	/* caculate the length of the file */
	ifs.seekg(0, ifs.end);
	file_length = ifs.tellg();

	ifs.seekg(32+10240+12*index_search+8, ifs.beg);

	/* read the offset of the key */
	ifs.read((char*)&offset_left, sizeof(uint32_t));

	/* read the offset of the next key */
	if (index_search==size_of_keys-1)
		offset_right = (uint32_t)file_length;
	else {
		ifs.seekg(8, ifs.cur);
		ifs.read((char*)&offset_right, sizeof(uint32_t));
	}

	/* read the string of the key */
	ifs.seekg(offset_left, ifs.beg);
	
	strt = new char[(int)offset_right - (int)offset_left];

	ifs.read(strt, (offset_right - offset_left));
	
	string ret_str(strt);	
	delete[] strt;
	ifs.close();

	return ret_str;
}

/* read ssstable-header in a sst file */
sstable_header* read_sst_header(string path_to_read, unsigned int serial_number) {

	uint64_t timestamp;
	uint64_t min_key;
	uint64_t max_key;
	
	uint64_t num;
	ifstream ifs(path_to_read, ios::in | ios::binary);
	ifs.seekg(0,ifs.beg);
	ifs.read((char*)&timestamp, sizeof(uint64_t));
	ifs.read((char*)&num, sizeof(uint64_t));
	ifs.read((char*)&min_key, sizeof(uint64_t));
	ifs.read((char*)&max_key, sizeof(uint64_t));
	
	
	sstable_header* ret=new sstable_header(timestamp, min_key, max_key, num, serial_number);

	ifs.close();
	return ret;
}



