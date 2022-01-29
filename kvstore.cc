#include "kvstore.h"

using namespace std;
//#define DEBUG
// #define DEBUG_A
//#define TEST_2
#define NORMAL
KVStore::KVStore(const string &dir): KVStoreAPI(dir){
	/* LOGIC:
	 * If file directory @dir exists :
	 * read the header of all files under it and store it in @abstract_dir ;
     * If file directory @dir does not exist :
	 * create it and return ;
	 */

	dir_path=dir;

	/******************* If file directory @dir does not exist : *******************/
	/* create it and return */

	if(!utils::dirExists(dir_path)){
		utils::mkdir(dir_path.c_str());
		timestamp=1;
		return;
	}

	/******************* If file directory @dir exists : *******************/
	/* read the header of all files under it and store it in @abstract_dir */

	/* level */
	unsigned int level=0;
	string level_dir_path=dir_path+"/level-"+to_string(level);
	vector<sstable_header*> tmp_dir;
	/* files */
	vector<string> files;
	unsigned long files_count;
	string file_path;
	/* sstable_header */
	sstable_header* header_tmp;
	/* serial_number */
	string tmp_str;
	unsigned int sst_start;
	unsigned int sst_end;
	unsigned int serial_number=0;
	/* timestamp */
	timestamp=1;

	/* read level */
	while(utils::dirExists(level_dir_path)){
		abstract_dir.push_back(tmp_dir);
		/* read files */
		utils::scanDir(level_dir_path, files);
		/* read sstable_headers */
		files_count=files.size();
		for(int i=0;i<(int)files_count;i++){
			file_path=files[i];
			/* read serial_number */
			//tmp_str="level-"+to_string(level)+"/";
			//sst_start=file_path.find(tmp_str)+tmp_str.size();
			sst_end=sst_start=0;
			while(file_path[sst_end]>='0'&&file_path[sst_end]<='9')
				sst_end++;
			serial_number=atoi(file_path.substr(sst_start,sst_end-sst_start).c_str());
			header_tmp=read_sst_header(level_dir_path+"/"+file_path,serial_number);
			/* find timestamp */
			if(header_tmp->timestamp>timestamp)
				timestamp=header_tmp->timestamp;

			abstract_dir[level].push_back(header_tmp);
		}
		level++;
		level_dir_path=dir_path+"/level-"+to_string(level);
		files.clear();
	}
	timestamp++;
}



KVStore::~KVStore(){
	/* LOGIC:
	 * Convert memtable to sst 
	 */
	toSSTable();
	int dir_size=abstract_dir.size();
	int files_size;
	for(int i=0;i<dir_size;i++){
		files_size=abstract_dir[i].size();
		for(int j=0;j<files_size;j++)
			delete abstract_dir[i][j];
	}
}



/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
string KVStore::get(uint64_t key){

	/* LOGIC:
	* First: check the Memtable
	* Second: find the file that may contain the key 
	* Third: find and return the value
	*/
	
	string ret;

	/******************** check the Memtable ********************/
	ret=memtable.get(key);
	if(ret=="~DELETED~")
		return "";
	else if(ret!="")return ret;
	

	/******************** find the file that may contain the key ********************/ 
	
	/*There are at most three files containing key, among which there are at most two at level0 */
	int dir_deepth=(int)abstract_dir.size();

	int dir_size;
	vector<int> suspect_level;
	vector<int> suspect_index;
	vector<uint64_t> timestamps;
	
	for(int level=0;level<dir_deepth;level++){

		dir_size=(int)abstract_dir[level].size();
		for(int index=dir_size-1;index>=0;index--){
			if(abstract_dir[level][index]->min_key<=key && abstract_dir[level][index]->max_key>=key){
				suspect_level.push_back(level);
				suspect_index.push_back(index);
				timestamps.push_back(abstract_dir[level][index]->timestamp);
			}
		}
	}
	
	
	if(suspect_level.size()==0)return"";

	/******************** find and return the value ********************/
	string path_to_read;
	uint64_t size_of_keys;
	vector<uint64_t> keys;
	vector<string> strs;
	string BloomFilter;
	unsigned int serial_number_to_read;
	string maybe_ret;
	int sus_size=(int)suspect_index.size();//min(1,(int)suspect_index.size());
	int i;
	for (int j=0;j<sus_size;j++) {
		// auto max_time_ptr=max_element(timestamps.begin(),timestamps.end());
		// i=max_time_ptr-timestamps.begin();
		// timestamps[i]=0;
		i=j;
		
		serial_number_to_read=abstract_dir[suspect_level[i]][suspect_index[i]]->serial_number;
		path_to_read=dir_path+"/level-"+to_string(suspect_level[i])+"/"+to_string(serial_number_to_read)+".txt";
		size_of_keys=abstract_dir[suspect_level[i]][suspect_index[i]]->num;

		//#ifdef NORMAL
		BloomFilter=read_sst_filter(path_to_read,size_of_keys);
		//#endif

		#ifdef TEST_2
		BloomFilter=memtable.find_filter_vector(path_to_read);
		#endif

		if(!checkBloomFilter(BloomFilter,key))continue;
		
		//#ifdef NORMAL
		keys=read_sst_keys(path_to_read,size_of_keys);
		//#endif

		#ifdef TEST_2
		keys=memtable.find_keys_vector(path_to_read);
		#endif

		/* binary search */
		unsigned int index_search;
		auto target = lower_bound(keys.begin(), keys.end(), key);

		if(target != keys.end() && *target == key){
			index_search=target - keys.begin();
			maybe_ret=read_a_str(path_to_read,index_search,size_of_keys);
			if(maybe_ret=="~DELETED~")
				return "";
			else
				return maybe_ret;
		}
		
		keys.clear();
		maybe_ret.clear();
		
	}

	return "";

}



/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const string &s){

	/* For testing Memtable only */ 
	//memtable.put(key,s);
	/* ************************** */


	memtable.put(key,s);
	unsigned long size_tobe=memtable.size_bits();
	if(size_tobe<=2097152){//2MB
		return;
	}
	memtable.del(key);

	toSSTable();
	
	memtable.put(key,s);

	

}



void KVStore::toSSTable(){

	if(memtable.size_bits()==10240+32)
		return;
	/* create new file */ 
	/* check does level-0 directory exist */
	string level0_path=dir_path+"/level-0";
	if(!utils::dirExists(level0_path)){
		/* DEBUG */
		#ifdef DEBUG
		cout<<"TRY TO MAKE DIR: "+level0_path<<endl;
		#endif
		utils::mkdir(level0_path.c_str());

		/* DEBUG */
		#ifdef DEBUG
		cout<<"MAKE DIR DONE"<<endl;
		cout<<endl;
		#endif

		vector<sstable_header*> tmp;
		abstract_dir.push_back(tmp);
	}

	/* add sstable */
	unsigned int serial_number=abstract_dir[0].size();
	string filepath=level0_path+"/"+to_string(serial_number)+".txt";//important!!

	sstable_header* newheader=memtable.toSSTable(timestamp,filepath,serial_number);
	/* Update abstract_dir and timestamp */
	abstract_dir[0].push_back(newheader);
		timestamp++;

	/* Compacte if needed */

	if(need_compaction(0))
		compaction();

}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key){

	bool ret=(get(key)!="");
	memtable.put(key,"~DELETED~");
	return ret;
}


/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset(){
	/* LOGIC:
	 * 1.tmestamp=1;
	 * 2.clear memtable;
	 * 3.clear abstract_dir
     * 4.clear files;
	 */

	/* 1 */
	timestamp=1;

	/* 2 */
	memtable.reset();

	/* 3 */
	abstract_dir.clear();

	/* 4.clear file */
	unsigned int level=0;
	string level_dir_path=dir_path+"/level-"+to_string(level);
	vector<string> files;

	while(utils::dirExists(level_dir_path)){
		/* read files */
		utils::scanDir(level_dir_path, files);
		/* delete files */
		for (auto item : files){
			string file_path=level_dir_path+"/"+item;
			utils::rmfile(file_path.c_str());
			#ifdef DEBUG_A
			cout<<"remove file "+file_path<<endl;
			#endif
		}
		/* delete empty dir */
		utils::rmdir(level_dir_path.c_str());
		#ifdef DEBUG_A
		cout<<"remove dir "+level_dir_path<<endl;
		#endif

		level++;
		level_dir_path=dir_path+"/level-"+to_string(level);
		files.clear();
	}

}



bool KVStore::need_compaction(unsigned int level){
		if((int)abstract_dir[level].size()<=(1<<(level+1)))return false;
		return true;
}



void KVStore::compaction(){
	
	/* LOGIC:
	* From levelX=0
	* If levelX need compaction, do compaction_from_levelA_to_levelB
	* If next level's directory doesn't exist, creat it.
	*/
	unsigned int levelX = 0;
	string new_dir_path;
	while(need_compaction(levelX)){
		levelX++;

		/* Creat new directory of next level */
		if(abstract_dir.size()<levelX+1){
			vector<sstable_header*> tmp;
			abstract_dir.push_back(tmp);
			new_dir_path=dir_path+"/level-"+to_string(levelX);
			if(!utils::dirExists(new_dir_path)){

				/* DEBUG */
				#ifdef DEBUG
				cout<<"TRY TO MAKE DIR: "+new_dir_path<<endl;
				#endif
				utils::mkdir(new_dir_path.c_str());

				/* DEBUG */
				#ifdef DEBUG
				cout<<"MAKE DIR DONE"<<endl;
				cout<<endl;
				#endif
			}
				
		}

		compaction_from_levelA_to_levelB(levelX-1,levelX);
	}

}



void KVStore::compaction_from_levelA_to_levelB(const unsigned long levelA, const unsigned long levelB){
	#ifdef DEBUG
	cout<<endl;
	cout<<"compaction_from_level "+to_string(levelA)+" to_level "+to_string(levelB)<<endl;
	#endif
	/* *************************************************************************** */
	/* LOGIC:
	* The first step:find the file to be deleted in levelA, and delete the headers in abstract_dir
	* The second step:find the file to be deleted in levelB, and delete the headers in abstract_dir
	* The third step:Find the addresses where the new files are stored in level B	
	* The fourth step: Sort and save to files, update cache 
	 */
	/* *************************************************************************** */
	
	/******************** the file to be read in level A ********************/

	/* Store the 
	* @files_tobe_read_path_levelA:     path
	* @files_tobe_read_timetamp_levelA: timestamp
	* @sizes_levelA:                    number of keys
	* @min_key,max_key:                 min key and max key
	* of the file to be compacted in levelA */
	vector<string> files_tobe_read_path_levelA;
	vector<uint64_t> files_tobe_read_timetamp_levelA;
	vector<uint64_t> sizes_levelA;
	uint64_t min_key_A=UINT64_MAX;
	uint64_t max_key_A=0;
	string tmp_path;

	/* Used in behind too */
	sstable_header* to_deleted_header_tmp0;
	sstable_header* to_deleted_header_tmp1;
	sstable_header* to_deleted_header_tmp2;
	sstable_header* to_deleted_header_tmp;

	/* LOGIC:
	 * If levelA=0,all the files( 3 ) should be compacted;
	 * Else, preferentially select several files with the smallest timestamp 
	 * (for the files with the same smallest timestamp, select the file with the smallest key), 
	 * so that the number of files meets the requirements 
	 * The headers of files to be compacted will be deleted from abstract_dir
	 */
	
	if(levelA==0){
		for(sstable_header* levelA_header : abstract_dir[0]){
			tmp_path=dir_path+"/level-0/"+to_string(levelA_header->serial_number)+".txt";
			files_tobe_read_path_levelA.push_back(tmp_path);
			files_tobe_read_timetamp_levelA.push_back(levelA_header->timestamp);
			sizes_levelA.push_back(levelA_header->num);
			#ifdef DEBUG
			cout<<"TO DELETE LEVEL0 "<<tmp_path<<endl;
			#endif
		}
		min_key_A=min(min(abstract_dir[0][0]->min_key,abstract_dir[0][1]->min_key),abstract_dir[0][2]->min_key);
		max_key_A=max(max(abstract_dir[0][0]->max_key,abstract_dir[0][1]->max_key),abstract_dir[0][2]->max_key);

		to_deleted_header_tmp0=abstract_dir[0][0];
		to_deleted_header_tmp1=abstract_dir[0][1];
		to_deleted_header_tmp2=abstract_dir[0][2];
		delete to_deleted_header_tmp0;
		delete to_deleted_header_tmp1;
		delete to_deleted_header_tmp2;

		abstract_dir[0].clear();
	}
	else{
		unsigned int size_current=abstract_dir[levelA].size();
		unsigned int number_of_files_tobe_deleted=size_current - (1<<(levelA+1));
     
		for(unsigned int i=0; i < number_of_files_tobe_deleted; i++){

			size_current=abstract_dir[levelA].size();

			/* The first traversal: find the smallest timestamp */
			uint64_t smallest_timestamp=UINT64_MAX;
			for(sstable_header* levelA_header : abstract_dir[levelA]){
				if(levelA_header->timestamp<smallest_timestamp)
					smallest_timestamp=levelA_header->timestamp;
			}

			/* Second traversal: find the file with the smallest timestamp and the smallest key */
			/* 此处有待优化 */
			uint64_t smallest_minkey=UINT64_MAX;
			unsigned int index;
			for(unsigned int j=0; j<size_current; j++){
				sstable_header* levelA_header=abstract_dir[levelA][j];
				if(levelA_header->timestamp==smallest_timestamp)
					if(levelA_header->min_key<smallest_minkey){
						smallest_minkey=levelA_header->min_key;
						index=j;
					}		
			}

			/* Now we have found the file 
			*  Delete its header after recording its information 
			*/
			tmp_path=dir_path+"/level-"+to_string(levelA)+"/"+to_string(abstract_dir[levelA][index]->serial_number)+".txt";
			files_tobe_read_path_levelA.push_back(tmp_path);
			files_tobe_read_timetamp_levelA.push_back(abstract_dir[levelA][index]->timestamp);
			sizes_levelA.push_back(abstract_dir[levelA][index]->num);
			if(abstract_dir[levelA][index]->min_key<min_key_A)
				min_key_A=abstract_dir[levelA][index]->min_key;
			if(abstract_dir[levelA][index]->max_key>max_key_A)
				max_key_A=abstract_dir[levelA][index]->max_key;
			/* Delete the header of the file */
			to_deleted_header_tmp=abstract_dir[levelA][index];

			abstract_dir[levelA].erase(abstract_dir[levelA].begin()+index);
			delete to_deleted_header_tmp;

			#ifdef DEBUG
			cout<<"Delete header of "+tmp_path<<endl;
			#endif
		}

	}





	/******************* the file to be read in level B ********************/

	/* Store the 
	* @files_tobe_read_path_levelB:     path
	* @files_tobe_read_timetamp_levelB: timestamp
	* @sizes_levelB:                    number of keys
	* @serial_tobe_deleted_in_levelB:   the serial numbers of files to be deleted in level B
	* of the file to be compacted in levelB */
	vector<string> files_tobe_read_path_levelB;
	vector<uint64_t> files_tobe_read_timetamp_levelB;
	vector<uint64_t> sizes_levelB;
	vector<unsigned int> serial_tobe_deleted_in_levelB;

	/* LOGIC:
	 * Found in Level B All SSTable files that
	 * have an intersection with the interval [min_key_A, max_key_A] 
	 */
	// cout<<endl;
	// cout<<"here!"<<endl;
	for(sstable_header* levelB_header : abstract_dir[levelB]){
		// cout<<levelB_header->serial_number<<endl;
		if(!(levelB_header->min_key>max_key_A||levelB_header->max_key<min_key_A)){
			tmp_path=dir_path+"/level-"+to_string(levelB)+"/"+to_string(levelB_header->serial_number)+".txt";
			files_tobe_read_path_levelB.push_back(tmp_path);
			files_tobe_read_timetamp_levelB.push_back(levelB_header->timestamp);
			sizes_levelB.push_back(levelB_header->num);
			serial_tobe_deleted_in_levelB.push_back(levelB_header->serial_number);
		}
	}

	/* Now, delete the headers */
	unsigned int serial_tobe_deleted_in_levelB_size=serial_tobe_deleted_in_levelB.size();
	for(unsigned int k=0;k<serial_tobe_deleted_in_levelB_size;k++){
	
		unsigned int index=0;
		/* Find the index of the serial number */
		for(index=0;index<abstract_dir[levelB].size();index++){
			if(abstract_dir[levelB][index]->serial_number==serial_tobe_deleted_in_levelB[k])
				break;
		}
		
		/* Delete */
		to_deleted_header_tmp= abstract_dir[levelB][index];

		abstract_dir[levelB].erase(abstract_dir[levelB].begin()+index);

		delete to_deleted_header_tmp;

		#ifdef DEBUG
		cout<<"Delete header of "+tmp_path<<endl;
		#endif
	}





	/******************** Find the addresses where the new files are stored in level B *******************/

	unsigned int number_of_files_tobe_stored=files_tobe_read_path_levelA.size()+files_tobe_read_path_levelB.size();

	#ifdef DEBUG
	cout<<"LEVELA "<<files_tobe_read_path_levelA.size()<<"  LEVELB "<<files_tobe_read_path_levelB.size()<<endl;
	#endif

	vector<string> address_of_files_stored_in_levelB;

	unsigned int levelB_size=abstract_dir[levelB].size();

	unsigned int levelB_serial_number_max;
	if(!levelB_size)
		levelB_serial_number_max=-1;
	else
		levelB_serial_number_max=abstract_dir[levelB][levelB_size-1]->serial_number;


	unsigned int serial_number_tobe_used;
	
	vector<unsigned int> serial_of_files_stored_in_levelB;
	

	for(unsigned int m=0;m<number_of_files_tobe_stored;m++){
		serial_number_tobe_used=levelB_serial_number_max + m + 1;
		tmp_path=dir_path+"/level-"+to_string(levelB)+"/"+to_string(serial_number_tobe_used)+".txt";
		address_of_files_stored_in_levelB.push_back(tmp_path);
		serial_of_files_stored_in_levelB.push_back(serial_number_tobe_used);

		#ifdef DEBUG
		cout<< "To Use: "+tmp_path<<endl;
		#endif
	}





	/********************* Organize the data that has been obtained *******************/

	/* 
	* @address_of_files_stored_in_levelB:
	* @number_of_files_tobe_stored:
	* @files_tobe_read_path:
	* @files_tobe_read_timetamp:
	* @sizes: number of keys
	*/

	vector<string> files_tobe_read_path;
	files_tobe_read_path.insert(files_tobe_read_path.end(),files_tobe_read_path_levelA.begin(),files_tobe_read_path_levelA.end());
	files_tobe_read_path.insert(files_tobe_read_path.end(),files_tobe_read_path_levelB.begin(),files_tobe_read_path_levelB.end());

	vector<uint64_t> files_tobe_read_timetamp;
	files_tobe_read_timetamp.insert(files_tobe_read_timetamp.end(),files_tobe_read_timetamp_levelA.begin(),files_tobe_read_timetamp_levelA.end());
	files_tobe_read_timetamp.insert(files_tobe_read_timetamp.end(),files_tobe_read_timetamp_levelB.begin(),files_tobe_read_timetamp_levelB.end());

	vector<uint64_t> sizes;
	sizes.insert(sizes.end(),sizes_levelA.begin(),sizes_levelA.end());
	sizes.insert(sizes.end(),sizes_levelB.begin(),sizes_levelB.end());

	/* Except for these variables, all the above variables are no longer used below */




	/******************* Sort and save to files, update cache ********************/
	/*
	* @tmp_kvmap: store all the k-v in the files to be read
	* @timestamp_to_use_later
	* @path_to_read, @size_of_keys, keys, strs:Temporary use in loop
	*/

	map<uint64_t,string> tmp_kvmap;

	string path_to_read;
	int size_of_keys;
	vector<uint64_t> keys;
	vector<string> strs;

	uint64_t timestamp_to_use_later=max_timestamp(files_tobe_read_timetamp);
	/* Read files in a loop */
	unsigned int files_tobe_read_path_size=files_tobe_read_path.size();
	for(unsigned int i=0;i<files_tobe_read_path_size;i++){
		int index=min_timestamp_index(files_tobe_read_timetamp);
		path_to_read=files_tobe_read_path[index];
		size_of_keys=sizes[index];

		/* read the file */
		keys=read_sst_keys(path_to_read,size_of_keys);
		strs=read_sst_strs(path_to_read,size_of_keys);
		
		utils::rmfile(path_to_read.c_str());
		#ifdef DEBUG
		cout<<"DELET FILE "+path_to_read<<endl;
		#endif

		/* put the k-v's to tmp_kvmap */
		unsigned int keys_size=keys.size();
		for(unsigned int j=0;j<keys_size;j++)
			tmp_kvmap[keys[j]]=strs[j];
		
		keys.clear();
		strs.clear();
	}
	

	//MemTable temp_memtable;
	unsigned long size_tobe;
	sstable_header* new_header=NULL;

	

	unsigned int path_index=0;
	for (auto it : tmp_kvmap) {
		memtable.put(it.first,it.second);/////
		size_tobe=memtable.size_bits();///
		

		/* Write file */
		if(size_tobe>2097152){//2MB
			memtable.del(it.first);/////

			new_header=memtable.toSSTable(timestamp_to_use_later,address_of_files_stored_in_levelB[path_index],serial_of_files_stored_in_levelB[path_index]);/////
			path_index++;

			abstract_dir[levelB].push_back(new_header);
			memtable.put(it.first,it.second);/////

			#ifdef DEBUG
			cout<<"CREAT FILE: "+address_of_files_stored_in_levelB[path_index-1]<<endl;
			#endif

		}
		
	}
			new_header=memtable.toSSTable(timestamp_to_use_later,address_of_files_stored_in_levelB[path_index],serial_of_files_stored_in_levelB[path_index]);/////
			
			abstract_dir[levelB].push_back(new_header);

			#ifdef DEBUG
			cout<<"CREAT FILE: "+address_of_files_stored_in_levelB[path_index]<<endl;
			#endif
	
	/* HaoYe! Finally finished */
}

