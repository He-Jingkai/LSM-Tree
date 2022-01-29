
using namespace std;

struct sstable_header{
	unsigned long timestamp;
	unsigned long min_key;
	unsigned long max_key;
	unsigned long num;
	/* The serial number and the sst file are uniquely one-to-one, 
	*  even if the file is deleted, it will not be reused.
	*  If it is UINT_MAX ,meaning it will be deleted soon 
    *  In level-0,serial_number is equal to its index */
	unsigned int serial_number;

	sstable_header(unsigned long timestampt,unsigned long min_keyt,unsigned long max_keyt,uint64_t numt,unsigned int serial_numbert)
	:timestamp(timestampt),min_key(min_keyt),max_key(max_keyt),num(numt),serial_number(serial_numbert){}

};