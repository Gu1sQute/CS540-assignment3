#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
using namespace std;
#define hashnum 216
#define IndexPagePtr 0
#define hash(id) id % hashnum
#define NumEntriesPos (sizeof(int) + sizeof(int))
#define FreeSpacePos sizeof(int)
struct Pageindex {
    int numbucket;
    int pageptr;
    int indexptr;
    char resvd[1];
    bool fullbit;
    bool oflpagebit;
    bool validbit;
};
class Record {
public:
    long long int id, manager_id;
    string bio, name;

    Record(vector<std::string> fields) {
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }

    void print() {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }
};


class LinearHashIndex {

private:
    const int BLOCK_SIZE = 4096;
    const int RecordsperBuckets = 6;

    vector<int> blockDirectory; // Map the least-significant-bits of h(id) to a bucket location in EmployeeIndex (e.g., the jth bucket)
                                // can scan to correct bucket using j*BLOCK_SIZE as offset (using seek function)
								// can initialize to a size of 256 (assume that we will never have more than 256 regular (i.e., non-overflow) buckets)
    int n;  // The number of indexes in blockDirectory currently being used
    int i;	// The number of least-significant-bits of h(id) to check. Will need to increase i once n > 2^i
    int numRecords;    // Records currently in index. Used to test whether to increase n
    int nextFreeBlock; // Next place to write a bucket. Should increment it by BLOCK_SIZE whenever a bucket is written to EmployeeIndex
    int numIndex;
    int nextptr;
    string fName;      // Name of index file

    vector<char> page;

    int HashIdInBucket (int id) {
        int hashid = hash(id);
        int idbucket = hashid % static_cast<int>(pow(2,i));
        return idbucket;
    }

    int flipbit(int numbucket) {
        if (numbucket > n) {
            numbucket = numbucket % static_cast<int>(pow(2,i-1));
        }
        return numbucket;
    }

    void Writefile(int pagelocation, vector<char> _page) {
        // Write file function
        ofstream outfile;
        outfile.open(fName,ios::in|ios::out|ios::binary);
        outfile.seekp(pagelocation,ios::beg);
        outfile.write(_page.data(),BLOCK_SIZE);
        outfile.close();
    }
    void Readfile(int pagelocation, vector<char> &_page) {
        // Read file function
        ifstream infile;
        infile.open(fName,ios::in|ios::binary);
        infile.seekg(pagelocation,ios::beg);
        infile.read(_page.data(),BLOCK_SIZE);
        infile.close();

    }
    // Add index to index page
    Pageindex AddOverflowPage(int numbucket, int indexptr) {
        Pageindex newindex;
        memset(&newindex,0,sizeof(Pageindex));

        newindex.oflpagebit = true;
        newindex.numbucket = numbucket;
        newindex.pageptr = BLOCK_SIZE * numIndex;
        newindex.validbit = true;
        newindex.indexptr = indexptr;
        memcpy(&page[indexptr],&newindex,sizeof(Pageindex));
        Writefile(IndexPagePtr,page);
        vector<char> init(BLOCK_SIZE);
        Writefile(newindex.pageptr,init);
        init.clear();
        return newindex;
        
    }
    // Write specific page
    void WritePage(Record record, Pageindex& index) {

        vector<char> bucketpage(BLOCK_SIZE);

        Readfile(index.pageptr, bucketpage);

        int writeptr = 0;
        int nextfree = *reinterpret_cast<int *>(&bucketpage[BLOCK_SIZE - FreeSpacePos]);
        int start_record_ptr = nextfree;
        int NumEntries = *reinterpret_cast<int *>(&bucketpage[BLOCK_SIZE - NumEntriesPos]);
        memcpy(&bucketpage[nextfree], &record.id, sizeof(record.id));
        nextfree += sizeof(record.id);
        bucketpage[nextfree++] = ',';

        memcpy(&bucketpage[nextfree], record.name.data(), record.name.size());
        nextfree += record.name.size();
        bucketpage[nextfree++] = ',';

        memcpy(&bucketpage[nextfree], record.bio.data(), record.bio.size());
        nextfree += record.bio.size();
        bucketpage[nextfree++] = ',';

        memcpy(&bucketpage[nextfree], &record.manager_id, sizeof(record.manager_id));
        nextfree += sizeof(record.manager_id);

        NumEntries++;
        
        if (NumEntries == RecordsperBuckets) {
            index.fullbit = 1;
        }

        int recordsize = nextfree - start_record_ptr;
        cout<<"recordsize: "<<recordsize<<endl;
        int FreeSpacePtr = nextfree;
        nextfree = BLOCK_SIZE - FreeSpacePos;
        memcpy(&bucketpage[nextfree], &FreeSpacePtr, sizeof(FreeSpacePtr));

        nextfree = BLOCK_SIZE - NumEntriesPos;
        cout<<"NumEntries "<<nextfree<<endl;
        memcpy(&bucketpage[nextfree], &NumEntries, sizeof(NumEntries));

        nextfree = BLOCK_SIZE - NumEntriesPos - NumEntries * NumEntriesPos;
        memcpy(&bucketpage[nextfree], &start_record_ptr, sizeof(start_record_ptr));

        nextfree += sizeof(int);
        memcpy(&bucketpage[nextfree], &recordsize, sizeof(recordsize));
        
        Writefile(index.pageptr,bucketpage);
        bucketpage.clear();

    }


    // Insert new record into index
    void insertRecord(Record record) {

        // No records written to index yet
        if (numRecords == 0) {
            // Initialize index with first blocks (start with 4)
            
        }

        vector<char> init(BLOCK_SIZE);
        page = init;
        // init.clear();

        Pageindex Pindex;

        // Find bucket to insert
        int idbucket = HashIdInBucket(record.id);
        cout<<"idbucket: "<<idbucket<<endl;
        idbucket = flipbit(idbucket);
        cout<<"idbucket after flip bit: "<<idbucket<<endl;


        // We want to find in index page first
        Readfile(IndexPagePtr,page);


        // Add record to the index in the correct block, creating a overflow block if necessary
        // First, find index from index page
        nextptr = 0;
        for (int k = 0; k < numIndex; k++) {
            memcpy(&Pindex,&page[nextptr],sizeof(Pageindex));
            if (idbucket == Pindex.numbucket) {
                if (!Pindex.fullbit) { // If the page is good not full
                    // Write the record in file
                    WritePage(record,Pindex);
                    // Update index on indexpage (update for full)
                    memcpy(&page[nextptr],&Pindex,sizeof(Pageindex));
                    Writefile(IndexPagePtr,page);
                    return ;
                }
            }
            nextptr+=sizeof(Pageindex);
        }
        
        Pageindex newindex = AddOverflowPage(idbucket,nextptr);
        WritePage(record,newindex);



        // Take neccessary steps if capacity is reached:
		// increase n; increase i (if necessary); place records in the new bucket that may have been originally misplaced due to a bit flip


    }

public:
    LinearHashIndex(string indexFileName) {
        n = 4; // Start with 4 buckets in index
        i = 2; // Need 2 bits to address 4 buckets
        numIndex = 0;
        numRecords = 0;
        nextFreeBlock = 0;
        fName = indexFileName;

        vector<char> init(BLOCK_SIZE);

        // Create your EmployeeIndex file and write out the initial 4 buckets
        // make sure to account for the created buckets by incrementing nextFreeBlock appropriately
        ofstream fin(indexFileName,ios::out| ios::binary);
        if(!fin) { 
            cerr<<"Error in creating file!!!\n"; 
            return ; 
        } 

        int bucketpageoffst;
        int indexofbucket = 0;
        Pageindex Pindex;

        page = init;
        nextptr = 0;

        // We put first page as index of every bucket page, each 8 byte contain number of bucket for 4 byte and the offset of bucket page for 4 byte
        for (int k = 1; k <= n; k++) { 
            memset(&Pindex,0,sizeof(Pageindex));
            Pindex.pageptr = BLOCK_SIZE * k;
            Pindex.numbucket = indexofbucket;
            Pindex.indexptr = nextptr;
            Pindex.validbit = 1;
            memcpy(&page[nextptr],&Pindex,sizeof(Pageindex));
            indexofbucket++;
            numIndex++;
            nextptr+=sizeof(Pageindex);
        }
        fin.write(page.data(),BLOCK_SIZE);

        // Now we create bucket page by page, just in initialization
        for (int k = 1; k <= n; k++) {
            fill(page.begin(),page.end(),0); // fill with 0
            nextFreeBlock += BLOCK_SIZE;
            fin.write(page.data(),BLOCK_SIZE);
        }
        fin.close();
    }

    // Read csv file and add records to the index
    void createFromFile(string csvFName) {
        fstream fin;
        fin.open(csvFName,ios::in);
        if (!fin.is_open()) {
            cerr << "Couldn't read file: " << csvFName << "\n";
            return ;
        }
        string lineinfile;
        int i = 0;
        while (getline(fin,lineinfile)) {
            string sttrinfile;
            vector<string> attrvec;
            stringstream stringline(lineinfile);
            while(getline(stringline,sttrinfile,',')) {
                attrvec.push_back(sttrinfile);
            }
            Record somerecord(attrvec);
            insertRecord(somerecord);
            i++;
            if (i == 20) {
                break;
            }

        }
    }

    // Given an ID, find the relevant record and print it
    Record findRecordById(int id) {
        vector<string> deflt = {"0","","","0"};
        Record foundrecord = deflt;
        return foundrecord;
    }
};
