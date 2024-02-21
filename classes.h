#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
#include <queue>
using namespace std;
#define hashnum 216
#define IndexPagePtr 0
#define BSIZE 4096
#define hash(id) id % hashnum
#define FreeSpacePos BSIZE - sizeof(int)
#define NumEntriesPos FreeSpacePos - sizeof(int)
#define OFPtrPos NumEntriesPos - sizeof(int)
#define RecordIndexSize sizeof(int) * 2
struct Pageindex {
    int numbucket;
    int pageptr;
    int indexptr;
    char resvd[4];
    // bool validbit;
    // bool fullbit;
    // bool oflpagebit;
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
    int nextptr;
    string fName;      // Name of index file

    vector<char> page;

    int HashIdInBucket (int id) {
        int hashid = hash(id);
        int idbucket = hashid % static_cast<int>(pow(2,i));
        return idbucket;
    }

    int flipbit(int numbucket) {
        numbucket = numbucket % static_cast<int>(pow(2,i-1));
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

        // Add a blank page
    int AddPage(int numbucket, int indexptr) {
        Pageindex newindex;
        memset(&newindex,0,sizeof(Pageindex));
        newindex.numbucket = numbucket;
        newindex.pageptr = nextFreeBlock;
        newindex.indexptr = indexptr;
        memcpy(&page[indexptr],&newindex,sizeof(Pageindex));
        Writefile(IndexPagePtr,page);
        vector<char> init(BLOCK_SIZE);
        int of = -1;
        memcpy(&init[OFPtrPos],&of,sizeof(int));
        Writefile(newindex.pageptr,init);
        init.clear();
        // Add n if it's another bucket, if n > 2^i, increase i
        n+=1;
        if(n > static_cast<int>(pow(2,i))) {
            i+=1;
        }
        nextFreeBlock += BLOCK_SIZE;
        return newindex.pageptr;
    }

    // Add add a blank page legit
    void AddOverflowPage(int pageptr) {
        vector<char> init(BLOCK_SIZE);
        int of = -1;
        memcpy(&init[OFPtrPos],&of,sizeof(int));
        Writefile(pageptr,init);
        init.clear();
        nextFreeBlock += BLOCK_SIZE;
        
    }

    // Check if the capacity of hash bucket exceed 70%, if yes, add another page (increase n and i)
    bool CheckBucket() {
        float capacity = numRecords / (n * RecordsperBuckets);
        if (capacity > 0.7) {
            return true;
        }
        return false;
    }

    // Write specific page
    void WritePage(Record record, int pageptr) {

        vector<char> bucketpage(BLOCK_SIZE);

        Readfile(pageptr, bucketpage);

        int nextfree = *reinterpret_cast<int *>(&bucketpage[FreeSpacePos]);
        int NumEntries = *reinterpret_cast<int *>(&bucketpage[NumEntriesPos]);
        int OFPtr = *reinterpret_cast<int *>(&bucketpage[OFPtrPos]);



        // Record original pointer to freespace, to calculate the record size
        int start_record_ptr = nextfree;
        
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


        int recordsize = nextfree - start_record_ptr;
        int FreeSpacePtr = nextfree;

        if((NumEntries >= RecordsperBuckets) || (nextfree >= OFPtrPos - NumEntries * RecordIndexSize)) {
            if (OFPtr == -1) {
                int Overflowpage = nextFreeBlock;
                memcpy(&bucketpage[OFPtrPos],&Overflowpage,sizeof(Overflowpage));
                Writefile(pageptr,bucketpage);
                AddOverflowPage(Overflowpage);
                WritePage(record, Overflowpage);
                return ;
            }
            else {
                bucketpage.clear();
                WritePage(record,OFPtr);
                return ;
            }
        }

        NumEntries++;
        nextfree = FreeSpacePos;
        memcpy(&bucketpage[nextfree], &FreeSpacePtr, sizeof(FreeSpacePtr));

        nextfree = NumEntriesPos;
        memcpy(&bucketpage[nextfree], &NumEntries, sizeof(NumEntries));

        nextfree = OFPtrPos - NumEntries * RecordIndexSize;
        memcpy(&bucketpage[nextfree], &start_record_ptr, sizeof(start_record_ptr));

        nextfree += sizeof(int);
        memcpy(&bucketpage[nextfree], &recordsize, sizeof(recordsize));
        
        Writefile(pageptr,bucketpage);
        bucketpage.clear();

    }
    // Write specific page
    int WritePageRehash(Record record, vector<char>& tmppage,int pageptr) {

        int nextpagefree = *reinterpret_cast<int *>(&tmppage[FreeSpacePos]);
        int NumEntries = *reinterpret_cast<int *>(&tmppage[NumEntriesPos]);
        int OFPtr = *reinterpret_cast<int *>(&tmppage[OFPtrPos]);



        // Record original pointer to freespace, to calculate the record size
        int start_record_ptr = nextpagefree;
        
        memcpy(&tmppage[nextpagefree], &record.id, sizeof(record.id));
        nextpagefree += sizeof(record.id);
        tmppage[nextpagefree++] = ',';

        memcpy(&tmppage[nextpagefree], record.name.data(), record.name.size());
        nextpagefree += record.name.size();
        tmppage[nextpagefree++] = ',';

        memcpy(&tmppage[nextpagefree], record.bio.data(), record.bio.size());
        nextpagefree += record.bio.size();
        tmppage[nextpagefree++] = ',';

        memcpy(&tmppage[nextpagefree], &record.manager_id, sizeof(record.manager_id));
        nextpagefree += sizeof(record.manager_id);


        int recordsize = nextpagefree - start_record_ptr;
        int FreeSpacePtr = nextpagefree;

        if((NumEntries >= RecordsperBuckets) || (nextpagefree >= OFPtrPos - NumEntries * RecordIndexSize)) {
            int Overflowpage = nextFreeBlock;
            memcpy(&tmppage[OFPtrPos],&Overflowpage,sizeof(Overflowpage));
            Writefile(pageptr,tmppage);
            AddOverflowPage(Overflowpage);
            WritePage(record, Overflowpage);
            return Overflowpage;
        }

        NumEntries++;
        nextpagefree = FreeSpacePos;
        memcpy(&tmppage[nextpagefree], &FreeSpacePtr, sizeof(FreeSpacePtr));

        nextpagefree = NumEntriesPos;
        memcpy(&tmppage[nextpagefree], &NumEntries, sizeof(NumEntries));

        nextpagefree = OFPtrPos - NumEntries * RecordIndexSize;
        memcpy(&tmppage[nextpagefree], &start_record_ptr, sizeof(start_record_ptr));

        nextpagefree += sizeof(int);
        memcpy(&tmppage[nextpagefree], &recordsize, sizeof(recordsize));
        
        Writefile(pageptr,tmppage);

        return -1;

    }
// Rehash only original bucket and new bucket
    void Rehash(int PtrORG,int PtrNEW) {
        
        int OF = -1;
        vector<string> defaultRecord = {"0","","","0"};
        Record rehashrecord = defaultRecord;
        int PtrA = PtrORG;
        int PtrB = PtrNEW;

        while (true) {
            vector<char> nowPage(BLOCK_SIZE);
            vector<char> tmpPageA(BLOCK_SIZE);
            vector<char> tmpPageB(BLOCK_SIZE);
            Readfile(PtrORG, nowPage);
            int OFPtr = *reinterpret_cast<int *>(&nowPage[OFPtrPos]);
            int NumEntries = *reinterpret_cast<int *>(&nowPage[NumEntriesPos]);
            if (NumEntries ==0) {
                nowPage.clear();
                tmpPageA.clear();
                tmpPageB.clear();
                break; // No record in it, skip
            };
            memcpy(&tmpPageA[OFPtrPos], &OF, sizeof(int));
            Writefile(PtrORG, tmpPageA);// Clean the original page
            Readfile(PtrA,tmpPageA);
            Readfile(PtrB,tmpPageB);
            for (int k = 1; k <= NumEntries; k++) {
                int start = *reinterpret_cast<int *>(&nowPage[OFPtrPos - RecordIndexSize * k]);
                long long int recordid = (int)*reinterpret_cast<long long int *>(&nowPage[start]);
                int bucketid = HashIdInBucket(recordid);
                rehashrecord.id = recordid;
                start += sizeof(rehashrecord.id) + 1;
                string name;
                while(nowPage[start]!= ',') {
                    name += nowPage[start];
                    start++;
                }
                rehashrecord.name = name;
                start++;
                string bio;
                while(nowPage[start]!= ',') {
                    bio += nowPage[start];
                    start++;
                }
                rehashrecord.bio = bio;
                start++;
                rehashrecord.manager_id = *reinterpret_cast<long long int*>(&nowPage[start]);
                if (bucketid != n - 1) {
                    int OFA = WritePageRehash(rehashrecord, tmpPageA, PtrA);
                    if(OFA != -1) {
                        PtrA = OFA;
                    }
                }
                else {
                    int OFB = WritePageRehash(rehashrecord, tmpPageB, PtrB);
                    if(OFB != -1) {
                        PtrB = OFB;
                    }
                }
            }
            nowPage.clear();
            tmpPageA.clear();
            tmpPageB.clear();
            if (OFPtr == -1) {
                break;
            }
            else {
                PtrORG = OFPtr;
            }
        }


    }

    // Insert new record into index
    void insertRecord(Record record) {

        bool bucketfound = false;

        vector<char> init(BLOCK_SIZE);
        page = init;

        Pageindex Pindex;

        // We want to find in index page first
        Readfile(IndexPagePtr,page);

        // Find bucket to insert
        int idbucket = HashIdInBucket(record.id);
        if (idbucket >= n) {
            idbucket = flipbit(idbucket);
        }

        // Add record to the index in the correct block, creating a overflow block if necessary
        // First, find index from index page
        nextptr = 0;
        for (int k = 0; k < n; k++) {
            memcpy(&Pindex,&page[nextptr],sizeof(Pageindex));
            if (idbucket == Pindex.numbucket) {
                // Write the record in file
                WritePage(record,Pindex.pageptr);
                numRecords++;
                break;
            }
            nextptr+=sizeof(Pageindex);
        }
        int indexend = n * sizeof(Pageindex);
        
        // Take neccessary steps if capacity is reached:
		// increase n; increase i (if necessary); place records in the new bucket that may have been originally misplaced due to a bit flip
        if (CheckBucket()) {
            int newpagptr = AddPage(n,indexend);
            // Rehash
            int orgbucketptr = -1;
            nextptr = 0;
            int orgnumbucket = (n-1) % static_cast<int>(pow(2,i-1));
            while(orgbucketptr == -1) { // Find the old bucket to rehash
                int num = *reinterpret_cast<int *>(&page[nextptr]);
                if (num == orgnumbucket) {
                    nextptr += sizeof(int);
                    orgbucketptr = *reinterpret_cast<int *>(&page[nextptr]);
                    Rehash(orgbucketptr,newpagptr);
                }
                nextptr += sizeof(Pageindex);
            }
        }
        page.clear();
    }

public:
    LinearHashIndex(string indexFileName) {
        n = 4; // Start with 4 buckets in index
        i = 2; // Need 2 bits to address 4 buckets
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

        // We put first page as index of every bucket page, each 16 byte contain number of bucket for 4 byte and the offset of bucket page for 4 byte and others
        for (int k = 1; k <= n; k++) { 
            memset(&Pindex,0,sizeof(Pageindex));
            Pindex.pageptr = BLOCK_SIZE * k;
            Pindex.numbucket = indexofbucket;
            Pindex.indexptr = nextptr;
            memcpy(&page[nextptr],&Pindex,sizeof(Pageindex));
            indexofbucket++;
            nextptr+=sizeof(Pageindex);
        }
        fin.write(page.data(),BLOCK_SIZE);
        nextFreeBlock+= BLOCK_SIZE;

        // Now we create bucket page by page, just in initialization
        for (int k = 1; k <= n; k++) {
            fill(page.begin(),page.end(),0); // fill with 0
            int of = -1;
            memcpy(&page[OFPtrPos],&of,sizeof(int));
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
        int dataset = 0;
        while (getline(fin,lineinfile)) {
            string sttrinfile;
            vector<string> attrvec;
            stringstream stringline(lineinfile);
            while(getline(stringline,sttrinfile,',')) {
                attrvec.push_back(sttrinfile);
            }
            Record somerecord(attrvec);
            insertRecord(somerecord);
            dataset++;

        }
    }

    // Given an ID, find the relevant record and print it
    Record findRecordById(int id) {
        vector<string> deflt = {"0","","","0"};
        Record foundrecord = deflt;
        Pageindex index;
        vector<char> init(BLOCK_SIZE);
        page = init;
        init.clear();
        
        int bucket = HashIdInBucket(id);
        if (bucket >= n) {
            bucket = flipbit(bucket);
            }
        Readfile(IndexPagePtr,page);
        nextptr = 0;
        for (int k = 0; k < n; k++) {
            memcpy(&index,&page[nextptr],sizeof(Pageindex));
            if(index.numbucket == bucket) {
                vector<char> searchPage(BLOCK_SIZE);
                Readfile(index.pageptr,searchPage);
                int ofptr = 0;
                while(ofptr != -1) {
                    ofptr = *reinterpret_cast<int *>(&searchPage[OFPtrPos]); // check if OF
                    int NumEntries = *reinterpret_cast<int*>(&searchPage[NumEntriesPos]); // find how many entries in the page
                    for(int t = 1; t <= NumEntries; t++) {
                        int searchslotptr = OFPtrPos - RecordIndexSize * t; // start index of every records
                        int nextfree = *reinterpret_cast<int *>(&searchPage[searchslotptr]); // find where's records start
                        long long int foundid = *reinterpret_cast<long long int*>(&searchPage[nextfree]);
                        if (id == (int)foundid) {
                            foundrecord.id = foundid;
                            nextfree += sizeof(foundrecord.id) + 1;
                            string name;
                            while(searchPage[nextfree]!= ',') {
                                name += searchPage[nextfree];
                                nextfree++;
                            }
                            foundrecord.name = name;
                            nextfree++;
                            string bio;
                            while(searchPage[nextfree]!= ',') {
                                bio += searchPage[nextfree];
                                nextfree++;
                            }
                            foundrecord.bio = bio;
                            nextfree++;
                            foundrecord.manager_id = *reinterpret_cast<long long int*>(&searchPage[nextfree]);
                            cout<<"ID found!"<<endl;
                            foundrecord.print();
                            return foundrecord;
                        }
                    }
                Readfile(ofptr,searchPage);
                }
            }
            nextptr += sizeof(Pageindex);
        }
        cout<<"BAD ID!"<<endl;
        foundrecord.id = id;
        foundrecord.print();
        return foundrecord;
    }
};
