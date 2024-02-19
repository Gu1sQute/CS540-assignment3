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
#define NumEntriesPos BSIZE - (sizeof(int) + sizeof(int))
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

    // Add index to index page AND add a blank page
    Pageindex AddPage(int numbucket, int indexptr, bool overflow) {
        Pageindex newindex;
        memset(&newindex,0,sizeof(Pageindex));
        if (overflow) {
            newindex.oflpagebit = true;
        }
        newindex.numbucket = numbucket;
        newindex.pageptr = nextFreeBlock;
        newindex.validbit = true;
        newindex.indexptr = indexptr;
        memcpy(&page[indexptr],&newindex,sizeof(Pageindex));
        Writefile(IndexPagePtr,page);
        vector<char> init(BLOCK_SIZE);
        Writefile(newindex.pageptr,init);
        init.clear();
        // Add n if it's another bucket, if n > 2^i, increase i
        if (!overflow) {
            n+=1;
            if(n > static_cast<int>(pow(2,i))) {
                i+=1;
            }
        }
        nextFreeBlock += BLOCK_SIZE;
        return newindex;
        
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
    void WritePage(Record record, Pageindex& index) {

        vector<char> bucketpage(BLOCK_SIZE);

        Readfile(index.pageptr, bucketpage);

        int writeptr = 0;
        int nextfree = *reinterpret_cast<int *>(&bucketpage[FreeSpacePos]);
        int start_record_ptr = nextfree;
        int NumEntries = *reinterpret_cast<int *>(&bucketpage[NumEntriesPos]);
        // cout<<"NumEntries: "<<NumEntries<<endl;
        
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
        int FreeSpacePtr = nextfree;
        nextfree = FreeSpacePos;
        memcpy(&bucketpage[nextfree], &FreeSpacePtr, sizeof(FreeSpacePtr));

        nextfree = NumEntriesPos;
        memcpy(&bucketpage[nextfree], &NumEntries, sizeof(NumEntries));

        nextfree = NumEntriesPos - NumEntries * sizeof(int) * 2;
        memcpy(&bucketpage[nextfree], &start_record_ptr, sizeof(start_record_ptr));

        nextfree += sizeof(int);
        memcpy(&bucketpage[nextfree], &recordsize, sizeof(recordsize));
        
        Writefile(index.pageptr,bucketpage);
        bucketpage.clear();

    }
    void ReinsertRecord(Record record) {

    }

    void Rehash(int numbucket) {
        Pageindex index;
        int checkrehashbucket = numbucket % static_cast<int>(pow(2,i-1));
        int indexptr = 0;
        vector<Pageindex> tmp;
        vector<string> deflt = {"0","","","0"};
        Record tmpRecord = deflt;
        vector<Record> tempRecordDst;
        vector<char> searchPage(BLOCK_SIZE);
        int nextfree = 0;
        int searchslotptr = 0;
        // Search from index
        for (int k = 0; k < numIndex; k++) {
            memcpy(&index,&page[indexptr],sizeof(Pageindex));
            int flp = flipbit(numbucket);
            if (index.numbucket == flp) {
                tmp.push_back(index);
            }
            indexptr+=sizeof(Pageindex);
        }
        for (auto index:tmp) {
            Readfile(index.pageptr,searchPage);
            int NumEntries = *reinterpret_cast<int *>(&searchPage[NumEntriesPos]);

            if (NumEntries == 0) {
                continue;
            }

            for (int k = 1; k <= NumEntries; k++) {
                searchslotptr = NumEntriesPos - sizeof(int) * 2 * k;
                nextfree = *reinterpret_cast<int *>(&searchPage[searchslotptr]);
                tmpRecord.id = *reinterpret_cast<int*>(&searchPage[nextfree]);
                nextfree += sizeof(tmpRecord.id) + 1;
                string name;
                while(searchPage[nextfree]!= ',') {
                    name += searchPage[nextfree];
                    nextfree++;
                }
                tmpRecord.name = name;
                nextfree++;
                string bio;
                while(searchPage[nextfree]!= ',') {
                    bio += searchPage[nextfree];
                    nextfree++;
                }
                tmpRecord.bio = bio;
                nextfree++;
                tmpRecord.manager_id = *reinterpret_cast<int*>(&searchPage[nextfree]);

                tempRecordDst.push_back(tmpRecord);

                // if (HashIdInBucket(static_cast<int>(tmpRecord.id)) == numbucket) { // If it's flip bit records
                //     tempRecordDst.push_back(tmpRecord);
                // }
                // else {
                //     tempRecordOrg.push_back(tmpRecord);
                // }
            }
            numRecords -= NumEntries;
            fill(searchPage.begin(),searchPage.end(),0);
            Writefile(index.pageptr,searchPage);
            index.fullbit = 0;
            memcpy(&page[index.indexptr],&index,sizeof(Pageindex));
            Writefile(IndexPagePtr,page);
        }
        searchPage.clear();
        // now we reinsert the records
        if(!tempRecordDst.empty()) {
            for (auto& record:tempRecordDst) {
                cout<<"Rehashing these records: "<<record.id<<endl;
                insertRecord(record);
            }
            cout<<"Rehashing complete."<<endl;
        }


    }

    // Insert new record into index
    void insertRecord(Record record) {
        cout<<"Inserting Record: "<<record.id<<endl;

        bool bucketfound = false;

        vector<char> init(BLOCK_SIZE);
        page = init;

        Pageindex Pindex;

        // We want to find in index page first
        Readfile(IndexPagePtr,page);

        // Find bucket to insert
        int idbucket = HashIdInBucket(record.id);
        cout<<"idbucket: "<<idbucket<<endl;
        if (idbucket >= n) {
            idbucket = flipbit(idbucket);
            cout<<"flipbit occur! idbucket after flip bit: "<<idbucket<<endl;
        }


        // cout<<"numIndex: "<<numIndex<<endl;

        // Add record to the index in the correct block, creating a overflow block if necessary
        // First, find index from index page
        nextptr = 0;
        for (int k = 0; k < numIndex; k++) {
            memcpy(&Pindex,&page[nextptr],sizeof(Pageindex));
            if (idbucket == Pindex.numbucket && !Pindex.fullbit) {
                // Write the record in file
                WritePage(record,Pindex);
                // Update index on indexpage (update for full)
                memcpy(&page[nextptr],&Pindex,sizeof(Pageindex));
                Writefile(IndexPagePtr,page);
                bucketfound = true;
                numRecords++;
                cout<<"Find bucket in: "<<Pindex.numbucket<<" inserted "<<record.id<<endl;
                break;
            }
            nextptr+=sizeof(Pageindex);
        }
        int indexend = numIndex * sizeof(Pageindex);
        
        // Take neccessary steps if capacity is reached:
		// increase n; increase i (if necessary); place records in the new bucket that may have been originally misplaced due to a bit flip
        // Overflow page if bucket is full for idbucket
        if (!bucketfound) {
            cout<<"overflow page occur!! Add overflow page!"<<endl;
            Pageindex newindex = AddPage(idbucket,indexend,true);
            // Write the record in file
            WritePage(record,newindex);
            // Update index on indexpage
            memcpy(&page[nextptr],&newindex,sizeof(Pageindex));
            Writefile(IndexPagePtr,page);
            indexend+=sizeof(Pageindex);
            numIndex++;
            numRecords++;
        }
        if (CheckBucket()) {
            cout<<"Exceed current bucket capacity, creat new bucket!\n"<<endl;
            Pageindex newindex = AddPage(n,indexend,false);
            // First rehash then increase the numIndex
            numIndex++;
            Rehash(newindex.numbucket);
            indexend+=sizeof(Pageindex);
        }
        page.clear();
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

        // We put first page as index of every bucket page, each 16 byte contain number of bucket for 4 byte and the offset of bucket page for 4 byte and others
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
        nextFreeBlock+= BLOCK_SIZE;

        // Now we create bucket page by page, just in initialization
        for (int k = 1; k <= n; k++) {
            fill(page.begin(),page.end(),0); // fill with 0
            nextFreeBlock += BLOCK_SIZE;
            fin.write(page.data(),BLOCK_SIZE);
        }
        // cout<<"nextFreeBlock: "<<nextFreeBlock<<endl;
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
            if (dataset == 50) {
                break;
            }

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
            cout<<"flipbit occur! idbucket after flip bit: "<<bucket<<endl;
            }
        Readfile(IndexPagePtr,page);
        nextptr = 0;
        for (int k = 0; k < numIndex; k++) {
            memcpy(&index,&page[nextptr],sizeof(Pageindex));
            if(index.numbucket == bucket) {
                vector<char> searchPage(BLOCK_SIZE);
                Readfile(index.pageptr,searchPage);
                int NumEntries = *reinterpret_cast<int*>(&searchPage[NumEntriesPos]); // find how many entries in the page
                for(int t = 1; t <= NumEntries; t++) {
                    int searchslotptr = NumEntriesPos - sizeof(int) * 2 * t; // start index of every records
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
            }
            nextptr += sizeof(Pageindex);
        }
        cout<<"BAD ID!"<<endl;
        foundrecord.print();
        return foundrecord;
    }
};
