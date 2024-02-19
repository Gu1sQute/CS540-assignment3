/*
Skeleton code for linear hash indexing
*/

#include <string>
#include <ios>
#include <fstream>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cmath>
#include "classes.h"
using namespace std;

vector<string> split(string s, string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    string token;
    vector<string> res;

    while ((pos_end = s.find(delimiter, pos_start)) != string::npos) {
        token = s.substr (pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back (token);
    }

    res.push_back (s.substr (pos_start));
    return res;
}
int main(int argc, char* const argv[]) {

    // Create the index
    LinearHashIndex emp_index("EmployeeIndex");
    emp_index.createFromFile("Employee.csv");
    
    // Loop to lookup IDs until user is ready to quit
    string ids;
    int id;
    cout<<"Try to found your IDs,space is delimiter(-1 to quit):"<<endl;
    while(true) {
        cin>>ids;
        if (ids == "-1") {
            break;
        }

        vector<string> k;
        k = split(ids," ");
        for (int i = 0;i<k.size();i++) {
            id = stoi(k[i]);
            emp_index.findRecordById(id);
        }

    }

    return 0;
}
