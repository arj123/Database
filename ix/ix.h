#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <iostream>
#include "../pf/pf.h"
#include "../rm/rm.h"

# define IX_EOF (-1) // end of the index scan

const int int_Real= 340;
const int intReal= 170;

using namespace std;

class IX_IndexHandle;

class IX_Manager {



 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName, // create new index
const string attributeName);
  RC DestroyIndex(const string tableName, // destroy an index
const string attributeName);
  RC OpenIndex(const string tableName, // open an index
const string attributeName,
IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle); // close index

 protected:
  IX_Manager(); // Constructor
  ~IX_Manager(); // Destructor

 private:
  static IX_Manager *_ix_manager;
};

struct charArray
{

        char charKey[150];
};

template<class T>
struct indexnode
{

        int entries;
        T keys[int_Real];
int ptrs[int_Real+1];

indexnode()
{
for(int i=0;i<int_Real;i++)
{
        keys[i] = -1;
ptrs[i]=-1;
}
ptrs[int_Real]=-1;
entries=0;
}

};

template<class T>
struct leafnode
{

        int entries;
                T keys[int_Real];
RID rids[int_Real];
int fptr;
int bptr;

leafnode()
{

for(int i=0;i<int_Real;i++)
{
keys[i]=-1;
rids[i].pageNum = -1;
rids[i].slotNum = -1;
}
entries=0;
fptr = -1;
bptr = -1;
}
};

template<class charArray>
struct indexnodeChar
{

        int entries;
        charArray keys[20];
int ptrs[21];

indexnodeChar()
{
for(int i=0;i<20;i++)
{
        keys[i] = -1;
ptrs[i]=-1;
}
ptrs[20]=-1;
entries=0;
}

};

template<class charArray>
struct leafnodeChar
{

        int entries;
                charArray keys[20];
RID rids[20];
int fptr;
int bptr;

leafnodeChar()
{

for(int i=0;i<20;i++)
{
keys[i]=-1;
rids[i].pageNum = -1;
rids[i].slotNum = -1;
}
entries=0;
fptr = -1;
bptr = -1;
}
};




class IX_IndexHandle {

 public:
PF_FileHandle fileHandle;

IX_IndexHandle& operator=(const IX_IndexHandle &x)
{
this->fileHandle = x.fileHandle;

return (*this);
}


  IX_IndexHandle (); // Constructor
  ~IX_IndexHandle (); // Destructor

  // The following two functions are using the following format for the passed key value.
  // 1) data is a concatenation of values of the attributes
  // 2) For int and real: use 4 bytes to store the value;
  // For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid); // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid); // Delete index entry
template<class T>RC insert(int nodepointer,T k,const RID &rid,T &newchildentry,int &page,int fptr,int bptr);

template<class T>RC Start_new_index(int nodepointer, T k, const RID &rid);
template<class T>RC Insert_new_leaf( T k, const RID &rid, int &page, int fptr, int bptr);

template<class T>RC lazydelete(int nodepointer,T k,RID rid,int &flag);
//template<class charArray>RC insertChar(int nodepointer,char k[],const RID &rid,char &newchildentry[],int &page);

//template<class charArray>RC Start_new_indexChar(int nodepointer, charArray k, const RID &rid);
//template<class charArray>RC Insert_new_leafChar( charArray k, const RID &rid, int &page, int fptr, int bptr);

//template<class charArray>RC lazydeleteChar(int nodepointer,charArray k,RID rid,int &flag);
};
typedef enum {
           LIX_OP=0,      // <
           GIX_OP,      // >
           LGX_OP,      // <=
           NLGX_OP,      // >=

} CompOpe;

class IX_IndexScan {

vector<RID> rids;


 public:
int i;
  IX_IndexScan(){i = 0;}; // Constructor
  ~IX_IndexScan(){}; // Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
 RC OpenScan(const IX_IndexHandle &indexHandle, void*lowKey,void*highKey,bool  lowKeyInclusive, bool highKeyInclusive);
 template <class T> RC open(int root, CompOpe compOp, T lk,T hk, PF_FileHandle &fileHandle);
 template <class charArray> RC openChar(int root, CompOpe compOp, charArray lk,charArray hk, PF_FileHandle &fileHandle);
 RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan
};
// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif

