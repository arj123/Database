
#include <iostream>
#include<cstdlib>
using namespace std;

#include "ix.h"

PF_Manager *pf=PF_Manager::Instance();


IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance()
{
    if(!_ix_manager)
        _ix_manager = new IX_Manager();

    return _ix_manager;
}

IX_Manager::IX_Manager()
{

}

IX_Manager::~IX_Manager()
{
}


RC IX_Manager::CreateIndex(const string tableName,const string attributeName)
{
RM *rm = RM::Instance();

string fileName = tableName+"_"+attributeName;

int x = pf->CreateFile(fileName.c_str());

if(x!=0)
return x;

PF_FileHandle fileHandle;

x = pf->OpenFile(fileName.c_str(),fileHandle);

if(x!=0)
return x;

void *data = malloc(4096);

int root=0;

int m = int_Real;

memcpy((char*)data,&root,sizeof(int));

vector<Attribute> attrs;

 rm->getAttributes(tableName,attrs);

AttrType type;
AttrLength length;
for(unsigned i=0;i<attrs.size();i++)
{
if(attrs[i].name == attributeName)
type = attrs[i].type;
length =attrs[i].length;
}

memcpy((char*)data+4,&type,sizeof(type));


memcpy((char*)data+8,&length,sizeof(int));

x = fileHandle.AppendPage(data);

x = pf->CloseFile(fileHandle);

free(data);

return x;
}

RC IX_Manager::DestroyIndex(const string tableName,const string attributeName)
{
string fileName = tableName+"_"+attributeName;

return pf->DestroyFile(fileName.c_str());
}

RC IX_Manager::OpenIndex(const string tableName,const string attributeName,IX_IndexHandle &indexHandle)
{
string fileName = tableName+"_"+attributeName;

return pf->OpenFile(fileName.c_str(),indexHandle.fileHandle);
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
{
    return pf->CloseFile(indexHandle.fileHandle);
}

IX_IndexHandle::IX_IndexHandle()
{
}
IX_IndexHandle::~IX_IndexHandle()
{
int x = pf->CloseFile(fileHandle);
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)
{
int nodepointer,flag=0;

void *data = malloc(4096);

 fileHandle.ReadPage(0,data);

memcpy(&nodepointer,(int*)data,sizeof(int));

int page = -1;

int fptr = -1,bptr = -1;

AttrType type;
AttrLength length;
memcpy(&type,(char*)data+4,sizeof(type));
memcpy(&length,(char*)data+8,sizeof(int));
int keyInt,newchildentryInt = -1;
float keyFloat,newchildentryFloat = -1;
if(type == (AttrType)0)
{
memcpy(&keyInt,(char*)key,4);
flag = insert(nodepointer,keyInt,rid,newchildentryInt,page,fptr,bptr);
}
else if(type == (AttrType)1)
{
memcpy(&keyFloat,(char*)key,4);
flag = insert(nodepointer,keyFloat,rid,newchildentryFloat,page,fptr,bptr);
}
/*else if(type == (AttrType)2)
{
        char kVarChar[length+1] ;
        char newchildentryVarChar[length+1] ;

memcpy(&kVarChar,(char*)key,sizeof(kVarChar));
flag = insert(nodepointer,kVarChar,rid,newchildentryVarChar,page,fptr,bptr);
}
*/
free(data);

if(flag == 1)
return 1;

return 0;

}

template<class T>
RC IX_IndexHandle::insert(int nodepointer,T k,const RID &rid,T &newchildentry,int &page,int fptr,int bptr)
{
int root;
if(nodepointer == 0)
{
int rc = Start_new_index(nodepointer,k,rid);
return rc;
}
if(nodepointer == -1)
{
        int x =Insert_new_leaf(k, rid, page, fptr,bptr);
        return x;
}

void *data = malloc(4096);

fileHandle.ReadPage(nodepointer,data);
char type;

memcpy(&type,(char*)data,1);

if(type == 'I')
{
indexnode<T> N;

memcpy(&N,(char*)data+1,sizeof(N));

int i=0;
while(N.keys[i]<=k &&N.keys[i]!=-1 &&i<N.entries)
{
        i++;
}

int page1=page;
int newchildentry1=newchildentry;
int nodepointer1 = nodepointer;
nodepointer = N.ptrs[i];
bool split = false;
indexnode<T> N1;
indexnode<T> N2;
if(N.entries == int_Real)
{ split =true;
        //Split_indexnode();


        T *keys;
        int *ptrs;

        keys = (T *)malloc((int_Real)*sizeof(T));
        ptrs = (int *)malloc((int_Real+2)*sizeof(int));

        for(int p=0;p<intReal;p++)
        {
        ptrs[p] = N.ptrs[p];
        keys[p] = N.keys[p];
        }


        ptrs[intReal] = N.ptrs[intReal];
    ptrs[intReal+1]=-1;
    for(int p=intReal;p<int_Real;p++)
        {
        ptrs[p+2] = N.ptrs[p+1];
        keys[p] = N.keys[p];
        }


        for(int o=0;o<intReal;o++)
        {
        N1.ptrs[o] = ptrs[o];
        N1.keys[o] = keys[o];
        }
N1.entries = intReal;
        N1.ptrs[intReal] = ptrs[intReal];


        for(int g= intReal, j=0;g<int_Real+1;g++,j++)
        {
        N2.ptrs[j] = ptrs[g+1];
        if(g< int_Real)
        N2.keys[j] = keys[g];
        }
N2.entries =intReal;
//      N2.ptrs[] = -1;

        memcpy((char*)data+1,&N1,sizeof(N1));

        fileHandle.WritePage(nodepointer1,data);

        memcpy((char*)data+1,&N2,sizeof(N2));

        int numPage = fileHandle.GetNumberOfPages();

         fileHandle.AppendPage(data);

         page1 = numPage;

         newchildentry1 = keys[intReal];

         fileHandle.ReadPage(0,data);

        memcpy(&root,(int*)data,4);

        if(root == nodepointer1)
        {

        indexnode<T> N3;

        N3.keys[0] = keys[intReal];

        N3.ptrs[0] = nodepointer1;
        N3.ptrs[1] = page1;

        N3.entries++;

        numPage = fileHandle.GetNumberOfPages();

        char type = 'I';

        memcpy((char*)data,&type,sizeof(char));

        memcpy((char*)data+1,&N3,sizeof(N3));
         fileHandle.AppendPage(data);
        root = numPage;
        void *dataw = malloc(4096);

         fileHandle.ReadPage(0,dataw);

        memcpy((char*)dataw,&root,4);

         fileHandle.WritePage(0,dataw);

        free(dataw);
        //set_root(root);

}
        free(keys);
        free(ptrs);
}
if(i < int_Real)
{
if(N.ptrs[i+1]!=-1)
{
fptr = N.ptrs[i+1];
}
else if(fptr!=-1)
{
void *data1 = malloc(4096);

 fileHandle.ReadPage(fptr,data1);

indexnode<T> N6;

memcpy(&N6,(char*)data1+1,sizeof(N6));

fptr = N6.ptrs[0];

free(data1);
}
}
if(i > 0)
{
if(N.ptrs[i-1]!= -1)
{
bptr = N.ptrs[i-1];
}

else if(bptr!=-1)
{
void *data1 = malloc(PF_PAGE_SIZE);

 fileHandle.ReadPage(bptr,data1);

indexnode<T> N5;

memcpy(&N5,(char*)data1+1,sizeof(N5));

bptr = N5.ptrs[N5.entries];

free(data1);
}
}
insert(nodepointer,k,rid,newchildentry,page,fptr,bptr);
nodepointer = nodepointer1;
if(newchildentry == -1)
{
if(page!=-1)
{
        if(i<intReal || !split)
        { void* data2 = malloc(4096);
                fileHandle.ReadPage(nodepointer,data2);
                memcpy(&N1,(char*)data2+1,sizeof(N1));
N1.ptrs[i] = page;

memcpy((char*)data2+4,&N1,sizeof(N1));

 fileHandle.WritePage(nodepointer,data2);
free(data2);
}
        else
        {
                void* data2 = malloc(4096);
                                fileHandle.ReadPage(page1,data2);
                                memcpy(&N1,(char*)data2+1,sizeof(N1));
                N1.ptrs[i-intReal] = page1;
                memcpy((char*)data2+1,&N1,sizeof(N1));

                 fileHandle.WritePage(page1,data2);
                free(data2);
        }
}
newchildentry =newchildentry1;
page = page1;
free(data);

return 0;
}
else
{
        if(i<intReal || !split)
                { void* data2 = (T*)malloc(4096);
                        fileHandle.ReadPage(nodepointer,data2);
                        memcpy(&N1,(char*)data2+1,sizeof(N1));

        free(data2);

        }
                else
                {
                        void* data2 = (T*)malloc(4096);
                                        fileHandle.ReadPage(page1,data2);
                                        memcpy(&N1,(char*)data2+1,sizeof(N1));
                                        free(data2);
                }

for(int j = N1.entries;j>i;j--)
{
N1.keys[j] = N1.keys[j-1];
N1.ptrs[j+1] = N1.ptrs[j];
}

N1.ptrs[i+1] = page;

page = page1;

N1.keys[i] = newchildentry;
newchildentry = newchildentry1;
N1.entries++;

memcpy((char*)data+1,&N1,sizeof(N1));
if(i<intReal ||!split)
{
 fileHandle.WritePage(nodepointer,data);
}
else
{
         fileHandle.WritePage(page1,data);
}
free(data);
return 0;
}
free(data);
return 0;
}
else
{
leafnode<T> L;
int i;
memcpy(&L,(char*)data+1,sizeof(L));
if(L.entries == int_Real)
{

        leafnode<T> L2;

        T *keys = (T *)malloc((int_Real+1)*sizeof(T));
        RID *rids = (RID *)malloc((int_Real+1)*sizeof(RID));
int b;
        for(b=0; b<L.entries&&L.keys[b]<=k;b++)
        {
        keys[b] = L.keys[b];
        rids[b] = L.rids[b];
        }

        keys[b] = k;
        rids[b] = rid;

        b++;

        for(;b<int_Real+1;b++)
        {
        keys[b] = L.keys[b-1];
        rids[b] = L.rids[b-1];
        }

        leafnode<T> L1;

        L1.fptr = L.fptr;
        L1.bptr = L.bptr;

        for(b=0;b<intReal;b++)
        {
        L1.keys[b] = keys[b];
        L1.rids[b] = rids[b];
        L1.entries++;
        }

        for(int j=0;b<int_Real+1;b++,j++)
        {
        L2.keys[j] = keys[b];
        L2.rids[j] = rids[b];
        L2.entries++;
        }

        int numPage = fileHandle.GetNumberOfPages();

        L2.bptr = nodepointer;
        L2.fptr = L1.fptr;
        L1.fptr = numPage;

        char y = 'L';

        memcpy((char*)data,&y,sizeof(char));
        memcpy((char*)data+1,&L1,sizeof(L1));

         fileHandle.WritePage(nodepointer,data);

        memcpy((char*)data,&y,sizeof(char));

        memcpy((char*)data+1,&L2,sizeof(L2));

         fileHandle.AppendPage(data);

        page = numPage;

        newchildentry = L2.keys[0];


        leafnode<T> L3;
if(L2.fptr!=-1){
         fileHandle.ReadPage(L2.fptr,data);

        memcpy(&L3,(char*)data+1,sizeof(L3));

        L3.bptr = L1.fptr;

        memcpy((char*)data+1,&L3,sizeof(L3));

         fileHandle.WritePage(L2.fptr,data);
}
fileHandle.ReadPage(0,data);
memcpy(&root,(int*)data,4);
if(root==nodepointer)
{
                indexnode<T> N3;

                N3.keys[0] = keys[intReal];

                N3.ptrs[0] = nodepointer;
                N3.ptrs[1] = page;

                N3.entries++;

                numPage = fileHandle.GetNumberOfPages();

                char type ='I';

                memcpy((char*)data,&type,sizeof(char));

                memcpy((char*)data+1,&N3,sizeof(N3));
                 fileHandle.AppendPage(data);
                page = numPage;
                void *dataw = malloc(4096);

                 fileHandle.ReadPage(0,dataw);

                memcpy((char*)dataw,&page,4);

                 fileHandle.WritePage(0,dataw);

                free(dataw);
                //set_root(page);
}

        free(keys);
        free(rids);
        free(data);
        return 0;

}
else if(L.entries<int_Real)
{
int s=0;
while(L.keys[s]<=k && L.keys[s]!=-1)
{
        s++;
}

if(s == L.entries)
{
L.keys[L.entries] = k;
L.rids[L.entries] = rid;
}

else
{
for(int j=L.entries;j>s;j--)
{
L.keys[j] = L.keys[j-1];
L.rids[j] = L.rids[j-1];
}

L.keys[s] = k;
L.rids[s] = rid;
}

L.entries++;

memcpy((char*)data+1,&L,sizeof(L));

 fileHandle.WritePage(nodepointer,data);

newchildentry = -1;

free(data);

return 0;
}
 }
}
template<class T>
RC IX_IndexHandle::Insert_new_leaf( T k, const RID &rid,int &page, int fptr, int bptr)
{
        leafnode<T> L;

        L.keys[0] = k;
        L.rids[0] = rid;
        L.entries++;
        L.fptr = fptr;
        L.bptr = bptr;

        void *data = malloc(4096);

        char type = 'L';

        memcpy((char*)data,&type,1);

        memcpy((char*)data+4,&L,sizeof(L));

        int numPage = fileHandle.GetNumberOfPages();
        int x = fileHandle.AppendPage(data);

        page = numPage;

        if(fptr!=-1)
        {


        x = fileHandle.ReadPage(fptr,data);


        memcpy(&L,(char*)data+1,sizeof(L));

        L.bptr = page;

        memcpy((char*)data+1,&L,sizeof(L));

        x = fileHandle.WritePage(fptr,data);

        }

        if(bptr!=-1)
        {
        x = fileHandle.ReadPage(bptr,data);

        memcpy(&L,(char*)data+1,sizeof(L));

        L.fptr = page;

        memcpy((char*)data+1,&L,sizeof(L));

        x = fileHandle.WritePage(bptr,data);
        }

        free(data);

        return 0;
}

template<class T>
RC IX_IndexHandle::Start_new_index(int nodepointer, T key, const RID &rid)
{

leafnode<T> L;

L.keys[L.entries] = key;

L.rids[L.entries] = rid;

L.entries++;

void *data = malloc(4096);

char type = 'L';

memcpy((char*)data,&type,sizeof(char));

memcpy((char*)data+1,&L,sizeof(L));

fileHandle.AppendPage(data);

nodepointer = 1;
void *dataw = malloc(4096);

 fileHandle.ReadPage(0,dataw);

memcpy((char*)dataw,&nodepointer,4);

 fileHandle.WritePage(0,dataw);

free(dataw);

//set_root(nodepointer);

free(data);

return 0;
}
RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)
{
        int keyint;
        float keyfloat;
        int nodepointer,flag=0;
int page=-1;
        void *data = malloc(PF_PAGE_SIZE);

        fileHandle.ReadPage(0,data);

        memcpy(&nodepointer,(int*)data,4);
AttrType r;
memcpy(&r,(char*)data+4,4);
if(r == (AttrType)0)
{
memcpy(&keyint,(char*)key,4);
lazydelete(nodepointer,keyint,rid,page);
}
else if(r == (AttrType)1)
{
memcpy(&keyfloat,(char*)key,4);
lazydelete(nodepointer,keyfloat,rid,page);
}

free(data);

return 0;

}
template <class T>
RC IX_IndexHandle::lazydelete(int nodepointer, T key, const RID rid, int &page)
{
if(nodepointer==-1)
{
return 1;
}
void* data = malloc(4096);
char type;
fileHandle.ReadPage(nodepointer,data);
                memcpy(&type,(char*)data,4);
if(type=='I')
{
        indexnode <T> N;
        memcpy(&N,(char*)data+1,sizeof(N));
        int i=0;
        while(N.keys[i]<=key &&N.keys[i]!=-1)
        {
                i++;
        }
        int nodepointer1 = nodepointer;
        int page1=page;
        nodepointer = N.ptrs[i];
        lazydelete(nodepointer,key,rid,page);
        if(page != -1)
        {

                if(i==0 && N.entries !=1)
                {
                        N.ptrs[i]=-1;
                        for(int j=i;j<N.entries;j++)
                                                        {
                                                        N.keys[j] = N.keys[j+1];
                                                        N.ptrs[j] = N.ptrs[j+1];
                                                        }
                        N.ptrs[N.entries]=-1;
                                                N.keys[N.entries-1]=-1;
                        N.entries--;
                }
                else if(i<N.entries)
                {
                        for(int j=i;j<N.entries;j++)
                                {
                                N.keys[j-1] = N.keys[j];
                                N.ptrs[j] = N.ptrs[j+1];
                                }

                        N.ptrs[N.entries]=-1;
                        N.keys[N.entries-1]=-1;
                        N.entries--;
                }
                else if(i==N.entries)
                {
                        N.ptrs[i]=-1;
                        N.keys[i-1]=-1;
                        N.entries--;
                }
if(N.entries==0)
{
        page1=1;
}
                memcpy((char*)data+1,&N,sizeof(N));
                         fileHandle.WritePage(nodepointer1,data);
                         page=page1;
        }
}
else
{
        leafnode<T> L;
        memcpy(&L,(char*)data+1,sizeof(L));
        int i=0;
        while(L.keys[i]<key)
        {
                i++;
        }
        if(L.keys[i]==key)
        {
        for(int j=i;j<L.entries;j++)
        {
        L.keys[j] = L.keys[j+1];
        L.rids[j] = L.rids[j+1];
        }
        L.entries--;
        if(L.entries ==0)
        {
                page =1;
                memcpy((char*)data+1,&L,sizeof(L));

                fileHandle.WritePage(nodepointer,data);
                free(data);
                return 0;
        }
        else
        {
        memcpy((char*)data+1,&L,sizeof(L));
         fileHandle.WritePage(nodepointer,data);
free(data);
        return 0;
}
        }
        else
        {
                return -1;
        }
}
}
RC IX_IndexScan:: OpenScan(const IX_IndexHandle &indexHandle, void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive)
{

        void* data = malloc(4096);
        //char type;
        CompOpe compOp;
        if(lowKeyInclusive && highKeyInclusive)
        {
                compOp = (CompOpe)2;
        }
        else if (lowKeyInclusive && !highKeyInclusive)
        {
                compOp = (CompOpe)0;
        }
        else if(!lowKeyInclusive && !highKeyInclusive)
        {
                compOp = (CompOpe)3;
        }
        else if (!lowKeyInclusive && highKeyInclusive)
        {
                compOp = (CompOpe)1;
        }
        //PF_FileHandle &fileHandle = &indexHandle.fileHandle;
        PF_FileHandle &fileHandle = const_cast< PF_FileHandle &> ( indexHandle.fileHandle );
        if(!fileHandle.file.is_open())
                return -1;
fileHandle.ReadPage(0,data);
int root;
AttrType t;
AttrLength l;
memcpy(&root,(int*)data,4);
memcpy(&t,(char*)data+4,4);
memcpy(&l,(char*)data+8,4);

if(t==(AttrType)0)
{
        int lowkey=0,highkey=0;
        if(lowKey != NULL)
        memcpy(&lowkey,(char*)lowKey,4);
        if(highKey!=NULL)
        memcpy(&highkey,(char*)highKey,4);
        open( root, compOp, lowkey,highkey, fileHandle);
}
else if(t==(AttrType)1)
{
        float lowkey=0,highkey=0;
                if(lowKey != NULL)
                memcpy(&lowkey,(char*)lowKey,4);
                if(highKey!=NULL)
                memcpy(&highkey,(char*)highKey,4);
        open( root, compOp, lowkey,highkey, fileHandle);
}
else if(t==(AttrType)2)
{
        //      openChar( root, compOp, lowKey,highKey, fileHandle);
}
return 0;
}
template <class T> RC IX_IndexScan::open(int root, CompOpe compOp, T lk, T hk, PF_FileHandle &fileHandle)
{

        void* data = malloc(4096);
        rids.clear();
        fileHandle.ReadPage(root,data);
char type;
int doneFlag=0;
switch (compOp)
{
case (CompOpe)1:
                while(doneFlag!=1)
                                                                 {
                                                                 memcpy(&type,(char*)data,1);
                                                         if(type=='I')

                                                         {
                                                                 indexnode<T> N;
                                                memcpy(&N,(char*)data+1,sizeof(N));
                                                int i=0,j=0;
                                                if(lk !=0)
                                                {
                                                while(N.keys[i] <= lk && N.keys[i] != -1)
                                                {
                                                        i++;
                                                }
                                                }int start =N.ptrs[i];
                                                  fileHandle.ReadPage(start,data);
                                                         }
                                                         else
                                                         {
                                                                 leafnode<T> L;
                                                                 memcpy(&L,(char*)data+1,sizeof(L));
                                                                 int i=0;
                                                                 while(L.keys[i] <lk && L.keys[i] != -1)
                                                                 {
                                                                        i++;
                                                                 }

                        int j=0;
                        if(hk!=0){
                                                                                while(L.keys[j] <= hk && L.keys[j] != -1 &&j<L.entries)
                                                                                                {
                                                                                                        j++;
                                                                                                }
                        }
                        else
                        {
                                j = L.entries;
                        }
                        if(lk==0)
                                i=-1;
                                                                 for(int y=i+1; y<j ;y++)
                                                                         {
                                                                         rids.push_back(L.rids[y]);
                                                                //      cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                                }
                        int limit =1;
                                                                 while(L.fptr !=-1 && limit ==1){
                                        fileHandle.ReadPage(L.fptr,data);
                                        memcpy(&L,(char*)data+1,sizeof(L));
                                        j=0;
                                        if(hk !=0){
                                        while(L.keys[j] <= hk && L.keys[j] != -1 &&j<L.entries)
                                                                                                                {
                                                                                                                        j++;
                                                                                                                }
                                        }
                                        else
                                        {
                                                j=L.entries;
                                        }
                                        for(int y=0; y<j;y++)
                                                                         {
                                                                         rids.push_back(L.rids[y]);
                                                                        // cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                                }
                                        if(hk!=0){
                                        if (j!= L.entries)
                                        {
                                                 limit =0;
                                        }
                                        }
                                        else
                                        {
                                                limit = 1;
                                        }
                                                                 }


                                                                 doneFlag = 1;
                                                         }


                                                 }free(data); break;
case (CompOpe)0:
                while(doneFlag!=1)
                                                                 {
                                                                 memcpy(&type,(char*)data,1);
                                                         if(type=='I')
                                                         {
                                                                 indexnode<T> N;
                                                memcpy(&N,(char*)data+1,sizeof(N));
                                                int i=0,j=0;
                                                if(lk !=0)
                                                {
                                                while(N.keys[i] <= lk && N.keys[i] != -1)
                                                {
                                                        i++;
                                                }
                                                }int start =N.ptrs[i];
                                                  fileHandle.ReadPage(start,data);
                                                         }
                                                         else
                                                         {
                                                                 leafnode<T> L;
                                                                 memcpy(&L,(char*)data+1,sizeof(L));
                                                                 int i=0;
                                                                 if(lk!=0)
                                                                 {
                                                                 while(L.keys[i] <lk && L.keys[i] != -1 )
                                                                 {
                                                                        i++;
                                                                 }
                                                         }
                        int j=0;
                        if(hk!=0){
                                                                                while(L.keys[j] <= hk && L.keys[j] != -1 && j<L.entries)
                                                                                                {
                                                                                                        j++;
                                                                                                }
                        }
                        else
                        {
                                j = L.entries;
                        }

                                                                 for(int y=i; y<j ;y++)
                                                                         {
                                                                         rids.push_back(L.rids[y]);
                                                                //      cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                                }
                        int limit =1;
                                                                 while(L.fptr !=-1 && limit ==1){
                                        fileHandle.ReadPage(L.fptr,data);
                                        memcpy(&L,(char*)data+1,sizeof(L));
                                        j=0;
                                        if(hk !=0){
                                        while(L.keys[j] <= hk && L.keys[j] != -1 && j<L.entries)
                                                                                                                {
                                                                                                                        j++;
                                                                                                                }
                                        }
                                        else
                                        {
                                                j=L.entries;
                                        }
                                        for(int y=0; y<j;y++)
                                                                         {
                                                                         rids.push_back(L.rids[y]);
                                                                        // cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                                }
                                        if(hk!=0){
                                        if (j!= L.entries)
                                        {
                                                 limit =0;
                                        }
                                        }
                                        else
                                        {
                                                limit = 1;
                                        }
                                                                 }


                                                                 doneFlag = 1;
                                                         }


                                                 }free(data); break;
case (CompOpe)2:

                while(doneFlag!=1)
                                                                 {
                                                                 memcpy(&type,(char*)data,1);
                                                         if(type=='I')
                                                         {
                                                                 indexnode<T> N;
                                                memcpy(&N,(char*)data+1,sizeof(N));
                                                int i=0,j=0;
                                                if(lk !=0)
                                                {
                                                while(N.keys[i] <= lk && N.keys[i] != -1)
                                                {
                                                        i++;
                                                }
                                                }int start =N.ptrs[i];
                                                  fileHandle.ReadPage(start,data);
                                                         }
                                                         else
                                                         {
                                                                 leafnode<T> L;
                                                                 memcpy(&L,(char*)data+1,sizeof(L));
                                                                 int i=0;
                                                                 if(lk==0)
                                                                 {
                                                                 while(L.keys[i] <lk && L.keys[i] != -1)
                                                                 {
                                                                        i++;
                                                                 }}

                        int j=0;
                        if(hk!=0){
                                                                                while(L.keys[j] < hk && L.keys[j] != -1 && j<L.entries)
                                                                                                {
                                                                                                        j++;
                                                                                                }
                        }
                        else
                        {
                                j = L.entries-1;
                        }

                                                                 for(int y=i; y<j+1 ;y++)
                                                                         {
                                                                         rids.push_back(L.rids[y]);
                                                                //      cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                                }
                        int limit =1;
                                                                 while(L.fptr !=-1 && limit ==1){
                                        fileHandle.ReadPage(L.fptr,data);
                                        memcpy(&L,(char*)data+1,sizeof(L));
                                        j=0;
                                        if(hk !=0){
                                        while(L.keys[j] <= hk && L.keys[j] != -1 && j<L.entries)
                                                                                                                {
                                                                                                                        j++;
                                                                                                                }
                                        }
                                        else
                                        {
                                                j=L.entries;
                                        }
                                        for(int y=0; y<j;y++)
                                                                         {
                                                                         rids.push_back(L.rids[y]);
                                                                        // cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                                }
                                        if(hk!=0){
                                        if (j!= L.entries)
                                        {
                                                 limit =0;
                                        }
                                        }
                                        else
                                        {
                                                limit = 1;
                                        }
                                                                 }


                                                                 doneFlag = 1;
                                                         }


                                                 }free(data); break;
case (CompOpe)3:
         while(doneFlag!=1)
                                                 {
                                                 memcpy(&type,(char*)data,1);
                                         if(type=='I')
                                         {
                                                 indexnode<T> N;
                                memcpy(&N,(char*)data+1,sizeof(N));
                                int i=0,j=0;
                                if(lk !=0)
                                {
                                while(N.keys[i] <= lk && N.keys[i] != -1)
                                {
                                        i++;
                                }
                                }int start =N.ptrs[i];
                                  fileHandle.ReadPage(start,data);
                                         }
                                         else
                                         {
                                                 leafnode<T> L;
                                                 memcpy(&L,(char*)data+1,sizeof(L));
                                                 int i=0;
                                                 while(L.keys[i] <lk && L.keys[i] != -1)
                                                 {
                                                        i++;
                                                 }

        int j=0;
        if(hk!=0){
                                                                while(L.keys[j] <= hk && L.keys[j] != -1 && j<L.entries)
                                                                                {
                                                                                        j++;
                                                                                }
        }
        else
        {
                j = L.entries;
        }
        if(lk==0)
                i=-1;
                                                 for(int y=i+1; y<j ;y++)
                                                         {
                                                         rids.push_back(L.rids[y]);
                                                //      cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                }
        int limit =1;
        int v =1;
                                                 while(L.fptr !=-1 && limit ==1){
                                                        // cout<<"pg"<<v++<<endl;
                        fileHandle.ReadPage(L.fptr,data);
                        memcpy(&L,(char*)data+1,sizeof(L));
                        j=0;
                        if(hk !=0){
                        while(L.keys[j] <= hk && L.keys[j] != -1 &&j<L.entries)
                                                                                                {
                                                                                                        j++;
                                                                                                }
                        }
                        else
                        {
                                j=L.entries;
                        }
                        for(int t=0; t<j;t++)
                                                         {
                                                         rids.push_back(L.rids[t]);
                                                        //cout<<"rids"<<L.rids[y].slotNum<<":"<<L.rids[y].pageNum<<endl;
                                }
                        if(hk!=0){
                        if (j!= L.entries)
                        {
                                 limit =0;
                        }
                        else {
                                limit =1;
                        }
                        }
                        else
                        {
                                limit = 1;
                        }
                                                 }

//cout<<"ridsize"<<rids.size()<<endl;
                                                 doneFlag = 1;
                                         }


                                 }free(data); break;
}

return 0;
}
RC IX_IndexScan::GetNextEntry(RID &rid)
{
        if (i == (int) rids.size())
                        return IX_EOF;
                else {

                        memcpy(&rid,&rids.at(i),sizeof(rid));
                        i++;
                        return 0;
                }

}
RC IX_IndexScan::CloseScan()
{
        i=0;
        rids.clear();
        return 0;
}

