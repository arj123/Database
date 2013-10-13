

//#include "pf.cc"
//#include "..pf/pf.h"
#include "rm.h"
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>
#include <vector>
#include <iostream>
using namespace std;
RM* RM::_rm = 0;

RM* RM::Instance() {
        if (!_rm)
                _rm = new RM();

        return _rm;
}

RM::RM() {
        //fstream file1;
        //remove("table");
                //remove("columns");
        //remove("tbl_employee4");
        pf = PF_Manager::Instance();
        createCatalog();
        //remove("table");
        //remove("columns");
}

RM::~RM() {
}
/*sets slot directory value on the page pointed to by the pointer*/
RC RM::setslotdirectory(void* data, slotDirectory &sd) {
        int a = 4;
        int counter = 4092;
        memcpy((char*) data + counter, &sd.free_offset, 4);
        counter = counter - a;
        memcpy((char*) data + counter, &sd.entries, 4);
        counter = counter - a;
        for (int l = 0; l < sd.entries; l++) {
                memcpy((char*) data + counter, &sd.s[l].offset, 4);
                counter = counter - a;
                memcpy((char*) data + counter, &sd.s[l].start, 4);
                counter = counter - a;
        }
        return 0;
}
/*gets the slots directory from a oage and populates the struct.*/
RC RM::getslotdirectory(void* data, slotDirectory &sd) {

        int a = 4;
        int counter = 4092;
        //free(sd.s);
        memcpy(&sd.free_offset, (char*) data + counter, 4);
        counter = counter - a;
        memcpy(&sd.entries, (char*) data + counter, 4);
        //cout<<"entries"<<sd.entries<<endl;
        //cout<<sd.free_offset<<endl;
        counter = counter - a;
        sd.s = new slot[sd.entries];
        for (int l = 0; l < sd.entries; l++) {
                memcpy(&sd.s[l].offset, (char*) data + counter, 4);
                //cout<<sd.s[l].offset<<endl;
                counter = counter - a;
                memcpy(&sd.s[l].start, (char*) data + counter, 4);
                //cout<<"start"<<sd.s[l].start<<endl;
                counter = counter - a;
        }

        return 0;
}
/*gets the tuple size from the attributes of the table */
int RM::gettuplesize(vector<Attribute>&attrs, const void* data) {

        int varcharlengths;
        int tuplesize = 0;
        //int offset = 0;
        for (int y = 0; y < (int) attrs.size(); y++) {
                AttrType u = attrs.at(y).type;
                if (u == TypeVarChar) {
                        memcpy(&varcharlengths, (char *) data + tuplesize, sizeof(int));
                        tuplesize = tuplesize + sizeof(int) + varcharlengths;

                        //tuplesize = tuplesize + 4 + varcharlengths;
                } else {
                        tuplesize += sizeof(int);
                        //tuplesize = tuplesize + 4;
                }
        }
        return tuplesize;
}
/*Insert a created tuple into the catalog tables table and columns when a table is created.*/
RC RM::insertTuple_catalog(const string tableName, const void *data, RID &rid,
                int tuplesize) {

        PF_FileHandle FileHandle;
        const char* table = (char*) tableName.c_str();
        slotDirectory sd;

        void* readpage = malloc(4096);
        int rc = pf->OpenFile(table, FileHandle);
        if (rc == -1)
                return -1;
        unsigned pages = FileHandle.GetNumberOfPages();
        int flag = 0;
        if (pages == 0) {

                sd.free_offset = tuplesize;
                sd.entries = 1;
                sd.s = new slot[sd.entries];
                memcpy((char*) readpage, (char*) data, tuplesize);
                sd.s[0].offset = tuplesize;
                sd.s[0].start = 0;
                memcpy((char*) readpage + 4092, &tuplesize, 4);
                memcpy((char*) readpage + 4088, &sd.entries, 4);
                memcpy((char*) readpage + 4084, &sd.s[0].offset, 4);
                memcpy((char*) readpage + 4080, &sd.s[0].start, 4);
                rid.slotNum = 0;
                rid.pageNum = 0;
                FileHandle.WritePage(0, readpage);
                free(sd.s);

        } else {
                for (unsigned j = 0; j < pages && flag != 1; j++) {
                        slotDirectory sd4;

                        FileHandle.ReadPage(j, readpage);

                        getslotdirectory(readpage, sd4);
                        int Actualfreespace = 4096 - 8 * (sd4.entries + 2)
                                        - sd4.free_offset;
                        if (Actualfreespace >= tuplesize) {

                                memcpy((char*) readpage + sd4.free_offset, data, tuplesize);
                                int i = 0;
                                while (sd4.s[i].start != -1 && i != sd4.entries) {
                                        i++;
                                }

                                if (i == sd4.entries) {

                                        int offset = 4096 - 8 * (sd4.entries + 1) - 4;
                                        memcpy((char*) readpage + offset, &tuplesize, 4);
                                        offset -= 4;
                                        memcpy((char*) readpage + offset, &sd4.free_offset, 4);
                                        int a = 4;
                                        int counter = 4092;
                                        sd4.entries += 1;
                                        sd4.free_offset += tuplesize;
                                        memcpy((char*) readpage + counter, &sd4.free_offset, 4);
                                        counter = counter - a;
                                        memcpy((char*) readpage + counter, &sd4.entries, 4);
                                        counter = counter - a;
                                }

                                else {
                                        sd4.s[i].start = sd4.free_offset;
                                        sd4.s[i].offset = tuplesize;
                                        int offset = 4096 - 8 * (i + 1) - 4;
                                        memcpy((char*) readpage + offset, &tuplesize, 4);
                                        offset -= 4;
                                        memcpy((char*) readpage + offset, &sd4.free_offset, 4);

                                        sd4.free_offset += tuplesize;
                                        memcpy((char*) readpage + 4092, &sd4.free_offset, 4);

                                }
                                rc = FileHandle.WritePage(j, readpage);
                                free(sd4.s);

                                flag = 1;
                                rid.slotNum = i;
                                rid.pageNum = j;
                        }
                }
                if (flag != 1) {
                        void *datafree = malloc(4096);

                        sd.free_offset = 0;
                        sd.entries = 1;
                        memcpy((char*) datafree, (char*) data, tuplesize);
                        sd.s = new slot[sd.entries];
                        sd.s[0].offset = tuplesize;
                        sd.s[0].start = sd.free_offset;
                        sd.free_offset = sd.s[0].start + sd.s[0].offset;
                        //setslotdirectory(datafree, sd);
                        memcpy((char*) datafree + 4092, &tuplesize, 4);
                        memcpy((char*) datafree + 4088, &sd.entries, 4);
                        memcpy((char*) readpage + 4084, &sd.s[0].offset, 4);
                        memcpy((char*) readpage + 4080, &sd.s[0].start, 4);
                        FileHandle.AppendPage(datafree); //appending a page
                        free(sd.s);
                        rid.slotNum = 0;
                        rid.pageNum = pages;

                        free(datafree);
                }
        }
        free(readpage);
        rc = pf->CloseFile(FileHandle); //close the file

        return rc;

}
/*inserts a tuple into the table and returns an rid*/
RC RM::insertTuple(const string tableName, const void *data, RID &rid) {

        PF_FileHandle FileHandle;
        const char* table = (char*) tableName.c_str();
        vector<Attribute> attrs;
        slotDirectory sd;
        getAttributes(tableName, attrs);

        int tuplesize = gettuplesize(attrs, data);
        //cout << "tuplesize" << tuplesize << endl;
        void* readpage = malloc(4096);
        int rc = pf->OpenFile(tableName.c_str(), FileHandle);
        if (rc == -1)
                return -1;
        unsigned pages = FileHandle.GetNumberOfPages();
        int flag = 0;
        if (pages == 0) {

                sd.free_offset = tuplesize;
                sd.entries = 1;
                sd.s = new slot[sd.entries];
                memcpy((char*) readpage, (char*) data, tuplesize);
                sd.s[0].offset = tuplesize;
                sd.s[0].start = 0;
                memcpy((char*) readpage + 4092, &tuplesize, 4);
                memcpy((char*) readpage + 4088, &sd.entries, 4);
                memcpy((char*) readpage + 4084, &sd.s[0].offset, 4);
                memcpy((char*) readpage + 4080, &sd.s[0].start, 4);
                rid.slotNum = 0;
                rid.pageNum = 0;
                FileHandle.WritePage(0, readpage);
                free(sd.s);

        } else {
                for (unsigned j = 0; j < pages && flag != 1; j++) {
                        slotDirectory sd4;

                        FileHandle.ReadPage(j, readpage);

                        getslotdirectory(readpage, sd4);
                        int Actualfreespace = 4096 - 8 * (sd4.entries + 2)
                                        - sd4.free_offset;
                        if (Actualfreespace >= tuplesize) {

                                memcpy((char*) readpage + sd4.free_offset, data, tuplesize);
                                int i = 0;
                                while (i != sd4.entries) {
                                        i++;
                                }

                                if (i == sd4.entries) {

                                        int offset = 4096 - 8 * (sd4.entries + 1) - 4;
                                        memcpy((char*) readpage + offset, &tuplesize, 4);
                                        offset -= 4;
                                        memcpy((char*) readpage + offset, &sd4.free_offset, 4);
                                        int a = 4;
                                        int counter = 4092;
                                        sd4.entries += 1;
                                        sd4.free_offset += tuplesize;
                                        memcpy((char*) readpage + counter, &sd4.free_offset, 4);
                                        counter = counter - a;
                                        memcpy((char*) readpage + counter, &sd4.entries, 4);
                                        counter = counter - a;
                                }

                                /*else
                                 {
                                 sd4.s[i].start = sd4.free_offset;
                                 sd4.s[i].offset = tuplesize;
                                 int offset = 4096 - 8 * (i + 1) - 4;
                                 memcpy((char*) readpage + offset,&tuplesize , 4);
                                 offset -= 4;
                                 memcpy((char*) readpage + offset,&sd4.free_offset, 4);
                                 sd4.free_offset += tuplesize;
                                 memcpy((char*) readpage + 4092, &sd4.free_offset, 4);

                                 }*/
                                rc = FileHandle.WritePage(j, readpage);
                                free(sd4.s);

                                flag = 1;

                                rid.slotNum = i;
                                rid.pageNum = j;
                        }
                }
                if (flag != 1) {

                        sd.free_offset = 0;
                        sd.entries = 1;
                        memcpy((char*) readpage, (char*) data, tuplesize);
                        sd.s = new slot[sd.entries];
                        sd.s[0].offset = tuplesize;
                        sd.s[0].start = sd.free_offset;
                        sd.free_offset = sd.s[0].start + sd.s[0].offset;
                        memcpy((char*) readpage + 4092, &tuplesize, 4);
                        memcpy((char*) readpage + 4088, &sd.entries, 4);
                        memcpy((char*) readpage + 4084, &sd.s[0].offset, 4);
                        memcpy((char*) readpage + 4080, &sd.s[0].start, 4);
                        FileHandle.AppendPage(readpage);
                        //              int pagges=FileHandle.GetNumberOfPages();
                        free(sd.s);
                        rid.slotNum = 0;
                        rid.pageNum = pages;

                }
        }
        free(readpage);
        rc = pf->CloseFile(FileHandle);

        return rc;

}
RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {
        PF_FileHandle FileHandle;
        slotDirectory sd;
        void* readpage = malloc(4096);
        void* readpage2 = malloc(4096);
        int orgpage = rid.pageNum;
        int rc = pf->OpenFile(tableName.c_str(), FileHandle);
        FileHandle.ReadPage(orgpage, readpage);
        int pages = FileHandle.GetNumberOfPages();
        int u = rid.slotNum;
        int Updateoffset;
        int Updatestart;
        int a = 4;
        int updateindex;
        //int x=rid.pageNum;
        //int y=rid.slotNum;
        //int counter = 4092;
        //memcpy(&sd.free_offset, (char*) readpage + counter, 4);
        //counter = counter - a;
        //memcpy(&sd.entries, (char*) readpage + counter, 4);
        //counter = counter - a;
        getslotdirectory(readpage, sd);
        //cout<<"1"<<endl;
        if (sd.s[u].start == -1)
                return -1;
        if (sd.s[u].start >= 4096 || sd.s[u].start < 0) { //deleting an updated value.

        //free(readpage);
                void *readpag = malloc(4096);
                struct slotDirectory sd2;
                void* datar = malloc(4096);
                int offsetonupdatepage = abs(sd.s[u].start) % 4096;
                int updatespagedifference = abs(sd.s[u].start) / 4096; // losea the decimal bit
                //otherwise just call delete here. Deals with 4096 delete also.
                int pagenumber;
                if (sd.s[u].start > 0)
                        pagenumber = updatespagedifference;
                else {
                        pagenumber = (updatespagedifference);
                        //offsetonupdatepage = 4096 - offsetonupdatepage;
                }
                FileHandle.ReadPage(pagenumber, (char*) readpag);
                rc = getslotdirectory(readpag, sd2);
                int p = 0;
                while (sd2.s[p].start != offsetonupdatepage) {
                        p++;
                }
                //cout << "sss1" << sd.s[u].start << " " << updatespagedifference << " "
                //      << offsetonupdatepage << endl;
                RID rid1;
                rid1.slotNum = p;
                rid1.pageNum = pagenumber;
                free(readpag);
                free(sd2.s);
                free(sd.s);
                rc = updateTuple(tableName, data, rid1);
                free(readpage);
                rc = pf->CloseFile(FileHandle);
                return rc;
        }
        updateindex = 4096 - 8 * (rid.slotNum + 1) - 4;
        //Updateoffset = sd.s[u].offset;
        //Updatestart = sd.s[u].start;
        vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        int tuplesize = gettuplesize(attrs, data);
        //cout << " tuple size:" << tuplesize << endl;
        if (tuplesize <= sd.s[u].offset) {
                //      cout << "11" << endl;
                sd.s[u].offset = tuplesize;
                memcpy((char*) readpage + sd.s[u].start, (char*) data, tuplesize);
                memcpy((char*) readpage + updateindex, &sd.s[u].offset, 4);
                rc = FileHandle.WritePage(rid.pageNum, readpage);
                if (rc == -1)
                        return -1;
                free(readpage);
                free(sd.s);
                rc = pf->CloseFile(FileHandle);
                return rc;
        } else if ((4096 - (sd.entries + 2) * 8 - sd.free_offset) > tuplesize) {
                //      cout << "22" << endl;
                sd.s[u].offset = tuplesize;
                sd.s[u].start = sd.free_offset;
                sd.free_offset += tuplesize;
                memcpy((char*) readpage + sd.s[u].start, (char*) data, tuplesize);
                memcpy((char*) readpage + updateindex, &sd.s[u].offset, 4);
                updateindex = updateindex - a;
                memcpy((char*) readpage + updateindex, &sd.s[u].start, 4);
                memcpy((char*) readpage + 4092, &sd.free_offset, 4);
                memcpy((char*) readpage + 4088, &sd.entries, 4);
                FileHandle.WritePage(rid.pageNum, readpage);
                free(readpage);
                free(sd.s);
                rc = pf->CloseFile(FileHandle);
                return rc;
        } else {
                //cout << "33 :" << tuplesize << endl;

                int flag = 0;
                for (unsigned j = 0; j < pages && flag != 1; j++) {
                        slotDirectory sd4;

                        FileHandle.ReadPage(j, readpage2);

                        getslotdirectory(readpage2, sd4);
                        //cout << "up" << endl;
                        int Actualfreespace = 4096 - 8 * (sd4.entries + 2)
                                        - sd4.free_offset;
                        if (Actualfreespace >= tuplesize) {

                                int offset = 4096 - 8 * (sd4.entries + 1) - 4;
                                memcpy((char*) readpage2 + offset, &tuplesize, 4);
                                offset -= 4;
                                memcpy((char*) readpage2 + offset, &sd4.free_offset, 4);
                                int a = 4;
                                int counter = 4092;
                                sd4.entries += 1;
                                memcpy((char *) readpage2 + sd4.free_offset, data, tuplesize);
                                sd4.free_offset += tuplesize;
                                memcpy((char*) readpage2 + counter, &sd4.free_offset, 4);
                                counter = counter - a;
                                memcpy((char*) readpage2 + counter, &sd4.entries, 4);
                                counter = counter - a;
                                rc = FileHandle.WritePage(j, readpage2);
                                int index = 4096 - (rid.slotNum + 2) * 8; //1 or 2

                                int pagev = 4096 * j + sd4.free_offset - tuplesize; //-4096 ;

                                if (j < rid.pageNum) {
                                        pagev = -pagev;
                                }
                                memcpy((char*) readpage + index, &pagev, 4);

                                rc = FileHandle.WritePage(rid.pageNum, readpage);
                                if (rc == -1)
                                        return -1;

                                free(sd4.s);
                                free(readpage2);
                                free(readpage);
                                flag = 1;
                                free(sd.s);
                                rc = pf->CloseFile(FileHandle);
                                return rc;

                        }
                        free(sd4.s);
                }
                if (flag != 1) {

                        //cout << "1flag" << tuplesize << endl;
                        int free_offset = 0;
                        int entries = 1;
                        free(readpage2);
                        readpage2 = malloc(4096);
                        memcpy((char*) readpage2 + free_offset, (char*) data, tuplesize);

                        memcpy((char*) readpage2 + 4092, &tuplesize, 4);
                        memcpy((char*) readpage2 + 4088, &entries, 4);
                        memcpy((char*) readpage2 + 4084, &tuplesize, 4);
                        memcpy((char*) readpage2 + 4080, &free_offset, 4);

                        rc = FileHandle.AppendPage(readpage2);
                        int index = 4096 - (rid.slotNum + 2) * 8;

                        int pagev = 4096 * (pages) + free_offset;
                        if (pages < rid.pageNum) {
                                pagev = -pagev;
                        }
                        memcpy((char*) readpage + index, &pagev, 4);
                        FileHandle.WritePage(rid.pageNum, readpage);

                        free(sd.s);
                        free(readpage2);
                        free(readpage);

                        rc = pf->CloseFile(FileHandle);
                        return rc;

                }

        }

}

/*54s the table to obtain all the relvant tuples for a function*/
RC RM::scan(const string tableName, const string conditionAttribute,
                const CompOp compOp, const void *value,
                const vector<string> &attributeNames,
                RM_ScanIterator &rm_ScanIterator) {
        rm_ScanIterator.projections.clear();
        rm_ScanIterator.allrec.clear();
        rm_ScanIterator.i = 0;
        rm_ScanIterator.offsets.clear();
        PF_FileHandle fileHandle;
        int rc = pf->OpenFile(tableName.c_str(), fileHandle);
        if (rc == -1)
                return rc;
        slotDirectory sd;
        unsigned pages = fileHandle.GetNumberOfPages();
        vector<Attribute> attr;
        vector<Attribute> attrs;
        vector<int> offsets;
        void *readpage = malloc(4096);
        void *compattr = malloc(1000);
        void *data = malloc(4096);
        int length = 0;
        RID rid;
        vector<char *> projections;
        int x;
        int z;

        vector<RID> allrec;
        vector<RID> rec;
        RID rid1;
        int flag = 0;
        int flag1 = 0;
        rc = getAttributes(tableName, attr);
        for (int i = 0; i < attr.size(); i++) {
                for (int j = 0; j < attributeNames.size(); j++) {
                        if (attr.at(i).name == attributeNames.at(j)) {
                                attrs.push_back(attr.at(i));
                        }
                }
        }
        int offset = 0;
        int offsetnew = 0;
        //int m;
        void *attrdata = malloc(1000);
        for (unsigned i = 0; i < pages; i++) {

                //cout<<"entries :"<<sd.entries<<endl;
                fileHandle.ReadPage(i, readpage);
                getslotdirectory(readpage, sd);
                //cout<<"entries :"<<sd.entries<<endl;
                for (int l = 0; l < sd.entries; l++) {

                        offset = 0;
                        flag = 0;
                        rid.pageNum = i;
                        rid.slotNum = l;
                        //m=sd.s[l].start;
                        //readTuple(tableName,rid,data);
                        if (sd.s[l].start == -1) {
                                continue;
                        }
                        if (sd.s[l].start > 4096 || sd.s[l].start < 0) {
                                //cout<<"hereee"<<endl;
                                slotDirectory sd2;
                                void *readpage1 = malloc(4096);
                                int offsetonupdatepage = abs(sd.s[l].start) % 4096;
                                int updatespagedifference = abs(sd.s[l].start) / 4096;
                                int p = 0;
                                fileHandle.ReadPage(updatespagedifference, readpage1);
                                getslotdirectory(readpage1, sd2);
                                while (sd2.s[p].start != offsetonupdatepage) {
                                        p++;
                                }
                                memcpy((char *) readpage, (char *) readpage1, 4096);
                                sd.s[l].start = sd2.s[p].start;
                                sd.s[l].offset = sd2.s[p].offset;
                                free(readpage1);
                                free(sd2.s);
                        }
                        memcpy((char*) attrdata, (char*) readpage + sd.s[l].start,
                                        sd.s[l].offset);
                        for (unsigned n = 0; n < attr.size() && flag != 1; n++) {
                                flag = 0;
                                if (conditionAttribute.length() == 0) {
                                        //cout<<"here:"<<endl;
                                        flag = 1;
                                } else if (attr.at(n).name.compare(conditionAttribute) == 0) {
                                        //void *comp=malloc(1000);
                                        if (attr.at(n).type == 2) {
                                                void *comp = malloc(1000);
                                                //cout << "type " << attrs.at(n).type << endl;
                                                memcpy(&length, (char*) attrdata + offset, sizeof(int));
                                                offset = offset + 4;
                                                memcpy((char *) comp, (char*) attrdata + offset,
                                                                length);
                                                offset = offset + length;
                                                int y = strcmp((char *) comp, (char *) value);
                                                switch (compOp) {
                                                case 0:
                                                        if (y == 0)
                                                                flag = 1;
                                                        break;
                                                case 1:
                                                        if (y < 0)
                                                                flag = 1;

                                                        break;
                                                case 2:
                                                        if (y > 0)
                                                                flag = 1;
                                                        break;
                                                case 3:
                                                        if (y <= 0)
                                                                flag = 1;
                                                        break;
                                                case 4:
                                                        if (y >= 0)
                                                                flag = 1;
                                                        break;
                                                case 5:
                                                        if (y != 0)
                                                                flag = 1;
                                                        break;
                                                case 6:
                                                        flag = 1;
                                                        break;

                                                }
                                                free(comp);

                                        } else {

                                                memcpy(&z, (char*) attrdata + offset, sizeof(int));
                                                offset = offset + 4;
                                                memcpy(&x, (char *) value, sizeof(int));

                                                switch (compOp) {
                                                case 0:
                                                        if (z == x)
                                                                flag = 1;
                                                        break;
                                                case 1:
                                                        if (z < x)
                                                                flag = 1;

                                                        break;
                                                case 2:
                                                        if (z > x)
                                                                flag = 1;
                                                        break;
                                                case 3:
                                                        if (z <= x)
                                                                flag = 1;
                                                        break;
                                                case 4:
                                                        if (z >= x)
                                                                flag = 1;
                                                        break;
                                                case 5:
                                                        if (z != x)
                                                                flag = 1;
                                                        break;
                                                case 6:
                                                        flag = 1;
                                                        break;
                                                }
                                        }

                                } else {
                                        if (attr.at(n).type == 2) {
                                                memcpy(&x, (char*) attrdata + offset, sizeof(int));
                                                offset += sizeof(int);
                                                offset += x;

                                        } else {
                                                offset += sizeof(int);
                                        }
                                }

                        }
                        if (flag == 0)
                                continue;
                        if (flag == 1) {
                                offset = 0;
                                offsetnew = 0;

                                allrec.push_back(rid);
                                for (int k = 0; k < (int) attr.size(); k++) {
                                        flag1 = 0;
                                        //cout<<"TEST:"<<attrs.at(k).name<<endl;
                                        for (int j = 0; j < attributeNames.size() && flag1 == 0;
                                                        j++) {
                                                if (attr.at(k).name == attributeNames.at(j)) {
                                                        flag1 = 1;
                                                }
                                        }

                                        if (flag1 == 1) {

                                                if (attr.at(k).type == TypeVarChar) {
                                                        memcpy(&x, (char*) attrdata + offset, sizeof(int));
                                                        offset += sizeof(int);

                                                        memcpy((char*) data + offsetnew, &x, sizeof(int));
                                                        offsetnew += sizeof(int);

                                                        memcpy((char*) data + offsetnew,
                                                                        (char*) attrdata + offset, x);
                                                        offset += x;
                                                        offsetnew += x;
                                                } else {

                                                        memcpy((char*) data + offsetnew,
                                                                        (char*) attrdata + offset, sizeof(int));

                                                        offset += sizeof(int);
                                                        offsetnew += sizeof(int);
                                                }
                                        } else {
                                                if (attr.at(k).type == TypeVarChar) {
                                                        memcpy(&x, (char*) attrdata + offset, sizeof(int));
                                                        offset += sizeof(int);
                                                        offset += x;
                                                } else {
                                                        offset += sizeof(int);
                                                }
                                        }

                                }
                                void * tempCopy = malloc(offsetnew);
                                memcpy((char *) tempCopy, (char *) data, offsetnew);
                                //cout << "projections :" << *(int *) tempCopy;
                                projections.push_back((char *) tempCopy);
                                //cout<<"offs new :"<<offsetnew<<endl;
                                offsets.push_back(offsetnew);

                        }
                }

        }
        rm_ScanIterator.projections = projections;
        rm_ScanIterator.offsets = offsets;
        rm_ScanIterator.allrec = allrec;
        free(attrdata);
        free(readpage);
        free(data);
        //free(projection_cstring);
        rc = pf->CloseFile(fileHandle);
        //cout << " over " << endl;
        return rc;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        int offset = 0;

        if (i == (int) allrec.size())
                return RM_EOF;
        rid.pageNum = allrec.at(i).pageNum;
        rid.slotNum = allrec.at(i).slotNum;

        memcpy(data, projections.at(i), offsets.at(i));

        i++;

        return 0;
}
/*reads a tuple into a void buffer using an rid identifier*/
RC RM::readTuple(const string tableName, const RID &rid, void *data) {
        PF_FileHandle FileHandle;
        slotDirectory sd;
        void* readpage5 = malloc(4096);
        int orgpage = rid.pageNum;
        //cout<<"pa:"<<rid.pageNum<<"sl:"<<rid.slotNum<<endl;
        int rc = pf->OpenFile(tableName.c_str(), FileHandle);
        if (rc == -1)
                return rc;
        rc = FileHandle.ReadPage(orgpage, (char*) readpage5);
        if (rc == -1)
                return rc;
        if (FileHandle.GetNumberOfPages() == 0)
                return -1;
        //cout<<FileHandle.GetNumberOfPages()<<endl;
        getslotdirectory(readpage5, sd);
        //cout<<"read"<<endl;
        //cout<<sd.free_offset<<endl;
        if (sd.entries == 0)
                return -1;
        int y = rid.slotNum;
        if (sd.s[y].start == -1)
                return -1;

        if (sd.s[y].start >= 4096 || sd.s[y].start < 0) { //deleting an updated value.

                void *readpagee = malloc(4096);
                slotDirectory sd2;

                int offsetonupdatepage = abs(sd.s[y].start) % 4096;
                int updatespagedifference = abs(sd.s[y].start) / 4096; // losea the decimal bit
                //otherwise just call delete here. Deals with 4096 delete also.

                int pagenumber;
                if (sd.s[y].start > 0)
                        pagenumber = updatespagedifference;
                else {
                        pagenumber = (updatespagedifference);
                        //offsetonupdatepage =offsetonupdatepage;
                }
                FileHandle.ReadPage(pagenumber, readpagee);
                rc = getslotdirectory(readpagee, sd2);

                int p = 0;

                while (sd2.s[p].start != offsetonupdatepage && p < sd2.entries) {
                        p++;
                }
                //cout<<"slot no"<<offsetonupdatepage<<" "<<pagenumber<<" "<<p<<" "<<sd2.entries<<" "<<sd2.free_offset<<" "<<sd2.s[p].start<<" "<<sd2.s[p].offset<<endl;
                RID rid1;
                rid1.slotNum = p;
                rid1.pageNum = pagenumber;
                free(readpagee);
                free(sd2.s);
                free(sd.s);
                rc = readTuple(tableName, rid1, data);
                free(readpage5);
                rc = pf->CloseFile(FileHandle);
                return rc;
        }

        memcpy((char*) data, (char*) readpage5 + sd.s[y].start, sd.s[y].offset);

        free(readpage5);
        free(sd.s);
        rc = pf->CloseFile(FileHandle);

        return rc;
}
/*Reads a specified attribute of the tuple whos rid is specified*/
RC RM::readAttribute(const string tableName, const RID &rid,
                const string attributeName, void *data) {
        PF_FileHandle FileHandle;
        slotDirectory sd;
        vector<Attribute> attrs;
        attrs.clear();

        int flag = 0;
        int varcharlength = 0;
        int tuplesize = 0;

        void* readpage = malloc(4096);
        int orgpage = rid.pageNum;
        int rc = pf->OpenFile(tableName.c_str(), FileHandle);
        if (rc == -1)
                return -1;
        FileHandle.ReadPage(orgpage, (char*) readpage);
        rc = getslotdirectory(readpage, sd);

        int offset1 = sd.s[rid.slotNum].start;

        getAttributes(tableName, attrs);

        for (int p = 0; p < sd.entries && (flag != 1); p++) {

                if (p == (int) rid.slotNum) {
                        for (int k = 0; k < (int) attrs.size() && (flag != 1); k++) {
                                if (attrs.at(k).name.compare(attributeName) != 0) {

                                        AttrType z = attrs.at(k).type;

                                        if (z == TypeVarChar) {
                                                memcpy(&varcharlength, (char *) readpage + offset1, 4);
                                                offset1 = offset1 + 4 + varcharlength;
                                                tuplesize = tuplesize + 4 + varcharlength;
                                        } else {
                                                offset1 += 4;
                                                tuplesize = tuplesize + 4;
                                        }
                                } else {
                                        if (attrs.at(k).type == TypeVarChar)

                                        {
                                                memcpy(&varcharlength, (char*) readpage + offset1, 4);
                                                offset1 = offset1 + 4;
                                                tuplesize = tuplesize + varcharlength;
                                                memcpy((char *) data, (char*) readpage + offset1,
                                                                varcharlength);
                                        } else {

                                                memcpy((char *) data, (char *) readpage + offset1,
                                                                sizeof(int));
                                                int u;
                                                memcpy(&u, (char *) data, sizeof(int));

                                        }

                                        flag = 1;

                                }
                        }
                }

        }

        attrs.clear();
        free(sd.s);
        free(readpage);
        rc = pf->CloseFile(FileHandle);
        if (flag != 1)
                return -1;

        return 0;
}
/*Creates the catalog for this record manager*/
RC RM::createCatalog() {

        int tuplesize = 0, tuplesizeCol = 0;
        const char* tableName2 = "columns";
        const char* tableName = "table";
        int size = 5;
        int sizeCol = 7;
        pf->CreateFile("table");
        pf->CreateFile("columns");

        tableID = 1;
        int tc2 = 2;

        RID rid;
        vector<Attribute> attrs1;
        vector<Attribute> attrs2;
        Attribute attr1;
        Attribute attr2;

        attr1.name = "TID";
        attr1.type = TypeInt;
        attr1.length = 4;
        attrs1.push_back(attr1);
        attr1.name = "tableName";
        attr1.type = TypeVarChar;
        attr1.length = 25;
        attrs1.push_back(attr1);
        attr1.name = "fileName";
        attr1.type = TypeVarChar;
        attr1.length = 25;
        attrs1.push_back(attr1);

        attr2.name = "TID";
        attr2.type = TypeInt;
        attr2.length = 4;
        attrs2.push_back(attr2);
        attr2.name = "columnName";
        attr2.type = TypeVarChar;
        attr2.length = 25;
        attrs2.push_back(attr2);
        attr2.name = "columnType";
        attr2.type = TypeVarChar;
        attr2.length = 25;
        attrs2.push_back(attr2);

        attr2.name = "columnLength";
        attr2.type = TypeInt;
        attr2.length = 4;
        attrs2.push_back(attr2);

        void *data = malloc(100);
        memcpy((char *) data + tuplesize, &tableID, sizeof(int));

        tuplesize += sizeof(int);

        memcpy((char *) data + tuplesize, &size, sizeof(int));
        tuplesize += sizeof(int);

        memcpy((char *) data + tuplesize, tableName, size);
        tuplesize += size;

        memcpy((char *) data + tuplesize, &size, sizeof(int));
        tuplesize += sizeof(int);

        memcpy((char *) data + tuplesize, tableName, size);
        tuplesize += size;

        int rc = insertTuple_catalog("table", data, rid, tuplesize);
        if (rc == -1)
                return rc;
        void *data1 = malloc(100);
        memcpy((char *) data1 + tuplesizeCol, &tc2, sizeof(int));
        tuplesizeCol += sizeof(int);

        memcpy((char *) data1 + tuplesizeCol, &sizeCol, sizeof(int));
        tuplesizeCol += sizeof(int);

        memcpy((char *) data1 + tuplesizeCol, tableName2, sizeCol);
        tuplesizeCol += sizeCol;

        memcpy((char *) data1 + tuplesizeCol, &sizeCol, sizeof(int));
        tuplesizeCol += sizeof(int);

        memcpy((char *) data1 + tuplesizeCol, tableName2, sizeCol);
        tuplesizeCol += sizeCol;

        rc = insertTuple_catalog("table", data1, rid, tuplesizeCol);
        if (rc == -1)
                return rc;
        void *data2 = malloc(100);

        for (int i = 0; i < (int) attrs1.size(); i++)

        {
                Attribute a = attrs1.at(i);
                tuplesize = 0;
                memcpy((char *) data2 + tuplesize, &tableID, sizeof(int));
                tuplesize += sizeof(int);

                int size1 = (attrs1.at(i).name).length();

                memcpy((char *) data2 + tuplesize, &size1, sizeof(int));
                tuplesize += sizeof(int);

                memcpy((char *) data2 + tuplesize, attrs1.at(i).name.c_str(), size1);
                tuplesize += size1;

                int type = attrs1.at(i).type;
                memcpy((char *) data2 + tuplesize, &type, sizeof(int));
                tuplesize += sizeof(int);

                int length = attrs1.at(i).length;
                memcpy((char *) data2 + tuplesize, &length, sizeof(int));
                tuplesize += sizeof(int);
                rc = insertTuple_catalog("columns", data2, rid, tuplesize);
                if (rc == -1)
                        return rc;
        }

        void *data3 = malloc(100);

        for (int j = 0; j < (int) attrs2.size(); j++)

        {
                Attribute a = attrs2.at(j);
                tuplesize = 0;
                memcpy((char *) data3 + tuplesize, &tc2, sizeof(int));
                tuplesize += sizeof(int);

                int size2 = (attrs2.at(j).name).length();

                memcpy((char *) data3 + tuplesize, &size2, sizeof(int));
                tuplesize += sizeof(int);

                memcpy((char *) data3 + tuplesize, (char*) a.name.c_str(), size2);
                tuplesize += size2;

                int type = attrs2.at(j).type;
                memcpy((char *) data3 + tuplesize, &type, sizeof(int));
                tuplesize += sizeof(int);

                int length = attrs2.at(j).length;
                memcpy((char *) data3 + tuplesize, &length, sizeof(unsigned));
                tuplesize += sizeof(unsigned);

                rc = insertTuple_catalog("columns", data3, rid, tuplesize);
                if (rc == -1)
                        return rc;
        }
        tableID++;
        free(data);
        free(data1);
        free(data2);
        free(data3);
        return 0;
}
/*Creates a table and inserts into catalog*/
RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {

        int tuplesize = 0;

        RID rid;
        void *data = malloc(1000);
        int rc = pf->CreateFile(tableName.c_str());
        int size = tableName.length();
        if (rc == -1)
                return rc;

        tableID++;

        memcpy((char *) data + tuplesize, &tableID, sizeof(int));
        tuplesize += sizeof(int);

        memcpy((char *) data + tuplesize, &size, sizeof(int));
        tuplesize += sizeof(int);

        memcpy((char *) data + tuplesize, tableName.c_str(), size);
        tuplesize += size;

        memcpy((char *) data + tuplesize, &size, sizeof(int));
        tuplesize += sizeof(int);

        memcpy((char *) data + tuplesize, tableName.c_str(), size);
        tuplesize += size;

        rc = insertTuple_catalog("table", data, rid, tuplesize);
        if (rc == -1)
                return -1;
        free(data);
        //int length2;
        void *data1 = malloc(1000);
        for (int j = 0; j < (int) attrs.size(); j++)

        {
                Attribute a = attrs.at(j);
                tuplesize = 0;
                memcpy((char *) data1 + tuplesize, &tableID, sizeof(int));
                tuplesize += sizeof(int);

                int size2 = (a.name).length();

                memcpy((char *) data1 + tuplesize, &size2, sizeof(int));
                tuplesize += sizeof(int);

                memcpy((char *) data1 + tuplesize, a.name.c_str(), size2);
                tuplesize += size2;

                int type = (int) a.type;
                memcpy((char *) data1 + tuplesize, &type, sizeof(int));
                tuplesize += sizeof(int);

                int length = a.length;
                memcpy((char *) data1 + tuplesize, &length, sizeof(unsigned));
                tuplesize += sizeof(unsigned);
                rc = insertTuple_catalog("columns", data1, rid, tuplesize);

        }

        free(data1);
        return rc;
}
/*get the attributes */
RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {

        Attribute attr;

        PF_FileHandle FileHandle;
        attrs.clear();
        int rc;
        int rc1 = 1;
        int rc2 = 1;
        int tid = 0;
        pf->OpenFile(tableName.c_str(), FileHandle);
        pf->CloseFile(FileHandle);
        pf->OpenFile("table", FileHandle);
        void* readpage2 = malloc(4096);
        void* readcolumn = malloc(4096);

        unsigned pages = FileHandle.GetNumberOfPages();

        for (unsigned i = 0; i < pages && rc1 != 0; i++) {

                slotDirectory sd;
                char* name1=(char*)malloc(10) ;
                int tablelength;
                rc = FileHandle.ReadPage(i, (char*) readpage2);
                if (rc == -1)
                        return -1;
                getslotdirectory(readpage2, sd);
                for (int l = 0; l < sd.entries && rc1 != 0; l++) {
                        memcpy(&tid, (char*) readpage2 + sd.s[l].start, sizeof(int));

                        memcpy(&tablelength, (char*) readpage2 + 4 + sd.s[l].start,
                                        sizeof(int));
                memcpy(name1,(char*) readpage2 + 8 + sd.s[l].start,tablelength);
                        name1[tablelength]='\0';
                        rc1 = strcmp(name1, tableName.c_str());
                }
                delete[] sd.s;
free(name1);
        }

        free(readpage2);
        rc = pf->CloseFile(FileHandle);
        if (rc == -1)
                return -1;
        char* colname = (char*) malloc(100);
        int colnamelength;
        AttrType t;
        AttrLength l;
        //counter = 4092;
        rc = pf->OpenFile("columns", FileHandle);
        if (rc == -1)
                return rc;
        pages = FileHandle.GetNumberOfPages();
        for (unsigned i = 0; i < pages; i++) {
                slotDirectory sd;
                FileHandle.ReadPage(i, (char*) readcolumn);
                getslotdirectory(readcolumn, sd);

                int tid1 = -1;

                bool y = false;
                for (int u = 0; u < sd.entries && y != true; u++) {

                        memcpy(&tid1, (char*) readcolumn + sd.s[u].start, 4);
                        rc2 = memcmp(&tid1, &tid, 4);

                        if (rc2 == 0) {

                                memcpy(&colnamelength, (char*) readcolumn + sd.s[u].start + 4,
                                                4);
                                memcpy(colname, (char*) readcolumn + sd.s[u].start + 8,
                                                colnamelength);
                                colname[colnamelength] = '\0';
                                memcpy(&t,
                                                (char*) readcolumn + sd.s[u].start + 8 + colnamelength,
                                                4);
                                memcpy(&l,
                                                (char*) readcolumn + sd.s[u].start + 12 + colnamelength,
                                                4);
                                attr.length = l;
                                attr.type = (AttrType) t;
                                attr.name = colname;

                                attrs.push_back(attr);

                        }
                        if (tid1 >= tid + 1) {
                                y = true;
                        }

                }
                delete[] sd.s;
        }
        free(colname);
        free(readcolumn);

        rc = pf->CloseFile(FileHandle);

        return rc;

}
RC RM::reorganizePage(const string tableName, const unsigned pageNumber) {
        PF_FileHandle fileHandle;
        slotDirectory sdreorg;
        int newpageoffset = 0;
        void* data = malloc(4096);
        void* newpage = malloc(4096);
        int rc = pf->OpenFile(tableName.c_str(), fileHandle);
        if (rc == -1)
                return -1;
        fileHandle.ReadPage(pageNumber, data);
        getslotdirectory(data, sdreorg);

        for (int l = 0; l < sdreorg.entries; l++) {
                if (sdreorg.s[l].start < 0 && sdreorg.s[l].start > 4096) {
                        sdreorg.free_offset = sdreorg.free_offset - sdreorg.s[l].offset;

                } else {
                        memcpy((char*) newpage + newpageoffset,
                                        (char*) data + sdreorg.s[l].start, sdreorg.s[l].offset);
                        newpageoffset += sdreorg.s[l].offset;

                }

        }
        sdreorg.free_offset = newpageoffset;
        setslotdirectory(newpage, sdreorg);

        rc = fileHandle.WritePage(pageNumber, newpage);
        if (rc == -1)
                return -1;

        free(data);
        free(newpage);
        rc = pf->CloseFile(fileHandle);
        free(sdreorg.s);

        return rc;
}
RC RM::deleteTable(const string tableName) {

        RID rid;
        PF_FileHandle fileHandle;
        int rc;
        PF_FileHandle FileHandle;
        rc = pf->OpenFile((char *) tableName.c_str(), fileHandle);
        if (rc == -1)
                return -1;
        unsigned pages = FileHandle.GetNumberOfPages();

        void * readpage3 = malloc(4096);
        for (int i = 0; i < pages; i++) {

                slotDirectory sd;
                rc = FileHandle.ReadPage(i, readpage3);
                if (rc == -1)
                        return -1;
                getslotdirectory(readpage3, sd);
                for (int l = 0; l < sd.entries; l++) {
                        rid.pageNum = i;
                        rid.slotNum = l;
                        deleteTuple(tableName, rid);
                }
        }

        pf->DestroyFile((char *) tableName.c_str());
        return rc;

}
RC RM::deleteTuple(const string tableName, const RID &rid) {
        int rc;

        PF_FileHandle fileHandle;
        rc = pf->OpenFile((char *) tableName.c_str(), fileHandle);
        void *readpage = malloc(4096);
        fileHandle.ReadPage(rid.pageNum, readpage);

        if (rc == -1)
                return rc;

        slotDirectory d;
        getslotdirectory(readpage, d);
        int u = rid.slotNum;
        if ((d.s[u].start) == -1)
                return -1;

        d.s[rid.slotNum].start = -1;

        int offset = 4096 - 8 * (rid.slotNum + 2);
        memcpy((char*) readpage + offset, &d.s[rid.slotNum].start, 4);
        fileHandle.WritePage(rid.pageNum, (char*) readpage);
        rc = pf->CloseFile(fileHandle);
        free(d.s);
        free(readpage);
        return rc;

}
RC RM::deleteTuples(const string tableName) {
        PF_FileHandle FileHandle;
        int rc;

        rc = pf->OpenFile(tableName.c_str(), FileHandle);

        void *deletet = malloc(4096);

        int free_offset = 0, entries = 0;

        memcpy((char*) deletet + 4092, &free_offset, 4);
        memcpy((char*) deletet + 4088, &entries, 4);

        int pages = FileHandle.GetNumberOfPages();

        for (int i = 0; i < pages; i++) {
                rc = FileHandle.WritePage(i, deletet);
        }
        free(deletet);
        rc = pf->CloseFile(FileHandle);

        return rc;
}

