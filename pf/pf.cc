#include "pf.h"
using namespace std;
#include <fstream>
#include <iostream>

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{
	fstream file;
	file.open(fileName,ios::in|ios::binary);
	if(!file.is_open())
	{
		file.open(fileName,ios::out|ios::binary);
		file.close();
		return 0;
	}

	else
	{
			file.close();
			return -1;
	}
}


RC PF_Manager::DestroyFile(const char *fileName)
{

    if(remove(fileName)!=0)
    {
    	return -1;
    }
	return 0;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{

        if(fileHandle.file.is_open()) return -1;
		fileHandle.file.open(fileName,ios::in|ios::out|ios::binary);
		if(fileHandle.file.is_open())
		{

			return 0;
		}
		//fileHandle.file.close();
	return -1;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	if(fileHandle.file.is_open())
	{
		fileHandle.file.close();
		return 0;
	}
    return -1;
}


PF_FileHandle::PF_FileHandle()
{
}
 

PF_FileHandle::~PF_FileHandle()
{
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	unsigned pages=GetNumberOfPages();
	if(pageNum>=0&&pageNum<=pages&&file.is_open())
	{
		file.seekg((pageNum)*PF_PAGE_SIZE,ios::beg);
		file.read((char *) data,PF_PAGE_SIZE);
		file.seekg(0,ios::end);
		return 0;
	}
    return -1;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	  unsigned pages=GetNumberOfPages();
		if(pageNum>=0&&pageNum<=pages&&file.is_open())
		{
			file.seekp((pageNum)*PF_PAGE_SIZE,ios::beg);
			file.write((char *) data,PF_PAGE_SIZE);
			file.seekp(0,ios::end);
			return 0;
		}
	    return -1;



}


RC PF_FileHandle::AppendPage(const void *data)
{
    //unsigned page=GetNumberOfPages();
    if(file.is_open())
    {
    	//page=page+1;
    	file.seekg(0,ios::end);
    	file.write((char *) data,PF_PAGE_SIZE);
    	//file.seekp(0,ios::end);
    	return 0;
    }
	return -1;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
    if(file.is_open())
    {
    	file.seekg(0,ios::end);

    	return ((file.tellg())/4096);

    }
    return 0;
}


