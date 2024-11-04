#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int &frame) {

    int numPinnedPages = 0;

    while (true) {
        // If all buffers are currently pinned i.e.
        // in use, we cannot currently allocate and our
        // buffer is exceeded.
        if (numBufs == numPinnedPages) {
            return BUFFEREXCEEDED;
        }

        // Advance clock hand
        advanceClock();

        BufDesc* curBuffer = &bufTable[clockHand];
        Page* curPage = &bufPool[clockHand];

        // First check if at an invalid buffer, can immediately use it
        // (Clock LRU replacement algorithm)
        if (!curBuffer->valid) {
            frame = curBuffer->frameNo;
            curBuffer->Clear();
            return OK;
        } else if (curBuffer->refbit) {
            curBuffer->refbit = 0;
            continue;
        }

        // If this page is pinned, it is not eligble to be replaced
        if (curBuffer->pinCnt > 0) {
            numPinnedPages++;
            continue;
        }

        // If it is dirty, it's fine, but we have to write it to file first.
        if (curBuffer->dirty) {
            Status ret = curBuffer->file->writePage(curBuffer->pageNo, curPage);
            if (ret != OK) {
                return UNIXERR;
            }
            curBuffer->dirty = true;
        }

        // This buffer is now available, we can now allocate it
        // Remove old entry from hashtable
        Status ret = hashTable->remove(curBuffer->file, curBuffer->pageNo);
        if (ret != OK) {
            return ret;
        } else {
            // Allocate curBuffer
            curBuffer->Clear();
            frame = curBuffer->frameNo;
            return OK;
        }
    }
}

const Status BufMgr::readPage(File* file, const int pageNo, Page*& page) {
    int frame;
    Status getRet = hashTable->lookup(file, pageNo, frame);
    if (getRet == HASHNOTFOUND) {
        // Doesn't exist
        int frame;
        Status allocBufRet = allocBuf(frame);
        if (allocBufRet != OK) {
            return allocBufRet;
        }

        // Load page into memory
        Status readRet = file->readPage(pageNo, &bufPool[frame]);
        if (readRet != OK) {
            disposePage(file, pageNo);
            return readRet;
        }
        page = &bufPool[frame];

        // Insert this into hashTable
        Status insertRet = hashTable->insert(file, pageNo, frame);
        if (insertRet != OK) {
            return insertRet;
        }

        // Set() the buffer
        BufDesc *curBuffer = &bufTable[frame];
        curBuffer->Set(file, pageNo);
        curBuffer->frameNo = frame;

    } else if (getRet == HASHTBLERROR) {
        return HASHTBLERROR;
    } else {
        BufDesc *curBuffer = &bufTable[frame];

        curBuffer->refbit = true;
        curBuffer->pinCnt++;

        page = &bufPool[frame];
    }

    return OK;
}

const Status BufMgr::unPinPage(File* file, const int pageNo, const bool dirty) {
    // First get the corresponding frame number
    int frame;
    Status getRet = hashTable->lookup(file, pageNo, frame);
    if (getRet != OK) {
        return getRet;
    }

    // Get the actual buffer
    BufDesc *curBuffer = &bufTable[frame];
    // Check if it is already unpinned
    if (!curBuffer->pinCnt) {
        return PAGENOTPINNED;
    }
    // If not, decrement its pincount and mark it as dirty if dirty
    if (dirty)
        curBuffer->dirty = true;
    curBuffer->pinCnt--;

    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {
    // Allocate our page
    int pageNum;
    Status allocPageRet = file->allocatePage(pageNum);
    if (allocPageRet != OK) {
        return allocPageRet;
    }

    // Allocate the frame
    int frame;
    Status allocBufRet = allocBuf(frame);
    if (allocBufRet != OK) {
        return allocBufRet;
    }

    // Insert (file, pageNum) -> frame mapping
    Status insertRet = hashTable->insert(file, pageNum, frame);
    if (insertRet != OK) {
        return insertRet;
    }

    // Now we can Set()
    BufDesc *curBuffer = &bufTable[frame];
    pageNo = pageNum;
    curBuffer->Set(file, pageNum);
    page = &bufPool[frame];

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = false;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


