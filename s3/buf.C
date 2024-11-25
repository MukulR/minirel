/**
 * 
 * buf.C
 * Implements BufMgr class, the heart of the buffer manager
 * Responsible for allocating and reading pages as well as buffers
 * Uses clock hand algorithm to simulate LRU frame replacement
 * 
 * @author Mukul Rao <mgrao2@wisc.edu> <mukul@cs.wisc.edu> 9084190025
 * @author Joshua Han <jhan294@wisc.edu> 9084857490
 * @author Pranav Jayabalan <pjayabalan@wisc.edu> 9084665968
 * 
 */


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


/**
 * allocBuf()
 * This will allocate a buffer frame using the Clock replacement algorithm. 
 * 
 * This method will try to find a free buffer frame, if there is one then it will use that one. 
 * If all are being used, then it will use the Clock algorithm to evict a page. If the page that 
 * is being evicted is dirty then before it is evicted it will write back to the disk.
 * 
 * @param frame A reference to an integer on where to store the allocated frame number
 * 
 * @return Returns OK if it successfully allocates a buffer frame. 
 *         Returns BUFFEREXCEEDED if there are no free frames available (all buffers are pinned). 
 *         Returns UNIXERR if an error occurs while a dirty page to disk. 
 *         Returns anything else if any other error occurs.
 */
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

/**
 * readPage()
 * This method will read the specified page of the file and put it into the buffer pool. 
 * 
 * This will first check if the page is already in the buffer pool or not. 
 * If it is then it will increment pin count and mark the page as referenced. 
 * If it isn't in the buffer pool, then it will call allocBuf and load the page from disk
 * add the page into the buffer pool and hash table. 
 * 
 * @param file The pointer to the file of the pages that needs to be read
 * @param pageNo The number of the page that needs to be read
 * @param page The reference to a pointer of the address that the loaded page needs to be stored at
 * 
 * @return Returns OK if the page was loaded or already in the buffer. 
 *         Returns HASHTBLERROR if there is an error with the hash table. 
 *         Returns an error if there are errors with allocBuf or readPage.
 */
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
/**
 * unPinPage()
 * Decreases the pin count of a page in the buffer pool and marks it as dirty if specified.
 * 
 * This method performs the following steps:
 * 1. Looks up the buffer frame containing the specified page using the hash table.
 * 2. Checks if the page is currently pinned. If it is not, returns an error indicating that the page is not pinned.
 * 3. If the page is pinned, decrements the pin count and marks the page as dirty if the 'dirty' parameter is set to true.
 * 
 * @param file   Pointer to the file containing the page to be unpinned.
 * @param pageNo The page number within the file to be unpinned.
 * @param dirty  Boolean flag indicating whether the page should be marked as dirty.
 * 
 * @return OK if the page was successfully unpinned.
 *         Returns HASHNOTFOUND if the page is not found in the hash table.
 *         Returns PAGENOTPINNED if the page is already unpinned (pin count is zero).
 */
const Status BufMgr::unPinPage(File* file, const int pageNo, const bool dirty) {
    // Step 1: Look up the frame number in the hash table using the (file, pageNo) pair.
    int frame;
    Status getRet = hashTable->lookup(file, pageNo, frame);
    if (getRet != OK) {
        return getRet;
    }

    // Step 2: Get a reference to the buffer descriptor for the found frame.
    BufDesc *curBuffer = &bufTable[frame];

    // Step 3: Check if the page is already unpinned (pin count is zero)
    if (!curBuffer->pinCnt) {
        return PAGENOTPINNED;
    }
    
    // Step 4: If the 'dirty' flag is set to true, mark the page as dirty.
    if (dirty)
        curBuffer->dirty = true;
    curBuffer->pinCnt--;

    return OK;
}
/**
 * allocPage()
 * Allocates a new page in the specified file and maps it to a buffer frame.
 * 
 * This method performs the following steps:
 * 1. Allocates a new page in the specified file using the file->allocatePage() method.
 * 2. Allocates a free frame in the buffer pool using the clock replacement algorithm (allocBuf()).
 * 3. Inserts the new page and its associated frame into the buffer manager's hash table.
 * 4. Sets up the allocated frame in the buffer pool using the Set() method.
 * 
 * @param file    Pointer to the file in which a new page is to be allocated.
 * @param pageNo  Output parameter that will hold the page number of the newly allocated page.
 * @param page    Output parameter that will hold a pointer to the buffer frame containing the allocated page.
 * 
 * @return OK if the page was successfully allocated and mapped to a buffer frame.
 *         Returns appropriate error codes if an error occurred during page allocation,
 *         buffer allocation, or hash table insertion.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {
    // Step 1: Allocate a new page in the specified file.
    int pageNum;
    Status allocPageRet = file->allocatePage(pageNum);
    if (allocPageRet != OK) {
        return allocPageRet;
    }

    // Step 2: Allocate a free frame in the buffer pool using the clock replacement algorithm.
    int frame;
    Status allocBufRet = allocBuf(frame);
    // Check if a free frame was successfully allocated.
    if (allocBufRet != OK) {
        return allocBufRet;
    }

    // Step 3: Insert the newly allocated page and its associated frame into the hash table.
    Status insertRet = hashTable->insert(file, pageNum, frame);
    if (insertRet != OK) {
        return insertRet;
    }

    // Step 4: Set up the buffer descriptor for the allocated frame.
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


