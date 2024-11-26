#include "heapfile.h"
#include "error.h"
#include <cstring>


/**
 * 
 * Create (initialize) a heapfile with a headerpage and
 * a single page to hold data (data page).
 * 
 * This method will ensure that the provided filename
 * is created if it does not exist, an existing
 * filename is not required.
 * 
 * @param fileName: The name of the file
 * @return status of create operation
 * 
 */
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		
        Status create = db.createFile(fileName);
        if (create != OK) {
            return create;
        }

        // Now actually open the file
        Status open = db.openFile(fileName, file);
        if (open != OK) {
            return open;
        }

        // Now construct header page
        Status h_alloc = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (h_alloc != OK) {
            return h_alloc;
        }

        hdrPage = (FileHdrPage*) newPage;
        strcpy(hdrPage->fileName, fileName.c_str());
        
        // Now construct data page
        Status d_alloc = bufMgr->allocPage(file, newPageNo, newPage);
        if (d_alloc != OK) {
            return d_alloc;
        }

        // Initialize our new page, mark that it has no next
        newPage->init(newPageNo);
        newPage->setNextPage(-1);

        // This is the only page, so first and last
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        // One page added
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

		// Unpin both the hdrPage and the newPage, flush the file, and close the file
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);
        bufMgr->flushFile(file);
        db.closeFile(file);
        return OK;
		
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * 
 * Instantiate a heapfile object with the given filename
 * 
 * This constructor opens the file and sets member variables accordingly
 * It reads the first page. This constructor assumes that the file exists
 * 
 * @param fileName: the name of an existing file
 * @param returnStatus: status variable that will be populated accordingly
 * 
 */
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // Successfully opened the file; attempt to read the first page (header page)
        int pageNo;
        
        // Get the first page number from the file
        status = filePtr->getFirstPage(pageNo);
        if (status != OK) {
            returnStatus = status;
            return;
        }

        // Read the header page into memory
        status = bufMgr->readPage(filePtr, pageNo, pagePtr);
        if (status != OK) {
            returnStatus = status;
            return;
        }

        // Cast the read page as a header page (FileHdrPage)
        headerPage = (FileHdrPage *) pagePtr;
        headerPageNo = pageNo;
        
        // Mark header page as not dirty initially (no modifications)
        hdrDirtyFlag = false;

        // Get the first data page number from the header page
        int firstNo = headerPage->firstPage;
        
        // Read the first data page
        status = bufMgr->readPage(filePtr, firstNo, pagePtr);
        if (status != OK) {
            // If the first data page could not be read, set the return status and exit
            returnStatus = status;
            return;
        }
        
        // Successfully read the first data page; set current page pointer
        curPage = pagePtr;
        curPageNo = firstNo;
        
        // Mark the current data page as not dirty initially
        curDirtyFlag = false;
        
        // Initialize current record ID to NULLRID
        curRec = NULLRID;

        // At this point everything succeeded
        returnStatus = OK;
        return;
    }
    else {
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

/**
 * 
 * Retrieves an arbitrary (this means an arbitrary record according to the given RID)
 * record from the file
 * 
 * @param rid: the given RID
 * @param record: populated based on what is selected to be returned
 * @return status: Status code
 */
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   
    if (curPage && curPageNo == rid.pageNo) {
        // Desired page is on the currently pinned page
        curRec = rid;
        status = curPage->getRecord(curRec, rec);
        if (status != OK) {
            return status;
        }
    } else {
        // Unpin currently pinned page, and use pageNo from RID to read into buffer pool
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);

        if (status != OK) {
            return status;
        }
        
        curPageNo = rid.pageNo;
        curRec = rid;
        curDirtyFlag = false;

        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status;
        }
        
        status = curPage->getRecord(curRec, rec);
        if (status != OK) {
            return status;
        }
    }

    return status;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        ((type_ == INTEGER && length_ != sizeof(int))
         || (type_ == FLOAT && length_ != sizeof(float))) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


/**
 * 
 * Returns the RID of the next record that satisfies the scan predicate
 * Scans one file at a time, going through all records.
 * 
 * @param outRid: RID that is populated with the matching record id (if exists)
 * @return status: Status code
 * 
 */
const Status HeapFileScan::scanNext(RID& outRid) {
    Status status = OK;
    RID nextRid;
    RID tmpRid;
    int nextPageNo;
    Record rec;

    // Initialize curPage if not yet done
    if (!curPage) {
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;

        // Read the page into memory
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status;
        }

        // Get the first record
        status = curPage->firstRecord(tmpRid);
        if (status != OK) {
            return status;
        }

        curRec = tmpRid;
    }

    while (true) {
        // First test if current record is the first on a newly loaded page
        status = curPage->nextRecord(curRec, nextRid);
        if (status == OK) {
            curRec = nextRid;
        } else {
            // If there's no next record, move to the next page
            status = curPage->getNextPage(nextPageNo);
            if (status != OK) return status;

            // Check if it's the last page
            if (nextPageNo == -1) {
                return FILEEOF;
            }

            // Unpin current page and read the next page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) {
                return status;
            }

            curPageNo = nextPageNo;
            curDirtyFlag = false;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) {
                return status;
            }

            status = curPage->firstRecord(curRec);
            if (status != OK) {
                continue; // If the first record on the new page is bad, go to the next page
            }
        }

        // Read current record and check predicate
        status = curPage->getRecord(curRec, rec);
        if (status != OK) return status;

        if (matchRec(rec)) {
            outRid = curRec;
            return OK;
        }
    }

    return FILEEOF; // nothing found? but this shouldn't happen
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

/**
 * 
 * Inserts the record described by rec into the file,
 * returning the RID of the inserted record in outRid.
 * 
 * Ensures that a current page is initialized (i.e. inserting when no pages)
 * 
 * @param rec: the record to insert
 * @param outRid: the RID of the inserted record (populated)
 * @return status: Status code
 * 
 */
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (!curPage) {
        // If there's no current page or we're not at the last page

        // Unpin this page, not needed anylonger
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            return status;
        }

        // Read the last page in (where our record will go)
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        curDirtyFlag = false;
        if (status != OK) {
            return status;
        }
        
        status = curPage->firstRecord(curRec);
        if (status != OK) {
            return status;
        }
    }

    status = curPage->insertRecord(rec, rid);
    if (status == OK) {
        // Done
        headerPage->recCnt++;
        curRec = rid;
        curDirtyFlag = true;
        outRid = rid;
        return OK;
    }
    else if (status == NOSPACE) {
        // Need a new page now

        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) {
            return status;
        }
        
        // Init it, set it as the last page
        newPage->init(newPageNo);
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        // Link to prev page
        curPage->setNextPage(newPageNo);
        curDirtyFlag = true;
        // Unpin it, this is not needed in buffer pool anylonger
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            return unpinstatus;
        }

        // Now we insert the record on this page, first read it in
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status;
        }

        status = curPage->insertRecord(rec, rid);
        if (status != OK) {
            return status;
        }
        
        headerPage->recCnt++;
        curRec = rid;
        curDirtyFlag = true;
        // End of the list
        curPage->setNextPage(-1);
        
        // Unpin this page
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            return unpinstatus;
        }
        // Done
        outRid = rid;
        return OK;
    }
    else {
        return status;
    }
}


