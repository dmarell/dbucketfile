/*
 * Copyright (c) 2011 Joel Binnquist, Daniel Marell
 * All rights reserved.
 *
 * Permission is hereby granted, free  of charge, to any person obtaining
 * a  copy  of this  software  and  associated  documentation files  (the
 * "Software"), to  deal in  the Software without  restriction, including
 * without limitation  the rights to  use, copy, modify,  merge, publish,
 * distribute,  sublicense, and/or sell  copies of  the Software,  and to
 * permit persons to whom the Software  is furnished to do so, subject to
 * the following conditions:
 *
 * The  above  copyright  notice  and  this permission  notice  shall  be
 * included in all copies or substantial portions of the Software.
 *
 * THE  SOFTWARE IS  PROVIDED  "AS  IS", WITHOUT  WARRANTY  OF ANY  KIND,
 * EXPRESS OR  IMPLIED, INCLUDING  BUT NOT LIMITED  TO THE  WARRANTIES OF
 * MERCHANTABILITY,    FITNESS    FOR    A   PARTICULAR    PURPOSE    AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE,  ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package se.marell.dbucketfile;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class BucketFileIndex {
    private Map<Long, RecordInfo> recordInfoMap;
    private int pageSize;
    private long firstDeallocatedPage;
    private long lastDeallocatedPage;

    private BucketFile bucketFile;
    private long nextFreePageAddress;
    private long numberOfBytes;
    private boolean isDirty;

    public BucketFileIndex() {
    }

    public BucketFileIndex(long nextFreePageAddress, int pageSize, BucketFile bucketFile) {
        this.bucketFile = bucketFile;
        this.pageSize = pageSize;
        recordInfoMap = new HashMap<Long, RecordInfo>(100);
        this.nextFreePageAddress = nextFreePageAddress;
    }

    public void init(long nextFreeAddress, long firstDeallocatedAddress, long lastDeallocatedFreeAddress, long numBytesInIndex, int pageSize) {
        this.nextFreePageAddress = nextFreeAddress;
        this.firstDeallocatedPage = firstDeallocatedAddress;
        this.lastDeallocatedPage = lastDeallocatedFreeAddress;
        this.numberOfBytes = numBytesInIndex;
        this.pageSize = pageSize;
    }

    public boolean isDirty() {
        return isDirty;
    }

    RecordInfo getRecordInfo(long recordId) {
        return recordInfoMap.get(recordId);
    }

    long getNextFreePageAddress() throws IOException {
        if (firstDeallocatedPage != 0) {
            long freePageAddress = firstDeallocatedPage;
            // Set new start of free pages to continuation page
            bucketFile.storageFile.seek(freePageAddress);
            firstDeallocatedPage = bucketFile.storageFile.readLong();
            if (firstDeallocatedPage == 0) {
                lastDeallocatedPage = 0; // The last page was taken
            }
            // Reset next page address
            bucketFile.storageFile.seek(freePageAddress);
            bucketFile.storageFile.writeLong(0);
            isDirty = true;
            return freePageAddress;
        } else {
            long retVal = nextFreePageAddress;
            nextFreePageAddress += pageSize;
            isDirty = true;
            return retVal;
        }
    }

    public static void writeIndex(BucketFileIndex index, BucketFile bucketFile) throws IOException {
        BucketFile.RecordWriter indexRecordWriter = bucketFile.internalGetRecordWriter(BucketFile.INDEX_RECORD_ID);
        ObjectOutputStream oos = new ObjectOutputStream(indexRecordWriter);
        oos.writeInt(index.recordInfoMap.size());
        for (Map.Entry<Long, RecordInfo> entry : index.recordInfoMap.entrySet()) {
            oos.writeLong(entry.getKey());
            oos.writeLong(entry.getValue().getStartPageAddress());
            oos.writeInt(entry.getValue().getNumberOfBytes());
        }
        oos.close();
        index.numberOfBytes = indexRecordWriter.getNumberOfBytes();
    }

    static BucketFileIndex readIndex(BucketFile bucketFile) throws IOException {
        BucketFile.RecordReader indexRecordReader = bucketFile.internalGetRecordReader(BucketFile.INDEX_RECORD_ID);
        ObjectInputStream ois = new ObjectInputStream(indexRecordReader);
        BucketFileIndex index = new BucketFileIndex();
        int size = ois.readInt();
        index.recordInfoMap = new HashMap<Long, RecordInfo>(size);
        for (int i = 0; i < size; i++) {
            long key = ois.readLong();
            long startPageAddress = ois.readLong();
            int numberOfBytes = ois.readInt();
            index.recordInfoMap.put(key, new RecordInfo(startPageAddress, numberOfBytes));
        }
        ois.close();
        index.bucketFile = bucketFile;
        return index;
    }

    void addRecordInfo(long recordId, RecordInfo recordInfo) {
        recordInfoMap.put(recordId, recordInfo);
        isDirty = true;
    }

    void deallocateRecord(long recordId) throws IOException {
        // Get start page
        PageProxy startPage = bucketFile.getStartPage(recordId);
        if (startPage == null) { // does not exist in the file; ignore
            return;
        }
        deallocatePages(startPage.getAddress());
        recordInfoMap.remove(recordId);
        isDirty = true;
    }

    void deallocatePages(long startPageAddress) throws IOException {
        if (lastDeallocatedPage != 0) { // Link the newly deallocated page to end of chain
            bucketFile.storageFile.seek(lastDeallocatedPage);
            bucketFile.storageFile.writeLong(startPageAddress);
        }

        if (firstDeallocatedPage == 0) {
            firstDeallocatedPage = startPageAddress;
        }

        // Find end of chain
        lastDeallocatedPage = startPageAddress;
        while (true) {
            bucketFile.storageFile.seek(lastDeallocatedPage);
            long nextPage = bucketFile.storageFile.readLong();
            if (nextPage == 0) {
                break;
            }
            lastDeallocatedPage = nextPage;
        }
        isDirty = true;
    }


    public void printAllocStatus(PrintStream out) throws IOException {
        out.println("Allocated records: " + recordInfoMap);
        out.println("Deallocated pages: ");
        int i = 0;
        long deallocatedPage = firstDeallocatedPage;
        while (true) {
            out.println("#" + i + ": " + deallocatedPage);
            if (deallocatedPage == 0) {
                break;
            }
            i++;
            bucketFile.storageFile.seek(deallocatedPage);
            deallocatedPage = bucketFile.storageFile.readLong();
        }
    }

    Set<Map.Entry<Long, RecordInfo>> getRecordSet() {
        return recordInfoMap.entrySet();
    }

    public void write(RandomAccessFile storageFile) throws IOException {
        storageFile.writeLong(numberOfBytes);
        storageFile.writeLong(nextFreePageAddress);
        storageFile.writeLong(firstDeallocatedPage);
        storageFile.writeLong(lastDeallocatedPage);
    }

    @Override
    public String toString() {
        return "BucketFileIndex{" +
                "recordInfoMap=" + recordInfoMap +
                ", pageSize=" + pageSize +
                ", firstDeallocatedPage=" + firstDeallocatedPage +
                ", lastDeallocatedPage=" + lastDeallocatedPage +
                ", bucketFile=" + bucketFile +
                ", nextFreePageAddress=" + nextFreePageAddress +
                ", numberOfBytes=" + numberOfBytes +
                ", isDirty=" + isDirty +
                '}';
    }

//  public String asString() throws IOException {
//    StringBuffer sb = new StringBuffer();
//    sb.append("Index\n");
//    sb.append("=====\n");
//    sb.append("  Records: " + recordInfoMap.toString() + "\n");
//    sb.append("  Page size: " + pageSize + "\n");
//    sb.append("  Next free page: " + nextFreePageAddress + "\n");
//    sb.append("  First deallocated page: " + firstDeallocatedPage + "\n");
//    sb.append("  Last deallocated page: " + lastDeallocatedPage + "\n");
//    sb.append("  Deallocated pages\n");
//    long address = firstDeallocatedPage;
//    if (address == 0) {
//      sb.append("    None.\n");
//    } else {
//      while (true) {
//        try {
//          sb.append("    Address: " + address + "\n");
//          bucketFile.storageFile.seek(address);
//          long addressOfNextPage = bucketFile.storageFile.readLong();
//          sb.append("    Next page: " + addressOfNextPage + "\n");
//          byte[] buf = new byte[bucketFile.getPageDataSize()];
//          int nBytes = bucketFile.storageFile.read(buf, 0, buf.length);
//          sb.append("    Data: " + Arrays.toString(buf) + (nBytes != buf.length ? " Mismatch in size; expected: " + buf.length + " actual: " + nBytes : "") + "\n");
//          address = addressOfNextPage;
//          if (address == 0) {
//            break;
//          }
//        } catch (IOException e) {
//          sb.append("    Error: " + e.getMessage());
//          break;
//        }
//      }
//    }
//    return sb + "";
//  }

    public long getFirstDeallocatedPage() {
        return firstDeallocatedPage;
    }
}
