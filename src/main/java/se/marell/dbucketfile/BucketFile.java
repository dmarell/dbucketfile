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
import java.util.*;

/**
 * A file that works as a memory heap, i.e. it is possible to allocate records in it and rewrite
 * those records. The BucketFile handles the case when a record size changes.
 *
 *
 * The file has the following structure:
 * <ol>
 * <li>Version info: String
 * <li>Size of data part in a page: int
 * <li>Number of bytes in index record: long
 * <li>Next free page address: long
 * <li>Address of first deallocated page: long
 * <li>Address of last deallocated page: long
 * <li>Start page for Record 0 (Index record)
 * <li>pages...
 * </ol>
 *
 * The file is built of pages with a fix size. One record is divided among several pages in case it does
 * not fit into one page. In these cases the pages for one record are linked.
 * The format of a page is :
 * <ul>
 * <li>Address to next page: long
 * <li>Data: byte[{@link #pageDataSize}]
 * </ul>
 * Information about all records are kept in an index so that look-up of a certain record is quick.
 *
 * Records are identified by a number {Long.MIN_VALUE..(Long.MAX_VALUE-1)}.
 * It is up to the application of the file to keep track of which record ID:s are used and how they map to application
 * objects.
 *
 * Usage:
 * <pre>
 * MyObject myObjectToWrite = getMyObject(); // Some object that shall be written
 *
 *  // Open a BucketFile
 *  BucketFile bucketFile = null;
 *  try {
 *    bucketFile = new BucketFile(new File("my_heap_file.hpf"));
 *
 *    // Write to record with ID = 1
 *    OutputStream writer = null;
 *    try {
 *      writer = bucketFile.getRecordWriter(1);
 *      ObjectOutputStream oos = new ObjectOutputStream(writer);
 *      oos.writeObject(myObjectToWrite);
 *    } finally {
 *      if (writer != null) {
 *        try {
 *          writer.close(); // Close recordWriter when done
 *        } catch (IOException e) {
 *          // This example ignores exception when closing
 *        }
 *      }
 *    }
 *
 *    // Read a record with ID = 1
 *    InputStream reader = null;
 *    try {
 *      reader = bucketFile.getRecordReader(1);
 *      ObjectInputStream ois = new ObjectInputStream(reader);
 *      MyObject myReadObject = (MyObject) ois.readObject();
 *      assert myReadObject.equals(myObjectToWrite);
 *    } finally {
 *      if (reader != null) {
 *        try {
 *          reader.close(); // Close recordReader when done
 *        } catch (IOException e) {
 *          // This example ignores exception when closing
 *        }
 *      }
 *    }
 *  } finally {
 *    if (bucketFile != null) {
 *      try {
 *        bucketFile.close();
 *      } catch (IOException ignore) {
 *      }
 *    }
 *  }
 * </pre>
 */
public class BucketFile {
    public static class AlreadyLockedException extends IOException {
        public AlreadyLockedException(File file) {
            super(file + " is locked by another application");
        }
    }

    public class RecordReader extends InputStream {
        private PageProxy recordPage;
        private boolean isRecordReaderClosed;
        private int availableBytes;
        private int numberOfBytes;
        private long recordId;

        public RecordReader(long recordId) throws IOException {
            this.recordId = recordId;
            openRecordReaders.add(this);
            if (recordId != INDEX_RECORD_ID) {
                availableBytes = index.getRecordInfo(recordId).getNumberOfBytes();
                numberOfBytes = availableBytes;
            }
            recordPage = getStartPage(recordId);
            numberOfReaders++;
        }

        @Override
        public int read() throws IOException {
            if (isRecordReaderClosed) {
                throw new IllegalStateException("Tried to read from closed record");
            }
            if (recordPage != null) {
                if (recordPage.hasMore()) {
                    if (recordId != INDEX_RECORD_ID && availableBytes <= 0) {
                        return -1;
                    }
                    availableBytes--;
                    return ((int) recordPage.readNextByte()) & 0xff;
                }
                if (recordPage.hasContinuation()) {
                    recordPage = recordPage.getContinuationPage();
                    availableBytes--;
                    return ((int) recordPage.readNextByte()) & 0xff;
                }
            }
            return -1;
        }

        @Override
        public int available() throws IOException {
            return availableBytes > 0 ? availableBytes : 0;
        }

        @Override
        public int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (isRecordReaderClosed) {
                throw new IllegalStateException("Tried to read from closed record");
            }
            if (recordPage != null) {
                int offset = off;
                int bytesToRead = len;
                int accumulatedBytesRead = 0;
                while (true) {
                    if (!recordPage.hasMore()) {
                        if (recordPage.hasContinuation()) {
                            recordPage = recordPage.getContinuationPage();
                        } else {
                            if (accumulatedBytesRead == 0) {
                                return -1;
                            }
                            return accumulatedBytesRead;
                        }
                    }
                    // Note that this call can return more bytes than stored because a page does not keep track of if record data ends before.
                    // page data ends. So we have to potentially limit the returned data size after this call.
                    int bytesRead = recordPage.readBytes(buf, offset, bytesToRead);
                    offset += bytesRead;
                    bytesToRead -= bytesRead;
                    accumulatedBytesRead += bytesRead;
                    if (accumulatedBytesRead >= Math.min(len, numberOfBytes)) {
                        break;
                    }
                }
                availableBytes -= accumulatedBytesRead;
                if (recordId == INDEX_RECORD_ID) {
                    return accumulatedBytesRead;
                }
                return Math.min(accumulatedBytesRead, numberOfBytes);
            }
            return -1;
        }

        @Override
        public void close() throws IOException {
            if (!isRecordReaderClosed) {
                openRecordReaders.remove(this);
                isRecordReaderClosed = true;
                numberOfReaders--;
            }
        }
    }


    public class RecordWriter extends OutputStream {
        private long recordId;
        private PageProxy recordPage;
        private int numberOfBytes;
        private boolean isRecordWriterClosed;
        private long startPageAddress;

        RecordWriter(long recordId) throws IOException {
            openRecordWriters.add(this);
            this.recordId = recordId;
            recordPage = getStartPage(recordId);
            if (recordPage == null) {
                recordPage = allocateNewPage();
            }
            startPageAddress = recordPage.getAddress();
            numberOfWriters++;
        }

        @Override
        public String toString() {
            return "Record ID:" + recordId;
        }

        public int getNumberOfBytes() {
            return numberOfBytes;
        }

        long getRecordId() {
            return recordId;
        }

        @Override
        public void write(int b) throws IOException {
            if (isRecordWriterClosed) {
                throw new IllegalStateException("Tried to write to closed record");
            }
            if (!recordPage.hasMore()) {
                PageProxy newPage;
                if (!recordPage.hasContinuation()) {
                    newPage = allocateNewPage();
                    recordPage.setContinuationPage(newPage);
                } else {
                    newPage = recordPage.getContinuationPage();
                }
                recordPage.flush();
                recordPage = newPage;
            }
            recordPage.writeData((byte) b);
            numberOfBytes++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (isRecordWriterClosed) {
                throw new IllegalStateException("Tried to write to closed record");
            }
            int newOffset = off;
            int bytesToWrite = len;
            while (true) {
                if (!recordPage.hasMore()) {
                    PageProxy newPage;
                    if (!recordPage.hasContinuation()) {
                        newPage = allocateNewPage();
                        recordPage.setContinuationPage(newPage);
                    } else {
                        newPage = recordPage.getContinuationPage();
                    }
                    recordPage.flush();
                    recordPage = newPage;
                }
                int bytesWritten = recordPage.writeBytes(b, newOffset, bytesToWrite);
                newOffset += bytesWritten;
                bytesToWrite -= bytesWritten;
                if (bytesToWrite <= 0) {
                    break;
                }
            }
            numberOfBytes += len;
        }

        @Override
        public void flush() throws IOException {
            // Note, the last page may still be unwritten
        }

        @Override
        public void close() throws IOException {
            if (!isRecordWriterClosed) {
                flushAtClose();
                openRecordWriters.remove(this);
                isRecordWriterClosed = true;
                numberOfWriters--;
            }
        }

        private void flushAtClose() throws IOException {
            if (recordPage.hasContinuation()) {
                PageProxy contPage = recordPage.getContinuationPage();
                index.deallocatePages(contPage.getAddress());
                recordPage.setContinuationPage(null);
            }
            recordPage.flush();
            flushRecordDirectory(this);
        }

        public long getStartPageAddress() {
            return startPageAddress;
        }
    }


    public static final int DEFAULT_PAGE_DATA_SIZE = 2 * 1024;
    private static final boolean SAFE_MODE = false;
    private static final int VERSION = 1;
    private static final String BUCKET_FILE_VERSION = BucketFile.class.getName() + " Version " + VERSION;
    static final long INDEX_RECORD_ID = 0;
    private int pageDataSize;
    RandomAccessFile storageFile;
    private long indexStartPage;
    private BucketFileIndex index;
    private List<RecordWriter> openRecordWriters = new ArrayList<RecordWriter>(100);
    private List<RecordReader> openRecordReaders = new ArrayList<RecordReader>(100);
    private int numberOfReaders;
    private int numberOfWriters;
    private boolean isClosed;
    private long allocDataSection;
    private boolean openedReadonly;

    /**
     * Creates or opens the file with the path specified in the file object.<br>
     * The page size is set to {@link #DEFAULT_PAGE_DATA_SIZE}.
     *
     * @param file The bucket file
     * @throws IOException If file creation/open failed.
     */
    public BucketFile(File file) throws IOException {
        this(file, DEFAULT_PAGE_DATA_SIZE, false);
    }

    /**
     * Creates or opens the file for read/write with the path specified in the file object.<br>
     * The page size is set to {@link #DEFAULT_PAGE_DATA_SIZE}.
     *
     * @param file The bucket file
     * @param lock true if file shall be locked to prevent other threads and processes to access the file
     * @throws IOException If file creation/open failed.
     */
    public BucketFile(File file, boolean lock) throws IOException {
        this(file, DEFAULT_PAGE_DATA_SIZE, lock);
    }

    /**
     * Creates or opens the file for read/write with the path specified in the file object.<br>
     * The page size can be defined by the application.
     *
     * @param file         The bucket file
     * @param pageDataSize size of one page
     * @param lock         true if file shall be locked to prevent other threads and processes to access the file
     * @throws IOException If file creation/open failed.
     */
    public BucketFile(File file, int pageDataSize, boolean lock) throws IOException {
        this(file, DEFAULT_PAGE_DATA_SIZE, lock, false);
    }

    /**
     * Creates or opens the file for with the path specified in the file object.<br>
     * The page size can be defined by the application.
     *
     * @param file           The bucket file
     * @param pageDataSize   size of one page
     * @param lock           true if file shall be locked to prevent other threads and processes to access the file
     * @param openedReadonly If true the file is opened for reading only. In this case it must be an existing file. Else
     *                       the file is opened for reading or writing
     * @throws IOException If file creation/open failed.
     */
    public BucketFile(File file, int pageDataSize, boolean lock, boolean openedReadonly) throws IOException {
        this.pageDataSize = pageDataSize;
        this.openedReadonly = openedReadonly;
        storageFile = new RandomAccessFile(file.getAbsolutePath(), openedReadonly ? "r" : "rw");
        if (lock) {
            if (storageFile.getChannel().tryLock() == null) {
                throw new AlreadyLockedException(file);
            }
        }
        try {
            initFile();
        } catch (IOException e) {
            if (storageFile != null) {
                storageFile.close();
            }
            throw e;
        }
    }

    /**
     * Flush output buffers to file.
     *
     * @throws IOException from close
     */
    public void flush() throws IOException {
        flushRecordDirectory();
    }

    /**
     * Closes this file.
     *
     * @throws IOException from close
     */
    public void close() throws IOException {
        if (!isClosed) {
            String openWritersStr = openRecordWriters.toString();
            boolean writersOpen = !openRecordWriters.isEmpty();
            if (!openRecordWriters.isEmpty()) {
                // Attempt to close the open record writers
                for (RecordWriter rw : new ArrayList<RecordWriter>(openRecordWriters)) {
                    rw.close();
                }
            }

            String openReadersStr = openRecordReaders.toString();
            boolean readersOpen = !openRecordReaders.isEmpty();
            if (!openRecordReaders.isEmpty()) {
                // Attempt to close the open record readers
                for (RecordReader rr : new ArrayList<RecordReader>(openRecordReaders)) {
                    rr.close();
                }
            }
            flushRecordDirectory();
            storageFile.close();
            isClosed = true;

            assert !writersOpen : "Tried to close file without closing the following record writers: " + openWritersStr;
            assert !readersOpen : "Tried to close file without closing the following record readers: " + openReadersStr;
        }
    }

    /**
     * Helper for reading a byte buffer from record reader.
     *
     * @param is     Input stream
     * @param buffer Destination byte buffer.
     * @return Number of bytes written to buffer.
     * @throws IOException If read failed
     */
    public int readBuffer(InputStream is, byte[] buffer) throws IOException {
        int offset = 0;
        int n;
        do {
            n = is.read(buffer, offset, buffer.length - offset);
            if (n != -1) {
                offset += n;
            }
        } while (n > 0);
        return offset;
    }

    private void initFile() throws IOException {
        if (storageFile.length() > 0) {
            storageFile.seek(0);
            String version = storageFile.readUTF();
            if (!isSupportedBucketFileVersion(version)) {
                throw new IOException("The version of the file does not match this reader/writer. Expected \"" +
                        BUCKET_FILE_VERSION + "\",found \"" + version + "\"");
            }
            pageDataSize = storageFile.readInt();
            allocDataSection = storageFile.getFilePointer();
            long numBytesInIndex = storageFile.readLong();
            long nextFreeAddress = storageFile.readLong();
            long firstDeallocatedAddress = storageFile.readLong();
            long lastDeallocatedFreeAddress = storageFile.readLong();
            indexStartPage = storageFile.getFilePointer();
            index = BucketFileIndex.readIndex(this);
            index.init(nextFreeAddress, firstDeallocatedAddress, lastDeallocatedFreeAddress, numBytesInIndex, getPageSize());
        } else {
            storageFile.writeUTF(BUCKET_FILE_VERSION);
            storageFile.writeInt(pageDataSize);
            allocDataSection = storageFile.getFilePointer();
            storageFile.writeLong(0); // num bytes in index
            storageFile.writeLong(0); // next free address
            storageFile.writeLong(0); // first deallocated page
            storageFile.writeLong(0); // last deallocated page
            indexStartPage = storageFile.getFilePointer();
            index = new BucketFileIndex(indexStartPage, getPageSize(), this);
            allocateNewPage().flush(); // First Index page
            BucketFileIndex.writeIndex(index, this);
        }
    }

    private boolean isSupportedBucketFileVersion(String version) {
        if (version.equals("class se.marell.bucketfile.BucketFile Version 1")) {
            return true;
        }
        if (version.equals(BUCKET_FILE_VERSION)) {
            return true;
        }

        return false;
    }

    /**
     * Gets a record writer for the specified record ID.
     *
     * A writer must have exclusive access to the file. It is checked that no other writer or reader is active.
     *
     * @param recordId the record ID
     * @return the record writer
     * @throws IOException           If the file could not be read
     * @throws IllegalStateException in case there exist another active writer or reader
     */
    public synchronized OutputStream getRecordWriter(long recordId) throws IOException {
        if (openedReadonly) {
            throw new IllegalStateException("getRecordWriter failed because the file is opened with openedReadonly=true");
        }
        if (numberOfReaders > 0 || numberOfWriters > 0) {
            throw new IllegalStateException("There exists open readers (" + numberOfReaders + ") or writers (" + numberOfWriters + ")");
        }
        if (recordId >= 0) {
            recordId++; // Positive record ID:s are changed to not collide with INDEX_RECORD_ID; negative record ID:s are kept as is.
        }
        return internalGetRecordWriter(recordId);
    }

    /**
     * Gets a reader for the specified record.
     *
     * Multiple readers may exist for a file but reading may not occur when a writer is active.
     * It is checked that no writer is active.
     *
     * @param recordId the record ID
     * @return the record reader; or <code>null</code> if that record does not not exist.
     * @throws IOException           If the file could not be read
     * @throws IllegalStateException in case there exist an active writer
     */
    public synchronized InputStream getRecordReader(long recordId) throws IOException {
        if (numberOfWriters > 0) {
            throw new IllegalStateException("There exist open writers (" + numberOfWriters + ")");
        }
        if (recordId >= 0) {
            recordId++; // Positive record ID:s are changed to not collide with INDEX_RECORD_ID; negative record ID:s are kept as is.
        }
        return internalGetRecordReader(recordId);
    }

    /**
     * Removes the record with the specified ID from the file.
     *
     * Deleting must be done with exclusive access to the file. It is checked that no other writer or reader is active.
     *
     * @param recordId the record ID
     * @throws IOException           If the file could not be read or written
     * @throws IllegalStateException in case there exist another active writer or reader
     */
    public synchronized void removeRecord(long recordId) throws IOException {
        if (openedReadonly) {
            throw new IllegalStateException("removeRecord failed because the file is opened with openedReadonly=true");
        }
        if (numberOfReaders > 0 || numberOfWriters > 0) {
            throw new IllegalStateException("There exists open readers (" + numberOfReaders + ") or writers (" + numberOfWriters + ")");
        }
        if (recordId >= 0) {
            recordId++; // Positive record ID:s are changed to not collide with INDEX_RECORD_ID; negative record ID:s are kept as is.
        }
        index.deallocateRecord(recordId);
    }

    RecordReader internalGetRecordReader(long recordId) throws IOException {
        if (recordId == INDEX_RECORD_ID) {
            return new RecordReader(recordId);
        }
        if (index.getRecordInfo(recordId) != null) {
            return new RecordReader(recordId);
        }
        return null;
    }

    RecordWriter internalGetRecordWriter(long recordId) throws IOException {
        return new RecordWriter(recordId);
    }

    private PageProxy allocateNewPage() throws IOException {
        return new PageProxy(index.getNextFreePageAddress(), true, this);
    }

    private void flushRecordDirectory() throws IOException {
        if (index.isDirty()) {
            BucketFileIndex.writeIndex(index, this);
            storageFile.seek(allocDataSection);
            index.write(storageFile);
        }
    }

    private void flushRecordDirectory(RecordWriter recordWriter) throws IOException {
        if (recordWriter.getRecordId() != INDEX_RECORD_ID) { // Already flushed
            RecordInfo recordInfo = index.getRecordInfo(recordWriter.getRecordId());
            if (recordInfo == null) {
                recordInfo = new RecordInfo(recordWriter.getStartPageAddress());
                index.addRecordInfo(recordWriter.getRecordId(), recordInfo);
            }
            recordInfo.setNumberOfBytes(recordWriter.getNumberOfBytes());
            if (SAFE_MODE) {
                BucketFileIndex.writeIndex(index, this);
            }
        }
    }

    PageProxy getStartPage(long recordId) throws IOException {
        if (recordId == INDEX_RECORD_ID) {
            return new PageProxy(indexStartPage, false, this);
        }
        RecordInfo recordInfo = index.getRecordInfo(recordId);
        if (recordInfo == null) {
            return null;
        }
        return new PageProxy(recordInfo.getStartPageAddress(), false, this);
    }

    int getPageDataSize() {
        return pageDataSize;
    }

    /**
     * For testing only
     *
     * @param out Where to write status
     * @throws IOException If write failed
     */
    void printAllocStatus(PrintStream out) throws IOException {
        index.printAllocStatus(out);
    }

    private int getPageSize() {
        return pageDataSize + 8 + 8;
    }

    void printContents(PrintStream out) throws IOException {
        //out.println(index.asString());
        printDeallocatedRecord(out);
        out.println("Index record");
        out.println("============");
        printRecord(indexStartPage, out);
        out.println("Data Records");
        out.println("============");
        Set<Map.Entry<Long, RecordInfo>> entrySet = index.getRecordSet();
        if (entrySet.isEmpty()) {
            out.println("  None.");
        } else {
            for (Map.Entry<Long, RecordInfo> entry : entrySet) {
                out.println("  Record ID = " + (entry.getKey() - 1));
                RecordInfo recordInfo = entry.getValue();
                out.println("    Number of bytes = " + recordInfo.getNumberOfBytes());
                long startPageAddress = recordInfo.getStartPageAddress();
                printRecord(startPageAddress, out);
            }
        }
    }

    private void printDeallocatedRecord(PrintStream out) {
        out.println("Index: + " + index);

        long address = index.getFirstDeallocatedPage();
        if (address == 0) {
            out.println("    None.");
        } else {
            while (true) {
                try {
                    out.println("    Address: " + address);
                    storageFile.seek(address);
                    long addressOfNextPage = storageFile.readLong();
                    out.println("    Next page: " + addressOfNextPage);
                    byte[] buf = new byte[getPageDataSize()];
                    int nBytes = storageFile.read(buf, 0, buf.length);
                    out.println("    Data: " + Arrays.toString(buf) + (nBytes != buf.length ? " Mismatch in size; expected: " + buf.length + " actual: " + nBytes : ""));
                    address = addressOfNextPage;
                    if (address == 0) {
                        break;
                    }
                } catch (IOException e) {
                    out.println("    Error: " + e.getMessage());
                    break;
                }
            }
        }
    }

    private void printRecord(long startPageAddress, PrintStream out) throws IOException {
        long address = startPageAddress;
        while (true) {
            out.println("    Address: " + address);
            storageFile.seek(address);
            long addressOfNextPage = storageFile.readLong();
            out.println("    Next page: " + addressOfNextPage);
            if (addressOfNextPage == 0) {
                out.println("    End address: " + (address + pageDataSize));
            }
            byte[] buf = new byte[pageDataSize];
            int nBytes = storageFile.read(buf, 0, buf.length);
            out.println("    Data: " + Arrays.toString(buf) + (nBytes != buf.length ? " Mismatch in size; expected: " + buf.length + " actual: " + nBytes : ""));
            address = addressOfNextPage;
            if (address == 0) {
                break;
            }
        }
    }
}
