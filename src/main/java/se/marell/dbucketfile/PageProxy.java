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

import java.io.IOException;

class PageProxy {
    private int dataIndex; // Index in page data (which is equal to the number of bytes written to it)
    byte[] pageData;
    private long pageAddress;
    private long continuationPageAddress;
    private boolean hasBeenFlushed;
    private BucketFile bucketFile;
    private static final int PAGE_MAGIC_NUMBER = 0xabfafcfd;

    PageProxy(long pageAddress, boolean init, BucketFile bucketFile) throws IOException {
        this.bucketFile = bucketFile;
        this.pageAddress = pageAddress;
        if (init) {
            pageData = new byte[bucketFile.getPageDataSize()];
        } else {
            readPageData();
        }
    }

    @Override
    public String toString() {
        return new String(pageData);
    }

    PageProxy getContinuationPage() throws IOException {
        if (continuationPageAddress != 0) {
            return new PageProxy(continuationPageAddress, false, bucketFile);
        }
        return null;
    }

    void setContinuationPage(PageProxy page) {
        if (page == null) {
            continuationPageAddress = 0;
        } else {
            continuationPageAddress = page.getAddress();
        }
    }

    boolean hasContinuation() {
        return continuationPageAddress != 0;
    }

    long getAddress() {
        return pageAddress;
    }

    byte readNextByte() {
        return pageData[dataIndex++];
    }

    private void readPageData() throws IOException {
        if (pageAddress != 0 && pageData == null) {
            bucketFile.storageFile.seek(pageAddress);
            continuationPageAddress = bucketFile.storageFile.readLong();
            long magicNo = bucketFile.storageFile.readLong();
            if (magicNo != PAGE_MAGIC_NUMBER) {
                throw new IOException("Magic number of page is wrong for page at address " + pageAddress);
            }
            pageData = new byte[bucketFile.getPageDataSize()];
            // Ignore return value because first time -1 will be returned when file is empty
            bucketFile.storageFile.read(pageData, 0, bucketFile.getPageDataSize());
        }
    }

    void writeData(byte data) {
        pageData[dataIndex++] = data;
    }

    boolean hasMore() throws IOException {
        readPageData();
        return dataIndex < pageData.length;
    }

    public void flush() throws IOException {
        if (!hasBeenFlushed) {
            bucketFile.storageFile.seek(pageAddress);
            bucketFile.storageFile.writeLong(continuationPageAddress);
            bucketFile.storageFile.writeLong(PAGE_MAGIC_NUMBER);
            bucketFile.storageFile.write(pageData, 0, dataIndex);
            hasBeenFlushed = true;
        }
    }

    /**
     * Write the specified array or part of it to this page.
     *
     * @param buf
     * @param offset
     * @param nBytesToWrite
     * @return The number of bytes written
     */
    int writeBytes(byte[] buf, int offset, int nBytesToWrite) {
        int nAvailableInPage = bucketFile.getPageDataSize() - dataIndex;
        int bytesWritten = Math.min(nAvailableInPage, nBytesToWrite);
        System.arraycopy(buf, offset, pageData, dataIndex, bytesWritten);
        dataIndex += bytesWritten;
        return bytesWritten;
    }

    int readBytes(byte[] buf, int offset, int bytesToRead) {
        int nAvailableInPage = bucketFile.getPageDataSize() - dataIndex;
        int bytesRead = Math.min(nAvailableInPage, bytesToRead);
        System.arraycopy(pageData, dataIndex, buf, offset, bytesRead);
        dataIndex += bytesRead;
        return bytesRead;
    }
}
