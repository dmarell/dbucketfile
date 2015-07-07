/*
 * Copyright (c) 2011 Daniel Marell, Joel Binnquist
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

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ByteArrayReadWriteTest {
    private static final File TEST_FILE = new File(System.getProperty("java.io.tmpdir"), "test.dbucketfile");
    private final int TEST_DATA_SIZE = 10000;
    private final int TEST_RECORD_NUMBER = 42;

    private void verifyBucket(BucketFile bf, InputStream is, int recNum) throws IOException {
        byte[] buffer = new byte[TEST_DATA_SIZE];
        int readSize = bf.readBuffer(is, buffer);
        assertEquals(TEST_DATA_SIZE, readSize);
        byte[] arr = new byte[readSize];
        System.arraycopy(buffer, 0, arr, 0, readSize);
        compareArrays(createTestData(recNum, TEST_DATA_SIZE), arr);
    }

    @Test
    public void testReadAndWriteFile() throws Exception {
        if (TEST_FILE.exists()) {
            assertTrue(TEST_FILE.delete());
        }

        BucketFile bf = new BucketFile(TEST_FILE);
        OutputStream os = bf.getRecordWriter(TEST_RECORD_NUMBER);
        os.write(createTestData(TEST_RECORD_NUMBER, TEST_DATA_SIZE));
        os.close();
        bf.close();

        bf = new BucketFile(TEST_FILE);
        InputStream is = bf.getRecordReader(TEST_RECORD_NUMBER);
        verifyBucket(bf, is, TEST_RECORD_NUMBER);
        is.close();
        bf.close();

        bf = new BucketFile(TEST_FILE);
        os = bf.getRecordWriter(TEST_RECORD_NUMBER);
        os.write(createTestData(TEST_RECORD_NUMBER + 1, TEST_DATA_SIZE));
        os.close();
        bf.close();

        bf = new BucketFile(TEST_FILE);
        is = bf.getRecordReader(TEST_RECORD_NUMBER);
        verifyBucket(bf, is, TEST_RECORD_NUMBER + 1);
        is.close();
        bf.close();
    }

    private void compareArrays(byte[] exp, byte[] arr) {
        assertEquals(exp.length, arr.length);
        for (int i = 0; i < exp.length; ++i) {
            assertEquals("index " + i + ":expected " + exp[i] + ",actual " + arr[i], exp[i], arr[i]);
        }
    }

    private byte[] createTestData(int dataStart, int size) {
        byte[] a = new byte[size];
        for (int i = 0; i < a.length; ++i) {
            a[i] = (byte) (i + dataStart);
        }
        return a;
    }

    private String toString(byte[] arr) {
        StringBuilder sb = new StringBuilder();
        for (byte b : arr) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(b);
        }
        return sb.toString();
    }
}
