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

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BucketFileLargeTest {
    private static final String TEST_FILE_NAME = "test.dbucketfile";
    private static final int BUCKET_SIZE = 5000;

    @Test
    public void testSmallFile() throws Exception {
        testFile(3000, false, false);
    }

    @Ignore // Verbose print outs
    @Test
    public void testSmallFileDebugPrint() throws Exception {
        testFile(10, true, true);
    }

    @Ignore // Shall only be run on request, takes time and disk space
    @Test
    public void testVeryLargeFile() throws Exception {
        testFile(3000000, true, false); // 6.3 GB ~ 5 minutes Core i7 2.67
    }

    private void testFile(int numBuckets, boolean debugPrint, boolean detailedDebugPrint) throws Exception {
        File file = new File(System.getProperty("java.io.tmpdir"), TEST_FILE_NAME);
        if (file.exists()) {
            file.delete();
        }

        if (debugPrint) {
            System.out.println("Buckets=" + numBuckets);
        }

        BucketFile bf = new BucketFile(file);

        // Write small data pieces in each bucket
        for (int i = 0; i < numBuckets; ++i) {
            writeBucket(bf, i, 1, detailedDebugPrint);
        }
        if (debugPrint) {
            System.out.println("Wrote small data arrays. Verifying content...");
        }

        // Verify content
        for (int i = 0; i < numBuckets; ++i) {
            InputStream reader = bf.getRecordReader(i);
            byte[] arr = new byte[2];
            int n = reader.read(arr);
            assertThat(n, is(1));
            assertThat(arr[0], is((byte) 1));
            reader.close();
        }

        if (debugPrint) {
            System.out.println("Verified content, writing varying size arrays...");
        }

        // Rewrite buckets with data pieces of varying size
        for (int i = 0; i < numBuckets; ++i) {
            writeBucket(bf, i, i % BUCKET_SIZE + 1, detailedDebugPrint);
        }

        if (debugPrint) {
            System.out.println("Wrote varying size data arrays");
        }

        bf.close();

        bf = new BucketFile(file);

        // Verify content
        for (int i = 0; i < numBuckets; ++i) {
            InputStream reader = bf.getRecordReader(i);
            byte[] arr = new byte[BUCKET_SIZE];
            int n = reader.read(arr);
            if (detailedDebugPrint) {
                byte[] arrCopy = new byte[n];
                System.arraycopy(arr, 0, arrCopy, 0, n);
                System.out.println("read bucket " + i + ":" + Arrays.toString(arrCopy));
            }
            assertThat("record number=" + i, n, is(i % BUCKET_SIZE + 1));
            for (int j = 0; j < n; ++j) {
                assertThat(arr[j], is((byte) (i % BUCKET_SIZE + 1)));
            }
            reader.close();
        }

        bf.close();

        if (debugPrint) {
            System.out.println("file size is " + file.length() + " (" + String.format("%.1f GB", file.length() / 1e9) + ")");
        }
    }

    private void writeBucket(BucketFile bf, int recordNum, int dataLength, boolean detailedDebugPrint) throws IOException {
        assert dataLength > 0;
        byte[] data = new byte[dataLength];
        Arrays.fill(data, (byte) dataLength);
        if (detailedDebugPrint) {
            System.out.println("writeBucket " + recordNum + ":" + Arrays.toString(data));
        }
        OutputStream writer = bf.getRecordWriter(recordNum);
        writer.write(data);
        writer.close();
    }
}
