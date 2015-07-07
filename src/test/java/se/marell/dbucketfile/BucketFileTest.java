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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static junit.framework.Assert.*;

public class BucketFileTest {
    private static final File TEST_FILE = new File(System.getProperty("java.io.tmpdir"), "test.dbucketfile");
    private static PrintStream out = new PrintStream(
            new OutputStream() {
                // NOP stream
                @Override
                public void write(int b) {
                }
            });
    //private static PrintStream out = System.out;

    @Before
    public void setUp() throws Exception {
        if (System.getProperty("ENABLE_LOG_IN_UNIT_TEST") != null) {
            out = System.out;
            out.println("Logging is enabled");
        }
        deleteTestFile();
    }

    private void deleteTestFile() {
        if (TEST_FILE.exists()) {
            if (!TEST_FILE.delete()) {
                throw new RuntimeException("Could not delete " + TEST_FILE.getAbsolutePath());
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        deleteTestFile();
    }

    @Test
    public void testReadingAndWriting() throws Exception {
        BucketFile testBucketFile = null;
        try {
            testBucketFile = new BucketFile(TEST_FILE);

            // Write a record (ID = 0)
            OutputStream writer = testBucketFile.getRecordWriter(0);
            byte[] data = new byte[111111];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Writing record 0");
            long t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            long t2 = System.nanoTime();
            out.printf(
                    "Writing record 0 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(0, data);

            // Write another record (ID = 2)
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(2);
            data = new byte[222222];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Writing record 2");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Writing record 2 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(2, data);

            // Update first record
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(0);
            data = new byte[333333];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Updating record 0");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Updating record 0 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(0, data);

            // Update 2nd record
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(2);
            data = new byte[444444];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Updating record 2");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Updating record 2 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(2, data);

            // Write a third record (ID = -1)
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(-1);
            data = new byte[555555];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Writing record -1");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            long t3 = System.nanoTime();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Updating record -1 done; write record duration: %f ms; %f us/byte\n",
                    (t3 - t1) / 1000000.0,
                    ((t3 - t1) / 1000.0) / data.length);
            out.printf(
                    "Updating record -1 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(-1, data);
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (IOException ignore) {
                    fail();
                }
            }
        }
    }

    static class MyObject implements Serializable {
        String name;
        int value;

        public MyObject() {
        }

        public MyObject(String name, int value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyObject myObject = (MyObject) o;

            if (value != myObject.value) {
                return false;
            }
            return !(name != null ? !name.equals(myObject.name) : myObject.name != null);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + value;
            return result;
        }

    }

    @Test
    public void testWriteObject() throws Exception {
        out.println("WriteObject");

        BucketFile testBucketFile = null;
        try {
            MyObject myObject = new MyObject("Kalle", 4711);
            testBucketFile = new BucketFile(TEST_FILE);
            // Write a record (ID = 1)
            OutputStream writer = testBucketFile.getRecordWriter(1);
            out.println("Writing record 1");
            long t1 = System.nanoTime();
            ObjectOutputStream oos = new ObjectOutputStream(writer);
            oos.writeObject(myObject);
            writer.close();
            long t2 = System.nanoTime();
            out.printf(
                    "Writing record 1 done; duration: %f ms\n",
                    (t2 - t1) / 1000000.0);

            InputStream reader = testBucketFile.getRecordReader(1);
            ObjectInputStream ois = new ObjectInputStream(reader);
            MyObject readObject = (MyObject) ois.readObject();

            assertEquals("Kalle", readObject.name);
            assertEquals(4711, readObject.value);

            reader.close();
            testBucketFile.close();
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testUsageExample() throws Exception {
        TEST_FILE.delete();

        MyObject myObjectToWrite = getMyObject(); // Some object that shall be written

        // Open a bucket file
        BucketFile bucketFile = null;
        try {
            bucketFile = new BucketFile(TEST_FILE);

            // Write to record with ID = 1
            OutputStream writer = null;
            try {
                writer = bucketFile.getRecordWriter(1);
                ObjectOutputStream oos = new ObjectOutputStream(writer);
                oos.writeObject(myObjectToWrite);
            } finally {
                if (writer != null) {
                    try {
                        writer.close(); // Always close recordWriter when done
                    } catch (IOException e) {
                        // This example ignores exception when closing
                    }
                }
            }

            // Read a record with ID = 1
            InputStream reader = null;
            try {
                reader = bucketFile.getRecordReader(1);
                ObjectInputStream ois = new ObjectInputStream(reader);
                MyObject myReadObject = (MyObject) ois.readObject();
                assert myReadObject.equals(myObjectToWrite);
            } finally {
                if (reader != null) {
                    try {
                        reader.close(); // Always close recordWriter when done
                    } catch (IOException e) {
                        // This example ignores exception when closing
                    }
                }
            }
        } finally {
            if (bucketFile != null) {
                try {
                    bucketFile.close();
                } catch (IOException e) {
                    // This example ignores exception when closing
                }
            }
        }
    }

    private MyObject getMyObject() {
        return new MyObject("Test", 22);
    }

    @Test
    public void testSimulateLazyLoad() throws Exception {
        out.println("SimulateLazyLoad");
        BucketFile testBucketFile = null;
        try {

            // Simulate writing of FooBar objects
            testBucketFile = new BucketFile(TEST_FILE);
            List<MyFooBarIndex> indices = new ArrayList<MyFooBarIndex>(10);
            MyObject[] fooBars = new MyObject[25000];
            int oid = 1000;
            OutputStream writer;
            for (int i = 0; i < fooBars.length; i++) {
                int fooBarOid = oid++;
                fooBars[i] = new MyObject("FooBar-" + fooBarOid, fooBarOid);
                writer = testBucketFile.getRecordWriter(fooBarOid);
                ObjectOutputStream oos = new ObjectOutputStream(writer);
                oos.writeObject(fooBars[i]);
                writer.close();
                for (int j = 0; j < 100; j++) {
                    // Sub index contains map from child component object to aggregating parent
                    getSubIndex(oid, indices).put((long) oid++, (long) fooBarOid);
                }
            }

            // Write indices
            ObjectOutputStream oos;
            int i = -1;
            for (MyFooBarIndex fooBarIndex : indices) {
                writer = testBucketFile.getRecordWriter(i--);
                oos = new ObjectOutputStream(writer);
                oos.writeObject(fooBarIndex);
                writer.close();
            }
            testBucketFile.close();

            // Simulate updating of one object on a parent
            long t1 = System.nanoTime();
            testBucketFile = new BucketFile(TEST_FILE);
            // Simulate read of a subindex
            long childComponentOid = 12345;
            long subIndexId = getSubIndexId(childComponentOid);
            InputStream recordReader = testBucketFile.getRecordReader(subIndexId);
            ObjectInputStream ois = new ObjectInputStream(recordReader);
            Map<Long, Long> subIndex = (Map<Long, Long>) ois.readObject();
            recordReader.close();
            long fooBarRecordId = subIndex.get(childComponentOid);
            // Simulate read of a fooBar object
            recordReader = testBucketFile.getRecordReader(fooBarRecordId);
            ois = new ObjectInputStream(recordReader);
            MyObject fooBar = (MyObject) ois.readObject();
            ois.close();
            long t2 = System.nanoTime();
            assertEquals("FooBar-" + fooBarRecordId, fooBar.name);
            assertEquals(fooBarRecordId, fooBar.value);

            // Simulate write of subindex
            writer = testBucketFile.getRecordWriter(subIndexId);
            oos = new ObjectOutputStream(writer);
            oos.writeObject(subIndex);
            writer.close();
            // Simulate write of fooBar object
            writer = testBucketFile.getRecordWriter(fooBarRecordId);
            oos = new ObjectOutputStream(writer);
            oos.writeObject(fooBar);
            oos.close();
            testBucketFile.close();
            long t3 = System.nanoTime();
            out.printf(
                    "Reading fooBar index (size=%d) and one parent object; duration: %f ms\n", subIndex.size(), (t2 - t1) / 1000000.0);
            out.printf("Updating fooBar index and one object; duration: %f ms\n", (t3 - t2) / 1000000.0);
        } catch (OutOfMemoryError ignore) {
            testBucketFile = null;
            fail("Out of memory");
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (AssertionError e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    private MyFooBarIndex getSubIndex(int oid, List<MyFooBarIndex> indices) {
        long indexId = getSubIndexId(oid);
        int pos = (int) ((-indexId) - 1);
        if (indices.size() - 1 < pos) {
            indices.add(new MyFooBarIndex());
        }
        return indices.get(pos);
    }

    private long getSubIndexId(long oid) {
        long indexId = oid / 50000;
        return (-indexId) - 1;
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        out.println("ConcurrentAccess");
        BucketFile testBucketFile = null;
        try {
            boolean fail = false;

            testBucketFile = new BucketFile(TEST_FILE);
            OutputStream writer = testBucketFile.getRecordWriter(1);
            writer.write(1);
            writer.close();
            writer = testBucketFile.getRecordWriter(2);
            writer.write(2);
            writer.close();
            testBucketFile.close();

            testBucketFile = new BucketFile(TEST_FILE);

            // Open 1st reader
            InputStream reader1 = testBucketFile.getRecordReader(1);

            // Open 2nd reader
            InputStream reader2 = null;
            try {
                reader2 = testBucketFile.getRecordReader(2);
            } catch (IllegalStateException e) {
                fail(e + "");
            }
            reader2.close();

            // Open writer
            OutputStream writer1 = null;
            try {
                writer1 = testBucketFile.getRecordWriter(1);
                fail = true;
            } catch (IllegalStateException e) {
                // Success
            }
            assertFalse(fail);
            reader1.close();

            // Open writer again (after that all readers have been closed)
            try {
                writer1 = testBucketFile.getRecordWriter(1);
            } catch (IllegalStateException e) {
                fail(e + "");
            }

            // Open 2nd writer
            OutputStream writer2 = null;
            try {
                writer2 = testBucketFile.getRecordWriter(2);
                fail = true;
            } catch (IllegalStateException e) {
                // Success
            }
            assertFalse(fail);

            // Open reader
            try {
                reader1 = testBucketFile.getRecordReader(1);
                fail = true;
            } catch (IllegalStateException e) {
                // Success
            }
            assertFalse(fail);
            if (writer1 != null) {
                writer1.close();
            }
            testBucketFile.close();
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testClose() throws Exception {
        out.println("Close");
        BucketFile testBucketFile = null;
        boolean fail;
        try {
            // Open two writes, write something and then close writers and BucketFile properly
            testBucketFile = new BucketFile(TEST_FILE);
            OutputStream writer = testBucketFile.getRecordWriter(1);
            writer.write(1);
            writer.close();
            writer = testBucketFile.getRecordWriter(2);
            writer.write(2);
            writer.close();
            testBucketFile.close();

            // Close BucketFile before record readers
            testBucketFile = new BucketFile(TEST_FILE);
            testBucketFile.getRecordReader(1);
            testBucketFile.getRecordReader(2);

            fail = true;
            try {
                testBucketFile.close();
            } catch (AssertionError e) {
                fail = false;
            }
            assertFalse(fail);


            // Try open two writers
            testBucketFile = new BucketFile(TEST_FILE);
            testBucketFile.getRecordWriter(1);

            // Should fail trying to open a 2nd writer
            try {
                testBucketFile.getRecordWriter(2);
            } catch (IllegalStateException e) {
                fail = false;
                try {
                    testBucketFile.close();
                } catch (AssertionError ignore) {
                }
            }
            assertFalse(fail);


            // Try open writer without closing reader
            testBucketFile = new BucketFile(TEST_FILE);
            testBucketFile.getRecordReader(1);

            // Should fail trying to open any writer
            try {
                testBucketFile.getRecordWriter(2);
            } catch (IllegalStateException e) {
                fail = false;
                try {
                    testBucketFile.close();
                } catch (AssertionError ignore) {
                }
            }
            assertFalse(fail);

            testBucketFile.close();
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testDeallocation() throws Exception {
        out.println("Deallocation");
        BucketFile testBucketFile = null;
        try {
            testBucketFile = new BucketFile(TEST_FILE);

            // Write a record (ID = 1)
            OutputStream writer = testBucketFile.getRecordWriter(1);
            byte[] data = new byte[10000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Writing record 1");
            long t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            long t2 = System.nanoTime();
            out.printf(
                    "Writing record 1 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(1, data);
            long size1 = TEST_FILE.length();

            // Update first record, but smaller
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(1);
            data = new byte[100];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Updating record 1");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Updating record 1 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(1, data);
            long size2 = TEST_FILE.length();
            assertTrue(size2 <= size1);

            // Write another record (ID = 2)
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(2);
            data = new byte[5000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Writing record 2");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Writing record 2 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(2, data);
            size2 = TEST_FILE.length();
            assertTrue(size2 <= size1);

            // Update 2nd record
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(2);
            data = new byte[100];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Updating record 2");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Updating record 2 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(2, data);
            size2 = TEST_FILE.length();
            assertTrue(size2 <= size1);

            // Write a third record (ID = 3)
            testBucketFile = new BucketFile(TEST_FILE);
            writer = testBucketFile.getRecordWriter(3);
            data = new byte[100];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Writing record 3");
            t1 = System.nanoTime();
            writer.write(data);
            writer.close();
            long t3 = System.nanoTime();
            testBucketFile.close();
            t2 = System.nanoTime();
            out.printf(
                    "Updating record 3 done; write record duration: %f ms; %f us/byte\n",
                    (t3 - t1) / 1000000.0,
                    ((t3 - t1) / 1000.0) / data.length);
            out.printf(
                    "Updating record 3 done; duration: %f ms; %f us/byte\n",
                    (t2 - t1) / 1000000.0,
                    ((t2 - t1) / 1000.0) / data.length);
            verifyData(2, data);
            size2 = TEST_FILE.length();
            assertTrue(size2 <= size1);
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testRemove2() throws Exception {
        out.println("Remove2");
        BucketFile testBucketFile = null;
        try {
            // Write a record (ID = 0)
            byte[] data0 = writeRecord(0, 12);
            verifyData(0, data0);

            // Write a record (ID = 1)
            byte[] data1 = writeRecord(1, 23);
            verifyData(1, data1);

            // Write a record (ID = 2)
            byte[] data2 = writeRecord(2, 34);
            verifyData(2, data2);

            // Write a record (ID = 3)
            byte[] data3 = writeRecord(3, 45);
            verifyData(3, data3);

            // Remove record 1
            testBucketFile = new BucketFile(TEST_FILE, 10, false);
            testBucketFile.removeRecord(1);
            assertNull(testBucketFile.getRecordReader(1));
            testBucketFile.close();

            // Write a record (ID = 4)
            byte[] data4 = writeRecord(4, 56);
            verifyData(4, data4);

            // Remove record 2
            testBucketFile = new BucketFile(TEST_FILE, 10, false);
            testBucketFile.removeRecord(2);
            assertNull(testBucketFile.getRecordReader(2));
            testBucketFile.removeRecord(2);
            testBucketFile.close();

            // Write a record which consumes all deallocated records (ID = 5)
            byte[] data5 = writeRecord(5, 1000);
            verifyData(5, data5);

            // Remove record 3
            testBucketFile = new BucketFile(TEST_FILE, 10, false);
            testBucketFile.removeRecord(3);
            assertNull(testBucketFile.getRecordReader(3));
            testBucketFile.close();
            verifyData(5, data5);
        } finally {
            if (testBucketFile != null) {
                testBucketFile.close();
            }
        }
    }

    private byte[] writeRecord(int id, int size) throws IOException {
        OutputStream writer = null;
        BucketFile testBucketFile = null;
        try {
            testBucketFile = new BucketFile(TEST_FILE, 10, false);
            writer = testBucketFile.getRecordWriter(id);
            out.println("writeRecord (before)");
            testBucketFile.printContents(out);
            byte[] data = new byte[size];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            out.println("Writing record " + id);
            writer.write(data);
            return data;
        } catch (IOException e) {
            fail(e.getMessage());
            throw e;
        } finally {
            if (writer != null) {
                writer.close();
            }
            out.println("writeRecord (after)");
            if (testBucketFile != null) {
                testBucketFile.printContents(out);
                testBucketFile.close();
            }
        }
    }

    @Test
    public void testReadEmptyRecord() throws Exception {
        out.println("ReadEmptyRecord");
        BucketFile testBucketFile = null;
        try {
            testBucketFile = new BucketFile(TEST_FILE);
            InputStream reader = testBucketFile.getRecordReader(4711);
            assertNull(reader);
            testBucketFile.close();
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testRemoveRecord() throws Exception {
        out.println("RemoveRecord");
        BucketFile testBucketFile = null;
        try {
            testBucketFile = new BucketFile(TEST_FILE);
            OutputStream writer = testBucketFile.getRecordWriter(4711);
            writer.write(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
            writer.close();
            InputStream reader = testBucketFile.getRecordReader(4711);
            assertNotNull(reader);
            reader.close();
            testBucketFile.printAllocStatus(out);
            testBucketFile.close();

            testBucketFile = new BucketFile(TEST_FILE);
            reader = testBucketFile.getRecordReader(4711);
            assertNotNull(reader);
            reader.close();
            testBucketFile.printAllocStatus(out);
            testBucketFile.removeRecord(4711);
            reader = testBucketFile.getRecordReader(4711);
            assertNull(reader);
            testBucketFile.printAllocStatus(out);
            testBucketFile.close();

            testBucketFile = new BucketFile(TEST_FILE);
            reader = testBucketFile.getRecordReader(4711);
            assertNull(reader);
            testBucketFile.printAllocStatus(out);
            testBucketFile.close();
        } finally {
            if (testBucketFile != null) {
                try {
                    testBucketFile.close();
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    private static class LargeObject implements Serializable {
        private byte[] bytes = new byte[1000000];

        {
            Arrays.fill(bytes, (byte) 23);
        }
    }

    @Test
    public void testCompareWithNormalFileWrite() throws Exception {
        out.println("CompareWithNormalFileWrite");

        BucketFile bucketFile = null;
        try {
            int[] pageDataSizes = new int[]{2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024};

            for (int i = 0; i < pageDataSizes.length * 5; i++) {
                deleteTestFile();
                //Thread.sleep(100);

                int pageDataSize = pageDataSizes[i % pageDataSizes.length];
                LargeObject object = new LargeObject();
                // FOS Write
                long t1 = System.nanoTime();
                FileOutputStream fos = new FileOutputStream("test.fos");
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeObject(object);
                oos.close();
                long t2 = System.nanoTime();

                // HF Write
                long t5 = System.nanoTime();
                bucketFile = new BucketFile(TEST_FILE, pageDataSize, true);
                OutputStream os = bucketFile.getRecordWriter(0);
                oos = new ObjectOutputStream(os);
                oos.writeObject(object);
                oos.close();
                bucketFile.close();
                long t6 = System.nanoTime();
                object = null;

                // FOS read
                long t3 = System.nanoTime();
                FileInputStream fis = new FileInputStream("test.fos");
                ObjectInputStream ois = new ObjectInputStream(fis);
                LargeObject o1 = (LargeObject) ois.readObject();
                ois.close();
                long t4 = System.nanoTime();
                new File("test.fos").delete();
                o1 = null;

                // HF Read
                long t7 = System.nanoTime();
                bucketFile = new BucketFile(TEST_FILE, pageDataSize, true);
                InputStream is = bucketFile.getRecordReader(0);
                ois = new ObjectInputStream(is);
                o1 = (LargeObject) ois.readObject();
                ois.close();
                bucketFile.close();
                long t8 = System.nanoTime();
                o1 = null;


                out.printf("Page data size: %d\n", pageDataSize);
                out.printf("FOS write: %f ms\n", (t2 - t1) / 1000000.0);
                out.printf("FOS read: %f ms\n", (t4 - t3) / 1000000.0);
                out.printf("HF write: %f ms\n", (t6 - t5) / 1000000.0);
                out.printf("HF read: %f ms\n", (t8 - t7) / 1000000.0);
            }
        } finally {
            if (bucketFile != null) {
                bucketFile.close();
            }
        }
    }

    private void verifyData(int id, byte[] expectedData) throws IOException {
        InputStream reader = null;
        BucketFile bucketFile = null;
        try {
            bucketFile = new BucketFile(TEST_FILE);
            out.println("readRecord");
            bucketFile.printContents(out);

            reader = bucketFile.getRecordReader(id);
            verifyDataMultipleReads(expectedData, reader);
            reader.close();

            reader = bucketFile.getRecordReader(id);
            verifyDataSingleRead(expectedData, reader);

            bucketFile.printAllocStatus(out);
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (reader != null) {
                bucketFile.close();
            }
        }
    }

    private void verifyDataMultipleReads(byte[] expectedData, InputStream reader) throws IOException {
        int i = 0;
        for (byte b : expectedData) {
            assertEquals("Data mismatch in pos: " + i, b, (byte) reader.read());
            i++;
        }
        assertTrue("Unexpected additional data available", reader.read() == -1);
    }

    private void verifyDataSingleRead(byte[] expectedData, InputStream reader) throws IOException {
        byte[] buffer = new byte[expectedData.length + 1];
        int len = reader.read(buffer);
        assertEquals("read chunk", expectedData.length, len);

        int i = 0;
        for (byte b : expectedData) {
            assertEquals("Data mismatch in pos: " + i, b, buffer[i]);
            i++;
        }
    }

    static class MyFooBarIndex extends HashMap<Long, Long> implements Externalizable {
        public MyFooBarIndex() {
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                long key = in.readLong();
                long value = in.readLong();
                put(key, value);
            }
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(size());
            for (Map.Entry<Long, Long> entry : entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
            }
        }
    }
}
