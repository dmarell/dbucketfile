## dbucketfile
``dbucketfile`` is a library containing the class ``BucketFile`` implementing a file that works as a memory heap.

It allows for allocating records in the file and rewrite those records. ``BucketFile`` handles the case
when a record size changes. It is in a sense a document database with a key limited to an integer.

The library is packaged as an OSGi bundle.

### Release notes
* Version 1.1.8 - 2015-07-07  Moved to Github
* Version 1.1.7 - 2014-02-08
  * Java 7
  * Changed pom versioning mechanism.
  * Extended site information.
  * Updated bundle plugin version
* Version 1.0 - 2011-10-18  First version released open source.

### Maven usage
```
<repositories>
  <repository>
    <id>marell</id>
    <url>http://marell.se/nexus/content/repositories/releases/</url>
  </repository>
</repositories>
...
<dependency>
  <groupId>se.marell</groupId>
  <artifactId>dbucketfile</artifactId>
  <version>1.1.7</version>
</dependency>
```
### File structure
The file has the following structure:
  * Version info: ``String``
  * Size of data part in a page: ``int``
  * Number of bytes in index record: ``long``
  * Next free page address: ``long``
  * Address of first deallocated page: ``long``
  * Address of last deallocated page: ``long``
  * Start page for Record 0 (Index record)
  * pages...

The file is built of pages with a fix size. One record is divided among several pages in
case it does not fit into one page. In these cases the pages for one record are
linked. The format of a page is:
  * Address to next page: long
  * Data: ``byte[pageDataSize]``

Information about all records are kept in an index so that look-up of a certain record is quick. Records
are identified by a number {Long.MIN_VALUE..(Long.MAX_VALUE-1)}. It is up to the application of the file to
keep track of which record ID:s are used and how they map to application objects.

### Usage example

```
  MyObject myObjectToWrite = getMyObject(); // Some object that shall be written

  // Open a BucketFile
  BucketFile bucketFile = null;
  try {
    bucketFile = new BucketFile(new File("my_heap_file.hpf"));

    // Write to record with ID = 1
    OutputStream writer = null;
    try {
      writer = bucketFile.getRecordWriter(1);
      ObjectOutputStream oos = new ObjectOutputStream(writer);
      oos.writeObject(myObjectToWrite);
    } finally {
      if (writer != null) {
        try {
          writer.close(); // Close recordWriter when done
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
          reader.close(); // Close recordReader when done
        } catch (IOException e) {
          // This example ignores exception when closing
        }
      }
    }
  } finally {
    if (bucketFile != null) {
      try {
        bucketFile.close();
      } catch (IOException ignore) {
      }
    }
  }
```
