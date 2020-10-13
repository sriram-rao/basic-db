## Project 1 Report


### 1. Basic information
 - Team #: 4
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-4
 - Student 1 UCI NetID: srirar1
 - Student 1 Name: Sriram Rao
 - Student 2 UCI NetID (if applicable): subhamok
 - Student 2 Name (if applicable): Subhamoy Karmakar


### 2. Internal Record Format
- Show your record format design.

We keep fixed sized slots for the number of fields (N) and the N offsets to the end of each field value  
<insert diagram>

- Describe how you store a null field.

We store the offset as an invalid number (-1) for null values.

- Describe how you store a VarChar field.

We have the memory offsets specify the end of the corresponding field which will help us for variable length fields.

- Describe how your record design satisfies O(1) field access.

While accessing a field, following are the things we have access to beforehand: record schema, and amount of memory being used for record fields and offsets.
Since we know the number of bytes per metadata field (offset or record size) and memory offsets to each field, we can:
1. Find the offset for field (N-1) and offset for field N in constant time
2. Use these to access the field N.


### 3. Page Format
- Show your page format design.

<insert diagram>
 We keep a vector of records at the start of the page and the slot directory at the end.
 This leaves all free space as one fragment instead of having it distributed in between records and possibly even within the slot directory.
 On update/delete of records, we plan on shifting the records to uilize the newly freed space.
 We will update the documentation with our plan for slot directory on deletion of a record.

The slot directory is similar to the design described in the lecture.
* We store a vector with the following information: memory offset to the record inside the current page and record length.
* We have the following fixed length information: number of records and amount of free space remaining.
* We store these as 2-byte variables (revisit this line on finalization of data type).
* The number of records and free space are stored at the end of the page so that access to these can be a constant time operation.
* The vector with record offsets expands into the free space with inserts.


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.
TODO

- How many hidden pages are utilized in your design?

Currently, in this manner we use 1 hidden page to store the read, write and append counters, and other private metadata like the number of data pages and the map of page numbers to available free space.

- Show your hidden page(s) format design if applicable

Currently, we store the three unsigned int counters read, write and append page counters, and the number of data pages.
We also store a map of page number to available free space. 
In one hidden page, we can store metadata about X pages and their free spaces.
We plan to manage with this with the following logic: if a page is considered full (actually full or not enough space for even the fixed data fields of a record), we will remove it from this map.

### 5. Implementation Detail
- Other implementation details goes here.

We have started with using cstdio for file handling (i.e. fopen, fread, etc). This is easily changed in the paged file manager if we decide to migrate.

### 6. Member contribution (for team of two)
We discussed the design for PFM, Record, Slot Directory and Page at first.
We divided the code to be written. For the first project, Sriram started with the PagedFileManager and FileHandle. Subhamoy started with the models for Record and Slot Directory. The implementation for using these in Record Manager is split and in a sense, it is a combined effort.


### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)

We just want to mention that this is a very interesting project and helps us understand very well the intricacies of a DBMS. 
The scope for project 1 was rightly set so that it gave us some time to get comfortable with C++.

- Feedback on the project to help improve the project. (optional)
