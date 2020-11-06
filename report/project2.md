## Project 2 Report


### 1. Basic information
 - Team #: 4
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-fall20-team-4
 - Student 1 UCI NetID: srirar1
 - Student 1 Name: Sriram Rao

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

My catalog tables have the design discussed in the lectures.
	- Table 
		- table-id: int
		- table-name: varchar(50)
		- file-name: varchar(50)
	- Column 
		- table-id: int
		- column-name: varchar(50)
		- column-type: int - This is the data type of the column
		- column-length: int - This is the size of the column
		- column-position: int - This is the index position of the column in the record, relative to other columns.



### 3. Internal Record Format (in case you have changed from P1, please re-enter here)

Additional behaviour for the record format: if the record has been moved elsewhere:
- The attribute count is marked -1.
- The first offset correctly points to the end of the first field, which is the new page number.
- The second offset correctly points to the end of the second field, which is the new slot number.

The rest are **unchanged** from P1. They have been copy-pasted for convenience.

- Show your record format design.

We keep fixed sized slots for the number of fields (N) and the N offsets to the end of each field value  

![Record Structure](images/record_structure.png)

- Describe how you store a null field.

We store the offset as an invalid number (-1) for null values.

- Describe how you store a VarChar field.

We have the memory offsets specify the end of the corresponding field which will help us for variable length fields.

- Describe how your record design satisfies O(1) field access.

While accessing a field, following are the things we have access to beforehand: record schema and amount of memory being used for record fields and offsets.
Since we know the number of bytes per metadata field (offset or record size) and memory offsets to each field, we can:
1. Find the offset for field (N-1) and offset for field N in constant time
2. Use these to access the field N.


### 4. Page Format (in case you have changed from P1, please re-enter here)

Additional behaviour added: if the recordCount for the page is zero, it is treated as invalid/new.
For records that are deleted, the length in its metadata slot is marked as -1.

The rest are **unchanged** from P1. They have been copy-pasted for convenience.
![Page Structure](images/page_and_slot.png)

 We keep a vector of records at the start of the page and the slot directory at the end.
 This leaves all free space as one fragment instead of having it distributed in between records and possibly even within the slot directory.
 On update/delete of records, we plan on shifting the records to uilize the newly freed space.
 We will update the documentation with our plan for slot directory on deletion of a record.

The slot directory is similar to the design described in the lecture.
* I store a vector with the following information: memory offset to the record inside the current page and record length.
* I have the following fixed length information: number of records and amount of free space remaining.
* I store these as _short_ variables.
* The number of records and free space are stored at the end of the page so that access to these can be a constant time operation.
* The vector with record offsets expands into the free space with inserts.



### 5. Page Management (in case you have changed from P1, please re-enter here)

Currently, I use 10 hidden pages to store the read, write and append counters, and other private metadata like the number of data pages and a vector of page numbers to available free space (pageSpaceMap\[pageNumber\] will return the free space in that page).

- Show your hidden page(s) format design if applicable
These are **unchanged** from P1. They have been copy-pasted for convenience.

![Page Structure](images/hidden_page.png)

Currently, we store the three unsigned int counters read, write and append page counters, and the number of data pages.
I also store a vector of page number to available free space. 
One page will not be able to hold the page-space map after a certain point.
Proposal to handle this (this is not implemented right now and is only a proposal): we can, then, add a field to hold the page number with the continuation for this vector and use that page number as a hidden page as well. 



### 6. Describe the following operation logic.
- Delete a record

1. Retain the passed record ID and separately find the record's actual location and RID (considering movements due to update: logic described at the end)
2. If this is already deleted, do nothing and return.
3. Otherwise, perform a deep delete.

Deep delete: 
From the passed in record ID:
- Read the page number
- If deleted, return
- Get the record at the slot number
- If this record points to another record ID, recursively call deepDelete with the new record ID.
- Else, delete the record from the page
- Write this page to disk.


- Update a record

1. From the new record data, find the record metadata (offsets, field lengths, new record length)
2. From the record ID, find the current (old) record length.
3. If (newLength - oldLength) < (free space in page), update record in place.
	a) If there are records after the current slot number, move them to the right by (new length - old length) bytes
	b) Write the new record in its place
	c) Write the page to disk
	d) Return
4. Else, the same page cannot hold the new record's data.
5. Just like insertion, find a free page for this record
6. Get its new "internal" record ID
7. Add the new record to its new page and write to disk
8. Make a placeholder record with the new record ID. Populate its attributeCount field as -1 to indicate that the record is moved.
9. If the record is moved from the initial position (passed in RID), go to the pointed location and delete that record. This is done to maintain a single jump from initial to final location of the record and avoid long chains of reference to the record.
10. Update the new record ID placeholder in the initial page. While inserting a record, we ensure to reserve a minimum space that will hold a placeholder new record ID so that this update will always succeed.
11. Write page to disk.

- Scan on normal records

1. Store the current record ID in memory. The initial value for this is page:0, slot: -1
2. Read the page given by page number.
3. Increment the RID:
	a. If page's record count >= current slotNum + 2, increment the slot number and return true to indicate increment was successful.
	We need to check slot number + 2 here because we want to increment by 1, and my slot numbers start from zero.
	b. Else, if file's number of pages is > current page number + 2: 
		increment the page number
		set slot number = 0
		return true
	c. Else, return false to indicate increment was a failure.
4. If increment step returns false, mark as RBFM_EOF.
5. If page's slot number is marked deleted, call getNext recursively
6. Get the record at the slot number.
7. If record is marked as moved (due to update), call getNext recursively.
8. Check the condition given. If not successful, call getNext recursively.
9. Read the record in the same format as expected by caller (with null bytes and varchar lengths)
10. Iterating over the record descriptor
	a. If the attribute name is not present in the projection list, skip it
	b. Otherwise, copy the relevant bytes to the destination buffer.
11. Set the null bytes correspondingly.
12. Return successful read (0).


- Scan on deleted records
Point 5 in the above algorithm pertains to deleted records. We skip deleted records and get the next existing record.


- Scan on updated records
Point 7 in the above algorithm pertains to deleted records. We skip updated records in their old position and get the next existing record.
The data at the new position is read subsequently when the cursor (pageNum, slotNum) reaches it.


### 7. Implementation Detail
- Other implementation details goes here.
There were two attemps at code cleanliness here that I have currently reverted due to some map access errors. Here, I describe them since some commented code still exists for them.
1. For data parse in readRecord and condition checking in scanner, I had an unordered_map of Attribute type to parse function pointers.
When I needed to parse a particular field, the correct function was called by: parserMap.at(type)(sourceData, readStartOffset, readLength)
This removed the need of if-else or switch-case statements and separated the branch decision from the main flow of the program, increasing readability.
2. In relation manager, the insert, update and read tuple methods have the same exact structure apart from call to next layer (record manager). So I had a base method called operateTuple which took all the same arguments as these methods, and an additional function pointer to the method to be called in the next layer. The insert, update and read methods then became a single line to call operateTuple with the correct function pointer.

I was getting BAD_ACCESS errors while reading from the map, and so I undid these changes temporarily in a moment of panic thinking I did not undestand C++ function pointers correctly.
This idea is similar to using delegates in C# or Java for the same purpose: readability and reduction in code repetition.


### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.
I am the only team member.


### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 2, but not related to the other sections (optional)
Project 2 was interesting, demanding and difficult, frustrating at times since I am new to C++. I tried to do this project without checking in the office hours for advice and help, this turned out to be very difficult. 
One difficulty is definitely the fact that the libraries have different behaviour in macOS and Linux. I will set up an Ubuntu VM for next time.


- Feedback on the project to help improve the project. (optional)