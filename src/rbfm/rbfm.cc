#include "src/include/rbfm.h"
#include <utility>
#include <vector>
#include <map>
#include <unordered_map>
#include <cstring>

using namespace std;

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager &pagedFileManager = PagedFileManager::instance();
        return pagedFileManager.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        int fieldInfo[recordDescriptor.size()];
        short recordLength;
        vector<short> offsets (recordDescriptor.size(), 0);
        getRecordProperties(recordDescriptor, data, recordLength, offsets, fieldInfo);
        // Find the right page
        unsigned pageDataSize; bool append;
        Page page = findFreePage(recordLength + offsets.size() * sizeof(short) + sizeof(short) + sizeof(Slot),
                                 fileHandle, pageDataSize, rid.pageNum, rid.slotNum, append);
        Record record = prepareRecord(rid, recordDescriptor, data, recordLength, offsets, fieldInfo);
        addRecordToPage(page, record, rid, pageDataSize, recordLength);
        return writePage(rid.pageNum, page, fileHandle, append);
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        RID trueId = rid;
        Page page;
        findRecord(trueId, fileHandle, page);

        if (!page.checkValid())
            return -1; // Was deleted, return error

        int recordOffset = page.directory.slots[trueId.slotNum].offset,
            recordLength = page.directory.slots[trueId.slotNum].length;
        char* recordData = (char*) malloc(recordLength);
        copyAttribute(page.records, recordData, recordOffset, recordLength);
        Record record(recordData);
        free(recordData);

        // Null bitmap
        int nullBytes = ceil((float)record.attributeCount / 8);
        char nullBitMap [nullBytes];
        std::memset(nullBitMap, 0, nullBytes);
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (record.offsets[i] == -1)
                nullBitMap[i / 8] = nullBitMap[i / 8] | (1 << (7 - i % 8));
        }

        memcpy(data, nullBitMap, nullBytes);
        int currentOffset = nullBytes;
        int sourceOffset = 0;
        int nonNullIndex = -1;
        // Add back varchar length
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            int columnPosition = i;
            if (record.offsets[columnPosition] == -1)
                continue; // NULL
            int fieldSize = recordDescriptor[i].length;
            if (recordDescriptor[i].type == TypeVarChar) {
                // Get the size of the varchar and append
                int startIndex = nonNullIndex == -1 ? sizeof(short) * (recordDescriptor.size() + 1) : record.offsets[nonNullIndex];
                fieldSize = record.offsets[columnPosition] == -1 ? 0 : record.offsets[columnPosition] - startIndex;
                memcpy((char*) data + currentOffset, &fieldSize, 4);
                currentOffset += 4;
            }
            if (fieldSize == 0)
                continue;
            copyAttribute(record.values, data, sourceOffset, currentOffset, fieldSize);
            nonNullIndex = columnPosition;
        }

        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        static_assert(sizeof(float) == 4, "");
        int nullBytes = ceil(((float) recordDescriptor.size()) / 8);
        int currentOffset = nullBytes;

        for(unsigned int i = 0; i < recordDescriptor.size(); ++i) {
            if (((char *) data)[i / 8] & (1 << (7 - i % 8))) {
                out << recordDescriptor[i].name << ": NULL";
                if (i < recordDescriptor.size() - 1)
                    out << ", ";
                continue;
            }
            out << recordDescriptor[i].name << ": ";

            switch (recordDescriptor[i].type) {
                case TypeInt:
                    out << to_string(parseTypeInt(data, currentOffset, recordDescriptor[i].length));
                    break;
                case TypeReal:
                    out << to_string(parseTypeReal(data, currentOffset, recordDescriptor[i].length));
                    break;
                case TypeVarChar:
                    out << parseTypeVarchar(data, currentOffset);
                    break;
            }

            if (i < recordDescriptor.size() - 1)
                out << ", ";
        }
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        RID trueId = rid;
        Page page;
        findRecord(trueId, fileHandle, page);
        if (!page.checkValid()) // Already deleted, nothing to do
            return 0;
        deepDelete(rid, fileHandle);
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        // Get existing record
        RID trueId = rid;
        Page page;
        findRecord(trueId, fileHandle, page);

        if (!page.checkValid())
            return -1; // Was deleted, return error

        int recordLength = page.directory.slots[trueId.slotNum].length;

        // Find change in record length
        int fieldInfo[recordDescriptor.size()];
        short newLength;
        vector<short> offsets (recordDescriptor.size(), 0);
        getRecordProperties(recordDescriptor, data, newLength, offsets, fieldInfo);
        Record record = prepareRecord(rid, recordDescriptor, data, newLength, offsets, fieldInfo);

        if (newLength <= recordLength || newLength - recordLength < page.directory.freeSpace) {
            page.updateRecord(rid.slotNum, record, newLength);
            return writePage(rid.pageNum, page, fileHandle, false);
        }

        // 2. If current page cannot hold, find a new page, insert record there
        unsigned pageDataSize, pageNum;
        unsigned short slotNum; bool append;
        Page newPage = findFreePage(newLength, fileHandle, pageDataSize, pageNum, slotNum, append);
        RID newRid = { pageNum, slotNum };
        record.rid = newRid;
        addRecordToPage(newPage, record, newRid, pageDataSize, newLength);
        writePage(newRid.pageNum, newPage, fileHandle, append);

        // Write the new RID in old place and shift other records left
        Record ridData = getRidPlaceholder(newRid);
        updateRid(rid, ridData, fileHandle);
        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        RID trueId = rid;
        Page page;
        findRecord(trueId, fileHandle, page);
        if (!page.checkValid()) // Deleted
            return -1;

        Record record;
        page.getRecord(trueId.slotNum, record);
        if (record.absent())
            return -1;

        return readAttribute(record, recordDescriptor, rid, attributeName, data);
    }

    RC RecordBasedFileManager::readAttribute(Record &record, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        // Get the index of the attribute from descriptor
        int attributeIndex = -1;
        for (int i = 0; i < recordDescriptor.size(); ++i){
            if (attributeName == recordDescriptor[i].name) {
                attributeIndex = i;
                break;
            }
        }
        if (attributeIndex == -1)
            return -1; // attribute deleted

        char nullMap = 0;
        memset(&nullMap, 0, 1);
        int attributeLength = record.getAttributeLength(attributeIndex);
        if (attributeLength == -1) {
            memset(&nullMap, 1, 1); // TODO: Set the correct bit to 1
            memcpy(data, &nullMap, 1);
            return 0;
        }

        memcpy(data, &nullMap, 1);
        int copiedLength = 1;
        if (TypeVarChar == recordDescriptor[attributeIndex].type) {
            memcpy((char *) data + 1, &attributeLength, sizeof(attributeLength));
            copiedLength += sizeof(attributeLength);
        }
        char *attributeData = (char*) malloc(attributeLength);
        record.readAttribute(attributeIndex, attributeData);
        memcpy((char*)data + copiedLength, attributeData, attributeLength);
        free(attributeData);
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator = RBFM_ScanIterator(recordDescriptor, conditionAttribute, compOp, const_cast<void *>(value),
                                              attributeNames, fileHandle);
        rbfm_ScanIterator.setFile(std::move(fileHandle.file));
        return 0;
    }

    RC RecordBasedFileManager::readPage(PageNum pageNum, Page &ogPage, FileHandle &file) {
        char* pageData = (char*) malloc(PAGE_SIZE);
        if (file.readPage(pageNum, pageData) == -1)
            return -1;

        // Read slot directory
        short recordCount, freeSpace;
        memcpy(&recordCount, pageData + PAGE_SIZE - sizeof(short), sizeof(short));
        memcpy(&freeSpace, pageData + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
        long slotsSize = static_cast<int>(sizeof(Slot)) * recordCount;

        Page page (recordCount, freeSpace);
        memcpy(page.directory.slots.data(), pageData + PAGE_SIZE - sizeof(short) * 2 - slotsSize, slotsSize);

        // Read records
        u_short recordsSize = PAGE_SIZE - sizeof(short) * 2 - slotsSize - page.directory.freeSpace;
        memcpy(page.records, pageData, recordsSize);
        free(pageData);
        ogPage = page;
        return 0;
    }

    RC RecordBasedFileManager::writePage(PageNum pageNum, Page &page, FileHandle &file, bool toAppend) {
        long slotsSize = static_cast<int>(sizeof(Slot)) * page.directory.slots.size();
        unsigned short recordsSize = 0;
        for (short i = 0; i < page.directory.recordCount; ++i)
            recordsSize += page.directory.getRecordLength(i);

        char* pageData = (char*) malloc(PAGE_SIZE);
        memcpy(pageData, page.records, recordsSize);
        short freeBytes = PAGE_SIZE - recordsSize - sizeof(short) * 2 - slotsSize;
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2 - slotsSize, page.directory.slots.data(), slotsSize);
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2, &freeBytes, sizeof(short));
        memcpy(pageData + PAGE_SIZE - sizeof(short), &page.directory.recordCount, sizeof(short));
        RC writeSuccess = toAppend ? file.appendPage(pageData) : file.writePage(pageNum, pageData);
        free(pageData);
        file.setPageSpace(pageNum, freeBytes);

        return writeSuccess;
    }

    void RecordBasedFileManager::findRecord(RID& rid, FileHandle& fileHandle, Page& ogPage) {
        Page page;
        readPage(rid.pageNum, page, fileHandle);
        if (page.checkRecordDeleted(rid.slotNum))
            return;

        Record record;
        page.getRecord(rid.slotNum, record);
        while (record.absent()) {
            rid = record.getNewRid();
            readPage(rid.pageNum, page, fileHandle);
            if (page.checkRecordDeleted(rid.slotNum))
                return;
            page.getRecord(rid.slotNum, record);
        }
        ogPage = page;
    }

    void RecordBasedFileManager::deepDelete(RID rid, FileHandle& fileHandle) {
        Page page;
        readPage(rid.pageNum, page, fileHandle);
        if (page.checkRecordDeleted(rid.slotNum))
            return;
        Record record;
        page.getRecord(rid.slotNum, record);
        if (record.absent())
            deepDelete(record.getNewRid(), fileHandle);

        page.deleteRecord(rid.slotNum);
        writePage(rid.pageNum, page, fileHandle, false);
    }

    Page RecordBasedFileManager::findFreePage(short bytesNeeded, FileHandle& fileHandle, unsigned &pageDataSize,
                                              unsigned &pageNum, unsigned short &slotNum, bool &append) {
        pageNum = fileHandle.getNumberOfPages();
        slotNum = 0;
        pageDataSize = 0;
        int allottedPage = fileHandle.findFreePage(bytesNeeded);
        append = allottedPage == -1;
        Page page;
        if (!append) {
            readPage(allottedPage, page, fileHandle);
            pageNum = (unsigned short) allottedPage;
            slotNum = page.getFreeSlot();
            for (short i = 0; i < page.directory.recordCount; ++i)
                pageDataSize += page.directory.getRecordLength(i);
        }
        return page;
    }

    void RecordBasedFileManager::addRecordToPage(Page &page, Record &record, RID rid, unsigned pageDataSize, short recordLength) {
        rid.slotNum < page.directory.slots.size() ? page.directory.setSlot(rid.slotNum, recordLength) :
            page.directory.addSlot(rid.slotNum, pageDataSize, recordLength);
        page.addRecord(rid.slotNum, record, recordLength);
    }

    Record RecordBasedFileManager::prepareRecord(RID rid, const std::vector<Attribute> &recordDescriptor,
                                                 const void* data, int recordLength, vector<short> &offsets, int* fieldInfo) {
        short nullBytes = ceil(((float)recordDescriptor.size()) / 8); // Null map
        char* fieldData = (char*)malloc(recordLength);
        int currentOffset = nullBytes;
        int copiedOffset = 0;
        for(int i = 0; i < recordDescriptor.size(); i++) {
            int columnPosition = i;
            if (offsets[columnPosition] == -1) // skip nulls
                continue;
            if (recordDescriptor[i].type == TypeVarChar) // handle length descriptor for varchar
                currentOffset += 4;
            memcpy(fieldData + copiedOffset, (char *) data + currentOffset, fieldInfo[columnPosition]);
            currentOffset += fieldInfo[columnPosition];
            copiedOffset += fieldInfo[columnPosition];
        }
        Record record (rid, offsets.size(), offsets, fieldData);
        free(fieldData);
        return record;
    }

    int RecordBasedFileManager::copyAttribute(const void *data, void* destination, int& startOffset, int length) {
        if (length == 0) return startOffset;
        std::memcpy(destination, (char *) data + startOffset, length);
        startOffset += length;
        return startOffset;
    }

    int RecordBasedFileManager::copyAttribute(const void *data, void* destination, int& sourceOffset, int& destOffset, int length) {
        if (length == 0) return sourceOffset;
        std::memcpy((char*) destination + destOffset, (char *) data + sourceOffset, length); // Length too high
        sourceOffset += length;
        destOffset += length;
        return sourceOffset;
    }

    int RecordBasedFileManager::parseTypeInt(const void* data, int& startOffset, int length) {
        int field;
        copyAttribute(data, &field, startOffset, length);
        return field;
    }

    float RecordBasedFileManager::parseTypeReal(const void* data, int& startOffset, int length) {
        static_assert(sizeof(float) == 4, "");
        float field;
        copyAttribute(data, &field, startOffset, length);
        return field;
    }

    string RecordBasedFileManager::parseTypeVarchar(const void* data, int& startOffset){
        int fieldLength;
        std::memcpy(&fieldLength, (char *) data + startOffset, 4);
        startOffset += 4;
        char field [fieldLength + 2];
        copyAttribute(data, field, startOffset, fieldLength);
        field[fieldLength] = '\0';
        return field;
    }

     void RecordBasedFileManager::getRecordProperties(const std::vector<Attribute> &recordDescriptor, const void *data,
                                                          short &recordLength, vector<short> &offsets, int *fieldLengths) {
        short nullBytes = ceil(((float)recordDescriptor.size()) / 8);
        recordLength = sizeof(short) + sizeof(short) * recordDescriptor.size(); // space for fieldCount 'n' and n offsets
        int currentOffset = nullBytes;
        int dataOffset = sizeof(short) * (recordDescriptor.size() + 1);
        for (short i = 0; i < recordDescriptor.size(); ++i) {
            // check null
            if (((char*)data)[i / 8] & (1 << (7 - i % 8))) {
                offsets[i] = -1;
                fieldLengths[i] = -1;
                continue;
            }

            // get size for varchar
            int fieldSize = recordDescriptor[i].type == TypeVarChar ? 0 : recordDescriptor[i].length;
            if (recordDescriptor[i].type == TypeVarChar) {
                memcpy(&fieldSize, (char *) data + currentOffset, 4);
                currentOffset += 4;
            }
            recordLength += fieldSize;
            fieldLengths[i] = fieldSize;
            dataOffset += fieldSize;
            offsets[i] = dataOffset;
            currentOffset += fieldSize;
        }
        if (recordLength < MIN_RECORD_SIZE)
            recordLength = MIN_RECORD_SIZE;
    }

    Record RecordBasedFileManager::getRidPlaceholder(RID rid) {
        Record record;
        record.attributeCount = -1;
        record.offsets = vector<short>();
        record.offsets.push_back(sizeof(short) * 3 + sizeof(rid.pageNum));
        record.offsets.push_back(sizeof(short) * 3 + sizeof(rid.pageNum) + sizeof(rid.slotNum));
        record.values = (char *) malloc(sizeof(RID));
        memcpy(record.values, &rid.pageNum, sizeof(rid.pageNum));
        memcpy(record.values + sizeof(rid.pageNum), &rid.slotNum, sizeof(rid.slotNum));
        return record;
    }

    void RecordBasedFileManager::updateRid(RID rid, Record &newRidData, FileHandle &fileHandle) {
        Page initialPage;
        readPage(rid.pageNum, initialPage, fileHandle);
        Record record;
        initialPage.getRecord(rid.slotNum, record);
        short newSize = sizeof(short) * 3 + sizeof(rid.pageNum) + sizeof(rid.slotNum);
        if (record.absent()) {
            RID newId = record.getNewRid();
            Page nextPage;
            readPage(newId.pageNum, nextPage, fileHandle);
            nextPage.getRecord(newId.slotNum, record);
            nextPage.deleteRecord(newId.slotNum);
        }
        initialPage.updateRecord(rid.slotNum, newRidData, newSize);
        writePage(rid.pageNum, initialPage, fileHandle, false);
    }
} // namespace PeterDB

