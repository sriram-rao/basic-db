#include "src/include/rbfm.h"
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

    const unordered_map<int, copy> RecordBasedFileManager::parserMap = {
        { TypeInt, &parseTypeInt },
        { TypeReal, &parseTypeReal },
        { TypeVarChar, &parseTypeVarchar }
    };

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
        // Figure out record details
        // Null map
        short nullBytes = ceil(((float)recordDescriptor.size()) / 8);
        int fieldInfo[recordDescriptor.size()];

        short recordLength = sizeof(short) + sizeof(short) * recordDescriptor.size(); // space for fieldCount 'n' and n offsets
        short dataOffset = sizeof(short) * (recordDescriptor.size() + 1);
        short offsets[recordDescriptor.size()];
        int currentOffset = nullBytes;
        for (short i = 0; i < recordDescriptor.size(); ++i) {
            // check null
            if (((char*)data)[i / 8] & (1 << (7 - i % 8))) {
                offsets[i] = -1;
                fieldInfo[i] = -1;
                continue;
            }

            // get size for varchar
            int fieldSize = recordDescriptor[i].type == TypeVarChar ? 0 : recordDescriptor[i].length;
            if (recordDescriptor[i].type == TypeVarChar) {
                int varcharSize = 0;
                memcpy(&varcharSize, (char *) data + currentOffset, 4);
                fieldSize = varcharSize;
                currentOffset += 4;
            }
            recordLength += fieldSize;
            fieldInfo[i] = fieldSize;
            dataOffset += fieldSize;
            offsets[i] = dataOffset;
            currentOffset += fieldSize;
        }

        // Make the record
        unsigned char* fieldData = (unsigned char*)malloc(recordLength);
        currentOffset = nullBytes;
        int copiedOffset = 0;
        for(int i = 0; i < recordDescriptor.size(); i++) {
            if (offsets[i] == -1) // skip nulls
                continue;
            if (recordDescriptor[i].type == TypeVarChar) { // handle length descriptor for varchar
                currentOffset += 4;
            }
            memcpy(fieldData + copiedOffset, (char *) data + currentOffset, fieldInfo[i]);
            currentOffset += fieldInfo[i];
            copiedOffset += fieldInfo[i];
        }
        Record r = Record(rid, recordDescriptor.size(), offsets, fieldData);

        // Find the right page
        unsigned num = fileHandle.getNumberOfPages(); short pageDataSize = 0;
        unsigned short slotNum = 0;
        short pageNum = fileHandle.findFreePage(recordLength + sizeof(offsets) + sizeof(short) + sizeof(Slot));
        Page p = pageNum >= 0 ? readPage(pageNum, fileHandle) : Page();
        if (pageNum >= 0) {
            num = (unsigned short) pageNum;
            slotNum = p.getFreeSlot();
            for (short i = 0; i < p.directory.recordCount; ++i)
                pageDataSize += p.directory.getRecordLength(i);
        }
        rid.pageNum = num;
        rid.slotNum = slotNum;

        slotNum < p.directory.slots.size() ?
            p.directory.setSlot(slotNum, recordLength) :
            p.directory.addSlot(slotNum, pageDataSize, recordLength);

        p.addRecord(slotNum, r, recordLength);

        // Write to file
        writePage(num, p, fileHandle, pageNum == -1);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        RID trueId = rid;
        Page page = findRecord(trueId, fileHandle);

        if (!page.checkValid())
            return -1; // Was deleted, return error

        int recordOffset = page.directory.slots[trueId.slotNum].offset,
            recordLength = page.directory.slots[trueId.slotNum].length;
        unsigned char* recordData = (unsigned char*) malloc(recordLength);
        copyAttribute(page.records, recordData, reinterpret_cast<int &>(recordOffset), recordLength);
        Record record = Record::fromBytes(recordData);

        // Null bitmap
        int nullBytes = ceil((float)record.attributeCount / 8);
        char nullBitMap [nullBytes];
        std::memset(nullBitMap, 0, nullBytes);
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (record.fieldOffsets[i] == -1)
                nullBitMap[i / 8] = nullBitMap[i / 8] | (1 << (7 - i % 8));
        }

        memcpy(data, nullBitMap, nullBytes);
        int currentOffset = nullBytes;
        int sourceOffset = 0;
        int nonNullIndex = -1;
        // Add back varchar length
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (record.fieldOffsets[i] == -1)
                continue; // NULL
            int fieldSize = recordDescriptor[i].length;
            if (recordDescriptor[i].type == TypeVarChar) {
                // Get the size of the varchar and append
                int startIndex = nonNullIndex == -1 ? sizeof(short) * (recordDescriptor.size() + 1) : record.fieldOffsets[nonNullIndex];
                fieldSize = record.fieldOffsets[i] == -1 ? 0 : record.fieldOffsets[i] - startIndex;
                memcpy((char*) data + currentOffset, &fieldSize, 4);
                currentOffset += 4;
            }
            if (fieldSize == 0)
                continue;
            copyAttribute(record.values, data, sourceOffset, currentOffset, fieldSize);
            nonNullIndex = i;
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
            out << parserMap.at(recordDescriptor[i].type)(data, currentOffset, recordDescriptor[i].length);

            if (i < recordDescriptor.size() - 1)
                out << ", ";
        }
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        RID trueId = rid;
        Page page = findRecord(trueId, fileHandle);
        if (!page.checkValid()) // Already deleted
            return 0;
        page.deleteRecord(trueId.slotNum);
        return writePage(trueId.pageNum, page, fileHandle, false);
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        // Find change in record length
        // If new record is smaller, update in place, shift records (like delete)
        // If bigger
        // 1. If current page can hold the space, shift records right and update slots (length of updated record and offset of existing)
        // 2. If current page cannot hold, find a new page, insert record there, write the new RID in old place and shift other records left.
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        RID trueId = rid;
        Page page = findRecord(trueId, fileHandle);
        if (!page.checkValid()) // Deleted
            return -1;

        Record record = page.getRecord(trueId.slotNum);
        if (record.absent())
            return -1;

        // Get the index of the attribute from descriptor
        int attributeIndex = 0;
        for (int i = 0; i < recordDescriptor.size(); ++i){
            if (attributeName == recordDescriptor[i].name) {
                attributeIndex = i;
                break;
            }
        }

        record.readAttribute(attributeIndex, data);
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator = RBFM_ScanIterator(recordDescriptor, conditionAttribute, compOp, const_cast<void *>(value), attributeNames);
        return 0;
    }

    Page RecordBasedFileManager::readPage(PageNum pageNum, FileHandle &file) {
        char* pageData = (char*) malloc(PAGE_SIZE);
        file.readPage(pageNum, pageData);

        // Read slot directory
        short recordCount;
        short freeBytes;
        memcpy(&recordCount, pageData + PAGE_SIZE - sizeof(short), sizeof(short));
        memcpy(&freeBytes, pageData + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
        size_t slotSize = sizeof(Slot) * recordCount;
        vector<Slot> slots;
        slots.insert(slots.begin(), recordCount, { 0, 0 });
        memcpy(slots.data(), pageData + PAGE_SIZE - sizeof(short) * 2 - slotSize, slotSize);
        SlotDirectory dir = SlotDirectory(freeBytes, recordCount, slots);

        // Read records as array
        u_short recordsSize = PAGE_SIZE - sizeof(short) * 2 - slotSize - freeBytes;
        unsigned char* records = (unsigned char*) malloc(recordsSize);
        memcpy(records, pageData, recordsSize);
        free(pageData);
        return Page(dir, records);
    }

    RC RecordBasedFileManager::writePage(PageNum pageNum, Page &page, FileHandle &file, bool toAppend) {
        char* pageData = (char*) malloc(PAGE_SIZE);
        long slotsSize = static_cast<int>(sizeof(Slot)) * page.directory.slots.size();
        unsigned short recordsSize = 0;
        for (short i = 0; i < page.directory.recordCount; ++i)
            recordsSize += page.directory.getRecordLength(i);

        memcpy(pageData, page.records, recordsSize);
        short freeBytes = PAGE_SIZE - recordsSize - sizeof(short) * 2 - slotsSize;
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2 - slotsSize, page.directory.slots.data(), slotsSize);
        memcpy(pageData + PAGE_SIZE - sizeof(short) * 2, &freeBytes, sizeof(short));
        memcpy(pageData + PAGE_SIZE - sizeof(short), &page.directory.recordCount, sizeof(short));
        RC writeSuccess = toAppend ? file.appendPage(pageData) : file.writePage(pageNum, pageData);
        file.setPageSpace(pageNum, freeBytes);

        return writeSuccess;
    }

    Page RecordBasedFileManager::findRecord(RID& rid, FileHandle& fileHandle) {
        Page page = readPage(rid.pageNum, fileHandle);
        if (page.checkRecordDeleted(rid.slotNum))
            return Page();
        Record record = page.getRecord(rid.slotNum);

        while (record.absent()) {
            rid = record.getNewRid();
            page = readPage(rid.pageNum, fileHandle);
            if (page.checkRecordDeleted(rid.slotNum))
                return Page();
            record = page.getRecord(rid.slotNum);
        }
        return page;
    }

    int RecordBasedFileManager::copyAttribute(const void *data, void* destination, int& startOffset, int length) {
        std::memcpy(destination, (char *) data + startOffset, length);
        startOffset += length;
        return startOffset;
    }

    int RecordBasedFileManager::copyAttribute(const void *data, void* destination, int& sourceOffset, int& destOffset, int length) {
        std::memcpy((char*) destination + destOffset, (char *) data + sourceOffset, length);
        sourceOffset += length;
        destOffset += length;
        return sourceOffset;
    }

    string RecordBasedFileManager::parseTypeInt(const void* data, int& startOffset, int length) {
        int field;
        copyAttribute(data, &field, startOffset, length);
        return to_string(field);
    }

    string RecordBasedFileManager::parseTypeReal(const void* data, int& startOffset, int length) {
        float field;
        copyAttribute(data, &field, startOffset, length);
        return to_string(field);
    }

    string RecordBasedFileManager::parseTypeVarchar(const void* data, int& startOffset, int length){
        int fieldLength;
        std::memcpy(&fieldLength, (char *) data + startOffset, 4);
        startOffset += 4;
        char field [fieldLength + 2];
        copyAttribute(data, field, startOffset, fieldLength);
        field[fieldLength] = '\0';
        return field;
    }

    RBFM_ScanIterator::RBFM_ScanIterator(std::vector<Attribute> recordDescriptor, std::string conditionAttribute,
                                         CompOp compOp, void *value, std::vector<std::string> attributeNames) {
        this->recordDescriptor = recordDescriptor;
        this->conditionAttribute = conditionAttribute;
        this->compOp = compOp;
        this->value = value;
        this->attributeNames = attributeNames;
        this->currentRecord = { 0, 0 };
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        // Increment RID:
        // If current page has more records, increment slot until we get one present in this page (un-deleted and un-moved)
        // If all records of page are done, increment page and go to slot 0
        // readRecord
        return RBFM_EOF;
    }

    RC RBFM_ScanIterator::close() {
        // Get rid of state
        // Close file? Probably not
        return -1;
    }
} // namespace PeterDB

