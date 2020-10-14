#include "src/include/rbfm.h"

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
        return -1;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        static_assert(sizeof(float) == 4, "");
        int flagBitsCount = ceil(((float) recordDescriptor.size()) / 8);
        std::vector<bool> nullFlagStatus;
        int memoryPointerCounter = flagBitsCount;
        unsigned short stringSizeOffsetMemSize = 4;

        std::string records;
        int tmpIntVal;
        float tmpFloatVal;

        for(unsigned int i = 0; i < recordDescriptor.size(); ++i) {
            if (((char*)data)[i / 8] & (1 << (7 - i % 8))) {
                records.append(recordDescriptor[i].name);
                records.append(": NULL");
            } else {
                if (recordDescriptor[i].type == TypeInt) {
                    std::memcpy(&tmpIntVal, (char *) data + memoryPointerCounter, recordDescriptor[i].length);
                    memoryPointerCounter = memoryPointerCounter + sizeof(TypeInt);
                    records.append(recordDescriptor[i].name);
                    records.append(": ");
                    records.append(std::to_string(tmpIntVal));
                } else if (recordDescriptor[i].type == TypeReal) {
                    std::memcpy(&tmpFloatVal, (char *) data + memoryPointerCounter, recordDescriptor[i].length);
                    memoryPointerCounter = memoryPointerCounter + sizeof(TypeReal);
                    records.append(recordDescriptor[i].name);
                    records.append(": ");
                    std::ostringstream ss;
                    ss << tmpFloatVal;
                    records.append(ss.str());
                } else if (recordDescriptor[i].type == TypeVarChar) {
                    std::memcpy(&tmpIntVal, (char *) data + memoryPointerCounter, stringSizeOffsetMemSize);
                    char *tmpStringVal = new char[tmpIntVal + 2];
                    std::memcpy(tmpStringVal, (char *) data + memoryPointerCounter + stringSizeOffsetMemSize,
                                tmpIntVal);
                    tmpStringVal[tmpIntVal] = '\0';
                    memoryPointerCounter = memoryPointerCounter + stringSizeOffsetMemSize + tmpIntVal;
                    records.append(recordDescriptor[i].name);
                    records.append(": ");
                    records.append(tmpStringVal);
                }
            }
            if (i != recordDescriptor.size() - 1) {
                records.append(", ");
            } else {
                // TODO :: is this required?
//                records.append("\n");
            }
        }
        out << records;
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

