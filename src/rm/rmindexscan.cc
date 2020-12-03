#include "src/include/rm.h"

namespace PeterDB {
    RM_IndexScanIterator::RM_IndexScanIterator() {}

    RM_IndexScanIterator::~RM_IndexScanIterator() {}

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC RM_IndexScanIterator::close() {
        return 0;
    }
}
