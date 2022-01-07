#include <src/include/qe.h>
#include "src/include/rm.h"

namespace PeterDB {
    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() {}

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        RC result = this->ixScanner.getNextEntry(rid, key);
        return result == IX_EOF ? QE_EOF : result;
    }

    RC RM_IndexScanIterator::close() {
        IndexManager::instance().closeFile(this->ixHandle);
        return this->ixScanner.close();
    }
}
