#include "src/include/ix.h"

namespace PeterDB {
    IX_ScanIterator::IX_ScanIterator() = default;

    IX_ScanIterator::~IX_ScanIterator() = default;

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if (pageNum == -1)
            return IX_EOF;

        char nodeBytes[PAGE_SIZE];
        ixFileHandle->readPage(pageNum, nodeBytes);
        Node node(nodeBytes);

        // Go to the right leaf
        node.getKeyData(slotNum, attribute, static_cast<char *>(key), rid);

        incrementCursor(node.getKeyCount(), node.nextPage);
        return meetsCondition(key)
            ? 0
            : getNextEntry(rid, key);
    }

    RC IX_ScanIterator::close() {
        return ixFileHandle->close();
    }

    bool IX_ScanIterator::meetsCondition(void *key) {
        // Parse key and compare with values
        return true;
    }

    void IX_ScanIterator::incrementCursor(int currentKeyCount, int nextPage) {
        if (slotNum < currentKeyCount - 1) {
            slotNum++;
            return;
        }

        pageNum = nextPage;
        slotNum = 0;
    }
}