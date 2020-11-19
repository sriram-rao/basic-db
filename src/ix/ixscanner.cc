#include "src/include/ix.h"
#include <src/utils/compare_utils.h>

namespace PeterDB {
    IX_ScanIterator::IX_ScanIterator() = default;

    IX_ScanIterator::~IX_ScanIterator() = default;

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        IndexManager &ixManager = IndexManager::instance();
        if (pageNum == -1)
            return IX_EOF;

        if (!ixManager.cached(pageNum))
            ixManager.refreshCache(*ixFileHandle, pageNum);

        // Go to the correct leaf
        while (NODE_TYPE_INTERMEDIATE == ixManager.cachedNode.type) {
            int location;
            int pageId = nullptr == lowKey ? ixManager.cachedNode.nextPage : ixManager.cachedNode.findChildNode(attribute, lowKey, {}, location, false);
            pageNum = pageId;
            ixManager.refreshCache(*ixFileHandle, pageId);
        }

        // Find the index in this leaf node
        if (searching && nullptr != lowKey) {
            searching = false;
            slotNum = ixManager.cachedNode.findKey(attribute, lowKey, {}, false, true);
        }

        while (!ixManager.cachedNode.validateIndex(slotNum)) {
            incrementCursor(ixManager.cachedNode.getKeyCount(), ixManager.cachedNode.nextPage);
            if (pageNum == -1)
                return IX_EOF;
            if (ixManager.cached(pageNum))
                continue;
            ixManager.refreshCache(*ixFileHandle, pageNum);
        }
        ixManager.cachedNode.getKeyData(attribute, slotNum, static_cast<char *>(key), rid);

        incrementCursor(ixManager.cachedNode.getKeyCount(), ixManager.cachedNode.nextPage);

        if (!meetsCondition(key))
            getNextEntry(rid, key);

        return 0;
    }

    RC IX_ScanIterator::close() {
        pageNum = -1;
        slotNum = -1;
        return 0;
    }

    bool IX_ScanIterator::meetsCondition(void *key) {
        if (CompareUtils::checkLessThan(attribute.type, key, lowKey))
            return false;

        if ((CompareUtils::checkGreaterThan(attribute.type, key, lowKey)
                || (lowKeyInclusive && CompareUtils::checkEqual(attribute.type, key, lowKey)))
            && (CompareUtils::checkLessThan(attribute.type, key, highKey)
                || (highKeyInclusive && CompareUtils::checkEqual(attribute.type, key, highKey))))
            return true;

        pageNum = -1;
        slotNum = -1;
        return false;
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