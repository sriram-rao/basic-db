#include <src/utils/parse_utils.h>
#include <src/utils/compare_utils.h>
#include "src/include/ix.h"

namespace PeterDB{
    Node::Node(char nodeType) {
        keys = nullptr;
        freeSpace = PAGE_SIZE - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type);
        type = nodeType;
        nextPage = -1;
    }

    Node::Node(char *bytes) {
        // Populate metadata
        std::memcpy(&type, bytes + PAGE_SIZE - sizeof(type), sizeof(type));
        std::memcpy(&freeSpace, bytes + PAGE_SIZE - sizeof(freeSpace) - sizeof(type), sizeof(freeSpace));
        std::memcpy(&nextPage,  bytes + PAGE_SIZE - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), sizeof(nextPage));
        int directoryCount = 0;
        std::memcpy(&directoryCount, bytes + PAGE_SIZE - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), sizeof(directoryCount));
        if (directoryCount > 0) {
            int directorySize = directoryCount * sizeof(Slot);
            directory = vector<Slot>(directoryCount, {0, 0});
            std::memcpy(directory.data(), bytes + PAGE_SIZE - directorySize - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), directorySize);
        }

        // Populate data
        keys = (char *) malloc(PAGE_SIZE);
        std::memcpy(keys, bytes, PAGE_SIZE);
    }

    int Node::getOccupiedSpace() const {
        return PAGE_SIZE - freeSpace;
    }

    void Node::populateBytes(char *bytes) {
        int directoryCount = directory.size();
        int directorySize = sizeof(Slot) * directoryCount;
        int dataSize = PAGE_SIZE - freeSpace - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type) - directorySize;
        if (nullptr != keys)
            std::memcpy(bytes, keys, dataSize);

        if (directoryCount > 0)
            std::memcpy(bytes + PAGE_SIZE - directorySize - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type),
                        directory.data(), directorySize);

        std::memcpy(bytes + PAGE_SIZE - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type),
                    &directoryCount, sizeof(directoryCount));
        std::memcpy(bytes + PAGE_SIZE - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type),
                    &nextPage, sizeof(nextPage));
        std::memcpy(bytes + PAGE_SIZE - sizeof(freeSpace) - sizeof(type),
                    &freeSpace, sizeof(freeSpace));
        std::memcpy(bytes + PAGE_SIZE - sizeof(type),
                    &type, sizeof(type));
    }

    bool Node::hasSpace(int dataSpace) const {
        return freeSpace > (dataSpace + sizeof(Slot));
    }

    int Node::findChildNode(const Attribute &keyField, const void *key, const RID &rid){
        // Find the correct sub-tree, return child page ID
        if (directory.empty())
            return -1;

        bool firstCheck = true;
        for (int i = 0; i < directory.size() - 1; ) {
            Slot current = directory.at(i);
            if (current.offset == -1) {
                ++i;
                continue;
            }

            int keyLength = current.length - sizeof(RID::pageNum) - sizeof(RID::slotNum) - sizeof(unsigned);
            char currentKey [keyLength];
            RID currentRid {};
            std::memcpy(currentKey, keys + current.offset, keyLength);
            std::memcpy(&currentRid.pageNum, keys + current.offset + keyLength, sizeof(currentRid.pageNum));
            std::memcpy(&currentRid.slotNum, keys + current.offset + keyLength + sizeof(currentRid.pageNum), sizeof(currentRid.slotNum));

            // given key < leftmost entry in node
            if (firstCheck) {
                firstCheck = false;
                if (CompareUtils::checkLessThan(keyField.type, key, currentKey))
                    return nextPage;
                if (CompareUtils::checkEqual(keyField.type, key, currentKey) &&
                (rid.pageNum < currentRid.pageNum || (rid.pageNum == currentRid.pageNum && rid.slotNum < currentRid.slotNum)))
                    return nextPage;
            }

            int nextIndex = i + 1;
            Slot next = directory.at(nextIndex);
            while (next.offset == -1) {
                ++nextIndex;
                next = directory.at(nextIndex);
            }

            int nextKeyLength = next.length - sizeof(RID::pageNum) - sizeof(RID::slotNum) - sizeof(unsigned);
            char nextKey [nextKeyLength];
            RID nextRid;
            std::memcpy(nextKey, keys + next.offset, nextKeyLength);
            std::memcpy(&nextRid.pageNum, keys + next.offset + nextKeyLength, sizeof(nextRid.pageNum));
            std::memcpy(&nextRid.slotNum, keys + next.offset + nextKeyLength + sizeof(nextRid.pageNum), sizeof(nextRid.slotNum));

            // current key <= given key < next key
            if ((CompareUtils::checkGreaterThan(keyField.type, key, currentKey) || CompareUtils::checkEqual(keyField.type, key, currentKey)) &&
                    CompareUtils::checkLessThan(keyField.type, key, nextKey)) {
                int childPage = -1;
                std::memcpy(&childPage, keys + current.offset + current.length - sizeof(childPage), sizeof(childPage));
                return childPage;
            }

            i = nextIndex;
        }

        // Not found
        return -1;
    }

    int Node::findKey(const Attribute &keyField, const void *key, const RID &rid, bool compareRid, bool getIndex){
        if (directory.empty())
            return -1;

        // Find the correct leaf entry
        int left  = 0;
        int right = directory.size() - 1;

        while (left <= right) {
            int middleIndex = (left + right) / 2;
            Slot middle = directory.at(middleIndex);
            while (middle.offset == -1) {
                --middleIndex;
                middle = directory.at(middleIndex);
            }
            int middleKeyLength = middle.length - sizeof(RID::pageNum) - sizeof(RID::slotNum);
            char middleKey[middleKeyLength];
            std::memcpy(middleKey, keys + middle.offset, middleKeyLength);

            if (CompareUtils::checkLessThan(keyField.type, middleKey, key)) {
                left = middleIndex + 1;
                continue;
            }
            if (CompareUtils::checkGreaterThan(keyField.type, middleKey, key)) {
                right = middleIndex - 1;
                continue;
            }

            if (!compareRid)
                return middleIndex;

            unsigned keyPageNum;
            unsigned short keySlotNum;
            std::memcpy(&keyPageNum, keys + middle.offset + middleKeyLength, sizeof(keyPageNum));
            std::memcpy(&keySlotNum, keys + middle.offset + middle.length - sizeof(keySlotNum),
                        sizeof(keySlotNum));
            if (keyPageNum < rid.pageNum || (keyPageNum == rid.pageNum && keySlotNum < rid.slotNum)) {
                left = middleIndex + 1;
            } else if (keyPageNum > rid.pageNum || (keyPageNum == rid.pageNum && keySlotNum > rid.slotNum)) {
                right = middleIndex - 1;
            } else {
                return middleIndex;
            }
        }
        // If not found
        return getIndex ? left : -1; // TODO: check which index to return when index is needed
    }

    void Node::insertKey(const Attribute &keyField, int dataSpace, const void *key, const RID &rid) {
        int keySize = 4;
        if (TypeVarChar == keyField.type) {
            std::memcpy(&keySize, key, sizeof(int));
        }

        int freeSpaceStart = PAGE_SIZE - freeSpace - sizeof(Slot) * directory.size()
                - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type);

        std::memcpy(keys + freeSpaceStart, key, keySize);
        std::memcpy(keys + freeSpaceStart + keySize, &rid.pageNum, sizeof(rid.pageNum));
        std::memcpy(keys + freeSpaceStart + keySize + sizeof(rid.pageNum), &rid.slotNum, sizeof(rid.slotNum));

        int index = findKey(keyField, key, rid, true, true);
        directory.insert(directory.begin() + index, { static_cast<short>(freeSpaceStart), static_cast<short>(dataSpace) });
        freeSpace = freeSpace - dataSpace - sizeof(Slot);
    }

    void Node::deleteKey(const Attribute &keyField, int index, const void *key, const RID &rid) {
        Slot current = directory.at(index);
        int dataToMove = PAGE_SIZE - current.offset - current.length; // TODO: check dataToMove

        std::memmove(keys + current.offset, keys + PAGE_SIZE, dataToMove);
        freeSpace += current.length;
        directory.at(index).offset = -1;
        directory.at(index).length = -1;
    }

    void Node::getKeyData(const Attribute &keyField, int index, char *key, RID &rid) {
        Slot keySlot = directory.at(index);
        int keySize =  getKeySize(index, keyField);
        std::memcpy(key, keys + keySlot.offset, keySize);
        std::memcpy(&rid.pageNum, keys + keySlot.offset + keySize, sizeof(rid.pageNum));
        std::memcpy(&rid.slotNum, keys + keySlot.offset + keySize + sizeof(rid.pageNum), sizeof(rid.slotNum));
    }

    std::string Node::toJsonString(const Attribute &keyField) {
        std::string json = "{ \"keys\": [";

        for(int i = 0; i < directory.size(); i++) {
            Slot current = directory.at(i);
            if (-1 == current.offset)
                continue;

            int keySize = getKeySize(i, keyField);
            char key [keySize];
            std::memcpy(key, keys + current.offset, keySize);
            json.append("\"" + ParseUtils::parse(keyField.type, key) + ":[");
            unsigned pageNum; unsigned short slotNum;
            std::memcpy(&pageNum, keys + current.offset + keySize, sizeof(pageNum));
            std::memcpy(&slotNum, keys + current.offset + keySize + sizeof(pageNum), sizeof(slotNum));
            json.append("(" + to_string(pageNum) + "," + to_string(slotNum) + ")");
            json.append("]\"");

            if (i < directory.size() - 1)
                json.append(", ");
        }
        json.append("]}");
        return json;
    }

    int Node::getKeySize(int index, const Attribute &keyField) const {
        if (TypeVarChar != keyField.type)
            return 4;

        Slot keySlot = directory.at(index);
        return keySlot.length - static_cast<int>(sizeof(unsigned)) - static_cast<int>(sizeof(unsigned short ));
    }

    int Node::getKeyCount() const {
        return directory.size();
    }

    Node::~Node() {
        if (nullptr != keys)
            free(keys);
    }
}