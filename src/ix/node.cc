#include <src/utils/parse_utils.h>
#include <src/utils/compare_utils.h>
#include "src/include/ix.h"

namespace PeterDB{

    Node::Node() {
        keys = nullptr;
        freeSpace = PAGE_SIZE - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type);
        type = NODE_TYPE_INTERMEDIATE;
        nextPage = -1;
    }

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

    void Node::reload(char *bytes) {
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
        if (nullptr != keys)
            free(keys);
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
        int spaceNeeded = dataSpace + static_cast<int>(sizeof(Slot));
        if (NODE_TYPE_INTERMEDIATE == type)
            spaceNeeded += sizeof(int);
        return freeSpace > spaceNeeded;
    }

    int Node::findChildNode(const Attribute &keyField, const void *key, const RID &rid, int &index, bool compareRids){
        // Key is expected formatted with varchar length at the start

        // Find the correct sub-tree, return child page ID
        if (directory.empty() || NODE_TYPE_LEAF == type)
            return -1;

        char firstKey [directory.at(0).length + sizeof(int)]; // Why adding sizeof(int)?
        RID firstRid {};
        populateFormattedKey(keyField.type, firstKey, firstRid, 0);

        // given key < leftmost entry in node
        if (CompareUtils::checkLessThan(keyField.type, key, firstKey)) {
            index = 0;
            return nextPage;
        }
        if (CompareUtils::checkEqual(keyField.type, key, firstKey) && compareRids &&
            (rid.pageNum < firstRid.pageNum || (rid.pageNum == firstRid.pageNum && rid.slotNum < firstRid.slotNum))) {
            index = 0;
            return nextPage;
        }

        int lastIndex = 0;
        for (int i = 0; i < directory.size() - 1; ) {
            Slot current = directory.at(i);
            if (current.offset == -1) {
                ++i;
                continue;
            }

            char currentKey [current.length + sizeof(int)];
            RID currentRid {};
            populateFormattedKey(keyField.type, currentKey, currentRid, i);

            int nextIndex = i + 1;
            Slot next = directory.at(nextIndex);
            while (next.offset == -1) {
                ++nextIndex;
                if (nextIndex >= directory.size())
                    break;
                next = directory.at(nextIndex);
            }
            if (nextIndex >= directory.size())
                break;
            lastIndex = nextIndex;

            char nextKey [next.length + sizeof(int)];
            RID nextRid;
            populateFormattedKey(keyField.type, nextKey, nextRid, nextIndex);

            // current key <= given key < next key
            if ((CompareUtils::checkGreaterThan(keyField.type, key, currentKey) || CompareUtils::checkEqual(keyField.type, key, currentKey)) &&
                    CompareUtils::checkLessThan(keyField.type, key, nextKey)) {
                index = nextIndex;
                int childPage = -1;
                std::memcpy(&childPage, keys + current.offset + current.length - sizeof(childPage), sizeof(childPage));
                return childPage;
            }

            i = nextIndex;
        }

        // Key >= last key in node, return last childNode
        int lastChild = -1;
        Slot last = directory.at(lastIndex);
        std::memcpy(&lastChild, keys + last.offset + last.length - sizeof(lastChild), sizeof(lastChild));
        index = lastIndex + 1;
        return lastChild;
    }

    int Node::findKey(const Attribute &keyField, const void *key, const RID &rid, bool compareRid, bool getIndex){
        if (directory.empty() || NODE_TYPE_INTERMEDIATE == type)
            return -1;

        // Find the correct leaf entry
        int left  = 0;
        int right = directory.size() - 1;

        while (left <= right) {
            int middleIndex = (left + right) / 2;
            Slot middle = directory.at(middleIndex);
            while (middle.offset == -1) {
                --middleIndex;
                if (middleIndex < 0)
                    break;
                middle = directory.at(middleIndex);
            }
            if (middleIndex < 0) break;

            int middleKeyLength = middle.length - sizeof(RID::pageNum) - sizeof(RID::slotNum);
            char middleKey[middleKeyLength + sizeof(int)];
            int copiedOffset = 0;
            if (TypeVarChar == keyField.type) {
                std::memcpy(middleKey, &middleKeyLength, sizeof(middleKeyLength));
                copiedOffset += sizeof(middleKeyLength);
            }
            std::memcpy(middleKey + copiedOffset, keys + middle.offset, middleKeyLength);

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
        int keyStart = 0;
        if (TypeVarChar == keyField.type) {
            std::memcpy(&keySize, key, sizeof(int));
            keyStart += sizeof(int);
        }

        int freeSpaceStart = getFreeSpaceStart();

        std::memcpy(keys + freeSpaceStart, (char *)key + keyStart, keySize);
        std::memcpy(keys + freeSpaceStart + keySize, &rid.pageNum, sizeof(rid.pageNum));
        std::memcpy(keys + freeSpaceStart + keySize + sizeof(rid.pageNum), &rid.slotNum, sizeof(rid.slotNum));

        int index = directory.empty() ? 0 : findKey(keyField, key, rid, true, true);
        directory.insert(directory.begin() + index, { static_cast<short>(freeSpaceStart), static_cast<short>(dataSpace) });
        freeSpace = freeSpace - dataSpace - sizeof(Slot);
    }

    void Node::deleteKey(const Attribute &keyField, int index) {
        Slot current = directory.at(index);
        int dataToMove = PAGE_SIZE - current.offset - current.length;

        std::memmove(keys + current.offset, keys + current.offset + current.length, dataToMove);
        freeSpace += current.length;
        for (auto & i : directory)
            if (i.offset > current.offset)
                i.offset -= current.length;

        directory.at(index).offset = -1;
        directory.at(index).length = -1;
    }

    void Node::getKeyData(const Attribute &keyField, int index, char *key, RID &rid) {
        Slot keySlot = directory.at(index);
        int keySize =  getKeySize(index, keyField);
        int copiedLength = 0;
        if (TypeVarChar == keyField.type) {
            std::memcpy(key, &keySize, sizeof(keySize));
            copiedLength += sizeof(keySize);
        }

        std::memcpy(key + copiedLength, keys + keySlot.offset, keySize);
        std::memcpy(&rid.pageNum, keys + keySlot.offset + keySize, sizeof(rid.pageNum));
        std::memcpy(&rid.slotNum, keys + keySlot.offset + keySize + sizeof(rid.pageNum), sizeof(rid.slotNum));
    }

    void Node::insertChild(const Attribute &keyField, int index, void *key, int keyLength, int childPageId) {
        int newKeySpace = keyLength + sizeof(childPageId);
        if (directory.empty()){
            std::memcpy(keys, key, keyLength);
            std::memcpy(keys + keyLength, &childPageId, sizeof(childPageId));
            directory.push_back({ 0, static_cast<short>(newKeySpace) });
            freeSpace -= newKeySpace + sizeof(Slot);
            return;
        }

        int freeSpaceStart = getFreeSpaceStart();
        std::memcpy(keys + freeSpaceStart, key, keyLength);
        std::memcpy(keys + freeSpaceStart + keyLength, &childPageId, sizeof(childPageId));
        directory.insert(directory.begin() + index, { static_cast<short>(freeSpaceStart), static_cast<short>(newKeySpace) });
        freeSpace -= newKeySpace + sizeof(Slot);
    }

    void Node::cleanDirectory() {
        for (int i = 0; i < directory.size(); ++i) {
            Slot current = directory.at(i);
            if (current.offset == -1) {
                directory.erase(directory.begin() + i);
                freeSpace += sizeof(Slot);
            }
        }
    }

    void Node::split(char *newNode, InsertionChild *child) {
        cleanDirectory();

        Node splitNode(type);
        splitNode.keys = (char *) malloc(PAGE_SIZE);
        int copyStartIndex = 0;
        int dataToKeep = 0;
        while (dataToKeep < PAGE_SIZE / 2) {
            dataToKeep += directory.at(copyStartIndex).length + sizeof(Slot);
            copyStartIndex++;
        }

        Slot copyStart = directory.at(copyStartIndex);
        child->keyLength = copyStart.length;
        std::memcpy(child->leastChildValue, keys + copyStart.offset, copyStart.length);
        splitNode.nextPage = this->nextPage;

        if (NODE_TYPE_INTERMEDIATE == type) {
            int pageId;
            std::memcpy(&pageId, keys + copyStart.offset + copyStart.length - sizeof(pageId), sizeof(pageId));
            splitNode.nextPage = pageId;
            child->keyLength -= sizeof(pageId);
        }

        int copiedLength = 0, copyEndIndex = directory.size();
        for (int i = copyStartIndex; i < copyEndIndex; ++i) {
            if (NODE_TYPE_INTERMEDIATE == type && i == copyStartIndex) {
                directory.erase(directory.begin() + copyStartIndex);
                continue;
            }
            Slot current = directory.at(copyStartIndex);
            std::memcpy(splitNode.keys + copiedLength, this->keys + current.offset, current.length);
            int dataToMove = PAGE_SIZE - current.offset - current.length;
            std::memmove(this->keys + current.offset, this->keys + current.offset + current.length, dataToMove);

            // Fixing offsets for all moved entries
            for (auto & j : directory)
                if (j.offset > current.offset)
                    j.offset -= current.length;

            splitNode.freeSpace -= current.length + sizeof(Slot);
            this->freeSpace += current.length + sizeof(Slot);
            splitNode.directory.push_back({ static_cast<short>(copiedLength), current.length });
            copiedLength += current.length;
            directory.erase(directory.begin() + copyStartIndex);
        }
        splitNode.populateBytes(newNode);
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

    int Node::getFreeSpaceStart() {
        return PAGE_SIZE - freeSpace - sizeof(Slot) * directory.size()
               - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type);
    }

    void Node::populateFormattedKey(AttrType attrType, char *key, RID &rid, int index) {
        Slot slot = directory.at(index);
        int keyLength = slot.length - sizeof(RID::pageNum) - sizeof(RID::slotNum) - sizeof(int);
        int copiedOffset = 0;
        if (TypeVarChar == attrType) {
            std::memcpy(key, &keyLength, sizeof(keyLength));
            copiedOffset += sizeof(keyLength);
        }
        std::memcpy(key + copiedOffset, keys + slot.offset, keyLength);
        std::memcpy(&rid.pageNum, keys + slot.offset + keyLength, sizeof(rid.pageNum));
        std::memcpy(&rid.slotNum, keys + slot.offset + keyLength + sizeof(rid.pageNum), sizeof(rid.slotNum));
    }

    Node::~Node() {
        if (nullptr != keys)
            free(keys);
    }

    bool Node::validateIndex(int index) {
        return index < directory.size() && directory.at(index).offset != -1;
    }
}