#include <src/include/qe.h>

namespace PeterDB {
    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->numPages = numPages;
        leftIn->getAttributes(this->leftAttrs);
        rightIn->getAttributes(this->rightAttrs);
        int maxRecordSize = ceil((float) leftAttrs.size() / 8);
        for(const Attribute &leftAttr : leftAttrs) {
            maxRecordSize += leftAttr.length;
            if (TypeVarChar == leftAttr.type)
                maxRecordSize += sizeof(int);
            if (condition.lhsAttr == leftAttr.name)
                this->leftJoinAttribute = leftAttr;
        }
        this->nextLeftTuple = (char *)malloc(maxRecordSize);
        this->tupleLimit = numPages * PAGE_SIZE / maxRecordSize;
        this->leftTuples.reserve(tupleLimit);
        // get max right record length
        int rightNullBytes = ceil((float) this->rightAttrs.size() / 8);
        int rightRecordMaxLength = rightNullBytes;
        for (const Attribute &rightAttr : this->rightAttrs){
            rightRecordMaxLength += rightAttr.length;
            if (rightAttr.type == TypeVarChar)
                rightRecordMaxLength += sizeof(int);
            if (condition.bRhsIsAttr && condition.rhsAttr == rightAttr.name)
                this->rightJoinAttribute = rightAttr;
        }
        this->nextRightTuple = (char *)malloc(rightRecordMaxLength);

        this->toReadLeft = true;
        this->toReadRight = true;
        this->mapIndexRead = 0;
        this->leftTableComplete = false;
    }

    BNLJoin::~BNLJoin() {
        if (nullptr != this->nextLeftTuple)
            free(nextLeftTuple);
        if (nullptr != this->nextRightTuple)
            free(nextRightTuple);
    }

    RC BNLJoin::getNextTuple(void *data) {
        if (!this->leftTableComplete)
            this->leftIn->getNextTuple(nextLeftTuple);
        populateLeftTuplesMap();

        int rightResult = 0;
        if (this->toReadRight) {
            bool found = false;
            while (!found && rightResult != QE_EOF) {
                int length = ceil((float) this->rightAttrs.size() / 8);
                rightResult = this->rightIn->getNextTuple(this->nextRightTuple);
                this->rightTupleJoinKey = getHash(length, this->rightAttrs, this->rightJoinAttribute, this->nextRightTuple);
                this->rightTupleLength = length;
                found = this->leftTuples.find(this->rightTupleJoinKey) != this->leftTuples.end();
            }
        }

        if (rightResult == QE_EOF) {
            // clear left tuples
            for (std::pair<long, std::vector<char *>> value : this->leftTuples)
                for (char *tupleData : value.second)
                    if (nullptr != tupleData)
                        free(tupleData);
            this->leftTuples.clear();
            if (this->leftTableComplete)
                return QE_EOF;
            toReadLeft = true;
            rightIn->setIterator();
            return getNextTuple(data);
        }

        toReadRight = false;
        char *leftTuple;
        // perform join using nextRightTuple
        std::vector<char *> tuples = this->leftTuples[this->rightTupleJoinKey];
        for (int i = 0; i < tuples.size(); ++i) {
            if (i < this->mapIndexRead)
                continue;
            leftTuple = tuples.at(i);
            this->mapIndexRead = i;
            break;
        }
        if (this->mapIndexRead == tuples.size() - 1)
            toReadRight = true;

        // copy to data and return
        int leftNullBytes = ceil((float) this->leftAttrs.size() / 8);
        int leftDataSize = 0;
        for (const Attribute &attr : this->leftAttrs) {
            if (TypeVarChar == attr.type) {
                int length;
                std::memcpy(&length, leftTuple + leftNullBytes + leftDataSize, sizeof(int));
                leftDataSize += length;
            }
            leftDataSize += sizeof(int);
        }
        int rightNullBytes = ceil((float) this->rightAttrs.size() / 8);
        int joinedNullBytes = ceil((float) (this->leftAttrs.size() + this->rightAttrs.size()) / 8);
        int joinedNullCounter = 0;
        char joinedBitMap [joinedNullBytes];
        std::memset(joinedBitMap, 0, joinedNullBytes);

        // compute null bytes
        for (int i = 0; i < this->leftAttrs.size(); ++i) {
            if (((char *) leftTuple)[i / 8] & (1 << (7 - i % 8)))
                joinedBitMap[joinedNullCounter / 8] = joinedBitMap[joinedNullCounter / 8] | (1 << (7 - joinedNullCounter % 8));
            joinedNullCounter++;
        }
        for (int i = 0; i < this->rightAttrs.size(); ++i) {
            if (((char *) this->nextRightTuple)[i / 8] & (1 << (7 - i % 8)))
                joinedBitMap[joinedNullCounter / 8] = joinedBitMap[joinedNullCounter / 8] | (1 << (7 - joinedNullCounter % 8));
            joinedNullCounter++;
        }

        std::memcpy(data, joinedBitMap, joinedNullBytes);
        std::memcpy((char *) data + joinedNullBytes, leftTuple + leftNullBytes, leftDataSize);
        std::memcpy((char *) data + joinedNullBytes + leftDataSize, this->nextRightTuple + rightNullBytes, this->rightTupleLength); // TODO: minus rightNullBytes

        return 0;
    }

    void BNLJoin::populateLeftTuplesMap(){
        if (!toReadLeft)
            return;
        int tuplesReadCount = 1;
        while (!this->leftTableComplete && tuplesReadCount <= tupleLimit) {
            // load from leftIn into unordered_map until numPages is filled
            int nullBytes = ceil((float) this->leftAttrs.size() / 8);

            int seenLength = nullBytes;
            long joinKeyHash = getHash(seenLength, this->leftAttrs, this->leftJoinAttribute, this->nextLeftTuple);
            char *exactData = (char *) malloc(seenLength);
            std::memcpy(exactData, nextLeftTuple, seenLength);
            if (this->leftTuples.find(joinKeyHash) == this->leftTuples.end())
                this->leftTuples[joinKeyHash] = std::vector<char *>();
            this->leftTuples[joinKeyHash].push_back(exactData);

            int leftResult = this->leftIn->getNextTuple(nextLeftTuple);
            this->leftTableComplete = leftResult == QE_EOF;
        }
        toReadLeft = false;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        // Left attributes + right attributes
        attrs.clear();
        if (this->leftIn->getAttributes(attrs) == -1)
            return -1;
        std::vector<Attribute> rightAttributes;
        if (this->rightIn->getAttributes(rightAttributes) == -1)
            return -1;
        for (const Attribute& rightAttr : rightAttributes)
            attrs.push_back(rightAttr);
        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
    }

    INLJoin::~INLJoin() = default;

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        // Left attributes + right attributes
        attrs.clear();
        if (this->leftIn->getAttributes(attrs) == -1)
            return -1;
        std::vector<Attribute> rightAttributes;
        if (this->rightIn->getAttributes(rightAttributes) == -1)
            return -1;
        for (const Attribute& rightAttr : rightAttributes)
            attrs.push_back(rightAttr);
        return 0;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    long BNLJoin::hash(char *str)
    {
        long hash = 5381;
        int c;

        while ((c = *str++))
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

        return hash;
    }

    long BNLJoin::getHash(int &seenLength, const std::vector<Attribute> &attrs, const Attribute &joinAttr, char *tupleData) {
        long joinKeyHash = 0;
        for (int i = 0; i < attrs.size(); ++i) {
            Attribute attr = attrs.at(i);
            char nullBit = 0;
            if (((char *) tupleData)[i / 8] & (1 << (7 - i % 8))) {
                nullBit = 1;
            }
            int attrLength = joinAttr.length;
            if (TypeVarChar == attr.type) {
                std::memcpy(&attrLength, tupleData + seenLength, sizeof(int));
                attrLength += sizeof(int);
            }
            if (nullBit == 0 && attr.name == joinAttr.name) {
                char *joinKey = (char *) malloc(attrLength);
                if (TypeVarChar == joinAttr.type) {
                    int length = attrLength - sizeof(int);
                    std::memcpy(joinKey, &length, sizeof(int));
                    std::memcpy(joinKey + sizeof(int), tupleData + seenLength + sizeof(int), length);
                } else {
                    std::memcpy(joinKey, tupleData + seenLength, attrLength);
                }
                joinKeyHash = hash(joinKey);
                free(joinKey);
            }
            seenLength += attrLength;
        }
        return joinKeyHash;
    }
}