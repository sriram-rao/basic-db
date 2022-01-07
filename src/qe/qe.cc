#include "src/include/qe.h"
#include <src/utils/compare_utils.h>
#include <unordered_map>
#include <limits>

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        this->condition = condition;
    }

    Filter::~Filter() = default;

    RC Filter::getNextTuple(void *data) {
        RC response = 0;
        do {
            response = this->input->getNextTuple(data);
        } while (response != QE_EOF && !conditionSatisfied(data, this->condition)) ;
        return response;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        this->input->getAttributes(attrs);
        return 0;
    }

    bool Filter::conditionSatisfied(void *data, Condition condition) {
        if (condition.bRhsIsAttr && condition.lhsAttr == condition.rhsAttr)
            return true;

        std::vector<Attribute> inputAttributes;
        this->input->getAttributes(inputAttributes);
        char *lhs, *rhs;

        int nullBytes = ceil((float)inputAttributes.size() / 8);
        int seenLength = nullBytes;
        for (int i = 0; i < inputAttributes.size(); ++i) {
            Attribute attribute = inputAttributes.at(i);
            if (((char *) data)[i / 8] & (1 << (7 - i % 8))) {
                if (condition.lhsAttr == attribute.name || (condition.bRhsIsAttr && condition.rhsAttr == attribute.name))
                    return false;
                continue;
            }

            int fieldLength = attribute.length;
            if (TypeVarChar == attribute.type)
            {
                std::memcpy(&fieldLength, (char *)data + seenLength, sizeof(fieldLength));
                fieldLength += sizeof(int);
            }

            if (condition.lhsAttr == attribute.name) {
                lhs = (char *) malloc(fieldLength);
                std::memcpy(lhs, (char *) data + seenLength, fieldLength);
            }

            if (condition.bRhsIsAttr && condition.rhsAttr == attribute.name) {
                rhs = (char *) malloc(fieldLength);
                std::memcpy(rhs, (char *) data + seenLength, fieldLength);
            }
            seenLength += fieldLength;
        }

        if (!condition.bRhsIsAttr)
            rhs = (char *)condition.rhsValue.data;

        bool result = CompareUtils::check(condition.rhsValue.type, condition.op, lhs, rhs);
        if (nullptr != lhs) free(lhs);
        if (condition.bRhsIsAttr && nullptr != rhs) free(rhs);
        return result;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->input = input;
        this->attrNames = attrNames;
        input->getAttributes(this->inputAttributes);
        for (const std::string& attrName : attrNames) {
            for (const Attribute& attr : this->inputAttributes) {
                if (attrName == attr.name) {
                    this->projectedAttributes.push_back(attr);
                    break;
                }
            }
        }
    }

    Project::~Project() = default;

    RC Project::getNextTuple(void *data) {
        // get next tuple from input
        int result = this->input->getNextTuple(data);
        if (QE_EOF == result)
            return QE_EOF;

        // make dictionary of values by schema name
        unordered_map<string , ColumnValue> dataRow;
        int nullBytes = ceil((float)this->inputAttributes.size() / 8);

        int copiedLength = nullBytes;
        for (int i = 0; i < this->inputAttributes.size(); ++i) {
            Attribute attr = this->inputAttributes.at(i);
            if (((char *) data)[i / 8] & (1 << (7 - i % 8))) {
                dataRow[attr.name] = {0, nullptr};
                continue;
            }

            int fieldLength = attr.length;
            if (TypeVarChar == attr.type) {
                std::memcpy(&fieldLength, (char *)data + copiedLength, sizeof(fieldLength));
                fieldLength += sizeof(int);
            }
            char *attributeData = (char *)malloc(fieldLength);
            std::memcpy(attributeData, (char *)data + copiedLength, fieldLength);
            dataRow[attr.name] = { fieldLength, attributeData };
            copiedLength += fieldLength;
        }

        // iterate through projectedAttributes and populate data
        int projectedNullBytes = ceil((float)this->projectedAttributes.size() / 8);
        copiedLength = projectedNullBytes;
        char nullBitMap [projectedNullBytes];
        std::memset(nullBitMap, 0, projectedNullBytes);
        for (int i = 0; i < this->projectedAttributes.size(); ++i) {
            Attribute attr = projectedAttributes.at(i);
            if (0 == dataRow[attr.name].length) {
                nullBitMap[i / 8] = nullBitMap[i / 8] | (1 << (7 - i % 8));
                continue;
            }
            std::memcpy((char *)data + copiedLength, dataRow[attr.name].data, dataRow[attr.name].length);
            copiedLength += dataRow[attr.name].length;
        }
        std::memcpy(data, nullBitMap, projectedNullBytes);

        // clear the map
        for (std::pair<std::string, ColumnValue> value : dataRow)
            if (nullptr != value.second.data)
                free(value.second.data);

        return result;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->projectedAttributes;
        return 0;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->reading = true;
        this->currentIndex = 0;
        input->getAttributes(this->inputAttributes);
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->input = input;
        this->aggAttr = aggAttr;
        this->groupAttr = groupAttr;
        this->op = op;
        this->reading = true;
        this->currentIndex = 0;
        input->getAttributes(this->inputAttributes);
    }

    Aggregate::~Aggregate() = default;

    RC Aggregate::getNextTuple(void *data) {
        int result = QE_EOF;
        if (reading)
            result = this->input->getNextTuple(data);

        while (result != QE_EOF) {
            // make dictionary of values by groupAttr field
            int nullBytes = ceil((float) this->inputAttributes.size() / 8);
            string groupAttrValue;
            float aggValue = 0;
            int copiedLength = nullBytes;
            for (int i = 0; i < this->inputAttributes.size(); ++i) {
                Attribute attr = this->inputAttributes.at(i);
                if (((char *) data)[i / 8] & (1 << (7 - i % 8)))
                    continue;

                int fieldLength = attr.length;
                if (TypeVarChar == attr.type) {
                    std::memcpy(&fieldLength, (char *) data + copiedLength, sizeof(fieldLength));
                    copiedLength += sizeof(fieldLength);
                }

                if (groupAttr.name == attr.name) {
                    char attributeData[fieldLength];
                    std::memcpy(attributeData, (char *) data + copiedLength, fieldLength);
                    switch (groupAttr.type) {
                        case TypeInt:
                            groupAttrValue = to_string(*(int *) attributeData);
                            break;
                        case TypeReal:
                            groupAttrValue = to_string(*(float *) attributeData);
                            break;
                        case TypeVarChar:
                            groupAttrValue = string(attributeData);
                            break;
                    }
                }

                if (aggAttr.name == attr.name) {
                    char attributeData[fieldLength];
                    std::memcpy(attributeData, (char *) data + copiedLength, fieldLength);
                    if (TypeInt == aggAttr.type)
                        aggValue += (float) (*(int *) (attributeData));
                    else if (TypeReal == aggAttr.type)
                        aggValue += *(float *) (attributeData);
                }
                copiedLength += fieldLength;
            }

            bool firstEntry = false;
            if (dataRow.find(groupAttrValue) == dataRow.end())
                firstEntry = true;
            switch (op) {
                case MIN:
                    if (firstEntry)
                        dataRow[groupAttrValue] = { std::numeric_limits<float>::infinity(), 0 };
                    if (aggValue < dataRow[groupAttrValue].agg)
                        dataRow[groupAttrValue].agg = aggValue;
                    break;
                case MAX:
                    if (firstEntry)
                        dataRow[groupAttrValue] = { - std::numeric_limits<float>::infinity(), 0 };
                    if (aggValue > dataRow[groupAttrValue].agg)
                        dataRow[groupAttrValue].agg = aggValue;
                    break;
                case AVG:
                case COUNT:
                    if (firstEntry)
                        dataRow[groupAttrValue] = { 0, 0 };
                    dataRow[groupAttrValue].count++;
                    firstEntry = false;
                case SUM:
                    if (firstEntry)
                        dataRow[groupAttrValue] = { 0, 0 };
                    dataRow[groupAttrValue].agg += aggValue;
                    break;
            }
            result = this->input->getNextTuple(data);
        }
        reading = false;

        // Populate data
        int aggIndex = 0;

        for (std::pair<std::string, AggregateValue> value : dataRow) {
            if (aggIndex < currentIndex) {
                aggIndex++;
                continue;
            }
            char nullMap[1];
            std::memset(nullMap, 0, 1);
            std::memcpy(data, nullMap, 1);
            int writtenLength = 1;
            int attrValueInt;
            float attrValueFloat;
            if (!groupAttr.name.empty()) {
                switch (groupAttr.type) {
                    case TypeInt:
                        attrValueInt = std::stoi(value.first);
                        std::memcpy((char *) data + 1, &attrValueInt, sizeof(attrValueInt));
                        writtenLength += sizeof(attrValueInt);
                        break;
                    case TypeReal:
                        attrValueFloat = std::stof(value.first);
                        std::memcpy((char *) data + 1, &attrValueFloat, sizeof(attrValueFloat));
                        writtenLength += sizeof(attrValueFloat);
                        break;
                    case TypeVarChar:
                        int length = value.first.length();
                        std::memcpy((char *) data + 1, &length, sizeof(length));
                        std::memcpy((char *) data + 1 + sizeof(length), value.first.c_str(), length);
                        writtenLength += sizeof(length) + length;
                        break;
                }
            }
            float aggregation = value.second.agg;
            if (COUNT == op)
                aggregation = value.second.count;
            if (AVG == op)
                aggregation = value.second.agg / value.second.count;
            std::memcpy((char *) data + writtenLength, &aggregation, sizeof(aggregation));
            break;
        }

        int aggResult = currentIndex < dataRow.size() ? 0 : QE_EOF;
        currentIndex++;
        return aggResult;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        string aggPrefix = "";
        switch (op) {
            case MIN: aggPrefix = "MIN(";
                break;
            case MAX: aggPrefix = "MAX(";
                break;
            case COUNT: aggPrefix = "COUNT(";
                break;
            case SUM: aggPrefix = "SUM(";
                break;
            case AVG: aggPrefix = "AVG(";
                break;
        }
        Attribute aggAttribute = aggAttr;
        aggAttribute.name = aggPrefix + aggAttr.name + ")";
        aggAttribute.type = TypeReal;
        attrs.clear();
        if (!groupAttr.name.empty())
            attrs.push_back(groupAttr);
        attrs.push_back(aggAttribute);
        return 0;
    }
} // namespace PeterDB