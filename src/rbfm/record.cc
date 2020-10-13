#include "src/include/record.h"

namespace PeterDB {
    Record::Record() {}

    Record::Record(int countOfAttributes, int *fieldOffset, unsigned char* values) {
        this->countOfAttributes = countOfAttributes;
        this->fieldOffset = fieldOffset;
        this->values = values;
    }

    Record::operator const char *() {
        // TODO :: logic from converting byte array to string
    }

    Record::~Record() = default;
}