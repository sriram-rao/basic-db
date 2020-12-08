#include <src/include/qe.h>

namespace PeterDB {
    RC Filter(Iterator *input, const Condition &condition) {

    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        return Iterator::getAttributes(attrs);
    }

    RC Filter::getNextTuple(void *data) {

    }
}
